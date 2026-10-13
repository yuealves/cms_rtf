#!/usr/bin/env python3
"""Independent CMS factor service. Gateway, paged, cron and source data are untouched."""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

SOURCE = Path(__file__).resolve().parents[1]


def atomic_json(path, data):
    temp = path.with_name(path.name + '.tmp')
    temp.write_text(json.dumps(data, indent=2) + '\n'); temp.replace(path)


def process_start(pid):
    return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]


def running(prefix):
    path = prefix / 'run/service.json'
    if not path.exists():
        return None
    record = json.loads(path.read_text())
    try:
        pid = record['pid']
        if record['prefix'] != str(prefix) or process_start(pid) != record['start_ticks']:
            return None
        if Path(f'/proc/{pid}').stat().st_uid != os.getuid():
            return None
        argv = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
        if str(Path(record['source']) / 'deploy/service.py').encode() not in argv:
            return None
        if b'run' not in argv or str(prefix).encode() not in argv:
            return None
        return record
    except (FileNotFoundError, ProcessLookupError):
        return None


def status(prefix):
    record = running(prefix)
    result = dict(running=record is not None)
    if record:
        result.update(record)
        health = Path(record['attempt']) / 'live-status.json'
        if health.exists():
            result['health'] = json.loads(health.read_text())
    elif (prefix / 'run/last-result.json').exists():
        result['last_result'] = json.loads((prefix / 'run/last-result.json').read_text())
    return result


def prepare_references(config, day, attempt):
    from cms_rtf import refdata, native_aggregate
    import numpy as np
    ref = refdata.build_ref_tables(config['weights_dir'], config['threshold_dir'], day)
    thresh = np.array([(s, ref['dyn'][int(s)][1], ref['dyn'][int(s)][2]) for s in ref['symbols']],
                      dtype=[('symbol', '<i4'), ('xl', '<f8'), ('m', '<f8')])
    thresh.tofile(attempt / 'thresholds.bin')
    native_aggregate.write_reference(ref, attempt / 'reference.bin')
    paths = [refdata.latest_threshold_csv(config['threshold_dir'], day)]
    dash_day = day[:4] + '-' + day[4:6] + '-' + day[6:]
    paths += [refdata.latest_weights_file(Path(config['weights_dir']) / name, dash_day)
              for name in refdata.INDEX_WEIGHT_DIRS]
    atomic_json(attempt / 'references.json', dict(day=day, symbols=len(ref['symbols']),
        inputs={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}))


def run_service(prefix, day):
    deployment = json.loads((SOURCE / 'build/deployment.json').read_text())
    sys.path[:0] = [str(SOURCE / 'python'), deployment['python_root']]
    config = json.loads((prefix / 'config/live.json').read_text())
    for key in ('journal_root', 'weights_dir', 'threshold_dir', 'output_root'):
        if not Path(config[key]).is_absolute():
            raise RuntimeError(f'{key} must be absolute')
    if not 1 <= int(config['workers']) <= 64 or int(config['pending_mib']) < 1:
        raise RuntimeError('invalid worker/pending settings')
    with (prefix / 'run/service.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stamp = datetime.datetime.now().strftime('%H%M%S') + '-' + str(os.getpid())
        root = Path(config['output_root']) / 'index_500ms' / day
        attempt = root / 'runs' / stamp; attempt.mkdir(parents=True, exist_ok=False)
        record = dict(pid=os.getpid(), start_ticks=process_start(os.getpid()), day=day,
                      prefix=str(prefix), source=str(SOURCE), commit=deployment['commit'], attempt=str(attempt))
        prepare_references(config, day, attempt)
        cmd = [str(SOURCE / 'build/cms_index500ms_mt'), '--journal', str(Path(config['journal_root']) / day),
               '--journal-mode', 'follow', '--journal-clock', config.get('journal_clock', 'wall'), '--day', day,
               '--thresh', str(attempt / 'thresholds.bin'), '--ref', str(attempt / 'reference.bin'),
               '--work-dir', str(attempt), '--output-mode', 'stream', '--window-batch', '1',
               '--workers', str(config['workers']), '--pending-mib', str(config['pending_mib']),
               '--out', str(attempt / 'index_500ms_exch.arrow'),
               '--local-out', str(attempt / 'index_500ms_local.arrow')]
        with (attempt / 'kernel.log').open('wb') as err, (attempt / 'kernel.summary.json').open('wb') as out:
            child = subprocess.Popen(cmd, stdout=out, stderr=err)
            def stop(signum, frame):
                if child.poll() is None:
                    child.send_signal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
            record['kernel_pid'] = child.pid
            atomic_json(prefix / 'run/service.json', record)
            atomic_json(attempt / 'run.json', record)
            published = False
            try:
                while child.poll() is None:
                    health_path = attempt / 'live-status.json'
                    if not published and health_path.exists():
                        health = json.loads(health_path.read_text())
                        # Rebuild from the day's first frame on restart. Preserve
                        # the old current output until catch-up and first publish.
                        if health['caught_up'] and health['emitted_endpoints'] and health['status'] == 'running':
                            temp = root / '.current.next'; temp.unlink(missing_ok=True)
                            temp.symlink_to(attempt.relative_to(root)); temp.replace(root / 'current')
                            published = True
                    time.sleep(.2)
                code = child.wait()
                health_path = attempt / 'live-status.json'
                health = json.loads(health_path.read_text()) if health_path.exists() else {}
                if code == 0 and not published and health.get('emitted_endpoints'):
                    temp = root / '.current.next'; temp.unlink(missing_ok=True)
                    temp.symlink_to(attempt.relative_to(root)); temp.replace(root / 'current')
                atomic_json(prefix / 'run/last-result.json', dict(**record, exit_code=code, health=health))
                return code
            finally:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        child.kill(); child.wait()
                (prefix / 'run/service.json').unlink(missing_ok=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prefix', type=Path, required=True)
    p.add_argument('command', choices=['start', 'run', 'stop', 'status'])
    p.add_argument('--day', default=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%Y%m%d'))
    a = p.parse_args(); prefix = a.prefix.resolve()
    if len(a.day) != 8 or not a.day.isdigit():
        p.error('day must be YYYYMMDD')
    datetime.datetime.strptime(a.day, '%Y%m%d')
    if a.command == 'run':
        return run_service(prefix, a.day)
    if a.command == 'status':
        print(json.dumps(status(prefix))); return 0
    record = running(prefix)
    if a.command == 'stop':
        if record:
            # Bind the signal to this process, then recheck its identity.
            fd = os.pidfd_open(record['pid'])
            try:
                if running(prefix) == record:
                    signal.pidfd_send_signal(fd, signal.SIGTERM)
            finally:
                os.close(fd)
            limit = time.monotonic() + 35
            while running(prefix) and time.monotonic() < limit:
                time.sleep(.1)
            if running(prefix):
                raise RuntimeError('service did not stop within 35 seconds')
        print(json.dumps(status(prefix))); return 0
    if record:
        print(json.dumps(dict(status='already_running', **record))); return 0
    (prefix / 'log').mkdir(exist_ok=True)
    logfile = prefix / 'log' / f'service-{a.day}.log'
    with logfile.open('ab') as stream:
        child = subprocess.Popen([sys.executable, '-B', str(SOURCE / 'deploy/service.py'),
                                  '--prefix', str(prefix), 'run', '--day', a.day],
                                 stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
                                 start_new_session=True)
    limit = time.monotonic() + 15
    while time.monotonic() < limit:
        record = running(prefix)
        if record:
            print(json.dumps(dict(status='started', **record))); return 0
        if child.poll() is not None:
            raise RuntimeError(f'service start failed ({child.returncode}); see {logfile}')
        time.sleep(.1)
    raise RuntimeError(f'service still starting; inspect {logfile}')


if __name__ == '__main__':
    sys.exit(main())

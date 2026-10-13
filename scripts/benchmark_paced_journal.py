#!/usr/bin/env python3
"""Replay original CMS frames into a new private journal at a controlled speed.

The factor kernel is unmodified. Preserve all source timestamps, payloads and
sequence numbers; commit the destination status byte last. Never append a fake
END to a prefix. Warm up pre-open state, pace with a monotonic deadline, drain,
then SIGTERM only the benchmark child. No gateway/paged/production config edits.
"""
import argparse
from array import array
import csv
import datetime
import json
import math
import mmap
from pathlib import Path
import signal
import struct
import subprocess
import sys
import time

from summarize_latency import stats

PAGE_SIZE = 128 * 1024 * 1024
PAGE = struct.Struct('<B30shqqiih6s72s')
FRAME = struct.Struct('<BhqiIhBiqi')


def ns_at(day, hms):
    dt = datetime.datetime.strptime(day + ' ' + hms, '%Y%m%d %H:%M:%S')
    return int(dt.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8))).timestamp()) * 10**9


def replay(source, dest, start_ns, stop_ns, speed, drain):
    """Read immutable source pages; drain(records) waits for the child to catch up."""
    if not math.isfinite(speed) or speed <= 0 or stop_ns <= start_ns:
        raise ValueError('positive speed and increasing time range required')
    source, dest = Path(source).resolve(), Path(dest).resolve()
    if source == dest or source in dest.parents:
        raise ValueError('destination must be separate from the original journal')
    dest.mkdir(parents=True, exist_ok=False)
    # Exact histogram in microseconds up to one second; report overflow/max too.
    lag_hist = array('Q', [0]) * 1000001
    records = paced = total_bytes = 0
    anchor = None; first_ns = last_ns = 0; high_ns = 0
    lag_sum_ns = lag_max_ns = 0
    began = time.monotonic_ns()
    for page in range(1, 32767):
        path = source / f'yjj.cms_l2_raw_v1.{page}.journal'
        with path.open('rb') as src:
            if path.stat().st_size != PAGE_SIZE: raise ValueError('invalid page size')
            with mmap.mmap(src.fileno(), PAGE_SIZE, access=mmap.ACCESS_READ) as data:
                header = PAGE.unpack_from(data)
                if header[0] != 1 or header[2] != page or header[7] != 2:
                    raise ValueError('invalid source page header')
                with (dest / path.name).open('x+b') as out:
                    out.truncate(PAGE_SIZE)
                    with mmap.mmap(out.fileno(), PAGE_SIZE, access=mmap.ACCESS_WRITE) as written:
                        written[1:PAGE.size] = data[1:PAGE.size]
                        written[0] = 1
                        pos = PAGE.size
                        while pos + FRAME.size <= PAGE_SIZE:
                            state = data[pos]
                            if state == 2:
                                written[pos] = 2
                                break
                            if state != 1: raise ValueError('source ended before requested cutoff')
                            h = FRAME.unpack_from(data, pos)
                            length = h[3]
                            if h[5] != 22001 or h[9] or not FRAME.size + 96 <= length <= FRAME.size + 96 + 65535 or pos + length > PAGE_SIZE:
                                raise ValueError('invalid source frame')
                            nano = h[8] if h[8] > 0 else h[2]
                            if nano <= 0: raise ValueError('invalid source clock')
                            if nano > stop_ns:
                                if anchor is None: raise ValueError('no records in pacing interval')
                                elapsed = time.monotonic_ns() - anchor
                                def percentile(q):
                                    target = int(q * (paced - 1)); n = 0
                                    for us, count in enumerate(lag_hist):
                                        n += count
                                        if n > target: return us / 1000.
                                return dict(records=records, paced_records=paced, bytes=total_bytes, pages=page,
                                    speed_requested=speed, start_source_ns=start_ns, last_source_ns=last_ns,
                                    first_paced_source_ns=first_ns, elapsed_paced_seconds=elapsed / 1e9,
                                    source_seconds=(last_ns - start_ns) / 1e9,
                                    effective_speed=(last_ns - start_ns) / elapsed,
                                    warmup_seconds=(anchor - began) / 1e9, anchor_steady_ns=anchor,
                                    scheduler_lag_ms=dict(mean=lag_sum_ns / max(1, paced) / 1e6,
                                        p50=percentile(.5), p99=percentile(.99), max=lag_max_ns / 1e6,
                                        over_1s=lag_hist[-1]), stopped_before_source_ns=nano,
                                    prefix_only=True, synthesized_end=False)
                            if nano >= start_ns and anchor is None:
                                drain(records)
                                # Allow pending pre-open worker queues to drain; the
                                # measured first window starts 10 source seconds later.
                                time.sleep(1.)
                                anchor = time.monotonic_ns(); first_ns = nano
                            deadline = None
                            if anchor is not None:
                                high_ns = max(high_ns, nano)
                                deadline = anchor + int((high_ns - start_ns) / speed)
                                while True:
                                    delay = deadline - time.monotonic_ns()
                                    if delay <= 0: break
                                    time.sleep(delay / 1e9)
                            # x86 committed-frame protocol: bytes first, status last.
                            # Original frame header, checksum and CMS envelope stay intact.
                            written[pos + 1:pos + length] = data[pos + 1:pos + length]
                            written[pos] = 1
                            if deadline is not None:
                                lag = max(0, time.monotonic_ns() - deadline)
                                lag_hist[min(lag // 1000, len(lag_hist) - 1)] += 1
                                lag_sum_ns += lag; lag_max_ns = max(lag_max_ns, lag); paced += 1
                            records += 1; total_bytes += length; last_ns = nano; pos += length
                        else:
                            raise ValueError('invalid page tail')
    raise ValueError('page limit exceeded')


def load_rows(path, start, end):
    with Path(path).open() as f:
        return [r for r in csv.DictReader(f) if start + '.000' < r['endpoint'] <= end + '.000']


def latency_stats(rows):
    eligible = [r for r in rows if int(r['events']) > 0 and r['reason'] == 'watermark']
    return dict(windows=len(rows), eligible_windows=len(eligible),
        distinct_closes=len({r['close_id'] for r in eligible}),
        close_to_write_ms=stats([float(r['close_to_write_ms']) for r in eligible], 10.),
        last_read_to_write_ms=stats([float(r['last_read_to_write_ms']) for r in eligible], 10.),
        last_read_to_close_ms=stats([(int(r['close_start_steady_ns']) - int(r['last_read_steady_ns'])) / 1e6 for r in eligible], 10.))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--kernel', type=Path, required=True)
    p.add_argument('--journal', type=Path, required=True)
    p.add_argument('--day', required=True)
    p.add_argument('--thresh', type=Path, required=True)
    p.add_argument('--ref', type=Path, required=True)
    p.add_argument('--baseline-arrow', type=Path, required=True)
    p.add_argument('--baseline-latency', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--speed', type=float, default=5.)
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--start', default='09:30:00')
    p.add_argument('--end', default='09:45:00')
    p.add_argument('--tail-seconds', type=float, default=5.)
    a = p.parse_args()
    if not math.isfinite(a.speed) or a.speed <= 0 or not 1 <= a.workers <= 64 or not 0 <= a.tail_seconds <= 60:
        p.error('invalid speed/workers/tail')
    start = ns_at(a.day, a.start); end = ns_at(a.day, a.end)
    if not start < end: p.error('start must precede end')
    a.out.mkdir(parents=True, exist_ok=False)
    journal = a.out / 'journal'; factors = a.out / 'factors'; factors.mkdir()
    cmd = [str(a.kernel.resolve()), '--journal', str(journal.resolve()), '--journal-mode', 'follow',
           '--journal-clock', 'source', '--day', a.day, '--thresh', str(a.thresh.resolve()),
           '--ref', str(a.ref.resolve()), '--work-dir', str(factors.resolve()), '--workers', str(a.workers),
           '--output-mode', 'stream', '--out', str((factors / 'factors.arrow').resolve())]
    (a.out / 'command.json').write_text(json.dumps(cmd, indent=2))
    with (factors / 'kernel.summary.json').open('w') as stdout, (factors / 'kernel.log').open('w') as stderr:
        child = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
        def drain(records):
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                if child.poll() is not None: raise RuntimeError('kernel stopped before drain; inspect kernel.log')
                status = factors / 'live-status.json'
                if status.exists():
                    health = json.loads(status.read_text())
                    if health['records'] == records and health['caught_up']: return
                time.sleep(.02)
            raise RuntimeError('kernel drain timeout')
        try:
            paced = replay(a.journal, journal, start - 10 * 10**9, end + int(a.tail_seconds * 1e9), a.speed, drain)
            (a.out / 'pacer.json').write_text(json.dumps(paced, indent=2) + '\n')
            drain(paced['records'])
            child.send_signal(signal.SIGTERM)
            if child.wait(timeout=30) != 0: raise RuntimeError('kernel failed; inspect kernel.log')
        finally:
            if child.poll() is None:
                child.terminate()
                try: child.wait(timeout=30)
                except subprocess.TimeoutExpired: child.kill(); child.wait()
    # Pin PyArrow to the same native deployment when running on the CMS host.
    deployment = a.kernel.resolve().parent / 'deployment.json'
    if deployment.exists(): sys.path.insert(0, json.loads(deployment.read_text())['python_root'])
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'python'))
    import pyarrow as pa
    from cms_rtf.validate_replay import exact_factors
    def measured(path):
        batches = []
        with Path(path).open('rb') as f:
            reader = pa.ipc.open_stream(f); schema = reader.schema
            for batch in reader:
                if not batch.num_rows: continue
                when = batch.column(0).to_pylist()
                # Both supplied baselines and the replay are sorted by endpoint.
                if when[0].strftime('%H:%M:%S.%f') > a.end + '.000000': break
                keep = [a.start + '.000000' < t.strftime('%H:%M:%S.%f') <= a.end + '.000000' for t in when]
                batches.append(batch.filter(pa.array(keep)))
        return pa.Table.from_batches(batches, schema=schema)
    check = exact_factors(measured(a.baseline_arrow), measured(factors / 'factors.arrow'))
    rows = load_rows(factors / 'latency.csv', a.start, a.end)
    before = load_rows(a.baseline_latency, a.start, a.end)
    expected_windows = (end - start) // 500000000
    if len(rows) != expected_windows or check['exact_columns'] != 106 or not check['keys_equal'] or not check['keys_unique']:
        raise RuntimeError('window coverage/factor equality failed')
    health = json.loads((factors / 'live-status.json').read_text())
    report = dict(workers=a.workers, speed=a.speed, day=a.day, start=a.start, end=a.end, pacer=paced,
        baseline_arrow=str(a.baseline_arrow), baseline_latency=str(a.baseline_latency),
        paced=latency_stats(rows), fullspeed_same_windows=latency_stats(before),
        check={k:v for k,v in check.items() if k != 'columns'}, health=health,
        kernel=json.loads((factors / 'kernel.summary.json').read_text()),
        source_unchanged=True, clock='historical source; no gateway receive latency claimed')
    (a.out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2), flush=True)


if __name__ == '__main__': main()

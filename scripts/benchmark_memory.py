#!/usr/bin/env python3
"""Compare the old kernel with bounded output on identical replay input.

Measures cms_rtf only: exporter/replayer are absent. Each kernel has its own
/usr/bin/time -v and sampled /proc status (anonymous versus file-backed RSS).
Aggregation runs separately in C++. Logical payload hashes and all factor bits
are checked after timings, irrespective of the intermediate container version.
"""
import argparse
import json
from pathlib import Path
import platform
import subprocess
import time

import numpy as np
import pyarrow as pa

from cms_rtf import native_aggregate, refdata
from cms_rtf.record_blocks import logical_digest
from cms_rtf.validate_replay import exact_factors


def timed(cmd, dest, name, sample=False):
    samples = []
    t = time.monotonic()
    with (dest / f'{name}.json').open('w') as stdout, (dest / f'{name}.stderr').open('w') as stderr:
        proc = subprocess.Popen(['/usr/bin/time', '-v', '-o', str(dest / f'{name}.time.txt')] + cmd,
                                stdout=stdout, stderr=stderr)
        while proc.poll() is None:
            if sample:
                try:
                    children = Path(f'/proc/{proc.pid}/task/{proc.pid}/children').read_text().split()
                    for pid in children:
                        status = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/status').read_text().splitlines())
                        samples.append(dict(seconds=time.monotonic() - t, pid=int(pid),
                                            **{k: int(status[k].split()[0]) for k in
                                               ['VmRSS', 'VmHWM', 'RssAnon', 'RssFile', 'RssShmem'] if k in status}))
                except (FileNotFoundError, ProcessLookupError):
                    pass
            try:
                proc.wait(timeout=.25 if sample else 1)
            except subprocess.TimeoutExpired:
                pass
        if proc.returncode:
            raise RuntimeError(f'{name} failed ({proc.returncode}): see {dest}')
    elapsed = time.monotonic() - t
    stats = (dest / f'{name}.time.txt').read_text()
    rss = int(next(line.split(':', 1)[1] for line in stats.splitlines() if 'Maximum resident set size' in line))
    if sample:
        (dest / 'memory-samples.json').write_text(json.dumps(samples, indent=2) + '\n')
    return dict(command=cmd, seconds=elapsed, max_rss_kib=rss,
                summary=json.loads((dest / f'{name}.json').read_text()))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', required=True)
    p.add_argument('--baseline-kernel', required=True)
    p.add_argument('--kernel', default='build/cms_index500ms_mt')
    p.add_argument('--aggregator', default=str(native_aggregate.DEFAULT_BINARY))
    p.add_argument('--baseline-aggregator', help='defaults to the same C++ aggregator')
    p.add_argument('--weights-dir', required=True)
    p.add_argument('--threshold-dir', required=True)
    p.add_argument('--day', default='20261009')
    p.add_argument('--workers', type=int, default=16)
    p.add_argument('--repeats', type=int, default=1)
    p.add_argument('--include-identity-only', action='store_true')
    p.add_argument('--reference', help='previously accepted factor Arrow')
    p.add_argument('--out', required=True, type=Path)
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    ref = refdata.build_ref_tables(args.weights_dir, args.threshold_dir, args.day)
    threshold = args.out / 'threshold.bin'
    np.array([(s, ref['dyn'][int(s)][1], ref['dyn'][int(s)][2]) for s in ref['symbols']],
             dtype=[('s', '<i4'), ('xl', '<f8'), ('m', '<f8')]).tofile(threshold)
    reference = args.out / 'reference.bin'
    native_aggregate.write_reference(ref, reference)
    report = dict(host=platform.node(), platform=platform.platform(),
                  input=str(Path(args.input).resolve()), input_bytes=Path(args.input).stat().st_size,
                  workers=args.workers, runs=[], checks=[])
    variants = ['baseline', 'bounded']
    if args.include_identity_only:
        variants.insert(1, 'identity_only')
    for repeat in range(args.repeats):
        for variant in variants if repeat % 2 == 0 else list(reversed(variants)):
            dest = args.out / f'{variant}_{repeat}'
            dest.mkdir()
            cmd = [args.baseline_kernel if variant == 'baseline' else args.kernel, '--input', args.input,
                   '--thresh', str(threshold), '--work-dir', str(dest), '--workers', str(args.workers)]
            if variant != 'baseline':
                cmd += ['--output-mode', 'blocks' if variant == 'bounded' else 'legacy']
            kernel = timed(cmd, dest, 'kernel', sample=True)
            print(json.dumps(dict(stage='kernel', variant=variant, repeat=repeat, **kernel)), flush=True)
            books, flows, seeds = native_aggregate.worker_paths(dest)
            aggregator = (args.baseline_aggregator or args.aggregator) if variant == 'baseline' else args.aggregator
            aggregation = timed(native_aggregate.command(books, flows, seeds, reference, args.day,
                                                          dest / 'factors.arrow', aggregator), dest, 'aggregate')
            run = dict(variant=variant, repeat=repeat, directory=str(dest), kernel=kernel,
                       aggregate=aggregation, total_seconds=kernel['seconds'] + aggregation['seconds'])
            report['runs'].append(run)
            print(json.dumps(dict(stage='complete', variant=variant, repeat=repeat,
                                  total_seconds=run['total_seconds'], aggregate=aggregation)), flush=True)
            (args.out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    baseline = args.out / 'baseline_0'
    old_paths = native_aggregate.worker_paths(baseline)
    baseline_hashes = [{path.name.split('.')[0]: logical_digest(path, size) for path in paths}
                       for paths, size in zip(old_paths, [280, 176, 12])]
    expected = pa.ipc.open_stream(baseline / 'factors.arrow').read_all()
    health_keys = ['events', 'over_deduct', 'd_unknown', 'unknown_trade_ref', 'late_passive_ref',
                   'agg_skip', 'crossed_book', 'immediate_fill', 'sz_non_monotonic']
    old_health = json.loads((baseline / 'kernel.json').read_text())
    for run in report['runs']:
        dest = Path(run['directory'])
        check = exact_factors(expected, pa.ipc.open_stream(dest / 'factors.arrow').read_all())
        paths = native_aggregate.worker_paths(dest)
        payload_equal = all(logical_digest(path, size, expand_books=size == 280) == hashes[path.name.split('.')[0]]
                            for group, size, hashes in zip(paths, [280, 176, 12], baseline_hashes)
                            for path in group)
        health_equal = all(run['kernel']['summary'][k] == old_health[k] for k in health_keys)
        report['checks'].append(dict(variant=run['variant'], repeat=run['repeat'],
                                    payload_equal=payload_equal, health_equal=health_equal, **check))
    if args.reference:
        check = exact_factors(pa.ipc.open_stream(args.reference).read_all(), expected)
        report['reference_check'] = check
    report['passed'] = all(c['payload_equal'] and c['health_equal'] and c['keys_equal'] and
                           c['keys_unique'] and c['exact_columns'] == 106 for c in report['checks'])
    if args.reference:
        report['passed'] &= check['keys_equal'] and check['keys_unique'] and check['exact_columns'] == 106
    (args.out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(passed=report['passed'], report=str(args.out / 'report.json'))), flush=True)
    if not report['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Paired aggregation benchmark on identical existing intermediate files.

One excluded warmup for each backend, then alternating order. Each trial gets
a fresh process and /usr/bin/time -v. Do not drop system page caches. Python
timing excludes imports/reference loading, as in benchmark_replay.py; native
timing includes the entire C++ subprocess. Exact checks are outside timings.
"""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

import pyarrow as pa

from cms_rtf import aggregate, native_aggregate, refdata
from cms_rtf.validate_replay import exact_factors


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--intermediate', required=True, type=Path)
    p.add_argument('--weights-dir', required=True)
    p.add_argument('--threshold-dir', required=True)
    p.add_argument('--day', default='20261009')
    p.add_argument('--out', required=True, type=Path)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--binary', default=str(native_aggregate.DEFAULT_BINARY))
    p.add_argument('--reference', type=Path, help='previously accepted Arrow')
    p.add_argument('--python-child', action='store_true', help=argparse.SUPPRESS)
    args = p.parse_args()
    if args.repeats < 1:
        p.error('repeats must be positive')
    ref = refdata.build_ref_tables(args.weights_dir, args.threshold_dir, args.day)
    books, flows, seeds = [sorted(args.intermediate.glob(f'w*.{kind}.bin'))
                            for kind in ('book', 'flow', 'seeds')]
    if not books or not flows or not seeds:
        p.error('missing worker intermediate files')
    if args.python_child:
        t = time.monotonic()
        grid, rows = aggregate.aggregate(books, flows, seeds, ref, args.day)
        agg_s = time.monotonic() - t
        t = time.monotonic()
        aggregate.write_arrow(grid, rows, args.day, args.out / 'factors.arrow')
        write_s = time.monotonic() - t
        print(json.dumps(dict(aggregate_seconds=agg_s, arrow_seconds=write_s,
                             seconds=agg_s + write_s)))
        return
    args.out.mkdir(parents=True, exist_ok=False)
    reference = args.out / 'reference.bin'
    native_aggregate.write_reference(ref, reference)
    report = dict(input=str(args.intermediate.resolve()), day=args.day, repeats=args.repeats,
                  runs=[], warmups=[], checks=[])
    for repeat in range(-1, args.repeats):
        for backend in (['python', 'cpp'] if repeat % 2 == 0 else ['cpp', 'python']):
            name = f'{backend}_{repeat}' if repeat >= 0 else f'{backend}_warmup'
            dest = args.out / name
            dest.mkdir()
            if backend == 'cpp':
                cmd = native_aggregate.command(books, flows, seeds, reference, args.day,
                                                dest / 'factors.arrow', args.binary)
            else:
                cmd = [sys.executable, __file__, '--python-child', '--intermediate', str(args.intermediate),
                       '--weights-dir', args.weights_dir, '--threshold-dir', args.threshold_dir,
                       '--day', args.day, '--out', str(dest)]
            t = time.monotonic()
            with (dest / 'stages.json').open('w') as stdout, (dest / 'stderr.log').open('w') as stderr:
                subprocess.run(['/usr/bin/time', '-v', '-o', str(dest / 'time.txt')] + cmd,
                               stdout=stdout, stderr=stderr, check=True)
            wall = time.monotonic() - t
            stage = json.loads((dest / 'stages.json').read_text())
            rss = int(next(line.split(':', 1)[1] for line in (dest / 'time.txt').read_text().splitlines()
                           if 'Maximum resident set size' in line))
            run = dict(name=name, backend=backend, seconds=wall if backend == 'cpp' else stage['seconds'],
                       process_wall_seconds=wall, max_rss_kib=rss, stages=stage)
            report['warmups' if repeat < 0 else 'runs'].append(run)
            print(json.dumps(run), flush=True)
            (args.out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    for run in report['runs']:
        reference_arrow = args.reference or args.out / 'python_0/factors.arrow'
        check = exact_factors(pa.ipc.open_stream(reference_arrow).read_all(),
                              pa.ipc.open_stream(args.out / run['name'] / 'factors.arrow').read_all())
        report['checks'].append(dict(name=run['name'], **check))
    report['median_seconds'] = {backend: statistics.median(r['seconds'] for r in report['runs']
                                                         if r['backend'] == backend)
                                for backend in ['python', 'cpp']}
    report['speedup'] = report['median_seconds']['python'] / report['median_seconds']['cpp']
    report['passed'] = all(c['keys_equal'] and c['keys_unique'] and c['exact_columns'] == 106
                           for c in report['checks'])
    (args.out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ['median_seconds', 'speedup', 'passed']}), flush=True)
    if not report['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()

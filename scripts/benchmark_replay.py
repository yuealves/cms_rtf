#!/usr/bin/env python3
"""Compare two builds on the same replay-exported DTYPE stream and reference tables.

Run with PYTHONPATH=python; results and all subprocess logs stay under a NEW --out.
No journal is modified. Both kernels are timed on the same host and compiler.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import subprocess
import time

import numpy as np
import pyarrow as pa

from cms_rtf import aggregate, native_aggregate, refdata
from cms_rtf.validate_replay import exact_factors


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(8 << 20), b''):
            h.update(block)
    return h.hexdigest()


def load(path):
    return pa.ipc.open_stream(str(path)).read_all()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', required=True)
    p.add_argument('--baseline-root', required=True)
    p.add_argument('--weights-dir', required=True)
    p.add_argument('--threshold-dir', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--day', default='20261009')
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--check-workers', type=int, help='also verify another worker count')
    p.add_argument('--reference', help='optional previously accepted CMS factor Arrow')
    p.add_argument('--aggregate-backend', choices=['cpp', 'python'], default='cpp',
                   help='optimized aggregation backend; baseline always uses its own Python implementation')
    p.add_argument('--aggregator', help='optimized native aggregation executable override')
    args = p.parse_args()
    root = Path(__file__).resolve().parents[1]
    baseline = Path(args.baseline_root).resolve()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    ref = refdata.build_ref_tables(args.weights_dir, args.threshold_dir, args.day)
    thresh = out / 'thresh.bin'
    np.array([(s, ref['dyn'][int(s)][1], ref['dyn'][int(s)][2]) for s in ref['symbols']],
             dtype=[('symbol', '<i4'), ('xl', '<f8'), ('m', '<f8')]).tofile(thresh)
    spec = importlib.util.spec_from_file_location('cms_rtf.baseline_aggregate',
                       baseline / 'python/cms_rtf/aggregate.py')
    old_aggregate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(old_aggregate)
    report = dict(host=platform.node(), platform=platform.platform(),
                  numpy=np.__version__, pyarrow=pa.__version__, input=str(Path(args.input).resolve()),
                  input_bytes=Path(args.input).stat().st_size,
                  commits={name: subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'],
                           text=True).strip() for name, repo in [('baseline', baseline), ('optimized', root)]},
                  optimized_aggregate_backend=args.aggregate_backend, runs={}, checks={})
    runs = [('baseline', baseline, old_aggregate, args.workers),
            ('optimized', root, aggregate, args.workers)]
    if args.check_workers is not None:
        runs.append((f'optimized_w{args.check_workers}', root, aggregate, args.check_workers))
    for name, repo, module, workers in runs:
        dest = out / name
        dest.mkdir()
        cmd = [str(repo / 'build/cms_index500ms_mt'), '--input', args.input,
               '--thresh', str(thresh), '--work-dir', str(dest), '--workers', str(workers)]
        t = time.monotonic()
        with open(dest / 'kernel.json', 'w') as stdout, open(dest / 'kernel.stderr', 'w') as stderr:
            subprocess.run(['/usr/bin/time', '-v', '-o', str(dest / 'time.txt')] + cmd,
                           stdout=stdout, stderr=stderr, check=True)
        kernel_s = time.monotonic() - t
        health = json.loads((dest / 'kernel.json').read_text())
        print(json.dumps(dict(stage='kernel', run=name, seconds=kernel_s, health=health)), flush=True)
        t = time.monotonic()
        books, flows, seeds = [sorted(dest.glob(f'w*.{kind}.bin')) for kind in ('book', 'flow', 'seeds')]
        if name != 'baseline' and args.aggregate_backend == 'cpp':
            native_aggregate.run(books, flows, seeds, ref, args.day, dest / 'factors.arrow', args.aggregator)
        else:
            grid, rows = module.aggregate(books, flows, seeds, ref, args.day)
            module.write_arrow(grid, rows, args.day, dest / 'factors.arrow')
        agg_s = time.monotonic() - t
        report['runs'][name] = dict(kernel_seconds=kernel_s, aggregate_seconds=agg_s,
                                   total_seconds=kernel_s + agg_s, health=health)
        if name != 'baseline':
            check = exact_factors(load(out / 'baseline/factors.arrow'), load(dest / 'factors.arrow'))
            report['checks'][name] = check
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(dict(stage='aggregate', run=name, seconds=agg_s)), flush=True)
    ignored = {'workers', 'spin_waits', 'input_rows', 'dispatch_seconds', 'total_seconds'}
    old_health = report['runs']['baseline']['health']
    report['health_equal'] = all(all(r['health'].get(k) == v for k, v in old_health.items()
                                if k not in ignored) for r in report['runs'].values())
    report['binary_equal'] = {path.name: digest(path) == digest(out / 'optimized' / path.name)
                             for path in sorted((out / 'baseline').glob('w*.bin'))}
    if args.reference:
        report['checks']['accepted_reference'] = exact_factors(load(args.reference),
                                                              load(out / 'optimized/factors.arrow'))
    report['passed'] = (report['health_equal'] and all(report['binary_equal'].values())
                        and all(c['keys_equal'] and c['keys_unique'] and
                                c['exact_columns'] == c['total_columns']
                                for c in report['checks'].values()))
    (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(passed=report['passed'], report=str(out / 'report.json'))), flush=True)
    if not report['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()

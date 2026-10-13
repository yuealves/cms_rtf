#!/usr/bin/env python3
"""A/B cms_rtf alone on one DTYPE file: blocks+aggregate versus integrated Arrow.

Use an existing file so exporter/replay memory and timing are excluded. The
order alternates between repeats. Validate every output against baseline and an
optional previously accepted Arrow; count actual per-stock intermediate bytes.
"""
import argparse
import json
from pathlib import Path
import platform

import numpy as np
import pyarrow as pa

from benchmark_memory import timed
from cms_rtf import native_aggregate, refdata
from cms_rtf.validate_replay import exact_factors


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', required=True)
    p.add_argument('--baseline-kernel', required=True)
    p.add_argument('--baseline-aggregator', required=True)
    p.add_argument('--kernel', default='build/cms_index500ms_mt')
    p.add_argument('--weights-dir', required=True)
    p.add_argument('--threshold-dir', required=True)
    p.add_argument('--day', default='20261009')
    p.add_argument('--workers', type=int, default=16)
    p.add_argument('--window-batch', type=int, default=64)
    p.add_argument('--repeats', type=int, default=2)
    p.add_argument('--reference')
    p.add_argument('--out', required=True, type=Path)
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    ref = refdata.build_ref_tables(args.weights_dir, args.threshold_dir, args.day)
    threshold = args.out / 'threshold.bin'
    np.array([(s, ref['dyn'][int(s)][1], ref['dyn'][int(s)][2]) for s in ref['symbols']],
             dtype=[('s', '<i4'), ('xl', '<f8'), ('m', '<f8')]).tofile(threshold)
    reference = args.out / 'reference.bin'
    native_aggregate.write_reference(ref, reference)
    report = dict(host=platform.node(), platform=platform.platform(), workers=args.workers,
                  input=str(Path(args.input).resolve()), input_bytes=Path(args.input).stat().st_size,
                  window_batch=args.window_batch, runs=[], checks=[])
    report_path = args.out / 'report.json'
    for repeat in range(args.repeats):
        variants = ['blocks', 'stream'] if repeat % 2 == 0 else ['stream', 'blocks']
        for variant in variants:
            dest = args.out / f'{variant}_{repeat}'; dest.mkdir()
            cmd = [args.baseline_kernel if variant == 'blocks' else args.kernel,
                   '--input', args.input, '--thresh', str(threshold), '--work-dir', str(dest),
                   '--workers', str(args.workers), '--output-mode', variant]
            if variant == 'stream':
                cmd += ['--ref', str(reference), '--day', args.day, '--out', str(dest / 'factors.arrow'),
                        '--window-batch', str(args.window_batch)]
            kernel = timed(cmd, dest, 'kernel', sample=True)
            aggregation = None
            if variant == 'blocks':
                aggregation = timed(native_aggregate.command(*native_aggregate.worker_paths(dest), reference,
                                   args.day, dest / 'factors.arrow', args.baseline_aggregator), dest, 'aggregate')
            run = dict(variant=variant, repeat=repeat, directory=str(dest), kernel=kernel, aggregate=aggregation,
                       total_seconds=kernel['seconds'] + (aggregation['seconds'] if aggregation else 0),
                       max_rss_kib=max(kernel['max_rss_kib'], aggregation['max_rss_kib'] if aggregation else 0),
                       intermediate_bytes=sum(p.stat().st_size for p in dest.glob('w*.*')),
                       arrow_bytes=(dest / 'factors.arrow').stat().st_size)
            report['runs'].append(run)
            report_path.write_text(json.dumps(report, indent=2) + '\n')
            print(json.dumps({k: v for k, v in run.items() if k not in ['kernel', 'aggregate']}), flush=True)
    expected = pa.ipc.open_stream(args.out / 'blocks_0/factors.arrow').read_all()
    health = report['runs'][0]['kernel']['summary']
    keys = ['events', 'input_rows', 'over_deduct', 'd_unknown', 'unknown_trade_ref', 'late_passive_ref',
            'agg_skip', 'crossed_book', 'immediate_fill', 'sz_non_monotonic',
            'sh_identities_at_eof', 'live_orders_at_eof']
    for run in report['runs']:
        actual = pa.ipc.open_stream(Path(run['directory']) / 'factors.arrow').read_all()
        check = exact_factors(expected, actual)
        check.update(variant=run['variant'], repeat=run['repeat'],
                     health_equal=all(run['kernel']['summary'][k] == health[k] for k in keys))
        report['checks'].append(check)
    if args.reference:
        report['reference_check'] = exact_factors(pa.ipc.open_stream(args.reference).read_all(), expected)
    report['passed'] = all(c['exact_columns'] == 106 and c['keys_equal'] and c['keys_unique'] and
                           c.get('health_equal', True) for c in report['checks'] +
                           ([report['reference_check']] if args.reference else []))
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(passed=report['passed'], report=str(report_path))), flush=True)
    if not report['passed']: raise SystemExit(1)


if __name__ == '__main__':
    main()

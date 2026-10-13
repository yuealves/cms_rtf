"""日级批处理驱动：CMS parquet → events → 内核 → 聚合 → index_500ms_exch.arrow。

用法：
  python -m cms_rtf.run_day --day 20261009 \
      --data-root /data/tickl2_data_cms \
      --weights-dir /path/to/index/weight --threshold-dir /path/to/163-raw-minute \
      --out /path/to/output_root --kernel build/cms_index500ms_day \
      [--keep-intermediate] [--compare /tmp/cms_rtf_ref/index_500ms_exch.arrow]

输出布局与线上一致：<out>/index_500ms/<YYYYMMDD>/index_500ms_exch.arrow
中间产物在 <out>/index_500ms/<YYYYMMDD>/intermediate/（默认保留以便审计）。
"""
import argparse
import json
import subprocess
import time
from pathlib import Path

from . import codec, native_aggregate, refdata


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--day', required=True)
    p.add_argument('--data-root', default='/data/tickl2_data_cms')
    p.add_argument('--weights-dir', required=True)
    p.add_argument('--threshold-dir', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--kernel', default=str(Path(__file__).resolve().parents[2]
                                           / 'build/cms_index500ms_day'))
    p.add_argument('--compare', default=None, help='参考 arrow 路径（对拍报告）')
    p.add_argument('--aggregate-backend', choices=['cpp', 'python'], default='cpp')
    p.add_argument('--aggregator', help='native aggregation executable override')
    args = p.parse_args()

    out_dir = Path(args.out) / 'index_500ms' / args.day
    inter = out_dir / 'intermediate'
    inter.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    ref = refdata.build_ref_tables(args.weights_dir, args.threshold_dir, args.day)
    print(json.dumps(dict(stage='refdata', symbols=len(ref['symbols']),
                          official=ref['official'].round(4).tolist())), flush=True)

    events_bin = inter / 'events.bin'
    stats = codec.write_events(args.data_root, args.day, ref['symbols'],
                               events_bin, inter / 'thresh.bin', ref['dyn'])
    print(json.dumps(dict(stage='codec', **stats)), flush=True)

    for name in ('book.bin', 'flow.bin', 'seeds.bin'):
        (inter / name).unlink(missing_ok=True)
    result = subprocess.run([args.kernel, '--events', str(events_bin),
                             '--thresh', str(inter / 'thresh.bin'),
                             '--out-book', str(inter / 'book.bin'),
                             '--out-flow', str(inter / 'flow.bin'),
                             '--out-seeds', str(inter / 'seeds.bin')],
                            check=True, capture_output=True, text=True)
    print(json.dumps(dict(stage='kernel', **json.loads(result.stdout))), flush=True)

    arrow_path = out_dir / 'index_500ms_exch.arrow'
    rows = native_aggregate.run_backend(args.aggregate_backend, inter / 'book.bin',
                                        inter / 'flow.bin', inter / 'seeds.bin',
                                        ref, args.day, arrow_path, args.aggregator)
    print(json.dumps(dict(stage='aggregate', rows=rows, path=str(arrow_path),
                          elapsed_s=round(time.time() - t0, 1))), flush=True)

    if args.compare:
        from . import compare
        report = compare.compare(arrow_path, args.compare)
        report_path = out_dir / 'compare_report.json'
        compare.write_report(report, report_path)
        print(json.dumps(dict(stage='compare', report=str(report_path),
                              summary=report['summary'])), flush=True)


if __name__ == '__main__':
    main()

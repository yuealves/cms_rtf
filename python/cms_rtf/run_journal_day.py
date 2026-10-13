"""journal 直读跑因子：cms_journal_export → journal_events → 内核 → 聚合 → arrow。

用于回放链路验证与性能测试：因子引擎不经 parquet，直接消费（回放的）journal。

用法：
  PYTHONPATH=python python3 -m cms_rtf.run_journal_day \
    --journal /data/cms_replay_test/journal_replayed --day 20261009 \
    --exporter <cms_journal_export 路径> --gateway-build <cms_market_gateway 构建根> \
    --weights-dir ... --threshold-dir ... --out ...
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

import numpy as np

from . import native_aggregate, refdata
from .journal_events import JOURNAL_DTYPE, rows_to_events
from .codec import EVENT_DTYPE


def stream_events(exporter, journal, day, constituents, lib_dirs):
    """exporter stdout → 分块映射 → 按 symbol 排序的 events 数组。"""
    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = ':'.join(str(d) for d in lib_dirs)
    proc = subprocess.Popen([str(exporter), '--input', str(journal), '--day', str(day),
                             '--summary', os.devnull],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if proc.stdout.read(8) != b'CMSPQ01\0':
        raise RuntimeError('invalid exporter: ' + proc.stderr.read()[-2000:].decode(errors='replace'))
    parts = []
    batch = 500000 * JOURNAL_DTYPE.itemsize
    while True:
        blob = proc.stdout.read(batch)
        if not blob:
            break
        if len(blob) % JOURNAL_DTYPE.itemsize:
            raise RuntimeError('truncated exporter stream')
        part = rows_to_events(np.frombuffer(blob, dtype=JOURNAL_DTYPE), constituents)
        if part is not None:
            parts.append(part)
    if proc.wait() != 0:
        raise RuntimeError('journal export failed: ' + proc.stderr.read()[-2000:].decode(errors='replace'))
    n = sum(len(p['symbol']) for p in parts)
    events = np.zeros(n, dtype=EVENT_DTYPE)
    record_seq = np.concatenate([p['record_seq'] for p in parts])
    event_index = np.concatenate([p['event_index'] for p in parts])
    for name in EVENT_DTYPE.names:
        if name != 'pad':
            events[name] = np.concatenate([p[name] for p in parts])
    order = np.lexsort((event_index, record_seq, events['symbol']))
    return events[order], n


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--journal', required=True)
    p.add_argument('--day', required=True)
    p.add_argument('--exporter', required=True)
    p.add_argument('--gateway-build', required=True,
                     help='cms_market_gateway 的构建根（取 kungfu 运行库）')
    p.add_argument('--weights-dir', required=True)
    p.add_argument('--threshold-dir', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--aggregate-backend', choices=['cpp', 'python'], default='cpp')
    p.add_argument('--aggregator', help='native aggregation executable override')
    p.add_argument('--kernel', default=str(Path(__file__).resolve().parents[2]
                                           / 'build/cms_index500ms_day'))
    args = p.parse_args()

    out_dir = Path(args.out) / 'index_500ms' / args.day
    inter = out_dir / 'intermediate'
    inter.mkdir(parents=True, exist_ok=True)
    gb = Path(args.gateway_build)
    lib_dirs = [gb / 'build/kungfu/yijinjing/journal', gb / 'build/kungfu/yijinjing/paged',
                gb / 'build/kungfu/yijinjing/log']
    boost = Path('/home/linwei/.local/opt/kungfu/toolchain/boost-1.62.0/lib')
    if boost.is_dir():
        lib_dirs.append(boost)

    t0 = time.time()
    ref = refdata.build_ref_tables(args.weights_dir, args.threshold_dir, args.day)
    events, n = stream_events(args.exporter, args.journal, args.day, ref['symbols'], lib_dirs)
    events.tofile(inter / 'events.bin')
    t1 = time.time()
    print(json.dumps(dict(stage='journal_read', events=int(n), seconds=round(t1 - t0, 1))),
          flush=True)

    thresh = np.zeros(len(ref['symbols']), dtype=[('symbol', '<i4'), ('xl', '<f8'), ('m', '<f8')])
    for i, s in enumerate(ref['symbols']):
        thresh[i] = (s, ref['dyn'][int(s)][1], ref['dyn'][int(s)][2])
    thresh.tofile(inter / 'thresh.bin')

    for name in ('book.bin', 'flow.bin', 'seeds.bin'):
        (inter / name).unlink(missing_ok=True)
    subprocess.run([args.kernel, '--events', str(inter / 'events.bin'),
                    '--thresh', str(inter / 'thresh.bin'),
                    '--out-book', str(inter / 'book.bin'),
                    '--out-flow', str(inter / 'flow.bin'),
                    '--out-seeds', str(inter / 'seeds.bin')], check=True)
    arrow_path = out_dir / 'index_500ms_exch.arrow'
    rows = native_aggregate.run_backend(args.aggregate_backend, inter / 'book.bin',
                                        inter / 'flow.bin', inter / 'seeds.bin',
                                        ref, args.day, arrow_path, args.aggregator)
    print(json.dumps(dict(stage='completed', rows=rows, path=str(arrow_path),
                          total_seconds=round(time.time() - t0, 1))), flush=True)


if __name__ == '__main__':
    main()

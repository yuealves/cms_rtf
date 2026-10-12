"""journal → 多线程 C++ 引擎 → index_500ms 因子（性能测试版驱动）。

链路（全 C++ 热路径，无 Python 映射）：
  cms_journal_export（C++ 解析 journal → DTYPE 行流文件）
  → cms_index500ms_mt（reader 线程 + symbol hash 分发 + N worker 每股状态机）
  → Python 聚合（按 symbol 升序，位级一致与 worker 数无关）

用法：
  PYTHONPATH=python python3 -m cms_rtf.run_journal_mt \
    --journal <journal 日目录> --day 20261009 --workers 8 \
    --exporter <cms_journal_export 路径> --gateway-build <网关构建根> \
    --weights-dir ... --threshold-dir ... --out ... [--keep-stream]
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

import numpy as np

from . import aggregate, refdata


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--journal', required=True)
    p.add_argument('--day', required=True)
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--exporter', required=True)
    p.add_argument('--gateway-build', required=True)
    p.add_argument('--weights-dir', required=True)
    p.add_argument('--threshold-dir', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--kernel', default=str(Path(__file__).resolve().parents[2]
                                           / 'build/cms_index500ms_mt'))
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
    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = ':'.join(str(d) for d in lib_dirs)

    timings = {}
    t0 = time.time()
    ref = refdata.build_ref_tables(args.weights_dir, args.threshold_dir, args.day)
    timings['refdata'] = round(time.time() - t0, 1)

    # 1) journal → DTYPE 行流（C++ 解析，单线程）
    t = time.time()
    stream_path = inter / 'stream.bin'
    with open(inter / 'export.summary.json', 'wb') as devnull, \
         open(stream_path, 'wb') as out:
        proc = subprocess.Popen([args.exporter, '--input', args.journal, '--day', args.day,
                                 '--summary', '/dev/null'],
                                stdout=subprocess.PIPE, stderr=devnull, env=env)
        assert proc.stdout.read(8) == b'CMSPQ01\0', 'invalid exporter magic'
        out.write(b'CMSPQ01\0')
        while True:
            blob = proc.stdout.read(64 * 1024 * 1024)
            if not blob:
                break
            out.write(blob)
        if proc.wait() != 0:
            raise RuntimeError('journal export failed')
    timings['export_stream'] = round(time.time() - t, 1)
    rows = (os.path.getsize(stream_path) - 8) // 128
    print(json.dumps(dict(stage='export', rows=rows, seconds=timings['export_stream'])),
          flush=True)

    # 2) 多线程内核
    thresh = np.zeros(len(ref['symbols']), dtype=[('symbol', '<i4'), ('xl', '<f8'), ('m', '<f8')])
    for i, s in enumerate(ref['symbols']):
        thresh[i] = (s, ref['dyn'][int(s)][1], ref['dyn'][int(s)][2])
    thresh.tofile(inter / 'thresh.bin')
    for old in inter.glob('w*.*.bin'):
        old.unlink()
    t = time.time()
    result = subprocess.run([args.kernel, '--input', str(stream_path),
                             '--thresh', str(inter / 'thresh.bin'),
                             '--work-dir', str(inter), '--workers', str(args.workers)],
                            check=True, capture_output=True, text=True)
    timings['kernel_mt'] = round(time.time() - t, 1)
    print(json.dumps(dict(stage='kernel_mt', seconds=timings['kernel_mt'],
                          **json.loads(result.stdout))), flush=True)

    # 3) 聚合（按 symbol 升序 → 与 worker 数位级一致）
    t = time.time()
    books = sorted(inter.glob('w*.book.bin'))
    flows = sorted(inter.glob('w*.flow.bin'))
    seeds = sorted(inter.glob('w*.seeds.bin'))
    grid, all_rows = aggregate.aggregate(books, flows, seeds, ref, args.day)
    arrow_path = out_dir / 'index_500ms_exch.arrow'
    rows = aggregate.write_arrow(grid, all_rows, args.day, arrow_path)
    timings['aggregate'] = round(time.time() - t, 1)
    timings['total'] = round(time.time() - t0, 1)
    print(json.dumps(dict(stage='completed', rows=rows, path=str(arrow_path),
                          timings=timings)), flush=True)


if __name__ == '__main__':
    main()

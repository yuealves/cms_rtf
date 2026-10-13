"""journal → 多线程 C++ 引擎 → index_500ms 因子（性能测试版驱动）。

链路（全 C++ 热路径，无 Python 映射）：
  cms_journal_export（C++ 解析 journal → DTYPE 管道（--keep-stream 可保留文件））
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


def run_pipeline(export_cmd, kernel_cmd, inter, env, keep_stream=False):
    """Only return success when BOTH producer and consumer have succeeded."""
    timings = {}
    t = time.monotonic()
    with open(inter / 'export.stderr.log', 'wb') as err:
        if keep_stream:
            stream = inter / 'stream.bin'
            with open(stream, 'wb') as out:
                subprocess.run(export_cmd, stdout=out, stderr=err, env=env, check=True)
            timings['export_stream'] = time.monotonic() - t
            t = time.monotonic()
            result = subprocess.run(kernel_cmd + ['--input', str(stream)],
                                    check=True, capture_output=True, text=True)
            timings['kernel_mt'] = time.monotonic() - t
        else:
            proc = subprocess.Popen(export_cmd, stdout=subprocess.PIPE, stderr=err, env=env)
            try:
                result = subprocess.run(kernel_cmd + ['--input', '-'], stdin=proc.stdout,
                                        capture_output=True, text=True)
                proc.stdout.close()
                export_code = proc.wait()
                if export_code or result.returncode:
                    raise RuntimeError(f'journal pipeline failed: exporter={export_code}, '
                                       f'kernel={result.returncode}; {result.stderr}; '
                                       f'see {inter / "export.stderr.log"}')
            finally:
                proc.stdout.close()
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait()
            timings['export_kernel'] = time.monotonic() - t
    return json.loads(result.stdout), timings


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--journal', required=True)
    p.add_argument('--day', required=True)
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--export-threads', type=int, default=1,
                   help='gateway mmap exporter threads (parallel exporter buffers all rows)')
    p.add_argument('--keep-stream', action='store_true',
                   help='materialize stream.bin for repeated kernel benchmarks')
    p.add_argument('--exporter', required=True)
    p.add_argument('--gateway-build', required=True)
    p.add_argument('--weights-dir', required=True)
    p.add_argument('--threshold-dir', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--kernel', default=str(Path(__file__).resolve().parents[2]
                                           / 'build/cms_index500ms_mt'))
    args = p.parse_args()
    if not 1 <= args.workers <= 64 or args.export_threads < 1:
        p.error('workers must be 1..64 and export-threads must be positive')

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
    env['LD_LIBRARY_PATH'] = ':'.join([str(d) for d in lib_dirs] +
                                       [env.get('LD_LIBRARY_PATH', '')])

    timings = {}
    t0 = time.time()
    ref = refdata.build_ref_tables(args.weights_dir, args.threshold_dir, args.day)
    timings['refdata'] = round(time.time() - t0, 1)

    # Thresholds must be available before starting the streaming consumer.
    thresh = np.zeros(len(ref['symbols']), dtype=[('symbol', '<i4'), ('xl', '<f8'), ('m', '<f8')])
    for i, s in enumerate(ref['symbols']):
        thresh[i] = (s, ref['dyn'][int(s)][1], ref['dyn'][int(s)][2])
    thresh.tofile(inter / 'thresh.bin')
    for old in inter.glob('w*.*.bin'):
        old.unlink()
    export_cmd = [args.exporter, '--input', args.journal, '--day', args.day,
                  '--summary', str(inter / 'export.summary.json'),
                  '--threads', str(args.export_threads)]
    kernel_cmd = [args.kernel, '--thresh', str(inter / 'thresh.bin'),
                  '--work-dir', str(inter), '--workers', str(args.workers)]
    health, stages = run_pipeline(export_cmd, kernel_cmd, inter, env, args.keep_stream)
    timings.update({k: round(v, 3) for k, v in stages.items()})
    (inter / 'kernel.summary.json').write_text(json.dumps(health, indent=2) + '\n')
    print(json.dumps(dict(stage='export_kernel', timings=timings, **health)), flush=True)

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

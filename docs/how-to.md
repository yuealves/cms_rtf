# 操作手册：cms_rtf（招商数据生产 index_500ms 因子）

写给**第一次接触本项目**的人：看完应能用 CMS 的逐笔 parquet 跑出一整天的
index_500ms 因子，并知道产出物是什么、怎么和线上参考对拍。

## 这是什么

输入：`/data/tickl2_data_cms/<YYYYMMDD>/` 下招商逐笔的 order/trade parquet
（cms_market_gateway 每天自动同步过来）。
输出：4 个宽基指数（000016/000300/000905/000852）的 500ms 因子，
`index_500ms_exch.arrow`（115,200 行 × 108 列：datetime + code + 106 个 Float32 因子）。
口径与华泰线上 light_rtf v2 逐字一致（[设计说明](design.md)）。

## 跑一天（最小命令）

```bash
# 0) 一次性准备：编译内核及聚合器（cmake + gcc11 + Arrow C++ 或现有 PyArrow）
cmake -S . -B build && cmake --build build -j

# 1) 跑某天（权重/阈值目录里放 ≤ 当天的最新文件即可）
PYTHONPATH=python python3 -m cms_rtf.run_day \
  --day 20261009 \
  --data-root /data/tickl2_data_cms \
  --weights-dir <含 csi_50/csi_300/csi_500/csi_1000 子目录的权重目录> \
  --threshold-dir <含 threshold_YYYYMMDD.csv 的目录> \
  --out <输出根目录>

# 产出物：<输出根目录>/index_500ms/20261009/index_500ms_exch.arrow
```

2026-10-09 的完整示例（含对拍）：`bash scripts/run_20261009.sh`（端到端 < 10 分钟）。

`run_day`、`run_journal_day`、`run_journal_mt` 默认使用 C++ 聚合和 Arrow 写出；
`--aggregate-backend python` 可切回原聚合器对拍，`--aggregator <path>` 指定 C++ 可执行文件。
多 Python 环境构建可指定 `-DPython3_EXECUTABLE=/path/to/python`，以找到正确的 PyArrow
头文件/动态库；系统 Arrow CMake 包存在时优先使用。只需逐笔内核可用
`-DCMS_RTF_BUILD_AGGREGATE=OFF`，此时运行要显式选 Python 聚合。

## 产出物长什么样

```python
import pyarrow.ipc as ipc
t = ipc.open_stream('out/index_500ms/20261009/index_500ms_exch.arrow').read_all()
# 108 列：datetime(北京墙钟 timestamp[us])、code（如 000300.XSHG）、106 个 float 因子
# 行序：每个 500ms 端点 4 行（4 个指数），端点从 09:30:00.500 到 15:00:00
```

列的语义与顺序是**对拍契约**（`python/cms_rtf/catalog.py`，对应 light_rtf 的
`catalog.h`）：0–34 盘口组、35–71 事件流量 500ms 组、72–105 同内容的 5s 滚动组。
簿列无效时为 null；流量列恒非 null（空桶=0）。

## 对拍（验证我的输出与线上参考一致）

```bash
# run_day 加 --compare <参考 arrow> 即可；报告写在输出目录 compare_report.json
PYTHONPATH=python python3 -m cms_rtf.run_day ... --compare /path/to/index_500ms_exch.arrow
```

参考因子在 ht_idc 的 insight_yjj_test 容器 `/data/shenrun/index_500ms/<YYYYMMDD>/`；
2026-10-09 的对拍结果与结论见 [validation/20261009-compare-results.md](validation/20261009-compare-results.md)
（104/106 列 corr>0.99，两个例外均有定性解释）。

## 测试

```bash
ctest --test-dir build --output-on-failure                    # 簿/流量内核
PYTHONPATH=python python3 -m unittest discover -s tests/python -v   # 聚合层
```

## Journal replay 与性能对拍

使用已经完成回放的 journal（不需要起 paged），默认省去约 64 GiB/天的 DTYPE 中间文件：

```bash
PYTHONPATH=python python3 -m cms_rtf.run_journal_mt \
  --journal <回放 journal 目录> --day 20261009 --workers 8 \
  --exporter ../cms_market_gateway/build/gateway/bin/cms_journal_export \
  --gateway-build ../cms_market_gateway \
  --weights-dir <权重目录> --threshold-dir <阈值目录> --out <输出根目录>
```

网关需包含 mmap 提交 `0abaa13`。`--export-threads N` 可启用该版本的页并行导出；
当前并行 exporter 会缓存全天全部行，需额外约 64 GiB 内存，默认使用 1 个线程。
`--keep-stream` 保留 stream.bin，并分别记录 export/kernel 耗时，方便重复测内核。
`intermediate/export.summary.json`、`export.stderr.log` 和 `kernel.summary.json`
分别保留导出质量计数、诊断日志和内核统计。

原生多线程默认 `--output-mode stream`，窗口贡献在内存中聚合后释放，直接写最终
Arrow，没有 `w*.book/flow/seeds` 文件。`--window-batch 64` 为加速 replay 的批量
关窗调度；逐可用端点发布可改为 `1`，500ms 因子本身的粒度不变。
`--pending-mib 256` 是待聚合 payload 预算，按 worker 均分；水位停滞超限会报错，
不会自动写磁盘。遇到已关窗迟到事件也报错，旧 `--output-mode blocks` 可作诊断对照。
只有 exporter 与 kernel 都成功，正式 Arrow 才被替换；运行中的临时名称文件
就是最终 Arrow 格式。已有的逐股对照文件不会被用于新路径的聚合。

与上一版分块落盘路径做同机交替对照（计时不含 exporter）：

```bash
PYTHONPATH=python python3 scripts/benchmark_stream.py \
  --input <stream.bin> --workers 16 --repeats 2 \
  --baseline-kernel <旧 worktree>/build/cms_index500ms_mt \
  --baseline-aggregator <旧 worktree>/build/cms_index500ms_aggregate \
  --weights-dir <权重目录> --threshold-dir <阈值目录> \
  --reference <已验收 CMS Arrow> --out <新的基准目录>
```

逐进程 `/usr/bin/time -v` 记录峰值 RSS，`/proc` 采样区分匿名/文件页；比较完整耗时、
106 列有效值位模式、空值掩码、键、健康计数以及逐股中间文件字节数。

对同一份 replay DTYPE 流比较旧版和新版，输出目录必须尚不存在：

```bash
PYTHONPATH=python python3 scripts/benchmark_replay.py \
  --input <stream.bin> --baseline-root <旧版已编译 worktree> \
  --weights-dir <权重目录> --threshold-dir <阈值目录> \
  --workers 8 --check-workers 1 --out <新的基准结果目录> \
  --reference <已验收的 CMS 因子 arrow>
```

报告记录两个提交、机器/依赖版本、阶段耗时、健康计数、内核文件 SHA-256 对比和
最终 106 列逐位检查（包括 null mask、键、schema）。计时复用相同输入，
不包含一次性的 parquet/cache/journal 准备；机器负载与缓存会影响结果。

新版本 `benchmark_replay.py` 默认对优化版计入 C++ 聚合；复现原 Python 对比时加
`--aggregate-backend python`。旧版一侧始终加载其自身的 Python 聚合实现。

只比较聚合器，复用相同的 `w*.{book,flow,seeds}.bin`：

```bash
PYTHONPATH=python python3 scripts/benchmark_aggregate.py \
  --intermediate <已有 worker 文件目录> \
  --weights-dir <权重目录> --threshold-dir <阈值目录> --day 20261009 \
  --reference <已验收 Arrow> --repeats 3 --out <新的结果目录>
```

每版预热一次后交替顺序运行，各轮独立进程，记录阶段时间及 `/usr/bin/time -v`。
Python 时间不含导入/参考表加载，C++ 时间包含完整进程启动/退出；逐位验收在计时外。
详见 [C++ 聚合与内存分析](validation/20261013-cpp-aggregate.md)。

## 故障排查

| 现象 | 处理 |
|---|---|
| `no parquet shards under ...` | 当天数据还没同步过来：查 `/data/tickl2_data_cms/log/sync-<day>.json` |
| `no weights file ≤ ...` | 权重目录缺当天及之前的文件；把最新 `YYYY-MM-DD.parquet` 放进对应 csi_* 子目录 |
| `missing liquidity thresholds` | 阈值 CSV 缺成分股；换 ≤ 当天最新的完整 `threshold_*.csv` |
| 内核健康统计非零（over_deduct 等） | 数据有缺单/乱序；看当日内核 stdout 的 JSON 汇总，对照 `docs/validation/20261009-field-mapping.md` 复核口径 |

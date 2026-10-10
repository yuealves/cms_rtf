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
# 0) 一次性准备：编译内核（机器需 cmake + gcc11）
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

## 故障排查

| 现象 | 处理 |
|---|---|
| `no parquet shards under ...` | 当天数据还没同步过来：查 `/data/tickl2_data_cms/log/sync-<day>.json` |
| `no weights file ≤ ...` | 权重目录缺当天及之前的文件；把最新 `YYYY-MM-DD.parquet` 放进对应 csi_* 子目录 |
| `missing liquidity thresholds` | 阈值 CSV 缺成分股；换 ≤ 当天最新的完整 `threshold_*.csv` |
| 内核健康统计非零（over_deduct 等） | 数据有缺单/乱序；看当日内核 stdout 的 JSON 汇总，对照 `docs/validation/20261009-field-mapping.md` 复核口径 |

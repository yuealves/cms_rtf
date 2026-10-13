# cms_rtf

招商（CMS）逐笔行情的**指数 500ms 因子**离线批处理。输入 `/data/tickl2_data_cms/<YYYYMMDD>/`
的 order/trade parquet 分片（archive_journal 产物），输出与线上 light_rtf v2 同口径的
`index_500ms/<YYYYMMDD>/index_500ms_exch.arrow`（Arrow IPC stream：
datetime[us 北京墙钟] + code + 106×Float32，NaN→null）。

- **操作入口（跑一天/对拍/测试/故障排查）**：[docs/how-to.md](docs/how-to.md)
- 设计（批处理架构与口径移植边界）：[docs/design.md](docs/design.md)
- 验证记录：[docs/validation/](docs/validation/)
- AI 协作入口：[AGENTS.md](AGENTS.md)

语义基准：light_rtf v2 的 index_500ms family。本项目是它的
**CMS 数据源批处理移植**：同一套 106 列定义、同一套簿/流量/聚合语义，但
- 只有交易所网格（local 网格不实现——不同采集机的接收时间本来也不可比）；
- 批量处理全天，无实时水位/关窗逻辑（全量历史 ⇒ 28,800 端点全发布）。

## 架构

```
python/cms_rtf/codec.py      parquet → canonical 事件流（字段映射实证见
                             docs/validation/20261009-field-mapping.md）
build/cms_index500ms_day     C++ 内核：簿重建 + 流量状态机 + 端点捕获
                             （逐字移植 light_rtf l2_book / state.cpp / book_capture）
build/cms_index500ms_aggregate  C++ 跨股票加权聚合 + 派生列 + 5s 滚动 → Arrow
python/cms_rtf/aggregate.py    保留的 Python 对照实现（--aggregate-backend python）
python/cms_rtf/run_day.py    日级驱动；--compare 生成对拍报告
```

C++ 与 Python 之间用二进制文件接口（`events.bin`/`book.bin`/`flow.bin`/`seeds.bin`，
布局与 `include/cms_rtf/events.h` 的结构体逐字节一致、有 static_assert 守护）。

多线程 journal 驱动 `cms_rtf.run_journal_mt` 默认将内核结果分块写入
`w*.book.cmsb` / `w*.flow.cmsb`，避免在内存中留存全天结果；C++ 聚合读取版本化索引。
`--aggregate-backend python` 自动使用旧稠密 `.bin` 格式，内核也可显式指定
`--output-mode legacy` 对照。格式与实盘接入边界见 [设计说明](docs/design.md)。

## 构建与测试

聚合默认使用 C++。构建需要 Arrow C++，也可直接使用现有 PyArrow 安装内的头文件和
动态库；CMake 会自动发现。多 Python 环境可指定 `-DPython3_EXECUTABLE=/path/to/python`。
仅构建逐笔内核可用 `-DCMS_RTF_BUILD_AGGREGATE=OFF`，运行时需显式选 Python 聚合。

```bash
cmake -S . -B build && cmake --build build -j
ctest --test-dir build --output-on-failure                              # 簿/流量内核
PYTHONPATH=python python3 -m unittest discover -s tests/python -v       # 聚合层
```

## 跑一天

```bash
PYTHONPATH=python python3 -m cms_rtf.run_day --day 20261009 \
  --data-root /data/tickl2_data_cms \
  --weights-dir <index/weight 目录（csi_* 子目录）> \
  --threshold-dir <threshold_*.csv 目录> \
  --out <输出根> [--compare <参考 arrow>]
```

参考数据 fail-closed：权重取 `csi_{50,300,500,1000}/` 下 ≤ 当天的最新 parquet；
阈值取 ≤ 当天的最新 `threshold_*.csv`；成分股缺阈值拒绝运行。

## 红线

- 不修改线上任何组件；只读本机 parquet 与参考数据目录。
- 106 列顺序是**对拍契约**（`python/cms_rtf/catalog.py`），禁止改动。
- 与 HT 参考值的差异主要来自两路采集本身（丢包/口径），对拍口径见
  `python/cms_rtf/compare.py` 与 `docs/validation/`。

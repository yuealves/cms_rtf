# cms_rtf

招商（CMS）逐笔行情的**指数 500ms 因子**实时计算与离线批处理。实时入口直接跟读
gateway 正在写入的 `cms_l2_raw_v1` journal。离线入口输入 `/data/tickl2_data_cms/<YYYYMMDD>/`
的 order/trade parquet 分片（archive_journal 产物），输出与线上 light_rtf v2 同口径的
`index_500ms_exch.arrow`；journal 实盘/回放同时输出 `index_500ms_local.arrow`。
两路为 Arrow IPC stream：datetime[us 北京墙钟] + code + 106×Float32，NaN/Inf→null。

- **操作入口（跑一天/对拍/测试/故障排查）**：[docs/how-to.md](docs/how-to.md)
- **招商机源码部署与实时运行**：[docs/deploy-live.md](docs/deploy-live.md)
- 设计（批处理架构与口径移植边界）：[docs/design.md](docs/design.md)
- 验证记录：[docs/validation/](docs/validation/)
- AI 协作入口：[AGENTS.md](AGENTS.md)

语义基准：light_rtf v2 的 index_500ms family。本项目是它的
**CMS 数据源移植**：同一套 106 列定义、同一套簿/流量/聚合语义，但
- journal 双网格按华泰接收时钟 cutoff、严格通道水位和 11:31/15:01 会话兜底关窗；
  同目录 `latency.log` 每次 cutoff 一条，统计关窗触发至 local/exch 两路 Flush 完成的毫秒数；
- `--journal` 支持实时跟随、会话定时器与直接 Arrow 发布；`--journal-mode replay`
  使用原始记录时钟复现该路径。EOF 按华泰补 local 的普通 cutoff，不人为推进 exch 水位。
  原 parquet/DTYPE 离线接口保留 exch-only 对照及其 EOF 补齐契约。

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

多线程 journal 驱动 `cms_rtf.run_journal_mt` 默认使用 `--output-mode stream`：
逐股贡献在内存内按窗口聚合、消费后释放，直接写最终 Arrow；**不生成逐股中间文件**。
`--window-batch 64` 批量调度 replay 窗口，`1` 可逐可用端点发布。`--pending-mib 256`
限制待聚合记录 payload；通道停滞超预算或出现已关窗的迟到事件会报错，不丢弃数据。
正式 Arrow 文件名只在 producer 和 consumer 都成功后替换，运行中写的是同一份
最终格式 Arrow 的临时名称。全部边界见 [设计说明](docs/design.md)。

显式 `--output-mode blocks` 保留 `.cmsb` 中间文件对照；`--aggregate-backend python`
自动使用旧 `.bin` 路径。单线程 parquet 驱动仍使用原有离线接口。
全天性能、内存、逐窗口调度开销及实盘接入边界见
[内存聚合验收记录](docs/validation/20261013-in-memory-aggregate.md)。

## 构建与测试

聚合默认使用 C++。构建需要 Arrow C++，也可直接使用现有 PyArrow 安装内的头文件和
动态库；CMake 会自动发现。多 Python 环境可指定 `-DPython3_EXECUTABLE=/path/to/python`。
仅构建逐笔内核可用 `-DCMS_RTF_BUILD_AGGREGATE=OFF`，运行时需显式选 Python 聚合。
gy_dev 当前 PyArrow 11 使用旧 libstdc++ ABI，配置时另加
`-DCMAKE_CXX_FLAGS=-D_GLIBCXX_USE_CXX11_ABI=0`；否则 Arrow 的字符串符号无法链接。

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

- cms_rtf 独立部署；只读 journal、parquet 与参考输入，不修改网关、paged 和归档组件。
- 106 列顺序是**对拍契约**（`python/cms_rtf/catalog.py`），禁止改动。
- 与 HT 参考值的差异主要来自两路采集本身（丢包/口径），对拍口径见
  `python/cms_rtf/compare.py` 与 `docs/validation/`。

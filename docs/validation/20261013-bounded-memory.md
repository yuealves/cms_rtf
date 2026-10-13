# cms_rtf 内存归因与优化验证（2026-10-13）

代码基线 `36c86da`（已有 C++ 聚合），优化 `880eff2`（初版 `3b343fa` 的索引布局随后调整），分支
`perf/bounded-output-20261013`。本记录针对 20261009 CMS 行情；不是实际托管机实盘测量。

## 归因与实盘边界

计划实盘链路为 CMS gateway 写 Kungfu journal，cms_rtf 读 journal。当前仓库的
`run_journal_mt` 使用 exporter 接内核，EOF 后聚合 Arrow，尚未实现实时 journal follower、
跨 worker 水位和逐窗发布。本次改共享计算状态与结果留存，不修改线上 gateway。

之前的 23,991,824 KiB（22.88 GiB）来自 `/usr/bin/time -v cms_index500ms_mt
--input /tmp/par_t8.bin ...`，不是整个 pipeline 的内存；当时没有 exporter 同时运行。
因此确实属于 cms_rtf，但不能把它解释为实盘所必需的常驻状态。
旧版全天 BookRec / FlowRec 载荷分别为 14,482,944,000 / 5,764,063,888 字节，合计
18.856 GiB；vector capacity、活跃簿、全天 SH 委托身份和分配器还有额外占用。
载荷大小与峰值 RSS 不相等，也不能将各阶段峰值直接相加。

## 改动

- SH 身份表改成分页连续记录 + uint32 开放寻址索引，减少每笔单独分配和指针开销。
  索引最多半满，保留序号局部性并折叠高 32 位，避免相邻委托被强混合散列打散。
  所有全天身份、价格/数量历史保留；桶内遍历和浮点求和顺序保持原样。
- 每股盘口/流量缓冲各最多 512 条，满块写入版本化 CMSBLK01 容器。
  每 worker 文件各有 1 MiB I/O 缓冲；只有小块索引随全天记录数增长。
- 连续无事件端点只存一个快照，聚合按旧有 carry-forward 恢复网格，避免无变化快照反复复制。
- 单线程分股入口每股结束即写出并复用缓冲；管道入口改 read(2)，避免等待填满 8 MiB。

这消除了全天结果向量的内存增长；实际交易状态仍包含活跃订单和全天 SH 身份，
随当日行情增长。结果文件仍是离线中间文件，尚非实盘在线聚合器。

## 测量方法

`benchmark_memory.py` 对相同已导出 DTYPE 文件运行旧版与完整优化版；gy_dev 最终验收
第一轮旧→新，第二轮新→旧，每版 16 workers。初步归因另外跑过紧凑身份表但保留
旧结果缓存（identity_only）的对照，最终验收数据单列如下。
内核和 C++ 聚合分别由 `/usr/bin/time -v` 测峰值，wall time 用 monotonic 覆盖子进程执行；
总耗时为两阶段之和，包括输入读取、EOF 收尾、中间文件写入、聚合及 Arrow 输出。
不含 export/replay、参考数据准备、构建及对拍；也不包含 fsync 强制持久化。
RSS 表列仅为内核进程；独立 C++ 聚合进程的优化版峰值约 0.183 GiB。
Python 启动/测量脚本自身内存未计入这些进程 RSS。
内核另每 250ms 采样 `/proc/PID/status`，区分 RssAnon 与 RssFile；采样峰值可能漏过瞬时峰值。
各股 EOF capacity 求和不是同一时刻的堆快照，不能当成完整 RSS 分解。

## 本地 09:35 前数据

40,271,350 原始行，17,892,045 成分股事件。输入 SHA256：
`986c7b6c3acd3183e2c14674af104749b88660e134b659d6f7231150a8f61ddd`。
本地 Ryzen 9950X / WSL2、GCC 13.3；所有三版本两轮均通过健康计数、逻辑记录字节及
106 列（包括 null mask/有效浮点位）对拍，且与先前接受的 Arrow 相同。
初版详情来自 `work/bounded-output-20261013/local_16_final/report.json`；
最终 `880eff2` 的独立对照为 `work/bounded-output-20261013/local_16_locality/report.json`，
全部记录与 106 列同样通过。

| 最终版本本地对照 | 内核 | C++ 聚合/Arrow | 合计 | 内核峰值 |
|---|---:|---:|---:|---:|
| 旧版 36c86da | 6.26 s | 3.27 s | 9.53 s | 1.527 GiB |
| 优化 880eff2 | 1.25 s | 1.72 s | 2.97 s | 0.894 GiB |

本地多次实验的文件缓存/I/O 波动明显，上表是最终版本的一次配对，不将单次速度比外推全天。

## gy_dev 全天

535,706,255 原始行、227,956,156 成分股事件，输入 `/tmp/par_t8.bin`（68,570,400,648 字节）。
同机 GCC 11.4、Release、PyArrow 11。系统 PyArrow 的 Arrow C++ 为旧 libstdc++ ABI，
两个版本均用 `-DCMAKE_CXX_FLAGS=-D_GLIBCXX_USE_CXX11_ABI=0` 构建。

| 版本 / 次序 | 内核 | C++ 聚合/Arrow | 合计 | 内核峰值 |
|---|---:|---:|---:|---:|
| 旧版 / 第一轮先跑 | 59.92 s | 12.77 s | 72.68 s | 22.879 GiB |
| 优化 / 第一轮后跑 | 60.21 s | 10.12 s | 70.32 s | 3.459 GiB |
| 优化 / 第二轮先跑 | 57.88 s | 9.92 s | 67.79 s | 3.458 GiB |
| 旧版 / 第二轮后跑 | 54.70 s | 12.42 s | 67.12 s | 22.901 GiB |

两轮平均总耗时 69.90 → 69.06 秒，属于本机波动范围内的基本持平；内核阶段平均
57.31 → 59.04 秒（约 +3%），聚合 12.59 → 10.02 秒。这里分别报告两阶段，约束以包含聚合和输出的总耗时判断。
峰值按各版本两次的较高值计，22.901 → 3.459 GiB，下降约 84.9%。

优化版 EOF 计数：64,886,586 条 SH 全天身份，身份表容量 2,840,256,512 字节
（2.645 GiB）；4,172,008 条当前簿订单；输出 vector 容量 419,315,712 字节
（399.89 MiB）。另外还有 ring、I/O 缓冲、活跃簿/价位结构和 allocator 开销。
RssAnon/RssFile 的采样值见配套 JSON；文件页约 3.6 MiB，主体确为匿名内存。

**84.9% 是消除离线全天结果留存后的引擎回放 RSS 降幅，不能外推成实盘内存下降
84.9%。** 实盘共享计算状态直接受益于紧凑身份表；已经成交的身份仍保留以维持累计量、
已知价和大小单分类。真正 journal follower 的页驻留及在线窗口聚合器需接入后另行测量。

全天四次运行均通过：

- book 按原网格展开后、flow / seeds 原始字节 SHA256 一致。
- 115,200 行键、唯一性、106 列 null mask 和有效 Float32 位全部一致。
- 全部健康计数一致；与此前接受的 `out_v2_direct.arrow` 也为 106/106 一致。
- 本地与 gy_dev 均通过 5 项 CTest、36 项 Python unittest。

机器可读摘要：[20261013-bounded-memory.json](20261013-bounded-memory.json)。
原始日志、逐进程 `/usr/bin/time -v`、每 250ms RSS 采样、结果与完整逐列对拍都在
`gy_dev:/data/users/linwei/cms_work/memory_20261013/full16-locality/`。
`run-locality.sh` 保留构建和运行命令；旧原型在相邻 `full16/`，不作为最终性能验收。

复现（gy_dev，在独立 worktree `cms_rtf_memory_locality_20261013`）：

```bash
cmake -S . -B build -DPython3_EXECUTABLE=/usr/bin/python3 \
  -DCMAKE_CXX_FLAGS=-D_GLIBCXX_USE_CXX11_ABI=0
cmake --build build -j8
PYTHONPATH=python /usr/bin/python3 scripts/benchmark_memory.py \
  --input /tmp/par_t8.bin \
  --baseline-kernel ../cms_rtf_cpp_base_36c86da/build/cms_index500ms_mt \
  --baseline-aggregator ../cms_rtf_cpp_base_36c86da/build/cms_index500ms_aggregate \
  --weights-dir /data/users/linwei/cms_work/ref/weights \
  --threshold-dir /data/users/linwei/cms_work/ref/thresholds \
  --reference /data/users/linwei/cms_work/out_v2_direct.arrow \
  --workers 16 --repeats 2 --out /path/to/new/experiment-directory \
  > /path/to/experiment.log 2>&1
```

基线 worktree 为 `36c86da`，使用同样的 CMake 参数预先构建。输出目录必须新建，
避免混入旧 worker 文件。对拍在计时结束后执行，读写较多，不计入因子运行耗时。

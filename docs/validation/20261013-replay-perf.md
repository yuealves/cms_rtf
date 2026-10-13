# Replay 性能优化验证（2026-10-13）

实现提交：`1b14d18`，分支 `perf/replay-pipeline-20261013`。
基线：`1e22619`；网关沿用已有 `0abaa13`（mmap 页扫描 / 可选页并行导出），
本轮没有修改网关和 Kungfu 运行库，也没有改动线上组件。

## 输入与复现

- 全天 replay DTYPE：`gy_dev:/tmp/par_t8.bin`，535,706,255 行，68,570,400,648 字节。
  来源是 `/data/cms_replay_test/journal_replayed` 的网关 exporter 输出。
- 本地 09:35 前缀：按原始 `recv_ns < 2026-10-09 09:35:00 Asia/Shanghai`
  截取连续前缀，保留所有市场/股票及到达顺序。40,271,350 行，5,154,732,808 字节；
  接收范围 09:15:00.045043330 至 09:34:59.999532199。
- 前缀 SHA-256：`986c7b6c3acd3183e2c14674af104749b88660e134b659d6f7231150a8f61ddd`。
  Zstd 后 676,516,958 字节，经 6 路分块续传下载，解压后 SHA-256 一致。
- 参考表：2026-10-09 的四指数权重及当日阈值；本地、远端同源。

本地输入与日志：`work/perf-20261013/local_0935/`，详细报告为
`comparison/report.json`。远端报告：
`/data/users/linwei/cms_work/perf_20261013/full/report.json`。
两个仓库版本在独立 worktree 用同一主机的相同编译器重新编译。

通用复现入口为 `scripts/benchmark_replay.py`（命令见 `docs/how-to.md`）。
同一进程依次运行基线 8 worker、优化 8 worker、优化 1 worker，保留各自中间文件、
Arrow、健康统计和 `/usr/bin/time -v`。时间是一次配对实验，受缓存和共享主机负载影响；
不将两台机器之间的速度差算作优化收益。

## 本地 09:35 前缀

环境：Ryzen 9 9950X / WSL2，32 个逻辑 CPU，GCC 13.3.0，NumPy 2.2.6，PyArrow 23.0.1。
成分股事件 17,892,045 条。

| 阶段（8 worker） | 基线 | 优化后 | 加速 |
|---|---:|---:|---:|
| C++ 内核（含 EOF / 输出） | 16.35 s | 9.01 s | 1.81× |
| Python 聚合 + Arrow | 23.30 s | 8.80 s | 2.65× |
| 合计 | 39.65 s | 17.81 s | 2.23× |
| 内核峰值 RSS | 13.26 GiB | 1.39 GiB | 降低 89.5% |

前缀测试仍遵循原批处理 EOF 语义：把最后捕获簿延续到全天网格，输出仍为
115,200 行。这不是“09:35 以后真实因子”，也不能把该耗时直接外推为纯逐笔吞吐。
前缀 RSS 的显著下降主要来自 EOF 逐股写出后立即释放缓冲；全天峰值内存见下一节。

验收 PASS：24 个同 worker 分片逐字节一致；全部健康计数相同；
8 worker 优化前后、优化后 1/8 worker 的 106 列 Float32 有效值位模式、null mask、
schema、行键完全一致，均为 115,200 行且无重复键。

## gy_dev 全天回放

环境：双路 EPYC 7763，256 个逻辑 CPU，GCC 11.4.0，NumPy 1.21.5，PyArrow 11.0.0。
成分股事件 227,956,156 条。

| 阶段（8 worker） | 基线 | 优化后 | 加速 |
|---|---:|---:|---:|
| C++ 内核（含 EOF / 输出） | 260.07 s | 64.76 s | 4.02× |
| Python 聚合 + Arrow | 88.07 s | 23.94 s | 3.68× |
| 合计 | 348.14 s | 88.69 s | 3.93× |
| 内核峰值 RSS | 22.89 GiB | 22.88 GiB | 基本不变 |

内核累计 user + system CPU 时间从 1,910.30 s 降至 504.77 s，
因此收益不只来自文件缓存差异。这里复用已导出的 DTYPE，未计入 parquet/cache/journal
准备及 exporter；journal 直连管道另行验证。

全天验收 PASS：24 个同 worker 分片逐字节一致，所有健康计数一致；
8 worker 优化前后、优化后 1/8 worker、优化后对历史 CMS 直读 v2 parquet 结果
`/data/users/linwei/cms_work/out_v2_direct.arrow` 均为 106/106 列逐位一致，
115,200 行且行键、schema、null mask 均一致。
1 worker 只用于确定性回归：内核 360.99 s，聚合 27.56 s。

Journal 直接读取的额外测试遇到 gy_dev 磁盘读取等待：exporter 曾处于
`D / wait_on_page_bit_common`，内核等待管道输入。停止了本轮启动的该测试，
没有处理机器故障。为把验证范围限定在 cms_rtf，改用已验收的全天 DTYPE
通过真实 OS pipe 输入，该项已 PASS：535,706,255 行完整输入、227,956,156 条
成分股事件，106/106 列与基线逐位一致，115,200 行、schema、键及 null mask 一致；
管道生产者 + 内核耗时 60.59 s。该测试使用 `cat` 发送已导出的真实 DTYPE，
不替代受磁盘读取影响的 journal exporter 全链路计时。
报告在远端 `perf_20261013/cached_pipe/report.json`，本地副本在
`work/perf-20261013/remote_full/cached-pipe-report.json`。
上述提速数字不包含 journal 导出阶段，
不能据此声称整条冷盘 journal 链路提速 3.93×。

## 改动和回归

1. 成分股过滤前移、快速解析六位代码、SPSC 按 256 条批量发布/消费；
   维持每股 FIFO，完成标志 acquire 后再次 drain 队列。
2. 同桶内不再反复计算最终会被覆盖的十档快照 / 31 个特征；
   开盘前值、跨端点、乱序以及 15:00 后冻结捕获维持原值。
3. 写完一只股票立即释放输出缓冲；聚合遍历 mmap 分组而不拼接全量记录；
   流量端点唯一时走向量索引累加，重复端点保留 `np.add.at`。
4. exporter stdout 直接连接内核 stdin；保留 `--keep-stream` 和
   `--export-threads`。两端均成功后才聚合，错误 magic、半条记录、写盘失败拒绝成功。
   网关当前多线程 exporter 会缓存全天约 64 GiB DTYPE，所以默认仍为 1 个 mmap reader。

31 个公式、流量状态机、106 列目录、二进制布局、股票聚合顺序及 Float32 量化点不变。

4 项 CTest 通过；25 项 Python unittest 通过。新增覆盖 eager/lazy 捕获等价、
乱序/午休/收盘、队列 wraparound/背压、不同 worker 数、管道/文件/无头输入、
截断记录/阈值、生产者或消费者失败、输出设备写失败、重复端点及空 worker 文件。
同时启用了 Release 下原有 C++ 测试的断言，修正两处原先被 `NDEBUG` 隐藏的错误预期：
盘口价格单位、SH 跨桶事件及新增量金额；未为修测试改变生产口径。

可机读结果摘要：[20261013-replay-perf.json](20261013-replay-perf.json)。

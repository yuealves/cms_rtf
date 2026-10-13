# 华泰双网格与 cutoff latency 对齐验收（2026-10-13）

本次将 journal 实盘/回放入口对齐 ht_idc / insight_yjj_test 源码
`546aba2fa733d4f7f7656af0572115287418b12d`。完整数值、命令与源码 SHA256
见 [JSON 回执](20261013-huatai-alignment.json)。华泰机器仅用于只读检查，未运行业务。

## 行为变更

| 项目 | 当前 journal 行为（与华泰对齐） |
|---|---|
| 输出 | 独立 local/exch Arrow，106 列顺序不变，NaN/Inf→null |
| local | 纳秒级接收时间右闭 500ms 桶，当前簿、独立开盘前值/流量/滚动环 |
| exch | 每通道最大时间、跨通道最小水位；严格 `watermark > endpoint` |
| 普通关窗 | 在第一条 `frame.nano > cutoff` 的帧分发前逐个执行；先 local 再 exch |
| 会话宽限 | 只有 11:31、15:01 的显式定时点可绕过水位，不是每窗统一超时 |
| 尾部 | 去掉 CMS 原有额外收盘等待；15:00 后有帧则继续 500ms 水位检查 |
| idle | 同交易日且已读空时按华泰边界核销普通/定时 cutoff，不制造尾部轮询 |
| EOF/stop | FireRemaining 补 local 普通网格，再做真实末帧 barrier；不制造未来 exch 超时 |
| latency | 每个 cutoff 一条：接收 cutoff 时间 + 关窗到两路 Arrow Flush 完成的毫秒数 |

日志起点在 worker 屏障前，终点紧接 local→exch 两路 Flush 返回；不包含等水位，
没有 fsync。普通 cutoff 与 15:01 定时 cutoff 重合时，两次都记日志；没有 exch
新行也照样记录。删去旧的每帧计时、逐窗口计时槽及 CSV/meta 日志生成。
普通交易时段不额外执行固定“最长等若干毫秒”规则。

与华泰一致，盘中正常 stop 的 local 会补至全天网格；停止之后的行是最后状态
的延续，不是实际观测到的全天行情。实盘文件隔离到新 run、旧输出拒绝覆盖等
部署保护继续保留。parquet/DTYPE 离线接口仍为原 exch-only 对照路径。

## 正确性

- 6 项 CTest 全通过：包括严格端点、1ns 越界、idle 边界、无午休普通 cutoff、
  尾部/会话定时并列、EOF 不产生超时，以及两路独立开盘收益/流量/age。
- Python 57 项：56 通过，1 项服务 pidfd 用例因本机 Python 缺少
  `os.pidfd_open` 跳过。涵盖 growing journal、迟到不可回改、新股票继承发布位置、
  水位在 15:01 前释放尾窗、两路输出可读后才记日志、日志开关不改因子。
- 在本地直接编译华泰未修改的 `state.cpp/state.h` 和指定提交的 codec/time 头，
  64,000 条沪深事件覆盖两时钟错位、会话边界、逆序及已发布窗抑制，882 个
  非空桶基础量值全部相等。这是流量状态机对拍，不宣称完成整套华泰引擎全量对拍。
- 10/09 的 9:35 前 40,271,350 条原始行情、17,892,045 条成分股行情：最终版本
  1/8 worker 的两路键、空值掩码及全部 106 列逐位一致。local 含 EOF 补窗共
  115,200 行；exch 为 599 端点、2,396 行，亦与此前验收的离线 exch 前缀逐位一致。
- C++ 5×回放 + 8 worker follower，与全速 replay 两路 106 列逐位一致。

数据仍为此前从 gy_dev 下载的 10/09 DTYPE 前缀。为测试 journal 入口，用临时
C++ 工具重建 EFH/CMS payload，保留 DATA 行情字段、接收 nano 和原 packet 分组；
原控制包不在 DTYPE 内，因此这是重建测试输入，不是声称逐字节复制了原始 journal。
限速测试只调整新建夹具的 END 触发时间至 09:35 之后，确保有界 replayer 在 END
之前停止；所有 DATA 字节不变，replayer 不伪造 END。跟读追平后正常停止。
临时夹具、上游源码与一次性驱动均在忽略的 `work/huatai-align-20261013/`，不加入
生产代码或另一套 replay 实现；源码原始数据未修改、未删除。

## 本地性能

测试主机为 linwei-pc / WSL2；这里只覆盖 09:30–09:35，不能外推全天内存或
招商机实盘延迟。两个进程分开，以下 RSS 是 **cms_index500ms_mt 自身**的内核
峰值（含自身 journal mmap，不含 C++ replayer）。

统计 599 个实际到期的普通 cutoff：09:30:00.500–09:34:59.500。5×统计在 stop 前
保存日志，排除 EOF 合成窗口和最终不在 500ms 网格上的真实末帧 barrier。

| 8 worker | 平均 ms | P50 ms | P95 ms | P99 ms | 最大 ms | 内核峰值 GiB |
|---|---:|---:|---:|---:|---:|---:|
| 全速回放 | 1.971 | 1.827 | 2.536 | 6.434 | 7.662 | 0.894 |
| C++ 5×回放 | 2.763 | 2.548 | 3.210 | 6.859 | 79.318 | 0.892 |

5×实际倍速 4.999993×，生产者投放 lag P99 3.506ms、最大 15.210ms。
消费者有 2/599 次 cutoff 超过 10ms，最大值发生于 09:31:38；没有删除长尾样本，
本次未对这一次耗时做因果归因。它们都是同一个“关窗触发→双路 Flush”的统计，
不能与旧 last_read_to_write_ms 混比，也不用于宣称比华泰不同硬件上的实盘更快。

全速进程共 28.941s，包含 EOF 补完剩余 local 普通网格及其 Arrow 写出，不能与
旧 exch-only 前缀约数秒的内核耗时直接横比。新增 local 的 SH 全天身份状态独立，
身份计数现在包含两个时钟的状态项；盘口仍共享，不保存全天逐股 local 结果，
不写逐股中间文件。本轮没有重测双路全天峰值。

## 复核命令

```bash
cmake --build build -j 6
ctest --test-dir build --output-on-failure
PYTHONPATH=python python3 -m unittest discover -s tests/python -v
python3 scripts/summarize_latency.py <run目录>/latency.log
```

复现实盘路径使用 `--journal ... --journal-mode replay --output-mode stream`；
限速生产者仍为现有 `cms_journal_replayer --speed 5`。完整测试命令保存在 JSON，
需给每次运行使用新输出目录。Git bundle 部署流程不变，本次任务仅提交与推送 main。

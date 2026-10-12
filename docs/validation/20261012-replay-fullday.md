# 20261009 全天 parquet → journal → parquet 因子一致性验证

验证开始于 2026-10-12。状态：运行中，以下命令和输入已核实；最终结果待填。

## 范围与输入

本机确定性 UDP 回放完整 CMSREP1 缓存，经 `cms_gateway` 写新 journal，再归档为
cms-parquet-v2，交给同一个 cms_rtf 内核计算四指数、28,800 个 500ms 端点。
全部操作在本机进行。初始代码为 cms_market_gateway `3b082c5`、cms_rtf `9718ee5`；
两仓库开始时工作树干净。

| 输入 | 路径 |
|---|---|
| 源 v2 parquet | `/data/tickl2_data_cms_v2/20261009` |
| CMSREP1 缓存 | `/data/cms_replay_test/20261009.replay`（43,102,369,988 bytes） |
| 回放 journal | `/data/cms_replay_test/work/journal` |
| 回放 parquet | `/data/cms_replay_test/parquet_replayed/20261009` |
| 直读因子 | `/tmp/cms_rtf_v2out/index_500ms/20261009/index_500ms_exch.arrow` |
| HT 参考 | `/tmp/cms_rtf_ref/index_500ms_exch.arrow` |
| 权重 / 阈值 | `/tmp/cms_rtf_ref/weights`、`/tmp/cms_rtf_ref/thresholds` |

接手时既有回放进程仍存活、journal 持续增长，因而继续等待该次回放，没有删除或
重建 journal。缓存全量校验在启动网关之前完成，回放通过 loopback UDP 逐包
READY/ACK 发送，END/EOF 后由同一驱动自动重归档。

`/tmp` 所在分区当时仅剩约 34GB；中间产物放到 `/data/cms_replay_test/factor_out`，
`/tmp/cms_rtf_replay_out` 是指向它的软链。`parquet_replayed_root` 是指向
`parquet_replayed` 的软链，因此 data-root 下直接包含 `20261009`。

## 验收口径

1. 源与回放 parquet 的实际分片集合、文件大小、页脚行数与各自 manifest 一致；
   schema 均为 v2，总行数均为 535,706,255，分市场/业务类型计数相等。
2. 回放 journal 的最后记录为 END，归档器全量验证帧哈希与记录序列后原子发布；
   END 无 incomplete 标记、queue_drops 为 0，`input_incomplete=false`。
3. 两份 CMS 因子的 schema、列目录顺序与键序列相等；115,200 个键无重复；
   106 列的 null 掩码逐行相等，所有有效 Float32 值用 uint32 位模式比较。
   Arrow null 下的未定义存储字节不比较，正负零和有效 NaN 的不同位模式会失败。
4. HT 参考必须具有完全相同的键网格。对它分别重算两份 CMS 的 Pearson 相关，
   要求 `corr > 0.99` 的列集合完全相同且均为 104 列；HT 值沿用 `compare.py`
   的 Float32 转换口径。

END 和行数守恒说明录得的数据已完整走过本次链路，不证明原采集覆盖了交易所的
所有事件；本次因子验收覆盖四指数成分股及其 106 列，没有宣称重建了原始 UDP 字节。

## 重跑命令

输出目录必须使用新目录；现有回放完整时直接复用其 parquet。
长任务输出都重定向到日志；本次由已启动的回放驱动完成第一步。

```bash
cd /data/users/linwei/Code/zs_l2/cms_market_gateway
PYTHONPATH=tools:deploy:tests python3 tests/replay_fullday.py \
  --cache /data/cms_replay_test/20261009.replay \
  --work /data/cms_replay_test/work \
  --archive-out /data/cms_replay_test/parquet_replayed/20261009 \
  > /data/cms_replay_test/validation/replay.log 2>&1

cd /data/users/linwei/Code/zs_l2/cms_rtf
PYTHONPATH=python python3 -m cms_rtf.run_day --day 20261009 \
  --data-root /data/cms_replay_test/parquet_replayed_root \
  --weights-dir /tmp/cms_rtf_ref/weights \
  --threshold-dir /tmp/cms_rtf_ref/thresholds \
  --out /tmp/cms_rtf_replay_out \
  > /data/cms_replay_test/validation/run_day.log 2>&1

PYTHONPATH=python python3 -m cms_rtf.validate_replay \
  --replay /tmp/cms_rtf_replay_out/index_500ms/20261009/index_500ms_exch.arrow \
  --direct /tmp/cms_rtf_v2out/index_500ms/20261009/index_500ms_exch.arrow \
  --reference /tmp/cms_rtf_ref/index_500ms_exch.arrow \
  --source-parquet /data/tickl2_data_cms_v2/20261009 \
  --replay-parquet /data/cms_replay_test/parquet_replayed/20261009 \
  --out /data/cms_replay_test/validation \
  > /data/cms_replay_test/validation/three_way.log 2>&1
```

## 结果

待回放、归档及因子计算完成后填写；当前不作最终通过结论。

## 本地证据

`/data/cms_replay_test/validation/` 保存接续任务日志 `continuation.log`、因子计算日志
`run_day.log`、严格对拍结果 `validation.json`、两份 `*_vs_reference.json`、
权重/阈值文件 SHA-256 `reference-inputs.json` 及测试日志。尚未完成的阶段不会提前
产生成功报告。网关日志在 `/data/cms_replay_test/work/gateway.log`。

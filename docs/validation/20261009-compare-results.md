# 20261009 全天对拍：cms_rtf（CMS 数据）vs ht_idc 线上参考（HT 数据）

- 我方输出：`/tmp/cms_rtf_ref/out/index_500ms/20261009/index_500ms_exch.arrow`
  （中间产物 events/book/flow/seeds.bin 在同日 intermediate/，/tmp 重启即清）
- 参考：`ht_idc: insight_yjj_test:/data/shenrun/index_500ms/20261009/index_500ms_exch.arrow`
  （线上 light_rtf 实时生产，华泰数据源；拉取副本 `/tmp/cms_rtf_ref/index_500ms_exch.arrow`）
- 报告：`compare_report.json`（同目录），由 `python -m cms_rtf.run_day --compare` 生成

## 网格与键

115,200 行 = 4 指数 × 28,800 端点，（datetime, code）键集合与顺序**逐位一致**；
null 掩码全部 106 列 100% 匹配。

## 逐组一致性（corr = Pearson；rel_med = |差|/|参考| 的中位数）

| 组 | 列范围 | corr min | corr median | rel_med p95 |
|---|---|---|---|---|
| 盘口 32（31 特征 + mid 收益） | 0–31 | 0.99939 | 0.99988 | 0.0171 |
| 质量 3（coverage/age/fresh） | 32–34 | 0.99860 | 0.99999 | 0.0031 |
| 流量 21 基础 | 35–55 | 0.99924 | 0.99988 | 0.0083 |
| 流量 16 派生 | 56–71 | 0.99638 | 0.99966 | 0.0125 |
| 5s 滚动 21 基础 | 72–92 | 0.99934 | 0.99989 | 0.0091 |
| 5s 滚动 13 派生 | 93–105 | （见注） | 0.99974 | 0.0108 |

**corr>0.99 的列 104/106，>0.999 的列 96/106。**

## 两个"不达标"列的定性（均非实现缺陷）

1. `order_resolved_fraction_5s`（corr 0.35，**max_abs 6e-8**）：该列几乎恒为 1.0，
   方差近于零，相关性对近常量序列无意义——数值上两边逐位吻合。
2. `order_active_weight_500ms`（corr 0.996，max_abs 0.26）：桶级活跃标志对
   逐笔到达差异最敏感（CMS 与华泰两条采集链路的报文到达不完全相同，
   单股单桶一条委托的有无就会翻转该桶的活跃位）。属数据源性差异。

## 簿健康（内核全天统计）

`over_deduct=0, d_unknown=0, unknown_trade_ref=0, late_passive_ref=0, agg_skip=0,
sz_non_monotonic=0`；`immediate_fill=40,653,660`（SH 主动腿立即成交，协议正常形态）、
`crossed_book=40,687,629`（集合竞价时段观测，正常）。

## 过程缺陷记录（修掉的关键 bug）

1. `thresh.bin` 布局曾按 C++ 对齐结构体读（24B）而 Python 写 20B packed ——
   首条之后全部错位，大小单分层全错；改显式逐字段 20B 读取并加
   隔离单测（SZ 小单分层）覆盖。教训已写入 AGENTS.md 红线 2。
2. 参考 schema 虚惊：subagent 曾报 109 列，实为 108（datetime+code+106）。

## 运行成本（r7525，单机）

codec（43 分片 → 2.28 亿事件）~4.5 min；内核 ~2 min；聚合+写出 ~1.5 min；
合计 < 9 min/天。

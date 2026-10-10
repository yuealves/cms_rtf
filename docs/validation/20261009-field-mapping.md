# CMS parquet → canonical 事件映射的实证（2026-10-09 数据）

逐条结论都来自对 `/data/tickl2_data_cms/20261009/` 实测，非协议文档推断。

## 类型分布（order 首分片）

| (exchange, action, message_type) | 行数 | 含义 |
|---|---|---|
| SH A, 62 | 4,854,387 | 沪逐笔委托 |
| SH D, 62 | 983,850 | 沪撤单 |
| SZ A, 23 | 7,164,889 | 深逐笔委托 |
| SZ D, 24 | 1,361,255 | 深撤单（走委托流） |

trade 文件只含真实成交（SH T / SZ F）。

## side / id 编码

- **SH A/D**：`side_raw` 0 → `buy_ref` 置位（买），1 → `sell_ref` 置位（卖），100%。
  本单 id = 该侧引用。D 引用与 A id 按侧 100% 连通。
- **SH T**：`side_raw` ∈ {0,1,2}；09:25 集合竞价段**全部**为 2 ⇒ 2=N（无主动方），
  0=B（买主动）、1=S（卖主动）。双腿引用恒非零。
- **SZ A**：`side_raw` 49('1')→buy_ref 置 id（买）、50('2')→sell_ref（卖），100%；
  `sz_order_type_raw` ∈ {49('1' 市价对手方), 50('2' 限价), 85('U' 市价本方)}。
- **SZ D**：无 side；buy_ref/sell_ref 恰其一非零 ⇒ 方向=非零位置；
  引用与 A id 按侧 100% 连通。
- **SZ T**：`side_raw` 恒 0；`buy_ref`/`sell_ref` = BidApplSeqNum/OfferApplSeqNum。
  买主动 ⇔ buy_ref > sell_ref（light_rtf 既有启发式）。

## SH A 的数量口径 = 残余 Balance

每个 orderid 全天**至多一条** A（首分片 2,906 单无一重复）。对已带
`sh_already_traded_quantity>0` 的 A 逐单核对：A 之后该单的成交量合计恰等于
`quantity`（`already` 恰好等于同报文/之前成交的量）。即
`quantity = 原始申报 − 已成交 = 残余 Balance`——与 light_rtf 簿内核的 SH 口径一致，
直接挂单，无需加工。

## business_seq 单调性

`business_seq` 在 channel 内对委托/成交均严格不减（首分片 18 个 SZ channel 全验证）；
SZ 前序补价的"严格前序 = biz_index 更小"成立。批处理按 journal 序处理事件，
"前序成交 arrival ≤ 委托 arrival"的实时约束天然满足。

## 一个实证陷阱（已在 codec 规避）

order 与 trade 分片**独立按 256MiB 切分**，覆盖的时间段不同
（如 order 第 1 片到 09:31:23，trade 第 1 片到 09:34:49）。
任何"分片内闭环"的验证/处理都会假阳性丢单——必须全天合并后再按
(symbol, record_seq, event_index) 排序。

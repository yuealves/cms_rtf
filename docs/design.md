# cms_rtf 设计：index_500ms 的 CMS 批处理移植

> 语义基准：light_rtf v2 `docs/design/families/index_500ms.md` +
> `docs/design/note/index-500ms-semantics-mapping.md`（先读那两个）。本文只记录
> CMS 批处理版与它的**差异**，相同口径不复述。

## 为什么是这个形态

light_rtf 是实时引擎（Kungfu journal 驱动、worker 并行、双网格、水位关窗）。
cms_rtf 的目标是**用已归档的 CMS parquet 离线重现同一天四指数的 106 列**，
验收 = 与 ht_idc 线上 `index_500ms_exch.arrow` 的交易所网格"基本一致"。
因此：

1. **只实现交易所网格**。local 网格 = 接收时钟 500ms 桶，两台机器/两条线路的
   接收时序本来就不同，对拍无意义（用户拍板不做）。
2. **批量 ⇒ 无水位**。全量历史下通道水位必越过全部端点，28,800 端点全发布；
   午休/收盘兜底、关窗倒计时等实时机制全部不需要。
3. **每股独立 ⇒ 按 symbol 分组处理**。引擎里跨 worker 收集器/共享锁在批处理下
   退化为"每股顺序处理 + 聚合层固定顺序汇总"。浮点确定性：聚合按 symbol 升序。

## 事件源差异（CMS parquet vs HT KyStd）

light_rtf 消费华泰 KyStd journal；cms_rtf 消费 cms_market_gateway 归档的
`cms-parquet-v1`（order/trade 分片）。字段映射经实盘验证（证据见
`docs/validation/20261009-field-mapping.md`）：

| canonical | CMS parquet |
|---|---|
| symbol | `symbol`（6 位字符串 → int32） |
| time（HHMMSSmmm → ms） | `time_hhmmssmmm`（SH 源精度 10ms、SZ 1ms） |
| channel / biz_index | `channel` / `business_seq` |
| price（tick ×10000） | `price_e4` |
| volume | `quantity`（SH A = 残余 Balance，已实证） |
| SH order side | `side_raw`：0=买、1=卖 |
| SH order id | side 侧引用（买=buy_ref / 卖=sell_ref） |
| SH trade side | `side_raw`：0=B、1=S、2=N（集合竞价） |
| SZ order kind | `sz_order_type_raw`：49='1'、50='2'、85='U' |
| SZ order side/id | `side_raw`：49('1')=买、50('2')=卖；id 按侧取引用 |
| SZ 撤单 | order 文件的 action D（message_type 24）→ trade_type='4'，方向=非零引用位置 |
| SZ/SH 成交引用 | `buy_ref`/`sell_ref` |

## 流水线

```
parquet 分片 --codec.py--> events.bin (56B/条, 按 symbol 分组, 组内 journal 序)
                          + thresh.bin（dyn_xl/m 每股）
events.bin --cms_index500ms_day--> book.bin  (端点捕获：31 特征 + mid + act + valid)
                                 + flow.bin  (非空桶 21 基础量)
                                 + seeds.bin (开盘收益前值 mid)
book/flow/seeds --aggregate.py--> index_500ms_exch.arrow (115,200 行 × 108 列)
```

内核移植自 light_rtf，逐字保留三处关键语义：
- `src/book.cpp` ⇐ `l2_book.cpp`（SH Balance/乱序跳过/立即成交、SZ 市价迁移/U 定价）
- `include/cms_rtf/flow.h` ⇐ `state.cpp` 交易所半边（SH 全天身份跟踪 + 桶末已知价结算；
  SZ 前序补价 ×1.001/0.999；批处理按 journal 序处理 ⇒ arrival 约束天然成立）
- `include/cms_rtf/book_features.h` ⇐ `book_capture.cpp`（31 公式，一字不改）

聚合层（aggregate.py）对应 `aggregate.cpp`：有效权重分母 + 80% 覆盖门、
官方权重分母流量、Float32 量化后入 5s 环（Float64 求和、session 重启、无减法）、
派生列 imb0/ratio0 与 active_weight。

## 已知的批处理近似（与实时引擎的差异边界）

1. 引擎按全局水位关闭端点，迟到于水位的事件被丢弃；批处理每股保持桶到该股
   流末尾（等价于水位无限等待）。差异只影响"迟到超过水位等待"的极端事件
   （主要是 15:01 兜底之后到达的 SH 收盘段委托）。
2. SH 集合竞价成交（side N）不进 trade 族、不进 ShNote，但进簿（双腿按被动扣量）。
3. CMS 导出只含 A/D/T 三类可动作事件 ⇒ `act_ms` 恒等于最后事件时间；
   心跳/状态行本来就被 archiver 排除，无需处理。

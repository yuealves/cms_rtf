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

## Replay 性能路径（2026-10-13）

`run_journal_mt` 默认把现有网关 mmap exporter 的 stdout 直接接入内核 stdin，
不再完整落盘 128B DTYPE 流。`--keep-stream` 保留分阶段文件模式；
`--export-threads N` 接入网关 `0abaa13` 的页并行导出，默认 1。
当前网关并行实现会缓存全天全部 DTYPE 行（约 64 GiB），因此线程数显式选取。
生产者和消费者必须都成功才允许聚合，截断记录、错误 magic 和输出写入失败拒绝成功。

内核成分股过滤提前到映射前；每 worker 用有界 256 条批次发布/消费 SPSC ring，
仍保持每股到达序，EOF acquire 后再 drain 一次，避免最后一次入队与空队列检查竞态。

盘口特征延迟到端点穿越/EOF 时计算，桶内被后续事件覆盖的中间快照无需计算。
第一次处理 15:00 后事件前冻结最后网格内快照；后续乱序回到网格内仍按旧逻辑更新。
开盘前值继续逐事件保留最后 midpoint（含无效簿 NaN），不受端点延迟计算影响。
等价对象为 `1e22619` 的 eager runner；上游对应
`light_rtf/src/factors/index_500ms/worker.cpp:161–186` 的 CaptureExchange。
31 个公式、流量状态机、网格和量化点均保持原实现。

聚合按 symbol 升序遍历各 worker 文件的 mmap 视图，免去全量 concatenate 拷贝。
每股流量端点严格递增时用向量索引相加；重复端点仍走 `np.add.at`，
股票加权顺序和每次 Float64 运算不变。

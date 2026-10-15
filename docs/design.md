# cms_rtf 设计：index_500ms 的 CMS 移植

## 当前 journal 实盘/回放契约（2026-10-13 双网格对齐）

本节取代下方历史章节中“只实现 exch”“保留 CMS 收盘批次等待”和旧 latency
定义。语义基准是 ht_idc / insight_yjj_test 实际源码 `546aba2fa733d4f7f7656af0572115287418b12d`：
`engine.cpp:91–224,280–349`、`index_500ms/family.cpp:8–46`、
`worker.cpp:104–184,235–353,374–497`、`state.cpp` 和 `arrow_stream_sink.cpp:96–175`。

`--journal`（follow/replay）现在始终输出 `index_500ms_local.arrow` 与
`index_500ms_exch.arrow`。`--out` 指定 exch；`--local-out` 可指定 local，默认
为同目录 `index_500ms_local.arrow`（自定义 exch 文件名则用 `<stem>_local.arrow`）。
二者都是最终 Arrow IPC stream，datetime[us 北京墙钟]、code 和原顺序的 106 个
Float32；按华泰把 NaN/Inf 写成 null。没有逐股中间文件。原 `--input` DTYPE 和
parquet 离线接口继续保留 exch-only 对照契约，不冒充双网格实盘入口。

### 两套时钟和状态

- local 使用 frame 的有效接收时间（extra_nano 优先），以纳秒精度划分右闭桶；
  `t+1ns` 属于下一桶，不能先截断成毫秒。exch 使用行情的交易所时间。
- 二者网格均为 `(09:30,11:30]`、`(13:00,15:00]`，步长 500ms。
  每股只有一份盘口；local 在接收截止点读当前簿，exch 用交易所端点捕获。
- 流量桶、SH 全天身份表、开盘 midpoint 前值、5s 滚动环分别独立。沪市某条
  委托可在一个时钟会话内而在另一个会话外，不能共用计数/已知量状态。
  沪市身份表仍用紧凑分页哈希；SZ 前序价检查保留业务序、交易所时间及到达时间
  三个条件。local 使用单个最近已发布流量槽，没有历史 local 记录数组。
- local 盘口 age 保留纳秒精度，2026-10-14 起采用下节说明的扣午休口径。
  固定 symbol 升序聚合、Float32 量化点与其余 105 列公式不变。
  新出现的股票继承已经发布的两路位置，等价于上游预创建成分股。
  `EventRec` 56B 等既有磁盘 ABI 不变；内部队列额外携带 int64 接收时间。

### local age v2：扣除午休（2026-10-14，用户授权）

与华泰 `worker.cpp:411–447` 的墙钟接收 age 明确不同：仅 local 的
`snapshot_age_seconds` 改为交易时段内的接收静默时长，exch 全部因子保持原口径。
每只股票先计算 `T - r - overlap([r,T], [11:30,13:00])`，其中 T 为 local
窗口端点、r 为最后可动作行情的有效接收时间；使用整数纳秒计算重叠再转秒。
然后沿用原有效簿筛选、权重归一化与 80% 覆盖门。上午到下午的区间扣 5400 秒；
若最后事件在午休内，只扣尚余的午休。没有午后清零，也不丢弃午休事件。
例如 11:29:59 到 13:00:06 从 5407 秒改为 7 秒；12:15 到 13:00:06 为 6 秒。
缓存未更新的盘口也在每个窗口重新计算 age。

`snapshot_fresh_weight_500ms` 单独按未经扣减的接收静默时长 `< 500ms`
判断，其他 105 列、两路关窗/latency、目录和 106 列顺序不变。新 local Arrow
schema metadata 标记 `cms_rtf.local_age_semantics=trading_time_excluding_lunch_v2`；
无该标记的历史文件视作 `wall_time_v1`。精确对拍 `exact_factors` 显式拒绝
不同 age 版本混用，消费者跨日读取/对拍时应检查该语义版本，不能将历史 age
视作新口径。exch 文件不新增该 local 标记。华泰线上本次未修改。

### 调度和关窗

1. 接收时钟 cutoff 就是上述 28,800 个交易时段端点。第一条时间**严格大于**
   cutoff 的帧到来时，先逐个处理所有到期 cutoff，再分发行情；恰好等于端点的
   行情仍在当前桶。不因一次 read 含多个窗口而合并或跳过 cutoff。
2. 每个普通 cutoff 先发布对应 local 窗，再检查 exch。每个已观察
   `(exchange, channel)` 取最大交易所时间，跨通道取最小水位，水位**严格大于**
   exch 端点才可发布。一个 cutoff 可发布零个或多个 exch 窗。
3. 只有 **11:31、15:01** 两个显式定时 cutoff 可绕过水位，补齐对应会话。
   60 秒是相对于 11:30/15:00 的会话结束宽限，不是每个窗口都有 60 秒超时。
   删除 CMS 原有的收盘批次额外保留规则；水位已满足时按华泰立即允许发布。
4. 午休不安排普通轮询。15:00 后如仍有帧，接收时钟每 500ms 继续检查 exch，
   不再产 local 行。普通尾部轮询与显式定时点重合时先普通、后定时，两次各记日志。
5. follow+wall 在读空、追平已提交帧后允许空闲时钟核销普通 cutoff 和会话定时点。
   普通 cutoff 需 `< wall` 且 `>= 最大已读 frame 时间`；定时点需 `<= wall`；
   15:00 后的尾部轮询仍只能由帧驱动。只接受与交易日一致的墙钟。
6. 已发布 exch 窗不可改写；迟到事件仍更新盘口/身份和前序价，抑制已发布桶流量。
   午休/收盘后的事件更新 SH 身份前先封该股的交易所会话桶，与上游顺序一致。
7. EOF / 正常 stop 按华泰执行：检查最后真实帧的精确时刻；补完剩余普通 local
   cutoff（FireRemaining）；最后用真实末帧时间再做一次 barrier。合成的普通
   cutoff **不是**会话超时，不能强制补未来 exch 窗。盘中停止可能因此生成全天
   local 文件，后半段为停止状态的延续，不能当作实际接收的完整交易日。
   journal replay 仍要求输入有正常 END；读取/解码失败不补齐成功输出。

### latency.log

完全采用华泰主日志的统计边界和采样单位：每次引擎 cutoff 一条，第一列是
**接收时钟 cutoff** 的 `HH:MM:SS.mmm`，第二列为毫秒（默认 6 位有效数字）。
用 steady_clock 在发送 worker 关窗/屏障前开始计时，完成计算及聚合后先 Flush
local、再 Flush exch，第二次 Flush 返回后立即结束计时，然后写并 flush 日志。
包含排队、计算、聚合和 Arrow 写出；不包含等待下一帧/水位的时间，不测“末条行情
接收→写出”，也没有 fsync。无输出行的 cutoff 也记一条；不按 exch 窗数重复计数。
一个 cutoff 的全部 Arrow 行一起 flush，不沿用离线的 1024 端点自动拆批策略。

删除原有逐窗口 read/recv 时间槽、每帧计时、latency.csv/meta.json 的生成。
`scripts/summarize_latency.py <目录或 latency.log>` 默认只统计交易时段；旧 CSV
只保留显式读取能力以复核历史报告。历史回放和线上必须各自统计，日志本身不再
附带 phase 标签。原生入口拒绝覆盖旧输出；服务重启时先保留上一份输出，
再在固定日期目录重新生成，运行状态和参考数据回执单独保存。

### 每日输出目录（2026-10-15）

服务配置 `output_root=/data/cms/rtf/data`，两路 Arrow 和 `latency.log` 直接写入
`/data/cms/rtf/data/index_500ms/YYYYMMDD/`，日期目录内没有 runs/current。
每次运行的参考二进制、回执、健康状态和内核日志仍隔离在
`/data/cms/rtf/run/history/YYYYMMDD/<时间-PID>/`，status 返回 `output_dir` 和
`attempt` 两个位置。当天重启先将既有三份输出移至
`/data/cms/rtf/data/index_500ms/.restarts/YYYYMMDD/<新运行时间-PID>/`，然后从当日
journal 首帧重建。追赶期间日期目录是新进程逐步生成的前缀，消费者需重新打开
文件并结合 `caught_up` 判断是否追平。旧布局的运行目录归档到
`/data/cms/rtf/run/legacy-layout/`；日期目录只保留直接可读的最终产物。

### 内存与验收

保留单份盘口、紧凑 SH 身份表、local 单槽和有预算的 exch 待聚合记录。新增 local
身份状态是上游双网格语义需要，不能以共用身份状态省内存而改变流量。没有把历史
数据转移到逐股中间文件。pending 256 MiB 约束的是待聚合 payload，不是整个进程。
验收、实际前缀性能和统计范围见 [双网格验证](validation/20261013-huatai-alignment.md)。

## 历史实现记录

以下记录保留各阶段离线性能验证的背景；涉及当前 journal 的输出、关窗、EOF 和
latency 时，以以上双网格契约为准。

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
book/flow/seeds --cms_index500ms_aggregate--> index_500ms_exch.arrow (115,200 行 × 108 列)
                 aggregate.py 保留为 Python 对照后端
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

## C++ 聚合和 Arrow 输出（2026-10-13）

`app/aggregate.cpp` 对照 gy_dev 上 light_rtf_v2 `546aba2` 的
`light_rtf/src/factors/index_500ms/aggregate.cpp:25–172` 与
`light_rtf/src/output/arrow_stream_sink.cpp` 移植。四指数、80% 覆盖门、31+1 盘口列、
21 基础量、16/13 派生列、10 桶 Float32→Float64 滚动、午休重启均沿用现有口径。

读取逐笔内核已经按 symbol 升序写出的 worker 文件，以固定缓冲合并股票分组，
每次只持有一只股票的记录和四指数全天累加器。避免全日 mmap 页长期计入进程 RSS。
同一 symbol 出现在多个文件时稳定保留输入文件顺序；流量重复/逆序端点逐条累加；
截断记录、非法参考版本、非升序 symbol/盘口端点拒绝运行。既有 BookRec280、
FlowRec176、Seed12 布局完全不变。输出列名由 catalog.py 在构建时生成，避免两份目录漂移。

Python 仅准备权重参考和启动 C++；新侧输入 `CMSAG01\0` 为 magic[8]、小端 u32
股票数、4 个 Float64 官方分母、逐股 int32 symbol + 4 个 Float64 权重。
官方分母直接保留现有 NumPy 求和值，不在 C++ 重排求和；禁止融合乘加。
5s 求和按已有 Python 窗口的时间顺序进行，不对滚动和做减法。
mid 使用 `log(mid)-log(prev)`，无效前值不回退。写出保持现有 CMS 的 NaN→null，
Inf 仍是有效值（华泰 sink 对 Inf 也置 null；本轮不改变既有 CMS 输出契约）。

Arrow 采用 1,024 端点/批的 IPC stream，列类型、行序、无时区北京墙钟时间不变；
批边界不属于数据契约。写入同目录临时文件，正常关闭后替换目标，失败保留旧输出。
逐笔内核仍然缓存全天逐股端点结果到 EOF；此次原生聚合没有改成实时水位关窗。
所以聚合阶段内存下降不代表全天逐笔内核的约 22.9 GiB 峰值已消除。

## 内核内存优化（2026-10-13，后续版本）

用户计划的实盘链路是 CMS gateway → Kungfu journal → cms_rtf。上节的
22.9 GiB 是单独 `cms_index500ms_mt` 读已导出的 DTYPE 文件时的进程 RSS，
不含 replay exporter；但旧内核把全天逐股 BookRec/FlowRec 保留在 vector 中，
这属于离线结果留存，不能当成实盘状态的必要内存。本节优化共享计算状态与
结果留存；当前入口仍在 EOF 后聚合 Arrow，不引入 journal 实时订阅、水位或实时发布。

多线程内核默认 `--output-mode blocks`，每股盘口/流量分别最多缓存 512 条，
满块立即交给每 worker 的 1 MiB 文件缓冲，EOF 只补尾块与索引。
`SymbolRunner` 通过 RecordSink 输出，内核不再持有全天结果。
无事件的连续网格保留一个盘口捕获，聚合的 last-record carry-forward 恢复每个端点；
act_ms、mid 和特征原样保留，年龄、收益链、午休与 106 列口径不变。
小块索引仍随记录数增长；它不属于严格常量内存，不声称整个引擎内存有固定上界。
单线程按股分组入口在一股完成后落盘并复用缓冲，不再累积所有股票输出。

新中间容器是 **CMSBLK01**（`.book.cmsb` / `.flow.cmsb`）：

- 16B header：magic[8]、u32 record_size、u32 index_entry_size (=16)。
- payload：原 BookRec280 / FlowRec176 字节布局，每块同一 symbol、1–512 条。
- EOF 索引：逐块 int32 symbol、u32 count、u64 offset；保留写出顺序。
- 32B footer：CMSIDX01[8]、u64 index_offset、u64 index_count、u64 record_count。

字段沿用现有 x86 小端布局。索引按 symbol 稳定合并，每次读取一股，保持求和顺序。
读取器检查版本、长度、块范围、记录 symbol 与端点顺序；无完整 footer 的文件拒绝聚合。
旧原始 `.bin` ABI 和 Seed12 不变；`--output-mode legacy` 保留稠密旧输出与 Python 对照。
新文件必须通过显式 `--book-blocks` / `--flow-blocks` 参数读取，旧 C++ reader 拒绝参数，
Python driver 拒绝块格式和混合新旧输出目录，避免默默误读。

SH 全天身份表保留全部成交/委托的身份信息，未删除“已成交”或“久未出现”的身份。
将每条单独分配的 unordered_map 节点改为 256 条一页的稳定记录和开放寻址 uint32 索引，
减少指针、分配器和散列节点开销。桶内迭代与浮点累加顺序没有改变。
活跃委托簿仍保留原语义；这一部分以及 SH 全天身份数都会随当日行情变化。

输入循环用 read(2) 接收管道当前可读字节并保留不完整记录，避免 fread 等待填满
8 MiB 或 EOF。该改动不等同于实现生产水位与低延迟发布。

`scripts/benchmark_memory.py` 独立计时 kernel / C++ aggregate，以 `/usr/bin/time -v`
测各进程峰值，另采样 `/proc/PID/status` 的 RssAnon/RssFile。运行时无 exporter。
JSON 中 output/identity capacity 是各股 EOF 前容量求和，不是 RSS 峰值时刻堆剖析；
allocator、ring、活跃簿等仍包含在进程 RSS 中。稀疏 BookRec 按网格展开后与旧版逐字节
SHA256 对照，FlowRec/Seed12 直接对照，再检查全部 106 列、null mask、键与健康计数。

## 内存内聚合路径（2026-10-13，替代逐股中间文件）

`run_journal_mt` 的 C++ 默认路径改为 `--output-mode stream`：dispatcher → worker
簿/流量 → 内存中的待聚合记录 → 固定 symbol 顺序聚合 → 单个最终 Arrow stream。
`blocks` / `legacy` 仅显式保留为离线对照路径。BookRec/FlowRec/Seed12 和 CMSBLK01
的旧接口未修改；新路径不生成这些文件。CMSAG01 权重及阈值仍是小型参考输入。

参考 Huatai `546aba2` 的 `worker.cpp:286–352`：跨 worker 的同通道时间取最大，
跨通道取最小；水位严格大于端点才关闭。FIFO 控制消息排在此前事件之后，所有
worker 到达屏障后封闭安静股票的流量与捕获；聚合仍逐 symbol 升序，不先做 worker
部分和。消费完的逐股记录立即移出缓存，只保留 carry-forward 捕获及前值。
派生公式与旧聚合器共用 C++ 模块，滚动环仅保留 10 桶 Float32，按时间序以
Float64 重算，午休重启。只缓存有限的最终 Arrow 行，不保留全天逐股结果。

`--window-batch 64` 是加速 replay 的调度批次（最多按已推进的 64 个端点成组触发），
并未合并 500ms 因子；逐端点发布选 `--window-batch 1`。每次读到可用输入后检查
水位，文件大块读取可能一次跨过多个端点。屏障聚合期间 worker 使用条件变量等待。
只跟踪参考成分股实际观察到的 `(exchange, channel)`，与当前 Huatai worker 一致。

与 Huatai 实盘和旧 CMS 离线路径的差异必须显式区分：

- Huatai 对已发布端点不回改，并抑制迟到流量；新 CMS 路径保守地**拒绝**时间戳
  落后于已发布端点的后续事件，报 symbol/event_ms/closed_ms，整个运行失败，
  不静默丢弃或给出成功输出。旧 blocks/legacy 保留无限等待的原有离线语义。
- replay 的 EOF 仍补齐 28,800 端点，以便沿用现有截断样本的全天输出契约；没有
  EOF 时按通道水位发布，不以普通轮询或 replay 加速墙钟充当会话超时。
  本修改没有新增 journal follower 或实盘会话定时器。
- `--pending-mib 256` 限制待聚合记录的有效 payload 总预算，worker 按需共享。
  通道长时间停滞会明确失败，不溢出丢数据，也不转而写磁盘。vector 容量、
  盘口和 SH 全天身份另计；此参数不是全进程 RSS 上限。
- Arrow 写入同目录 `.tmp.PID`，内容就是最终 108 列 Arrow，成功关闭后 rename；
  没有第二套逐股中间文件，也没有逐股数据回读。运行中每轮聚合 flush 可观察
  已完成的 record batch。Python 驱动还等待 exporter 成功，再发布正式文件名，
  防止 producer 失败被误当正常 EOF。失败保留上次正式输出。

本地 20261009 的 09:35 前 40,271,350 条原始记录，16 worker，初轮交替测试：
106 列/空值掩码/键与已接受输出逐位相同，健康计数相同；新侧约 2.51–2.62s，
峰值约 0.587 GiB，逐股中间文件 0 字节。全天指标以独立验证文档为准。
EOF 的压缩 SH 身份表与活跃委托没有删除；最终 Arrow 的 batch 边界可能不同。

全天验证发现，20261009 收盘尾部约 24,976 条成分股事件的时间仍早于 14:59:59.500，
而此前 14 个已观察通道都已经出现 15:00 时间戳。因此 CMS 通道的“最大已见时间”
在会话结束处不能直接充当旧批处理语义的完整性证明（Huatai 对已关窗迟到流量
采用抑制策略，不能照搬后仍声称与原 CMS 全量结果相同）。新路径在水位落入
`[AM close, PM open]` 或 `>= PM close` 时保留最后一次水位跳跃，等待下一会话
或正常 EOF 再封口；仅使用既有会话常量，不靠 replay 的墙钟加速制造超时。
这会暂存收盘段尚未聚合的记录，预算由全部 worker 共享，64 KiB 额度批量领取，
总 payload 上限默认 256 MiB。实际 vector 容量和全进程 RSS 另行实测。
日内真正越过已发布窗口的事件仍明确拒绝；不会为了完成回放而静默丢弃。

性能诊断细分 reader 的 read/map、屏障等待、聚合及 worker 处理/封口耗时。
`sum_worker_peak_pending_payload_bytes` 是各 worker 各自峰值之和，不是假称同一时刻
全局活跃 payload；`output_capacity_at_eof_bytes` 在 stream 模式记录消费后保留的
vector 容量。worker 的预算计数器按 cache line 对齐；空队列先短暂 yield，连续
空转后等待 25 微秒（有事件时恢复批量处理），减少 reader 工作时其他线程持续
调用 sched_yield 的 CPU 开销。真实峰值仍以进程 RSS 实测为准。

## 直接 journal 实盘入口及源码部署（2026-10-13）

用户确认复用招商机现有 gateway/paged，新增 cms_rtf 跟读入口。以上批处理章节
是历史实现边界；当前 `--journal DIR` 路径在同一 C++ 程序内完成读取、解析、
分发、窗口聚合及 Arrow 输出，不启用离线 exporter、不写逐股文件。

reader 每次只映射一页 128 MiB，O_RDONLY/MAP_SHARED，按已提交 status 字节
acquire 后检查 frame v2、长度、hash、cms_l2_raw_v1 版本、日期、seq、epoch；
跨页只向下一页前进，等待未初始化/未提交帧及尚未创建的下一页。
原网关 decoder/时间校验按精确源 hash 放在 third_party/cms_protocol，保持协议
同源且不链接 gateway/paged；ABI 对应 Kungfu a06af6c 的 137B/38B 页帧头。
帧中额外时间优先，控制记录仅推动时钟/质量统计，不进入因子事件 ABI。

实盘与新的 journal replay 采用同一 source-clock 调度，每 500ms 检查通道水位。
只有 follow 读空、确认目前已提交帧读完后，才允许此前采样的墙钟推动显式会话
定时器。Huatai 546aba2 worker.cpp:303–309 的 AM/PM close +60s 规则用于
11:31/15:01 收尾；保留已发现的 CMS 收盘批次等待，避免将 15:00 最大已见时间
误当成全部股票尾部已到。不会按日内普通空闲超时强行发布落后通道的窗口。

Huatai worker.cpp:119–122：已发布端点不可回改。journal 模式下迟到事件计数，
继续更新盘口、SH 身份及 SZ 前序价格，抑制已发布桶的流量；下一未发布端点可使用
更新后的盘口。不把迟到流量挤进后续桶，盘前 seed 在首窗发布后不再修改。
新出现股票继承全局已发布位置，不能重新创建历史捕获。
原 DTYPE 模式保留原拒绝迟到策略与 EOF 补齐契约，106 列、公式及固定求和顺序不改。
因此新实盘路径不承诺与无限等待历史聚合在迟到事件处逐位相同。

follow 直接创建新 Arrow 文件并逐批 flush；失败保留已发布前缀与 failed 状态，
既有文件拒绝覆盖。stop 不补未来窗口；日期 current 在新进程追平且有发布后切换。
重启从当日首帧重建状态，不存在热恢复快照。原离线路径继续原子提交最终文件。
默认 8 workers 与 256 MiB pending payload；账户状态、SH 身份、页映射另计，
全进程内存和实盘延迟仍需目标机实际验证，不将回放结果直接冒充实盘指标。

部署参考 Huatai sync_online_bundle.sh：查询实际线上 commit，bundle 增量、校验
SHA256/前置条件、独立 Git worktree、目标机原生编译及测试后原子切换。
用户级工具链和现有 Arrow 安装独立于系统包管理，不改网关/paged/cron。
实现与操作见 docs/deploy-live.md，所有发布以 deployment.json 与构建日志为凭。

## 窗口延迟日志（2026-10-13）

参考 ht_idc 的 insight_yjj_test 容器 `/opt/insight/light_rtf_v2`，commit `546aba2`：
`engine.cpp:179–205` 在 CloseOne 开始记录 steady_clock，
`arrow_stream_sink.cpp:113–128` 在两路 Arrow Flush 后记录结束，写出的
`latency.log` 第二列为 **毫秒**。它测关窗触发到写出，不是接收行情到写出的总延迟；
该实现未调用 fsync，不能把注释里的 durable 理解为持久化到物理磁盘。

cms_rtf 的 journal 路径默认开启 `--latency auto`，DTYPE 对照路径默认关闭；
可显式 `--latency on|off`。Arrow 同目录新增 latency.log、latency.csv 和
latency.meta.json，不修改因子目录或值。当前 CMS 只计算 exch 流；日志明确标记
clock=exch，不因为记录了本地接收时间而声称新增 local 因子。

- latency.log 两列为交易所窗口端点 HH:MM:SS.mmm、close_to_write_ms；与华泰
  使用相同耗时边界，但华泰第一列是引擎 cutoff，不一定等于此次关闭的 exch 端点。
- close_to_write_ms：发送 worker 关窗/排队屏障前，到相应 Arrow record batch
  WriteRecordBatch 和 Flush 成功返回后，使用 steady_clock，包含排队、封口、聚合和写出。
- last_read_to_write_ms：同一个右闭交易所窗口内，最后读到的有效成分股事件到写出，
  使用 steady_clock。journal 在确认 frame committed 后、hash/解码前每帧采样一次；
  同帧事件共用采样。DTYPE 以每个输入 read 完成时间为边界，回放的排队形态不同。
- last_recv_to_write_ms：写出时 system_clock 减去该窗口有效成分股事件最大的
  CMS gateway recv_ns，涵盖网关后排队、journal、读取、等待水位以及计算输出。
  仅 follow+wall 记录；历史 replay/source 模式留空。墙钟回拨导致的负值原样记录，
  不截断成零。catchup 阶段可包含数小时历史数据年龄，不能当成盘中实时表现。

有效事件指通过参考成分股过滤、处于连续竞价时段且未落后已发布窗口的事件。
已抑制迟到事件不能改写历史窗口的接收时间；空桶接收/read 延迟留空，不伪造为零。
这是当前窗口行情的度量，不枚举 5 秒滚动状态和历史盘口的全部依赖。

每个真实 Arrow Flush 回调记录完成时刻，覆盖自动 1024 端点刷新和显式刷新；
同一批中的端点共享完成时刻，close_id/flush_id 标明分组，不按端点数摊薄耗时。
日志本身的写入在结束采样之后，不包含在当前记录的延迟里；会产生少量真实开销。
按窗口固定保存 28,800 个时间戳/计数槽，约 0.88 MiB，没有逐事件留存或逐笔磁盘 IO。

phase 分 catchup（首次 journal Idle 之前）、tail（已见过 Idle）和 replay。
tail 是运行阶段标识，不能保证进程永不再积压；时间本身如实包含后续积压。
reason 分 watermark、am_timer、pm_timer、eof，收盘延迟不混入正常水位关窗统计。
`scripts/summarize_latency.py` 按 mode/phase/reason 分组，默认排除空窗口和交易时段外
样本，输出 mean/P50/P95/P99/P999/max 与超阈值数；支持正在追加的文件，忽略尾部
未写完整的一行。日志既有路径拒绝覆盖，运行失败时保留已经写出的诊断前缀。

## 限速 journal 验证（2026-10-13）

`cms_journal_replayer` 对照 Huatai `546aba2` 的
`replay/src/journal_replayer.cpp:275–309`：以 steady_clock 为基准，按源时间差 / speed
计算 deadline 并 sleep_until。CMS 保留单路原始帧序及 extra_nano 优先规则，不照搬
Huatai 的三路 KyStd 类型筛选/排序；不依赖或启停 paged，不改生产 journal。

`--source DIR --dest NEW_DIR --speed 5 --pace-from-ns NS --until-ns NS`：限速区间前
先快速写入历史以恢复盘口；目标目录必须不存在，拒绝源目录及其子目录。可选
`--wait-ready` 在限速起点输出 ready_records 并等待 stdin 的 `go`，让测试驱动先确认
consumer 追上盘前数据。全程只映射当前源页/目标页；与 reader 共用 journal ABI，
保留原帧字节、hash、接收时间和序号，payload 复制后 release 提交 status。

输出逐帧投放偏差的有界直方图摘要、实际倍速、帧数及源时间跨度。到截止点停止，
不伪造 END、不补未来窗口；测试跟读用 `--journal-clock source`，历史 recv_ns 到
当前墙钟的差值不能当作实盘延迟。一次性测试编排放在忽略的 work/，统计复用
`scripts/summarize_latency.py`，仓库不保留另一套 Python journal 投放器。

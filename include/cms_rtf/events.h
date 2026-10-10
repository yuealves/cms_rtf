#pragma once

// cms_rtf  canonical 事件与内核二进制接口布局。
//
// 语义对齐 light_rtf v2（market/codec/events.h）：side 66='B'/83='S'/78='N'；
// SH order_type 'A'/'D'；SZ order_type '1'/'2'/'U'；trade_type 'F'/'4'。
// 价格内核用 int64 tick（元×10000），与 light_rtf 一致，不用 double 做簿键。
//
// 与 light_rtf 的差异（batch 化裁剪）：id/ref 用 int64（CMS 引用位宽未验证上界）；
// 事件无 nano 字段（local 网格本期不实现）；时间只带交易所毫秒。

#include <cstddef>
#include <cstdint>

namespace cms_rtf {

inline constexpr int8_t kSideBuy = 66;    // 'B'
inline constexpr int8_t kSideSell = 83;   // 'S'
inline constexpr int8_t kSideNeutral = 78;

inline constexpr int8_t kShOrderAdd = 65;     // 'A'
inline constexpr int8_t kShOrderDelete = 68;  // 'D'

inline constexpr int8_t kSzOrderMarketCounterparty = 49;  // '1'
inline constexpr int8_t kSzOrderLimit = 50;               // '2'
inline constexpr int8_t kSzOrderMarketBest = 85;          // 'U'

inline constexpr int8_t kSzTradeFill = 70;      // 'F'
inline constexpr int8_t kSzTradeWithdraw = 52;  // '4'

inline bool IsSh(int32_t symbol) { return symbol >= 400000; }

// 与 python/cms_rtf/codec.py 写盘布局逐字节一致（numpy dtype align=True）。
struct alignas(8) EventRec {
  int32_t symbol;
  int32_t time_ms;  // 交易所时间，午夜起毫秒
  int32_t channel;
  int32_t biz_index;   // SZ 前序补价用严格序
  int64_t ref1;        // order: 本单 id；trade: buy_ref
  int64_t ref2;        // trade: sell_ref；order 为 0
  int64_t px_tick;     // 元×10000；无价 0
  int64_t volume;
  uint8_t kind;        // 0=order 1=trade
  uint8_t side;
  uint8_t order_type;
  uint8_t trade_type;
  uint8_t pad[4];
};
static_assert(sizeof(EventRec) == 56, "event record layout changed");
static_assert(offsetof(EventRec, ref1) == 16, "layout drift");

// 簿捕获记录（内核 → python 聚合）
struct alignas(8) BookRec {
  int32_t symbol;
  int32_t ep_ms;      // 网格端点（ms）
  double feat[31];    // book_capture 31 列（无效全 NaN）
  double mid;         // 端点 mid（NaN=无效）
  int32_t act_ms;     // 捕获时刻最后可动作事件时间；-1=无
  uint8_t valid;      // snap.valid && RefInScope
  uint8_t pad[7];
};
static_assert(sizeof(BookRec) == 280, "book record layout changed");

// 流量记录（内核 → python 聚合；只发非空桶，空缺=零）
struct alignas(8) FlowRec {
  int32_t symbol;
  int32_t ep_ms;
  double bases[21];  // 与 light_rtf EventBases 字段序一致
};
static_assert(sizeof(FlowRec) == 176, "flow record layout changed");

// 指数端点网格：[09:30:00.500, 11:30:00] ∪ [13:00:00.500, 15:00:00]，500ms 步长。
inline constexpr int32_t kGridMs = 500;
inline constexpr int32_t kAmOpenMs = 34'200'500;    // 首个端点（非开盘时刻）
inline constexpr int32_t kAmCloseMs = 41'400'000;   // 11:30:00
inline constexpr int32_t kPmOpenMs = 46'800'000;    // 13:00:00
inline constexpr int32_t kPmCloseMs = 54'000'000;   // 15:00:00
inline constexpr int32_t kMarketOpenMs = 34'200'000;
inline constexpr int32_t kNumEndpoints = 28'800;

// 交易所网格会话判定（light_rtf worker.cpp InSessionExchMs 逐字）
inline bool InSessionExchMs(int32_t ms) {
  return (ms > kMarketOpenMs && ms <= kAmCloseMs) || (ms > kPmOpenMs && ms <= kPmCloseMs);
}

// 右闭桶端点：(t-500, t]；恰在端点属当前桶
inline int32_t EndpointMs(int32_t time_ms) {
  return ((time_ms + kGridMs - 1) / kGridMs) * kGridMs;
}

// 板块范围（light_rtf worker.cpp RefInScope 逐字；同事 41b74ab 官方代码表口径）
inline bool RefInScope(int32_t s) {
  if (s >= 200000 && s < 210000) return false;
  if (s < 5000) return true;
  if (s >= 300000 && s < 310000) return true;
  if (s >= 600000 && s < 690000) return true;
  return false;
}

}  // namespace cms_rtf

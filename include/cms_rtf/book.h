#pragma once

// cms_rtf 订单簿内核：逐字移植 light_rtf v2 的 ob::SymbolBook（l2_book.h/.cpp），
// 规则规范见其 offline/docs/ob-reconstruction-rules.md。事件从 EventRec 直接进入，
// id 位宽提升到 int64；语义不变：
//   SH：A 的 volume 是残余 Balance 直接挂单；已挂单出现在成交主动侧 = A→T 乱序跳过；
//       未知主动腿 = 立即全额成交；未知被动腿跳过；D 按自带 volume 扣量。
//   SZ：kind '1' 市价未定价保留，首笔成交迁移到该单最新成交价；'U' 入场按有效本方
//       最优定价（本方空→未定价）；撤单由引用位置给方向。

#include <cstdint>
#include <map>
#include <unordered_map>

#include "cms_rtf/events.h"

namespace cms_rtf {

using PriceTick = int64_t;

struct BookHealth {
  int64_t over_deduct = 0;
  int64_t d_unknown = 0;
  int64_t unknown_trade_ref = 0;
  int64_t late_passive_ref = 0;
  int64_t agg_skip = 0;
  int64_t crossed_book = 0;
  int64_t immediate_fill = 0;
};

class SymbolBook {
 public:
  explicit SymbolBook(bool is_sh) : is_sh_(is_sh) {}

  void OnEvent(const EventRec& ev);

  // 捕获快照：十档价量 + 全簿总量 + 有效性（空簿/单边/交叉锁定 → 无效）
  struct Snapshot {
    double bid_px[10]{};
    double bid_vol[10]{};
    double ask_px[10]{};
    double ask_vol[10]{};
    double total_bid = 0.0;
    double total_ask = 0.0;
    bool valid = false;
  };
  Snapshot Capture() const;

  const BookHealth& health() const { return health_; }

 private:
  struct OrderState {
    PriceTick price;  // 0 = 未定价
    int64_t remain;
    int8_t side;
    int8_t kind;
  };
  using OrderIt = std::unordered_map<int64_t, OrderState>::iterator;

  bool is_sh_;
  std::unordered_map<int64_t, OrderState> orders_;
  std::map<PriceTick, int64_t, std::greater<PriceTick>> bids_;
  std::map<PriceTick, int64_t> asks_;
  int64_t total_bid_ = 0;
  int64_t total_ask_ = 0;
  BookHealth health_;

  void OnShOrder(const EventRec& ev);
  void OnShTrade(const EventRec& ev);
  void OnSzOrder(const EventRec& ev);
  void OnSzTrade(const EventRec& ev);
  void SzDeduct(OrderIt it, int64_t vol, PriceTick trade_px, bool is_fill);
  void AddLevel(int8_t side, PriceTick px, int64_t vol);
  void SubLevel(int8_t side, PriceTick px, int64_t vol);
  void MaybeCountCrossed(int32_t time_ms);
};

}  // namespace cms_rtf

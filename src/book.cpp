#include "cms_rtf/book.h"

namespace cms_rtf {

namespace {

// 连续竞价时段（HHMMSSmmm 形式不适用——内核收 ms；这里按午夜起毫秒）
inline bool IsContinuousAuctionMs(int32_t ms) {
  return (ms >= 34'200'000 && ms < 41'400'000) || (ms >= 46'800'000 && ms < 53'820'000);
}

}  // namespace

void SymbolBook::OnEvent(const EventRec& ev) {
  if (is_sh_) {
    if (ev.kind == 0) OnShOrder(ev); else OnShTrade(ev);
  } else {
    if (ev.kind == 0) OnSzOrder(ev); else OnSzTrade(ev);
  }
  MaybeCountCrossed(ev.time_ms);
}

// --------------------------------------------------------------------------- SH
void SymbolBook::OnShOrder(const EventRec& ev) {
  if (ev.order_type == kShOrderAdd) {
    if (ev.volume <= 0) {
      return;
    }
    orders_.emplace(ev.ref1, OrderState{ev.px_tick, ev.volume, (int8_t)ev.side, kShOrderAdd});
    AddLevel(ev.side, ev.px_tick, ev.volume);
  } else if (ev.order_type == kShOrderDelete) {
    auto it = orders_.find(ev.ref1);
    if (it == orders_.end()) {
      health_.d_unknown += 1;
      return;
    }
    OrderState& st = it->second;
    SubLevel(st.side, st.price, ev.volume);
    st.remain -= ev.volume;
    if (st.remain < 0) {
      health_.over_deduct += 1;
    }
    if (st.remain <= 0) {
      orders_.erase(it);
    }
  }
}

void SymbolBook::OnShTrade(const EventRec& ev) {
  const int8_t agg_side = (ev.side == kSideBuy)    ? kSideBuy
                          : (ev.side == kSideSell) ? kSideSell
                                                   : int8_t{0};
  const int64_t refs[2] = {ev.ref1, ev.ref2};
  const int8_t leg_sides[2] = {kSideBuy, kSideSell};
  for (int i = 0; i < 2; ++i) {
    const bool is_aggressor = (agg_side != 0 && leg_sides[i] == agg_side);
    auto it = orders_.find(refs[i]);
    if (it == orders_.end()) {
      if (is_aggressor) {
        health_.immediate_fill += 1;
      } else {
        health_.late_passive_ref += 1;
      }
      continue;
    }
    if (is_aggressor) {
      health_.agg_skip += 1;
      continue;
    }
    OrderState& st = it->second;
    SubLevel(st.side, st.price, ev.volume);
    st.remain -= ev.volume;
    if (st.remain < 0) {
      health_.over_deduct += 1;
    }
    if (st.remain <= 0) {
      orders_.erase(it);
    }
  }
}

// --------------------------------------------------------------------------- SZ
void SymbolBook::OnSzOrder(const EventRec& ev) {
  if (ev.order_type != kSzOrderLimit && ev.order_type != kSzOrderMarketCounterparty &&
      ev.order_type != kSzOrderMarketBest) {
    return;
  }
  if (ev.volume <= 0) {
    return;
  }
  PriceTick px = 0;
  if (ev.order_type == kSzOrderLimit) {
    px = ev.px_tick;
  } else if (ev.order_type == kSzOrderMarketBest) {
    px = (ev.side == kSideBuy) ? (bids_.empty() ? 0 : bids_.begin()->first)
                               : (asks_.empty() ? 0 : asks_.begin()->first);
  }
  orders_.emplace(ev.ref1, OrderState{px, ev.volume, (int8_t)ev.side, (int8_t)ev.order_type});
  if (px > 0) {
    AddLevel(ev.side, px, ev.volume);
  }
}

void SymbolBook::OnSzTrade(const EventRec& ev) {
  const bool is_fill = (ev.trade_type == kSzTradeFill);
  if (is_fill) {
    for (const int64_t ref : {ev.ref1, ev.ref2}) {
      auto it = orders_.find(ref);
      if (it == orders_.end()) {
        health_.unknown_trade_ref += 1;
        continue;
      }
      SzDeduct(it, ev.volume, ev.px_tick, true);
    }
    return;
  }
  if (ev.trade_type == kSzTradeWithdraw) {
    // side 由引用位置给出（CMS SZ 撤单只有单边引用非零）
    const int64_t ref = (ev.side == kSideBuy) ? ev.ref1 : ev.ref2;
    auto it = orders_.find(ref);
    if (it == orders_.end()) {
      health_.unknown_trade_ref += 1;
      return;
    }
    SzDeduct(it, ev.volume, 0, false);
  }
}

void SymbolBook::SzDeduct(OrderIt it, int64_t vol, PriceTick trade_px, bool is_fill) {
  OrderState& st = it->second;
  const int64_t old_remain = st.remain;
  if (st.price > 0) {
    SubLevel(st.side, st.price, old_remain);
  }
  st.remain = old_remain - vol;
  if (st.remain < 0) {
    health_.over_deduct += 1;
  }
  if (st.remain <= 0) {
    orders_.erase(it);
    return;
  }
  if (is_fill && trade_px > 0 &&
      (st.kind == kSzOrderMarketCounterparty ||
       (st.kind == kSzOrderMarketBest && st.price == 0))) {
    st.price = trade_px;
  }
  if (st.price > 0) {
    AddLevel(st.side, st.price, st.remain);
  }
}

// --------------------------------------------------------------------------- 价位档
void SymbolBook::AddLevel(int8_t side, PriceTick px, int64_t vol) {
  if (vol <= 0) {
    return;
  }
  if (side == kSideBuy) {
    bids_[px] += vol;
    total_bid_ += vol;
  } else {
    asks_[px] += vol;
    total_ask_ += vol;
  }
}

void SymbolBook::SubLevel(int8_t side, PriceTick px, int64_t vol) {
  if (vol <= 0) {
    return;
  }
  auto sub = [this, side, vol](auto& book, PriceTick px) {
    auto it = book.find(px);
    if (it == book.end()) {
      health_.over_deduct += 1;
      return;
    }
    const int64_t pre = it->second;
    const int64_t post = pre - vol;
    const int64_t removed = (post > 0) ? vol : pre;
    int64_t& total = (side == kSideBuy) ? total_bid_ : total_ask_;
    total -= removed;
    it->second = post;
    if (post <= 0) {
      book.erase(it);
    }
  };
  if (side == kSideBuy) {
    sub(bids_, px);
  } else {
    sub(asks_, px);
  }
}

SymbolBook::Snapshot SymbolBook::Capture() const {
  Snapshot s;
  int i = 0;
  for (const auto& [px, vol] : bids_) {
    if (i >= 10) break;
    s.bid_px[i] = static_cast<double>(px) / 10000.0;
    s.bid_vol[i] = static_cast<double>(vol);
    ++i;
  }
  i = 0;
  for (const auto& [px, vol] : asks_) {
    if (i >= 10) break;
    s.ask_px[i] = static_cast<double>(px) / 10000.0;
    s.ask_vol[i] = static_cast<double>(vol);
    ++i;
  }
  s.total_bid = static_cast<double>(total_bid_);
  s.total_ask = static_cast<double>(total_ask_);
  const bool has_bid = !bids_.empty(), has_ask = !asks_.empty();
  s.valid = has_bid && has_ask && s.ask_px[0] > s.bid_px[0];
  return s;
}

void SymbolBook::MaybeCountCrossed(int32_t time_ms) {
  if (!IsContinuousAuctionMs(time_ms)) {
    return;
  }
  if (!bids_.empty() && !asks_.empty() && bids_.begin()->first >= asks_.begin()->first) {
    health_.crossed_book += 1;
  }
}

}  // namespace cms_rtf

#pragma once

// cms_rtf 每股交易所网格流量状态机：移植 light_rtf v2 index_500ms/state.cpp
// 的交易所网格半边（batch 不需要 local 网格/水位/双网格隔离）。
//
// 口径要点（与 online_simu 实时版逐字一致，见 light_rtf 语义映射文档）：
//   - SH 委托增量：orderid 全天身份跟踪（已观察累计量、已知价买 max/卖 min、
//     计数只首次 +1）；每桶只发新增量；金额 = 新增量 × 桶末已知价；
//     大小单按 已知累计量×已知价 对动态阈值，不回改。
//   - SZ 市价('1')补价 = 同 channel 严格前序对手方向成交价 ×1.001/×0.999；
//     U('85') = 前序对手价本身；batch 按 journal 序处理 ⇒ 到达序约束天然成立。
//   - SH 成交：side∈{B,S} 且双引用非零进 trade 族 + 主动腿 ShNote；N 排除。
//   - SZ 撤单（trade_type='4'）进 cancel 族；撤单无金额。

#include <cstdint>
#include <unordered_map>
#include <vector>
#include "cms_rtf/identity_table.h"

#include "cms_rtf/events.h"

namespace cms_rtf {

// EventBases 字段序（= FlowRec.bases 顺序）：
//  0 order_buy_count      1 order_sell_count
//  2 order_buy_volume     3 order_sell_volume
//  4 order_buy_money      5 order_sell_money
//  6 order_large_money    7 order_small_money
//  8 order_resolved_count
//  9 cancel_buy_count    10 cancel_sell_count
// 11 cancel_buy_volume   12 cancel_sell_volume
// 13 trade_buy_count     14 trade_sell_count
// 15 trade_buy_volume    16 trade_sell_volume
// 17 trade_buy_money     18 trade_sell_money
// 19 trade_large_money   20 trade_small_money
inline constexpr int kNumBases = 21;

struct FlowBases {
  double v[kNumBases]{};
  bool active = false;
};

// SH 委托身份（全天保留，午休不清空）
struct ShOrderTrack {
  double known_volume = 0;
  double known_price = 0;
  bool counted = false;
  bool has_price = false;
  int8_t side = 0;
};

// 本桶内同一委托的碎片聚合（封口时按桶末已知价结算）
struct ShFrag {
  double delta_volume = 0;
  double max_px = 0;
  double min_px = 0;
  bool has_px = false;
};

// SZ 每 channel 最近两侧成交价
struct SzPriceBook {
  struct Mark {
    int64_t biz = 0;
    int32_t time_ms = 0;
    double px = 0.0;
    bool has = false;
  } last[2];  // [0]=买主动(B) [1]=卖主动(S)
};

class SymbolFlow {
 public:
  // thresh: (xl, m)；无阈值股不分类大小单
  explicit SymbolFlow(double thresh_xl, double thresh_m)
      : thresh_xl_(thresh_xl), thresh_m_(thresh_m) {}

  // 处理一个事件；session 边界封口、桶推进与发布（经 Emit 回调）都在内部完成。
  // 输出：非空桶 → Emit(ep_ms, bases21)。
  template <typename Emit>
  void OnEvent(const EventRec& ev, Emit&& emit, bool allow_flow = true) {
    // 时段跨越先封口（light_rtf worker.cpp OnEvent 的 session-boundary seal）
    if (ev.time_ms > kPmCloseMs) {
      AdvanceTo(kPmCloseMs + kGridMs, emit);
    } else if (ev.time_ms > kAmCloseMs && ev.time_ms <= kPmOpenMs) {
      AdvanceTo(kAmCloseMs + kGridMs, emit);
    }
    const bool in_session = InSessionExchMs(ev.time_ms) && allow_flow;
    if (ev.kind == 0) {
      OnOrder(ev, in_session, emit);
    } else {
      OnTrade(ev, in_session, emit);
    }
  }

  // 股流结束：封口当前未发布桶（若有）
  template <typename Emit>
  void Finish(Emit&& emit) {
    if (cur_ep_ >= 0 && (cur_.active || !sh_bucket_.empty())) {
      PublishCurrent(emit);
    }
  }

  int64_t non_monotonic() const { return non_monotonic_; }
  size_t sh_order_identities() const { return sh_orders_.size(); }
  size_t sh_identity_capacity_bytes() const { return sh_orders_.capacity_bytes(); }

  // Huatai AdvanceExchTo: the dispatcher has established that every observed
  // channel crossed this endpoint. Seal quiet stocks without waiting for an event.
  template <typename Emit>
  void CloseThrough(int32_t ep, Emit&& emit) { AdvanceTo(ep + kGridMs, emit); }

 private:
  double thresh_xl_, thresh_m_;
  FlowBases cur_{};
  int32_t cur_ep_ = -1;
  IdentityTable<ShOrderTrack> sh_orders_;
  std::unordered_map<int64_t, ShFrag> sh_bucket_;
  std::unordered_map<int32_t, SzPriceBook> sz_prices_;
  int64_t non_monotonic_ = 0;

  void ShNote(int64_t order_id, int8_t side, int64_t vol, double px) {
    ShOrderTrack& tr = sh_orders_[order_id];
    const int64_t count = tr.counted ? 0 : 1;
    tr.counted = true;
    tr.side = side;
    ShFrag& frag = sh_bucket_[order_id];
    frag.delta_volume += vol;
    if (!frag.has_px) {
      frag.max_px = frag.min_px = px;
      frag.has_px = true;
    } else {
      frag.max_px = std::max(frag.max_px, px);
      frag.min_px = std::min(frag.min_px, px);
    }
    if (side == kSideBuy) {
      cur_.v[0] += count;
      cur_.v[2] += vol;
    } else {
      cur_.v[1] += count;
      cur_.v[3] += vol;
    }
    cur_.v[8] += count;  // SH 委托价恒已知 → resolved 即 count
    cur_.active = true;
  }

  void ShSeedIdentity(int64_t order_id, int8_t side, int64_t vol, double px) {
    ShOrderTrack& tr = sh_orders_[order_id];
    tr.counted = true;
    tr.side = side;
    tr.known_volume += vol;
    tr.known_price = !tr.has_price ? px
                                   : (side == kSideBuy ? std::max(tr.known_price, px)
                                                       : std::min(tr.known_price, px));
    tr.has_price = true;
  }

  template <typename Emit>
  void PublishCurrent(Emit& emit) {
    // SH 碎片按桶末已知价结算
    for (auto& [id, frag] : sh_bucket_) {
      if (frag.delta_volume <= 0 || !frag.has_px) {
        continue;
      }
      ShOrderTrack& tr = sh_orders_[id];
      const double frag_px = (tr.side == kSideBuy) ? frag.max_px : frag.min_px;
      tr.known_price = !tr.has_price ? frag_px
                                     : (tr.side == kSideBuy
                                            ? std::max(tr.known_price, frag_px)
                                            : std::min(tr.known_price, frag_px));
      tr.has_price = true;
      tr.known_volume += frag.delta_volume;
      const double money = frag.delta_volume * tr.known_price;
      const double known_money = tr.known_volume * tr.known_price;
      if (tr.side == kSideBuy) {
        cur_.v[4] += money;
      } else {
        cur_.v[5] += money;
      }
      if (known_money >= thresh_xl_) {
        cur_.v[6] += money;
      }
      if (known_money < thresh_m_) {
        cur_.v[7] += money;
      }
    }
    emit(cur_ep_, cur_.v);
    cur_ = FlowBases{};
    sh_bucket_.clear();
  }

  template <typename Emit>
  void AdvanceTo(int32_t ep, Emit& emit) {
    if (cur_ep_ < 0) {
      cur_ep_ = ep;
      return;
    }
    while (cur_ep_ < ep) {
      if (!cur_.active && sh_bucket_.empty()) {
        cur_ep_ = ep;  // 空跨度 O(1) 快进：空桶=零
        break;
      }
      PublishCurrent(emit);
      cur_ep_ += kGridMs;
    }
  }

  template <typename Emit>
  void OnOrder(const EventRec& ev, bool in_session, Emit& emit) {
    const bool is_sh = IsSh(ev.symbol);
    if (is_sh && ev.order_type == kShOrderAdd) {
      if (in_session) {
        AdvanceTo(EndpointMs(ev.time_ms), emit);
        ShNote(ev.ref1, ev.side, ev.volume, ev.px_tick / 10000.0);
      } else {
        // 非会话时段 SH A：只播种身份，不进流量
        ShSeedIdentity(ev.ref1, ev.side, ev.volume, ev.px_tick / 10000.0);
      }
      return;
    }
    if (is_sh && ev.order_type == kShOrderDelete) {
      if (!in_session) {
        return;
      }
      AdvanceTo(EndpointMs(ev.time_ms), emit);
      if (ev.side == kSideBuy) {
        cur_.v[9] += 1;
        cur_.v[11] += ev.volume;
      } else {
        cur_.v[10] += 1;
        cur_.v[12] += ev.volume;
      }
      cur_.active = true;
      return;
    }
    if (!is_sh && (ev.order_type == kSzOrderLimit ||
                   ev.order_type == kSzOrderMarketCounterparty ||
                   ev.order_type == kSzOrderMarketBest)) {
      if (!in_session) {
        return;
      }
      AdvanceTo(EndpointMs(ev.time_ms), emit);
      double px = 0.0;
      if (ev.order_type == kSzOrderLimit) {
        px = ev.px_tick / 10000.0;
      } else {
        // 市价/本方最优：同 channel 严格前序对手方向成交价
        const SzPriceBook& pb = sz_prices_[ev.channel];
        const int i = (ev.side == kSideBuy) ? 1 : 0;  // 对手方向
        double ref = 0.0;
        if (pb.last[i].has && pb.last[i].biz < ev.biz_index &&
            pb.last[i].time_ms <= ev.time_ms) {
          ref = pb.last[i].px;
        }
        if (ref > 0) {
          px = (ev.order_type == kSzOrderMarketBest)
                   ? ref
                   : ref * (ev.side == kSideBuy ? 1.001 : 0.999);
        }
      }
      if (ev.side == kSideBuy) {
        cur_.v[0] += 1;
        cur_.v[2] += ev.volume;
      } else {
        cur_.v[1] += 1;
        cur_.v[3] += ev.volume;
      }
      if (px > 0) {
        const double money = ev.volume * px;
        if (ev.side == kSideBuy) {
          cur_.v[4] += money;
        } else {
          cur_.v[5] += money;
        }
        if (money >= thresh_xl_) {
          cur_.v[6] += money;
        }
        if (money < thresh_m_) {
          cur_.v[7] += money;
        }
        cur_.v[8] += 1;
      }
      cur_.active = true;
    }
    // 其他类型不进桶
  }

  template <typename Emit>
  void OnTrade(const EventRec& ev, bool in_session, Emit& emit) {
    const bool is_sh = IsSh(ev.symbol);
    if (is_sh) {
      if (!in_session) {
        return;
      }
      AdvanceTo(EndpointMs(ev.time_ms), emit);
      if ((ev.side == kSideBuy || ev.side == kSideSell) && ev.ref1 != 0 && ev.ref2 != 0) {
        const double px = ev.px_tick / 10000.0;
        const double money = ev.volume * px;
        if (ev.side == kSideBuy) {
          cur_.v[13] += 1;
          cur_.v[15] += ev.volume;
          cur_.v[17] += money;
        } else {
          cur_.v[14] += 1;
          cur_.v[16] += ev.volume;
          cur_.v[18] += money;
        }
        if (money >= thresh_xl_) {
          cur_.v[19] += money;
        }
        if (money < thresh_m_) {
          cur_.v[20] += money;
        }
        // SH 激进委托跟踪：主动腿 id 进 ShNote
        ShNote(ev.side == kSideBuy ? ev.ref1 : ev.ref2, ev.side, ev.volume, px);
      } else {
        cur_.active = true;  // N 成交：只标记活跃（驱动桶推进），不进基础量
      }
      return;
    }
    // SZ
    if (ev.trade_type == kSzTradeFill) {
      // 前序价缓冲：会话内外都记（盘前成交要参与补价）
      if (ev.ref1 != 0 && ev.ref2 != 0) {
        const int8_t side = (ev.ref1 > ev.ref2) ? kSideBuy : kSideSell;
        auto& mark = sz_prices_[ev.channel].last[side == kSideBuy ? 0 : 1];
        if (mark.has && mark.biz > ev.biz_index) {
          ++non_monotonic_;  // 防御：channel 内序号应单调
        }
        mark.biz = ev.biz_index;
        mark.time_ms = ev.time_ms;
        mark.px = ev.px_tick / 10000.0;
        mark.has = true;
        if (!in_session) {
          return;
        }
        AdvanceTo(EndpointMs(ev.time_ms), emit);
        const double px = ev.px_tick / 10000.0;
        const double money = ev.volume * px;
        if (side == kSideBuy) {
          cur_.v[13] += 1;
          cur_.v[15] += ev.volume;
          cur_.v[17] += money;
        } else {
          cur_.v[14] += 1;
          cur_.v[16] += ev.volume;
          cur_.v[18] += money;
        }
        if (money >= thresh_xl_) {
          cur_.v[19] += money;
        }
        if (money < thresh_m_) {
          cur_.v[20] += money;
        }
        cur_.active = true;
      }
      return;
    }
    if (ev.trade_type == kSzTradeWithdraw) {
      if (!in_session) {
        return;
      }
      AdvanceTo(EndpointMs(ev.time_ms), emit);
      if (ev.side == kSideBuy) {
        cur_.v[9] += 1;
        cur_.v[11] += ev.volume;
      } else {
        cur_.v[10] += 1;
        cur_.v[12] += ev.volume;
      }
      cur_.active = true;
    }
  }
};

}  // namespace cms_rtf

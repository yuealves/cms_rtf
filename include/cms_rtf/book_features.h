#pragma once

// 盘口 31 公式 + mid：逐字移植 light_rtf v2 index_500ms/book_capture.cpp
//（本身是同事 base.aggregate_book_features 的逐字移植）。输入十档快照：
// px=0 的补齐档转 NaN、量按 0 计；无效簿（空/单边/交叉）全 NaN；非有限→NaN。

#include <cmath>
#include <cstdint>

#include "cms_rtf/book.h"

namespace cms_rtf {

namespace book_feature_detail {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

inline double imb(double buy, double sell) { return (buy - sell) / (buy + sell); }

inline double Slope(const double* px, const double* vol, bool bid) {
  double sx = 0.0, sxx = 0.0, sxc = 0.0, cum = 0.0, cum_last = kNaN;
  for (int i = 0; i < 10; ++i) {
    cum += vol[i];
    cum_last = cum;
    if (std::isnan(px[i])) {
      continue;
    }
    const double x = bid ? (px[0] - px[i]) : (px[i] - px[0]);
    sx += x;
    sxx += x * x;
    sxc += x * cum;
  }
  const double denominator = 10 * sxx - sx * sx;
  const double numerator = 10 * sxc - sx * cum_last;
  if (std::abs(denominator) <= 1e-12) {
    return kNaN;
  }
  return numerator / denominator;
}

}  // namespace book_feature_detail

// 返回 mid（NaN 表示簿无效）；out31 填 31 个盘口特征
inline double ComputeBookFeatures(const SymbolBook::Snapshot& snap, double* out31) {
  using namespace book_feature_detail;
  if (!snap.valid) {
    for (int i = 0; i < 31; ++i) out31[i] = kNaN;
    return kNaN;
  }
  double bp[10], ap[10];
  for (int i = 0; i < 10; ++i) {
    bp[i] = snap.bid_px[i] == 0.0 ? kNaN : snap.bid_px[i];
    ap[i] = snap.ask_px[i] == 0.0 ? kNaN : snap.ask_px[i];
  }
  const double* bv = snap.bid_vol;
  const double* av = snap.ask_vol;
  const double btotal = snap.total_bid;
  const double atotal = snap.total_ask;
  double b10 = 0.0, a10 = 0.0, b3 = 0.0, a3 = 0.0, b5 = 0.0, a5 = 0.0;
  double bm5 = 0.0, am5 = 0.0, bpressure = 0.0, apressure = 0.0;
  double bvar_sum = 0.0, avar_sum = 0.0;
  for (int i = 0; i < 10; ++i) {
    b10 += bv[i];
    a10 += av[i];
    bvar_sum += bv[i] * bv[i];
    avar_sum += av[i] * av[i];
    if (i < 5) {
      b5 += bv[i];
      a5 += av[i];
      if (!std::isnan(bp[i])) bm5 += bp[i] * bv[i];
      if (!std::isnan(ap[i])) am5 += ap[i] * av[i];
      if (i < 3) {
        b3 += bv[i];
        a3 += av[i];
      }
    }
    if (!std::isnan(bp[i])) bpressure += bp[i] * bv[i];
    if (!std::isnan(ap[i])) apressure += ap[i] * av[i];
  }
  const double mid = (ap[0] + bp[0]) / 2.0;
  const double bs = Slope(bp, bv, true);
  const double ass = Slope(ap, av, false);
  const double br = (bp[0] - bp[9]) / mid * 10'000.0;
  const double ar = (ap[9] - ap[0]) / mid * 10'000.0;
  const double bmean = b10 / 10.0, amean = a10 / 10.0;
  const double bvar = bvar_sum / 10.0 - bmean * bmean;
  const double avar = avar_sum / 10.0 - amean * amean;
  const double micro = (ap[0] * bv[0] + bp[0] * av[0]) / (av[0] + bv[0]);
  const double bwap = bm5 / b5, awap = am5 / a5;
  const double weighted_mid = (bwap * a5 + awap * b5) / (b5 + a5);
  double wsp = 0.0;
  for (int i = 0; i < 5; ++i) {
    if (!std::isnan(bp[i]) && !std::isnan(ap[i])) {
      wsp += (ap[i] - bp[i]) * (av[i] + bv[i]);
    }
  }

  int k = 0;
  out31[k++] = (ap[0] - bp[0]) / mid * 10'000.0;            // spread_bps
  out31[k++] = std::log(ap[0] / bp[0]);                     // log_spread
  out31[k++] = wsp / (a5 + b5) / mid * 10'000.0;            // weighted_spread_5
  out31[k++] = imb(bv[0], av[0]);                           // obi_1
  out31[k++] = imb(b3, a3);                                 // obi_3
  out31[k++] = imb(b5, a5);                                 // obi_5
  out31[k++] = imb(b10, a10);                               // obi_10
  out31[k++] = imb(bm5, am5);                               // obi_money_5
  out31[k++] = imb(bv[0], av[0]) - imb(b5, a5);             // obi_gradient
  out31[k++] = b10;                                         // bid_depth_total
  out31[k++] = a10;                                         // ask_depth_total
  out31[k++] = b10 + a10;                                   // total_depth
  out31[k++] = bv[0] / b10;                                 // bid_concentration
  out31[k++] = av[0] / a10;                                 // ask_concentration
  out31[k++] = b3 / b10;                                    // bid_near_share
  out31[k++] = a3 / a10;                                    // ask_near_share
  out31[k++] = b10 / a10;                                   // depth_ratio
  out31[k++] = bs;                                          // bid_slope
  out31[k++] = ass;                                         // ask_slope
  out31[k++] = (bs - ass) / (bs + ass);                     // slope_imbalance
  out31[k++] = std::sqrt(bvar) / bmean;                     // bid_depth_cv
  out31[k++] = std::sqrt(avar) / amean;                     // ask_depth_cv
  out31[k++] = br;                                          // bid_range_bps
  out31[k++] = ar;                                          // ask_range_bps
  out31[k++] = 1.0 - std::abs(br - ar) / (br + ar);         // book_symmetry
  out31[k++] = (micro / mid - 1.0) * 10'000.0;              // micro_price_premium
  out31[k++] = (weighted_mid / mid - 1.0) * 10'000.0;       // weighted_mid_premium
  out31[k++] = imb(bpressure, apressure);                   // book_pressure_imb
  out31[k++] = imb(btotal, atotal);                         // total_order_volume_imb（全簿）
  out31[k++] = (awap - ap[0]) / ap[0] * 10'000.0;           // price_impact_buy_5
  out31[k++] = (bp[0] - bwap) / bp[0] * 10'000.0;           // price_impact_sell_5
  for (int i = 0; i < 31; ++i) {
    if (!std::isfinite(out31[i])) {
      out31[i] = kNaN;
    }
  }
  return mid;
}

}  // namespace cms_rtf

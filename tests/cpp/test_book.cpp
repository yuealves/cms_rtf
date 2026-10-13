// 簿内核单元测试：SH/SZ 核心规则（移植 light_rtf l2_book 的关键路径覆盖）
#include <cassert>
#include <cmath>
#include <cstdio>

#include "cms_rtf/book.h"
#include "cms_rtf/book_features.h"

using namespace cms_rtf;

static EventRec MkOrder(int32_t sym, int64_t id, int8_t side, int8_t type,
                        int64_t px_tick, int64_t vol, int32_t t_ms) {
  EventRec ev{};
  ev.symbol = sym;
  ev.kind = 0;
  ev.side = side;
  ev.order_type = type;
  ev.px_tick = px_tick;
  ev.volume = vol;
  ev.time_ms = t_ms;
  ev.ref1 = id;
  return ev;
}

static EventRec MkTrade(int32_t sym, int64_t buy_ref, int64_t sell_ref, int8_t side,
                        int8_t ttype, int64_t px_tick, int64_t vol, int32_t t_ms) {
  EventRec ev{};
  ev.symbol = sym;
  ev.kind = 1;
  ev.side = side;
  ev.trade_type = ttype;
  ev.px_tick = px_tick;
  ev.volume = vol;
  ev.time_ms = t_ms;
  ev.ref1 = buy_ref;
  ev.ref2 = sell_ref;
  return ev;
}

static void test_sh_book() {
  SymbolBook b(true);
  // 挂买单 100 @ 10.00、卖单 200 @ 10.05
  b.OnEvent(MkOrder(600000, 1, kSideBuy, kShOrderAdd, 100000, 100, 34'200'100));
  b.OnEvent(MkOrder(600000, 2, kSideSell, kShOrderAdd, 100500, 200, 34'200'200));
  auto s = b.Capture();
  assert(s.valid && s.bid_px[0] == 10.0 && s.ask_px[0] == 10.05);
  assert(s.bid_vol[0] == 100 && s.ask_vol[0] == 200);
  assert(s.total_bid == 100 && s.total_ask == 200);

  // 主动买成交 50：被动卖腿扣量（ask 200→150），主动买腿 id 未知 → immediate_fill
  b.OnEvent(MkTrade(600000, 999, 2, kSideBuy, 0, 100500, 50, 34'200'300));
  s = b.Capture();
  assert(s.ask_vol[0] == 150 && s.bid_vol[0] == 100);
  assert(b.health().immediate_fill == 1);

  // 被动买腿被成交 40：bid 100→60
  b.OnEvent(MkTrade(600000, 1, 888, kSideSell, 0, 100000, 40, 34'200'400));
  s = b.Capture();
  assert(s.bid_vol[0] == 60);

  // 撤单 60 → 买单清空（簿单边 → 无效）
  b.OnEvent(MkOrder(600000, 1, kSideBuy, kShOrderDelete, 0, 60, 34'200'500));
  s = b.Capture();
  assert(!s.valid && s.total_bid == 0 && s.total_ask == 150);
  std::printf("sh_book ok\n");
}

static void test_sz_book() {
  SymbolBook b(false);
  b.OnEvent(MkOrder(1, 10, kSideBuy, kSzOrderLimit, 200000, 300, 34'200'100));
  b.OnEvent(MkOrder(1, 11, kSideSell, kSzOrderLimit, 200500, 400, 34'200'200));
  // 市价卖单（'1' 未定价保留，不进档）
  b.OnEvent(MkOrder(1, 12, kSideSell, kSzOrderMarketCounterparty, 0, 100, 34'200'300));
  auto s = b.Capture();
  assert(s.valid && s.bid_px[0] == 20.0 && s.ask_px[0] == 20.05);
  assert(s.ask_vol[0] == 400);  // 市价单未定价不进档

  // 市价卖单 12 成交 100 @ 20.00 → 余量 0 直接出簿；限价买扣量 300→200
  b.OnEvent(MkTrade(1, 10, 12, 0, kSzTradeFill, 200000, 100, 34'200'400));
  s = b.Capture();
  assert(s.bid_vol[0] == 200 && s.ask_vol[0] == 400);

  // 撤单（'4'，side 由引用位置给）：撤卖 11 余 400
  b.OnEvent(MkTrade(1, 0, 11, kSideSell, kSzTradeWithdraw, 0, 400, 34'200'500));
  s = b.Capture();
  assert(!s.valid);  // 卖侧清空
  assert(s.total_ask == 0 && s.total_bid == 200);
  std::printf("sz_book ok\n");
}

static void test_book_features_basic() {
  SymbolBook b(true);
  for (int i = 1; i <= 10; ++i) {
    b.OnEvent(MkOrder(600000, 100 + i, kSideBuy, kShOrderAdd, (10000 - i), 100 * i,
                      34'200'000 + i));
    b.OnEvent(MkOrder(600000, 200 + i, kSideSell, kShOrderAdd, (10010 + i), 100 * i,
                      34'200'000 + 100 + i));
  }
  auto s = b.Capture();
  assert(s.valid);
  double feat[31];
  const double mid = ComputeBookFeatures(s, feat);
  assert(std::abs(mid - 1.0005) < 1e-12);  // (0.9999+1.0011)/2; ticks are yuan * 10000
  assert(feat[9] > 0 && feat[10] > 0);    // depth totals
  assert(std::isfinite(feat[17]) && std::isfinite(feat[18]));  // slopes
  std::printf("book_features ok mid=%.2f spread_bps=%.4f\n", mid, feat[0]);
}

int main() {
  test_sh_book();
  test_sz_book();
  test_book_features_basic();
  std::printf("all book tests ok\n");
  return 0;
}

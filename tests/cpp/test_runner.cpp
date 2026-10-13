#include <cstdlib>
#include <cstring>
#include <iostream>
#include <random>

#include "cms_rtf/runner.h"
#include "reference_runner.h"

using namespace cms_rtf;

static void check(bool ok) {
  if (!ok) { std::cerr << "eager/lazy runner mismatch\n"; std::exit(1); }
}

static void compare(int32_t symbol, const std::vector<EventRec>& events) {
  const auto grid = BuildGrid();
  std::vector<BookRec> a, b;
  std::vector<FlowRec> af, bf;
  SymbolRunner lazy(symbol, 100000, 20000, grid, &a, &af);
  eager_reference::SymbolRunner eager(symbol, 100000, 20000, grid, &b, &bf);
  for (const auto& ev : events) { lazy.OnEvent(ev); eager.OnEvent(ev); }
  lazy.Finish(); eager.Finish();
  check(a.size() == b.size() && af.size() == bf.size());
  for (size_t i = 0; i < a.size(); ++i) {
    check(a[i].symbol == b[i].symbol && a[i].ep_ms == b[i].ep_ms &&
          a[i].act_ms == b[i].act_ms && a[i].valid == b[i].valid);
    check(std::memcmp(a[i].feat, b[i].feat, sizeof(a[i].feat)) == 0);
    check(std::memcmp(&a[i].mid, &b[i].mid, sizeof(double)) == 0);
  }
  for (size_t i = 0; i < af.size(); ++i) {
    check(af[i].symbol == bf[i].symbol && af[i].ep_ms == bf[i].ep_ms);
    check(std::memcmp(af[i].bases, bf[i].bases, sizeof(af[i].bases)) == 0);
  }
  const double am = lazy.seed_mid(), bm = eager.seed_mid();
  check(std::memcmp(&am, &bm, sizeof(double)) == 0);
  check(lazy.non_monotonic() == eager.non_monotonic());
}

int main() {
  // Includes exact endpoints, backwards timestamps, lunch, post-close updates,
  // and re-entry after post-close: the frozen capture must survive those updates.
  const std::vector<int32_t> times = {
      kMarketOpenMs - 1, kMarketOpenMs, kMarketOpenMs + 1,
      kMarketOpenMs + 500, kMarketOpenMs + 501, kMarketOpenMs - 2,
      kAmCloseMs, kAmCloseMs + 1, kPmOpenMs, kPmOpenMs + 501,
      kPmCloseMs, kPmCloseMs + 1, kPmCloseMs + 2000,
      kPmCloseMs - 500, kPmCloseMs + 3000};
  for (int32_t sym : {1, 600000, 689009}) {
    std::vector<EventRec> events;
    compare(sym, events);
    std::mt19937 rng(123);
    int64_t id = 0;
    for (const auto t : times) {
      for (int j = 0; j < 100; ++j) {
        EventRec ev{};
        ev.symbol = sym;
        ev.time_ms = t;
        ev.channel = 1;
        ev.biz_index = ++id;
        ev.side = (rng() & 1) ? kSideBuy : kSideSell;
        ev.order_type = IsSh(sym) ? kShOrderAdd : kSzOrderLimit;
        ev.px_tick = (ev.side == kSideBuy ? 100000 : 101000) + rng() % 100;
        ev.volume = 1 + rng() % 100;
        ev.ref1 = id;
        events.push_back(ev);
      }
      compare(sym, events);  // EOF at each boundary, including before next seal.
    }
  }
  std::cout << "eager/lazy boundary and non-monotonic capture equivalence passed\n";
}

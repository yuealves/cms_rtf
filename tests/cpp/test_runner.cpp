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

static void local_clock_boundaries() {
  const auto grid = BuildGrid();
  std::vector<BookRec> books;
  std::vector<FlowRec> flows;
  SymbolRunner runner(1, 100000, 20000, grid, &books, &flows);
  runner.EnableLive(0); runner.EnableLocal(100000, 20000);
  const int64_t open = int64_t(kMarketOpenMs) * 1000000;
  EventRec ev{}; ev.symbol = 1; ev.channel = 1; ev.order_type = kSzOrderLimit;
  ev.time_ms = kMarketOpenMs - 1; ev.volume = 100;
  ev.ref1 = 1; ev.side = kSideBuy; ev.px_tick = 100000; runner.OnEvent(ev, open - 2);
  ev.ref1 = 2; ev.side = kSideSell; ev.px_tick = 100200; runner.OnEvent(ev, open - 1);
  // Exchange pre-open, receive post-open: different flow eligibility and return seeds.
  ev.ref1 = 3; ev.side = kSideBuy; ev.px_tick = 100100; runner.OnEvent(ev, open + 1);
  runner.CloseLocal(kAmOpenMs);
  auto& first = *runner.local();
  check(first.output.bases[0] == 1 && first.output.bases[2] == 100);
  check(first.ret == std::log(10.015) - std::log(10.01));
  check(first.age == .499999999);
  check(runner.seed_mid() == 10.015);
  // One nanosecond after the cutoff must go into the second LOCAL bucket.
  ev.ref1 = 4; ev.time_ms = kAmOpenMs; ev.px_tick = 100150;
  runner.OnEvent(ev, open + 500000001);
  runner.CloseLocal(kAmOpenMs + 500); runner.CloseThrough(1);
  check(runner.local()->output.bases[2] == 100);
  check(flows.size() == 1 && flows[0].ep_ms == kAmOpenMs && flows[0].bases[2] == 100);
  runner.CloseLocal(kAmOpenMs + 1000);
  check(runner.local()->output.bases[2] == 0 && runner.local()->ret == 0);
  // A newly observed stock inherits local progress, including clock inversions,
  // as if Huatai had precreated it with all reference stocks.
  SymbolRunner unseen(1, 100000, 20000, grid, &books, &flows);
  unseen.EnableLocal(100000, 20000, kAmOpenMs);
  unseen.OnEvent(ev, open + 1); // inverted receive clock after first cutoff
  unseen.CloseLocal(kAmOpenMs + 500);
  check(unseen.local()->output.bases[2] == 100);
}

static void local_age_excludes_only_lunch() {
  const auto grid = BuildGrid();
  std::vector<BookRec> books;
  std::vector<FlowRec> flows;
  SymbolRunner runner(1, 100000, 20000, grid, &books, &flows);
  runner.EnableLocal(100000, 20000);
  runner.CloseLocal(kAmOpenMs);
  check(std::isnan(runner.local()->age) && !runner.local()->fresh);
  EventRec ev{}; ev.symbol = 1; ev.channel = 1; ev.order_type = kSzOrderLimit;
  ev.time_ms = kAmCloseMs - 1000; ev.volume = 100;
  ev.ref1 = 1; ev.side = kSideBuy; ev.px_tick = 100000;
  const int64_t last_am = int64_t(kAmCloseMs - 1000) * 1000000;
  runner.OnEvent(ev, last_am);
  runner.CloseLocal(kAmCloseMs);
  check(runner.local()->age == 1.0 && !runner.local()->fresh);
  runner.CloseLocal(kPmOpenMs + 500);
  check(runner.local()->age == 1.5 && !runner.local()->fresh);
  runner.CloseLocal(kPmOpenMs + 6000);  // Reused cached book must keep aging.
  check(runner.local()->age == 7.0 && !runner.local()->fresh);

  // Receive during the break: subtract the remaining break, not a fixed 5400s.
  ev.ref1 = 2; ev.time_ms = kAmCloseMs + 2700000;
  runner.OnEvent(ev, int64_t(ev.time_ms) * 1000000);
  runner.CloseLocal(kPmOpenMs + 6500);
  check(runner.local()->age == 6.5 && !runner.local()->fresh);
  ev.ref1 = 3; ev.time_ms = kPmOpenMs + 7000;
  runner.OnEvent(ev, int64_t(ev.time_ms) * 1000000 + 1);
  runner.CloseLocal(kPmOpenMs + 7500);
  check(runner.local()->age == .499999999 && runner.local()->fresh);
  runner.CloseLocal(kPmOpenMs + 8000);
  check(runner.local()->age == .999999999 && !runner.local()->fresh);

  SymbolRunner at_boundary(2, 100000, 20000, grid, &books, &flows);
  at_boundary.EnableLocal(100000, 20000);
  ev.symbol = 2; ev.time_ms = kAmCloseMs;
  at_boundary.OnEvent(ev, int64_t(kAmCloseMs) * 1000000);
  // Even if called at the PM boundary, zero trading age is not fresh reception.
  at_boundary.CloseLocal(kPmOpenMs);
  check(at_boundary.local()->age == 0.0 && !at_boundary.local()->fresh);
  at_boundary.CloseLocal(kPmOpenMs + 500);
  check(at_boundary.local()->age == .5 && !at_boundary.local()->fresh);
}

int main() {
  local_clock_boundaries();
  local_age_excludes_only_lunch();
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

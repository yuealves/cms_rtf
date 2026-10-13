// cms_index500ms_day：单线程批处理驱动（参考实现/回归基线）。
// 每股 SymbolRunner 见 runner.h；生产路径用 app/cms_index500ms_mt.cpp（多线程）。
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "cms_rtf/events.h"
#include "cms_rtf/runner.h"

int main(int argc, char** argv) {
  using namespace cms_rtf;
  std::string events_path, thresh_path, book_path, flow_path, seeds_path;
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string k = argv[i], v = argv[i + 1];
    if (k == "--events") events_path = v;
    else if (k == "--thresh") thresh_path = v;
    else if (k == "--out-book") book_path = v;
    else if (k == "--out-flow") flow_path = v;
    else if (k == "--out-seeds") seeds_path = v;
    else { std::cerr << "unknown arg " << k << "\n"; return 2; }
  }
  if (events_path.empty() || thresh_path.empty() || book_path.empty() ||
      flow_path.empty() || seeds_path.empty()) {
    std::cerr << "usage: --events E --thresh T --out-book B --out-flow F --out-seeds S\n";
    return 2;
  }

  std::unordered_map<int32_t, Thresh> thresh;
  {
    std::ifstream in(thresh_path, std::ios::binary);
    int32_t sym;
    Thresh t;
    while (in.read(reinterpret_cast<char*>(&sym), 4) &&
           in.read(reinterpret_cast<char*>(&t.xl), 8) &&
           in.read(reinterpret_cast<char*>(&t.m), 8)) {
      thresh.emplace(sym, t);
    }
  }

  std::ifstream events(events_path, std::ios::binary);
  if (!events) { std::cerr << "cannot open " << events_path << "\n"; return 1; }

  const auto grid = BuildGrid();
  int64_t n_events = 0, n_symbols = 0;
  BookHealth total_health;
  int64_t total_nonmono = 0;
  EventRec ev;
  std::vector<BookRec> book_buf;
  std::vector<FlowRec> flow_buf;
  book_buf.reserve(kNumEndpoints);
  flow_buf.reserve(1024);
  SymbolRunner* runner = nullptr;
  int32_t cur_symbol = -1;
  std::ofstream seeds_out(seeds_path, std::ios::binary);
  std::ofstream book_out(book_path, std::ios::binary);
  std::ofstream flow_out(flow_path, std::ios::binary);
  seeds_out.exceptions(std::ios::failbit | std::ios::badbit);
  book_out.exceptions(std::ios::failbit | std::ios::badbit);
  flow_out.exceptions(std::ios::failbit | std::ios::badbit);
  auto finish_symbol = [&]() {
    runner->Finish();
    seeds_out.write(reinterpret_cast<const char*>(&cur_symbol), sizeof(cur_symbol));
    const double sm = runner->seed_mid();
    seeds_out.write(reinterpret_cast<const char*>(&sm), sizeof(sm));
    total_health.over_deduct += runner->health().over_deduct;
    total_health.d_unknown += runner->health().d_unknown;
    total_health.unknown_trade_ref += runner->health().unknown_trade_ref;
    total_health.late_passive_ref += runner->health().late_passive_ref;
    total_health.agg_skip += runner->health().agg_skip;
    total_health.crossed_book += runner->health().crossed_book;
    total_health.immediate_fill += runner->health().immediate_fill;
    total_nonmono += runner->non_monotonic();
    book_out.write(reinterpret_cast<const char*>(book_buf.data()),
                   static_cast<std::streamsize>(book_buf.size() * sizeof(BookRec)));
    flow_out.write(reinterpret_cast<const char*>(flow_buf.data()),
                   static_cast<std::streamsize>(flow_buf.size() * sizeof(FlowRec)));
    book_buf.clear(); flow_buf.clear();
    delete runner;
    ++n_symbols;
  };
  while (events.read(reinterpret_cast<char*>(&ev), sizeof(ev))) {
    ++n_events;
    if (ev.symbol != cur_symbol) {
      if (runner != nullptr) {
        finish_symbol();
      }
      const auto it = thresh.find(ev.symbol);
      runner = new SymbolRunner(ev.symbol,
                                it != thresh.end() ? it->second.xl : 0.0,
                                it != thresh.end() ? it->second.m : 0.0,
                                grid, &book_buf, &flow_buf);
      cur_symbol = ev.symbol;
    }
    runner->OnEvent(ev);
  }
  if (runner != nullptr) {
    finish_symbol();
  }
  book_out.close(); flow_out.close(); seeds_out.close();

  std::printf("{\"events\": %" PRId64 ", \"symbols\": %" PRId64
              ", \"over_deduct\": %" PRId64 ", \"d_unknown\": %" PRId64
              ", \"unknown_trade_ref\": %" PRId64 ", \"late_passive_ref\": %" PRId64
              ", \"agg_skip\": %" PRId64 ", \"crossed_book\": %" PRId64
              ", \"immediate_fill\": %" PRId64 ", \"sz_non_monotonic\": %" PRId64 "}\n",
              n_events, n_symbols, total_health.over_deduct, total_health.d_unknown,
              total_health.unknown_trade_ref, total_health.late_passive_ref,
              total_health.agg_skip, total_health.crossed_book,
              total_health.immediate_fill, total_nonmono);
  return 0;
}

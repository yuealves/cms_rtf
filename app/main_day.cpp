// cms_index500ms_day：批量日级内核。
// 输入 events.bin（按 symbol 分组、组内 journal 序的 EventRec 流）+
//       thresh.bin（{i32 symbol, f64 xl, f64 m}）。
// 输出 book.bin（BookRec：每个被穿越封口的端点一条，内容=穿越前捕获；
//                股流结束后剩余端点以最终捕获封口）、
//       flow.bin（FlowRec：非空流量桶）、
//       seeds.bin（{i32 symbol, f64 seed_mid}：开盘收益前值 = 09:30:00 前最后捕获的 mid）。
//
// 交易所网格 batch 语义（对照 light_rtf worker.cpp）：
//   - 全量历史下通道水位必越过全部端点 ⇒ 28,800 端点全部发布；
//   - 端点 e 的簿 = 最后一条 Time ≤ e 的事件处理后的簿（穿越封口 + 前向沿用）；
//   - 迟到（同股时间回退）事件不封口历史端点，只更新当前捕获（与引擎一致）。
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "cms_rtf/book.h"
#include "cms_rtf/book_features.h"
#include "cms_rtf/events.h"
#include "cms_rtf/flow.h"

namespace {

using namespace cms_rtf;

std::vector<int32_t> BuildGrid() {
  std::vector<int32_t> grid;
  grid.reserve(kNumEndpoints);
  for (int32_t t = kAmOpenMs; t <= kAmCloseMs; t += kGridMs) grid.push_back(t);
  for (int32_t t = kPmOpenMs + kGridMs; t <= kPmCloseMs; t += kGridMs) grid.push_back(t);
  return grid;
}

struct Thresh { double xl, m; };

struct CaptureState {
  SymbolBook::Snapshot snap{};
  double feat[31]{};
  double mid = 0.0;
  int32_t act_ms = -1;
  bool set = false;
};

class SymbolRunner {
 public:
  SymbolRunner(int32_t symbol, double xl, double m, const std::vector<int32_t>& grid,
               std::ofstream* book_out, std::ofstream* flow_out)
      : symbol_(symbol), book_(IsSh(symbol)), flow_(xl, m), grid_(grid),
        book_out_(book_out), flow_out_(flow_out) {
    seed_mid_ = std::numeric_limits<double>::quiet_NaN();
  }

  void OnEvent(const EventRec& ev) {
    // 1) 捕获封口（事件前簿）
    const int pos = static_cast<int>(
        std::lower_bound(grid_.begin(), grid_.end(), ev.time_ms) - grid_.begin());
    const bool in_grid = pos < static_cast<int>(grid_.size());
    if (in_grid) {
      if (grid_pos_ < 0) {
        grid_pos_ = pos;
      } else if (pos > grid_pos_) {
        SealTo(pos);
        grid_pos_ = pos;
      }
    }
    // 2) 簿（全事件）3) 流量（会话口径自闭）4) 捕获更新（事件后簿；15:00 后的事件
    //    不更新捕获——与引擎 CaptureExchange 的 pos>=size 早退一致）
    book_.OnEvent(ev);
    flow_.OnEvent(ev, [this](int32_t ep, const double* bases) { EmitFlow(ep, bases); });
    last_act_ms_ = ev.time_ms;  // CMS 导出的事件全部为可动作事件（A/D/T）
    if (in_grid) {
      cap_.snap = book_.Capture();
      cap_.mid = ComputeBookFeatures(cap_.snap, cap_.feat);
      cap_.act_ms = last_act_ms_;
      cap_.set = true;
      if (ev.time_ms <= kMarketOpenMs) {
        seed_mid_ = cap_.mid;  // 开盘收益前值：09:30:00 前最后捕获（不含 RefInScope）
      }
    }
  }

  void Finish() {
    flow_.Finish([this](int32_t ep, const double* bases) { EmitFlow(ep, bases); });
    if (grid_pos_ >= 0) {
      SealTo(static_cast<int>(grid_.size()));  // 股流结束：剩余端点沿用最终捕获
    }
  }

  double seed_mid() const { return seed_mid_; }
  const BookHealth& health() const { return book_.health(); }
  int64_t non_monotonic() const { return flow_.non_monotonic(); }

 private:
  void SealTo(int pos) {
    if (!cap_.set) {
      return;
    }
    const bool valid = cap_.snap.valid && RefInScope(symbol_);
    BookRec rec{};
    rec.symbol = symbol_;
    rec.act_ms = cap_.act_ms;
    rec.valid = valid ? 1 : 0;
    if (valid) {
      std::memcpy(rec.feat, cap_.feat, sizeof(rec.feat));
      rec.mid = cap_.mid;
    } else {
      for (double& f : rec.feat) f = std::numeric_limits<double>::quiet_NaN();
      rec.mid = std::numeric_limits<double>::quiet_NaN();
    }
    for (int e = grid_pos_; e < pos; ++e) {
      rec.ep_ms = grid_[e];
      book_out_->write(reinterpret_cast<const char*>(&rec), sizeof(rec));
    }
  }

  void EmitFlow(int32_t ep, const double* bases) {
    FlowRec rec{};
    rec.symbol = symbol_;
    rec.ep_ms = ep;
    std::memcpy(rec.bases, bases, sizeof(rec.bases));
    flow_out_->write(reinterpret_cast<const char*>(&rec), sizeof(rec));
  }

  int32_t symbol_;
  SymbolBook book_;
  SymbolFlow flow_;
  const std::vector<int32_t>& grid_;
  std::ofstream* book_out_;
  std::ofstream* flow_out_;
  int grid_pos_ = -1;
  CaptureState cap_{};
  int32_t last_act_ms_ = -1;
  double seed_mid_;
};

}  // namespace

int main(int argc, char** argv) {
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
    // 布局与 python 侧 dtype [('symbol','<i4'),('xl','<f8'),('m','<f8')]（无对齐）一致：20B
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
  std::ofstream book_out(book_path, std::ios::binary);
  std::ofstream flow_out(flow_path, std::ios::binary);
  std::ofstream seeds_out(seeds_path, std::ios::binary);

  const auto grid = BuildGrid();
  int64_t n_events = 0, n_symbols = 0;
  BookHealth total_health;
  int64_t total_nonmono = 0;
  EventRec ev;
  SymbolRunner* runner = nullptr;
  int32_t cur_symbol = -1;
  while (events.read(reinterpret_cast<char*>(&ev), sizeof(ev))) {
    ++n_events;
    if (ev.symbol != cur_symbol) {
      if (runner != nullptr) {
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
        delete runner;
        ++n_symbols;
      }
      const auto it = thresh.find(ev.symbol);
      const double xl = it != thresh.end() ? it->second.xl : 0.0;
      const double m = it != thresh.end() ? it->second.m : 0.0;
      runner = new SymbolRunner(ev.symbol, xl, m, grid, &book_out, &flow_out);
      cur_symbol = ev.symbol;
    }
    runner->OnEvent(ev);
  }
  if (runner != nullptr) {
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
    delete runner;
    ++n_symbols;
  }

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

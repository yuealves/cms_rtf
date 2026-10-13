#pragma once

// 每股批处理处理器：簿 + 流量 + 端点捕获（从 app/main_day.cpp 抽出共享）。
// 语义契约见 docs/design.md；线程模型：一个 runner 只属于一个 worker 线程，
// 无共享状态、无锁。

#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <vector>

#include "cms_rtf/book.h"
#include "cms_rtf/book_features.h"
#include "cms_rtf/events.h"
#include "cms_rtf/flow.h"

namespace cms_rtf {

inline std::vector<int32_t> BuildGrid() {
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

class RecordSink {
 public:
  virtual ~RecordSink() = default;
  virtual void Book(const BookRec& record) = 0;
  virtual void Flow(const FlowRec& record) = 0;
  virtual void BookRange(BookRec record, const std::vector<int32_t>& grid, int begin, int end) {
    for (int e = begin; e < end; ++e) { record.ep_ms = grid[e]; Book(record); }
  }
};

class SymbolRunner {
 public:
  SymbolRunner(int32_t symbol, double xl, double m, const std::vector<int32_t>& grid,
               std::vector<BookRec>* book_out, std::vector<FlowRec>* flow_out)
      : symbol_(symbol), book_(IsSh(symbol)), flow_(xl, m), grid_(grid),
        book_out_(book_out), flow_out_(flow_out) {
    seed_mid_ = std::numeric_limits<double>::quiet_NaN();
  }

  SymbolRunner(int32_t symbol, double xl, double m, const std::vector<int32_t>& grid,
               RecordSink* sink)
      : SymbolRunner(symbol, xl, m, grid, nullptr, nullptr) { sink_ = sink; }

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
    } else {
      // Freeze the last in-grid book before a post-close event mutates it.
      CapturePending();
    }
    // 2) 簿（全事件）3) 流量（会话口径自闭）4) 捕获更新（事件后簿；15:00 后的事件
    //    不更新捕获——与引擎 CaptureExchange 的 pos>=size 早退一致）
    book_.OnEvent(ev);
    flow_.OnEvent(ev, [this](int32_t ep, const double* bases) { EmitFlow(ep, bases); });
    last_act_ms_ = ev.time_ms;
    if (in_grid) {
      cap_.act_ms = last_act_ms_;
      cap_.set = true;
      capture_pending_ = true;
      if (ev.time_ms <= kMarketOpenMs) {
        // Same midpoint expression as ComputeBookFeatures, including invalid books.
        const auto snap = book_.Capture();
        seed_mid_ = snap.valid ? (snap.ask_px[0] + snap.bid_px[0]) / 2.0
                              : std::numeric_limits<double>::quiet_NaN();
      }
    }
  }

  void Finish() {
    flow_.Finish([this](int32_t ep, const double* bases) { EmitFlow(ep, bases); });
    if (grid_pos_ >= 0) {
      SealTo(static_cast<int>(grid_.size()));
    }
  }

  double seed_mid() const { return seed_mid_; }
  const BookHealth& health() const { return book_.health(); }
  int64_t non_monotonic() const { return flow_.non_monotonic(); }
  size_t live_orders() const { return book_.live_orders(); }
  size_t sh_order_identities() const { return flow_.sh_order_identities(); }
  size_t sh_identity_capacity_bytes() const { return flow_.sh_identity_capacity_bytes(); }

 private:
  void CapturePending() {
    if (capture_pending_) {
      cap_.snap = book_.Capture();
      cap_.mid = ComputeBookFeatures(cap_.snap, cap_.feat);
      capture_pending_ = false;
    }
  }

  void SealTo(int pos) {
    if (!cap_.set) {
      return;
    }
    CapturePending();
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
    if (sink_) sink_->BookRange(rec, grid_, grid_pos_, pos);
    else for (int e = grid_pos_; e < pos; ++e) { rec.ep_ms = grid_[e]; book_out_->push_back(rec); }
  }

  void EmitFlow(int32_t ep, const double* bases) {
    FlowRec rec{};
    rec.symbol = symbol_;
    rec.ep_ms = ep;
    std::memcpy(rec.bases, bases, sizeof(rec.bases));
    if (sink_) sink_->Flow(rec); else flow_out_->push_back(rec);
  }

  int32_t symbol_;
  SymbolBook book_;
  SymbolFlow flow_;
  const std::vector<int32_t>& grid_;
  std::vector<BookRec>* book_out_;
  std::vector<FlowRec>* flow_out_;
  RecordSink* sink_ = nullptr;
  int grid_pos_ = -1;
  CaptureState cap_{};
  bool capture_pending_ = false;
  int32_t last_act_ms_ = -1;
  double seed_mid_;
};

}  // namespace cms_rtf

#pragma once
#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>
#include "cms_rtf/events.h"

namespace cms_rtf {
// Huatai 546aba2 engine.cpp + index_500ms/family.cpp. All timestamps below
// are nanoseconds since Beijing midnight. Callback arguments: cutoff, timed.
class CloseSchedule {
 public:
  explicit CloseSchedule(const std::vector<int32_t>& grid) : grid_(grid) {}
  template <typename Close> void Frame(int64_t ns, Close&& close) {
    anchored_ = true; last_ = ns; maximum_ = std::max(maximum_, ns);
    Advance(ns, close);
  }
  template <typename Close> void Idle(int64_t ns, Close&& close) {
    if (!anchored_ || ns < 0 || ns >= 86400000000000LL) return;
    for (;;) {
      const int64_t regular = Regular();
      const bool r = next_ < grid_.size() && regular < ns && regular >= maximum_;
      const bool t = timer_ < 2 && Timer() <= ns;
      if (!r && !t) return;
      if (r && (!t || regular <= Timer())) { close(regular, false); ++next_; }
      else { close(Timer(), true); ++timer_; }
    }
  }
  template <typename Close> void Finish(Close&& close) {
    if (!anchored_) return;
    Advance(last_ + 1, close);
    // Exactly Huatai FireRemaining: local receives the remaining scheduled
    // cutoffs; these are NOT session timers and cannot invent exchange progress.
    while (next_ < grid_.size()) { close(Regular(), false); ++next_; }
    close(last_, false); // final actual event barrier; no unbounded tail at EOF
  }
 private:
  int64_t Regular() const {
    return next_ < grid_.size() ? int64_t(grid_[next_]) * 1000000 : tail_;
  }
  int64_t Timer() const {
    return timer_ < 2 ? (int64_t(timer_ == 0 ? kAmCloseMs : kPmCloseMs) + 60000) * 1000000
                      : std::numeric_limits<int64_t>::max();
  }
  template <typename Close> void Advance(int64_t ns, Close& close) {
    for (;;) {
      const int64_t r = Regular(), t = Timer();
      if (std::min(r, t) >= ns) return; // strictly greater than cutoff
      if (r <= t) {
        close(r, false);
        if (next_ < grid_.size()) ++next_; else tail_ += 500000000;
      } else { close(t, true); ++timer_; }
    }
  }
  const std::vector<int32_t>& grid_;
  size_t next_ = 0;
  int timer_ = 0;
  int64_t last_ = 0, maximum_ = 0, tail_ = (int64_t(kPmCloseMs) + kGridMs) * 1000000;
  bool anchored_ = false;
};
}

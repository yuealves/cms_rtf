#pragma once
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>
#include "cms_rtf/events.h"

namespace cms_rtf {
inline int64_t SteadyNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
inline int64_t WallNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

// Reader-thread-only accounting: fixed 28,800 slots, no per-event allocation/IO.
// Wall-clock receive latency and monotonic processing latency are kept separate.
class LatencyLog {
 public:
  LatencyLog(const std::string& directory, const std::string& mode);
  void Observe(int32_t event_ms, int emitted, int64_t recv_ns, int64_t read_ns) {
    if (!InSessionExchMs(event_ms)) return;
    const int i = Index(EndpointMs(event_ms));
    if (i < emitted) return;  // published history and suppressed late flow stay immutable
    auto& b = buckets_[i];
    ++b.events;
    if (recv_ns <= 0) ++b.missing_recv;
    else b.recv_ns = std::max(b.recv_ns, recv_ns);
    b.read_ns = std::max(b.read_ns, read_ns);
  }
  void BeginClose(const char* reason, const char* phase, int64_t start_ns) {
    reason_ = reason; phase_ = phase; start_ns_ = start_ns; ++close_id_;
  }
  // Called immediately AFTER successful WriteRecordBatch + FileOutputStream::Flush.
  void Written(const std::vector<int32_t>& endpoints, int64_t steady_ns, int64_t wall_ns);
 private:
  static int Index(int32_t ep) {
    return ep <= kAmCloseMs ? (ep - kAmOpenMs) / kGridMs
                            : kNumEndpoints / 2 + (ep - kPmOpenMs) / kGridMs - 1;
  }
  struct Bucket { int64_t events = 0, recv_ns = 0, read_ns = 0, missing_recv = 0; };
  struct CloseFile { void operator()(FILE* file) const { std::fclose(file); } };
  using File = std::unique_ptr<FILE, CloseFile>;
  File log_, details_;
  std::vector<Bucket> buckets_;
  std::string mode_;
  const char* reason_ = "", *phase_ = "";
  int64_t start_ns_ = 0, close_id_ = 0, flush_id_ = 0;
};
}

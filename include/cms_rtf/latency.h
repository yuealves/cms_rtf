#pragma once
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
namespace cms_rtf {
inline int64_t SteadyNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
// One measurement per engine cutoff, including cutoffs without factor rows.
// Begin before the worker barrier; Written after BOTH local/exch Arrow Flush.
class LatencyLog {
 public:
  explicit LatencyLog(const std::string& directory);
  void BeginClose(int64_t cutoff_ns_of_day, int64_t start_ns) {
    cutoff_ns_ = cutoff_ns_of_day; start_ns_ = start_ns;
  }
  void Written(int64_t done_ns);
 private:
  struct CloseFile { void operator()(FILE* file) const { std::fclose(file); } };
  std::unique_ptr<FILE, CloseFile> log_;
  int64_t cutoff_ns_ = 0, start_ns_ = 0;
};
}

#include "cms_rtf/latency.h"
#include <filesystem>
#include <stdexcept>
namespace cms_rtf {
LatencyLog::LatencyLog(const std::string& directory) {
  log_.reset(std::fopen((std::filesystem::path(directory) / "latency.log").c_str(), "wx"));
  if (!log_) throw std::runtime_error("latency log requires a new writable path");
}
void LatencyLog::Written(int64_t done_ns) {
  const int64_t ms = cutoff_ns_ / 1000000;
  std::fprintf(log_.get(), "%02d:%02d:%02d.%03d %.6g\n", int(ms / 3600000),
               int(ms / 60000 % 60), int(ms / 1000 % 60), int(ms % 1000),
               (done_ns - start_ns_) / 1e6);
  if (std::ferror(log_.get()) || std::fflush(log_.get()) != 0)
    throw std::runtime_error("cannot flush latency log");
}
}

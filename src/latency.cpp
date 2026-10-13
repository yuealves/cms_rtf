#include "cms_rtf/latency.h"
#include <filesystem>
#include <stdexcept>

namespace cms_rtf {
namespace {
std::string Hms(int32_t ms) {
  char s[20];
  std::snprintf(s, sizeof(s), "%02d:%02d:%02d.%03d", ms / 3600000,
                (ms / 60000) % 60, (ms / 1000) % 60, ms % 1000);
  return s;
}
void Flush(FILE* file) {
  if (std::ferror(file) || std::fflush(file) != 0)
    throw std::runtime_error("cannot flush latency log");
}
std::string Ms(int64_t ns) {
  char s[64]; std::snprintf(s, sizeof(s), "%.6f", ns / 1e6); return s;
}
}
LatencyLog::LatencyLog(const std::string& directory, const std::string& mode)
    : buckets_(kNumEndpoints), mode_(mode) {
  const std::filesystem::path root(directory);
  log_.reset(std::fopen((root / "latency.log").c_str(), "wx"));
  details_.reset(std::fopen((root / "latency.csv").c_str(), "wx"));
  File metadata(std::fopen((root / "latency.meta.json").c_str(), "wx"));
  if (!log_ || !details_ || !metadata) throw std::runtime_error("latency logs require new writable paths");
  std::fprintf(metadata.get(),
      "{\"schema\":\"cms-latency-v1\",\"unit\":\"ms\",\"factor_clock\":\"exch\",\"mode\":\"%s\","
      "\"latency_log\":\"endpoint_hms close_to_write_ms\","
      "\"write_boundary\":\"Arrow record batch write and Flush returned; no fsync\","
      "\"receive_scope\":\"latest gateway recv_ns among accepted constituent events in this right-closed exchange window\","
      "\"read_boundary\":\"journal frame observed committed, before hash/decode; DTYPE read completion per input batch\","
      "\"phase\":\"catchup until first journal idle; tail afterwards; replay for historical modes\","
      "\"empty_window\":\"receive/read latency absent; close latency still measured\","
      "\"clock_warning\":\"receive latency uses wall clock; signed values retained; replay receive latency absent\"}\n", mode_.c_str());
  Flush(metadata.get());
  std::fprintf(details_.get(), "endpoint,clock,mode,phase,reason,close_id,flush_id,close_to_write_ms,last_read_to_write_ms,last_recv_to_write_ms,last_recv_ns,last_read_steady_ns,write_wall_ns,write_steady_ns,close_start_steady_ns,events,missing_recv\n");
  Flush(details_.get());
}
void LatencyLog::Written(const std::vector<int32_t>& endpoints, int64_t steady_ns, int64_t wall_ns) {
  ++flush_id_;
  const std::string close = Ms(steady_ns - start_ns_);
  for (int32_t ep : endpoints) {
    const auto& b = buckets_.at(Index(ep));
    const auto hms = Hms(ep);
    const auto read = b.events && b.read_ns ? Ms(steady_ns - b.read_ns) : "";
    const auto recv = mode_ == "journal_follow_wall" && b.events && !b.missing_recv
        ? Ms(wall_ns - b.recv_ns) : "";
    std::fprintf(log_.get(), "%s %s\n", hms.c_str(), close.c_str());
    std::fprintf(details_.get(), "%s,exch,%s,%s,%s,%lld,%lld,%s,%s,%s,%lld,%lld,%lld,%lld,%lld,%lld,%lld\n",
                 hms.c_str(), mode_.c_str(), phase_, reason_, (long long)close_id_, (long long)flush_id_,
                 close.c_str(), read.c_str(), recv.c_str(), (long long)b.recv_ns, (long long)b.read_ns,
                 (long long)wall_ns, (long long)steady_ns, (long long)start_ns_,
                 (long long)b.events, (long long)b.missing_recv);
  }
  Flush(log_.get()); Flush(details_.get());
}
}

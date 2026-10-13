#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include "cms_rtf/dtype_row.h"

namespace cms_rtf {
struct JournalStats {
  uint64_t records = 0, rows = 0, sessions = 0, unclean_restarts = 0;
  uint64_t incomplete_records = 0, clock_inversions = 0;
  int page = 1;
  bool ended = false;
  int64_t source_clock_ns = 0;
};
// One read-only mapping at a time. A not-yet-written frame and a missing next
// page are idle, never EOF in follow mode. Control records deliver clock notices
// (action_code == 0), allowing session timers even with no stock events.
class JournalInput {
 public:
  enum class Result { Row, Idle, Eof };
  JournalInput(std::string directory, uint32_t day, bool follow);
  ~JournalInput();
  Result Next(DtypeRow& row);
  const JournalStats& stats() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}

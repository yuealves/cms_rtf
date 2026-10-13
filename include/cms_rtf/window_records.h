#pragma once

#include <stdexcept>
#include "cms_rtf/runner.h"

namespace cms_rtf {
// Owned by one worker. The aggregation barrier lends read/write access to the
// merger only after that worker has drained all events before the cutoff.
class WindowRecords : public RecordSink {
 public:
  explicit WindowRecords(size_t* pending, size_t limit) : pending_(pending), limit_(limit) {}
  std::vector<BookRec> books;
  std::vector<FlowRec> flows;
  BookRec current{};
  bool has_current = false;
  double previous_mid = std::numeric_limits<double>::quiet_NaN();
  double seed_mid = std::numeric_limits<double>::quiet_NaN();
  size_t book_pos = 0, flow_pos = 0;

  void Book(const BookRec& record) override {
    Account(sizeof(record)); books.push_back(record);
  }
  void Flow(const FlowRec& record) override {
    Account(sizeof(record)); flows.push_back(record);
  }
  void BookRange(BookRec record, const std::vector<int32_t>& grid, int begin, int end) override {
    if (begin < end) { record.ep_ms = grid[begin]; Book(record); }
  }
  void Consumed() {
    *pending_ -= book_pos * sizeof(BookRec) + flow_pos * sizeof(FlowRec);
    books.erase(books.begin(), books.begin() + book_pos);
    flows.erase(flows.begin(), flows.begin() + flow_pos);
    // Keep ordinary batches reusable; don't retain a pathological channel-lag
    // allocation for every stock after those pending windows have been consumed.
    if (books.capacity() > 256 && books.size() * 4 < books.capacity())
      std::vector<BookRec>(books).swap(books);
    if (flows.capacity() > 256 && flows.size() * 4 < flows.capacity())
      std::vector<FlowRec>(flows).swap(flows);
    book_pos = flow_pos = 0;
  }
 private:
  void Account(size_t bytes) {
    if (*pending_ + bytes > limit_)
      throw std::runtime_error("pending window budget exceeded: channel watermark stalled; no records were spilled or dropped");
    *pending_ += bytes;
  }
  size_t* pending_;
  size_t limit_;
};
}  // namespace cms_rtf

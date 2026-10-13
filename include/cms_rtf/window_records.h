#pragma once

#include <stdexcept>
#include <atomic>
#include "cms_rtf/runner.h"

namespace cms_rtf {
// Reserve payload credits in 64 KiB chunks, not an atomic operation per record.
// Share spare capacity across workers: symbol hashing does not balance closing
// auction records evenly. Pool capacity never changes and never spills to disk.
class PendingPool {
 public:
  explicit PendingPool(size_t limit) : limit_(limit) {}
  void Acquire(size_t bytes) {
    size_t used = reserved_.load(std::memory_order_relaxed);
    do {
      if (bytes > limit_ - used)
        throw std::runtime_error("pending window budget exceeded: channel watermark stalled; no records were spilled or dropped");
    } while (!reserved_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
  }
  void Release(size_t bytes) { reserved_.fetch_sub(bytes, std::memory_order_relaxed); }
 private:
  size_t limit_;
  std::atomic<size_t> reserved_{0};
};
class alignas(64) PendingBudget {
 public:
  explicit PendingBudget(PendingPool* pool) : pool_(pool) {}
  void Add(size_t bytes) {
    const size_t wanted = Round(used_ + bytes);
    if (wanted > reserved_) { pool_->Acquire(wanted - reserved_); reserved_ = wanted; }
    used_ += bytes;
    peak_ = std::max(peak_, used_);
  }
  void Subtract(size_t bytes) { used_ -= bytes; }
  void Reclaim() {
    const size_t wanted = Round(used_);
    pool_->Release(reserved_ - wanted); reserved_ = wanted;
  }
  size_t peak() const { return peak_; }
 private:
  static size_t Round(size_t size) { return (size + 65535) / 65536 * 65536; }
  PendingPool* pool_;
  size_t used_ = 0, reserved_ = 0, peak_ = 0;
};
// Owned by one worker. The aggregation barrier lends read/write access to the
// merger only after that worker has drained all events before the cutoff.
class WindowRecords : public RecordSink {
 public:
  explicit WindowRecords(PendingBudget* budget) : budget_(budget) {}
  std::vector<BookRec> books;
  std::vector<FlowRec> flows;
  BookRec current{};
  bool has_current = false;
  double previous_mid = std::numeric_limits<double>::quiet_NaN();
  double seed_mid = std::numeric_limits<double>::quiet_NaN();
  size_t book_pos = 0, flow_pos = 0;

  void Book(const BookRec& record) override {
    budget_->Add(sizeof(record)); books.push_back(record);
  }
  void Flow(const FlowRec& record) override {
    budget_->Add(sizeof(record)); flows.push_back(record);
  }
  void BookRange(BookRec record, const std::vector<int32_t>& grid, int begin, int end) override {
    if (begin < end) { record.ep_ms = grid[begin]; Book(record); }
  }
  void Consumed() {
    budget_->Subtract(book_pos * sizeof(BookRec) + flow_pos * sizeof(FlowRec));
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
  PendingBudget* budget_;
};
}  // namespace cms_rtf

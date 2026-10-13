#pragma once

// Append-only int64 identity table. SH identities must survive the whole session;
// removing filled orders would lose prior count/volume/price information. Values
// live in stable pages; the open-addressed index stores only 32-bit row handles.
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

namespace cms_rtf {
template<class Value> class IdentityTable {
 public:
  Value& operator[](int64_t key) {
    if (slots_.empty()) Rehash(16);
    size_t pos = Hash(key) & (slots_.size() - 1);
    while (slots_[pos]) {
      auto& entry = At(slots_[pos] - 1);
      if (entry.key == key) return entry.value;
      pos = (pos + 1) & (slots_.size() - 1);
    }
    if (size_ == std::numeric_limits<uint32_t>::max())
      throw std::runtime_error("SH identity table exceeds uint32 index capacity");
    if ((size_ + 1) * 4 > slots_.size() * 3) {
      Rehash(slots_.size() * 2);
      pos = Hash(key) & (slots_.size() - 1);
      while (slots_[pos]) pos = (pos + 1) & (slots_.size() - 1);
    }
    if (size_ % kPageEntries == 0) pages_.push_back(std::make_unique<Entry[]>(kPageEntries));
    auto& entry = At(size_);
    entry.key = key;
    slots_[pos] = static_cast<uint32_t>(++size_);
    return entry.value;
  }
  size_t size() const { return size_; }
  size_t capacity_bytes() const {
    return pages_.size() * kPageEntries * sizeof(Entry) + slots_.capacity() * sizeof(uint32_t) +
           pages_.capacity() * sizeof(std::unique_ptr<Entry[]>);
  }
 private:
  struct Entry { int64_t key = 0; Value value{}; };
  static constexpr size_t kPageEntries = 256;
  static uint64_t Hash(int64_t key) {
    uint64_t x = static_cast<uint64_t>(key);
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  }
  Entry& At(size_t row) { return pages_[row / kPageEntries][row % kPageEntries]; }
  void Rehash(size_t capacity) {
    std::vector<uint32_t> next(capacity);
    for (size_t row = 0; row < size_; ++row) {
      size_t pos = Hash(At(row).key) & (capacity - 1);
      while (next[pos]) pos = (pos + 1) & (capacity - 1);
      next[pos] = static_cast<uint32_t>(row + 1);
    }
    slots_.swap(next);
  }
  std::vector<uint32_t> slots_;
  std::vector<std::unique_ptr<Entry[]>> pages_;
  size_t size_ = 0;
};
}  // namespace cms_rtf

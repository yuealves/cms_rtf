#pragma once

// Versioned container for existing BookRec280 / FlowRec176 bytes. Append each
// bounded symbol block once; the EOF index permits fixed-symbol aggregation
// without rewriting/sorting the full payload or retaining it in process RAM.
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace cms_rtf {

inline constexpr size_t kOutputBlockRecords = 512;

struct BlockFileHeader {
  char magic[8];
  uint32_t record_size;
  uint32_t index_entry_size;
};
struct BlockIndexEntry {
  int32_t symbol;
  uint32_t count;
  uint64_t offset;
};
struct BlockFileFooter {
  char magic[8];
  uint64_t index_offset;
  uint64_t index_count;
  uint64_t record_count;
};
static_assert(sizeof(BlockFileHeader) == 16 && sizeof(BlockIndexEntry) == 16 &&
              sizeof(BlockFileFooter) == 32, "block container layout changed");

struct CloseRecordFile { void operator()(FILE* f) const { std::fclose(f); } };
using RecordFile = std::unique_ptr<FILE, CloseRecordFile>;

inline void ReadRecordBytes(FILE* f, void* out, size_t size) {
  if (std::fread(out, 1, size, f) != size)
    throw std::runtime_error("unreadable or truncated record blocks");
}
inline void SeekRecordFile(FILE* f, uint64_t offset) {
  if (offset > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
      fseeko(f, static_cast<off_t>(offset), SEEK_SET))
    throw std::runtime_error("record block seek failed");
}

template<class T> class BlockWriter {
 public:
  explicit BlockWriter(const std::string& path)
      : buffer_(1 << 20), file_(std::fopen(path.c_str(), "wb")) {
    if (!file_) throw std::runtime_error("cannot open record blocks: " + path);
    if (std::setvbuf(file_.get(), buffer_.data(), _IOFBF, buffer_.size()))
      throw std::runtime_error("cannot configure record output buffer");
    BlockFileHeader header{};
    std::memcpy(header.magic, "CMSBLK01", 8);
    header.record_size = sizeof(T);
    header.index_entry_size = sizeof(BlockIndexEntry);
    Write(&header, sizeof(header));
  }
  void Append(const T* records, size_t count) {
    if (!count) return;
    if (!file_ || count > kOutputBlockRecords)
      throw std::runtime_error("invalid record block length");
    const auto offset = offset_;
    Write(records, count * sizeof(T));
    index_.push_back({records[0].symbol, static_cast<uint32_t>(count), offset});
    records_ += count;
  }
  void Close() {
    if (!file_) return;
    BlockFileFooter footer{};
    std::memcpy(footer.magic, "CMSIDX01", 8);
    footer.index_offset = offset_;
    footer.index_count = index_.size();
    footer.record_count = records_;
    Write(index_.data(), index_.size() * sizeof(BlockIndexEntry));
    Write(&footer, sizeof(footer));
    FILE* raw = file_.release();
    if (std::fclose(raw)) throw std::runtime_error("closing record blocks failed");
  }
  uint64_t records() const { return records_; }
  size_t index_capacity_bytes() const { return index_.capacity() * sizeof(BlockIndexEntry); }
 private:
  void Write(const void* data, size_t bytes) {
    if (bytes && std::fwrite(data, 1, bytes, file_.get()) != bytes)
      throw std::runtime_error("writing record blocks failed");
    offset_ += bytes;
  }
  std::vector<char> buffer_;  // Must outlive FILE, including exception cleanup.
  RecordFile file_;
  std::vector<BlockIndexEntry> index_;
  uint64_t offset_ = 0, records_ = 0;
};

// Indexed block reader holds metadata only; payload is read one symbol at a time.
template<class T> class BlockReader {
 public:
  explicit BlockReader(const std::string& path) : file_(std::fopen(path.c_str(), "rb")) {
    if (!file_) throw std::runtime_error("cannot open record blocks: " + path);
    const auto bytes = std::filesystem::file_size(path);
    if (bytes < sizeof(BlockFileHeader) + sizeof(BlockFileFooter))
      throw std::runtime_error("truncated record block container");
    BlockFileHeader header{};
    ReadRecordBytes(file_.get(), &header, sizeof(header));
    if (std::memcmp(header.magic, "CMSBLK01", 8) || header.record_size != sizeof(T) ||
        header.index_entry_size != sizeof(BlockIndexEntry))
      throw std::runtime_error("unsupported record block version/layout");
    SeekRecordFile(file_.get(), bytes - sizeof(BlockFileFooter));
    BlockFileFooter footer{};
    ReadRecordBytes(file_.get(), &footer, sizeof(footer));
    const uint64_t payload_end = bytes - sizeof(BlockFileFooter);
    if (std::memcmp(footer.magic, "CMSIDX01", 8) || footer.index_offset < sizeof(header) ||
        footer.index_offset > payload_end ||
        footer.index_count != (payload_end - footer.index_offset) / sizeof(BlockIndexEntry) ||
        (payload_end - footer.index_offset) % sizeof(BlockIndexEntry))
      throw std::runtime_error("missing or invalid record block index");
    index_.resize(footer.index_count);
    SeekRecordFile(file_.get(), footer.index_offset);
    ReadRecordBytes(file_.get(), index_.data(), index_.size() * sizeof(BlockIndexEntry));
    uint64_t next = sizeof(header), records = 0;
    for (const auto& block : index_) {
      if (!block.count || block.count > kOutputBlockRecords || block.symbol < 0 ||
          block.symbol >= 1000000 || block.offset != next ||
          next > footer.index_offset || block.count > (footer.index_offset - next) / sizeof(T))
        throw std::runtime_error("invalid record block extent");
      next += block.count * sizeof(T);
      records += block.count;
    }
    if (next != footer.index_offset || records != footer.record_count)
      throw std::runtime_error("record block counts disagree");
    std::stable_sort(index_.begin(), index_.end(), [](const auto& a, const auto& b) {
      return a.symbol < b.symbol;
    });
  }
  bool Has() const { return pos_ < index_.size(); }
  int32_t symbol() const { return index_[pos_].symbol; }
  void ReadSymbol(std::vector<T>& records) {
    records.clear();
    const int32_t sym = symbol();
    size_t end = pos_, count = 0;
    while (end < index_.size() && index_[end].symbol == sym) count += index_[end++].count;
    records.resize(count);
    size_t row = 0;
    while (pos_ < end) {
      const auto offset = index_[pos_].offset;
      size_t n = index_[pos_++].count;
      // Coalesce adjacent blocks for this symbol into one read.
      while (pos_ < end && index_[pos_].offset == offset + n * sizeof(T)) n += index_[pos_++].count;
      SeekRecordFile(file_.get(), offset);
      ReadRecordBytes(file_.get(), records.data() + row, n * sizeof(T));
      row += n;
    }
    for (const auto& record : records)
      if (record.symbol != sym) throw std::runtime_error("record block symbol mismatch");
  }
 private:
  RecordFile file_;
  std::vector<BlockIndexEntry> index_;
  size_t pos_ = 0;
};

}  // namespace cms_rtf

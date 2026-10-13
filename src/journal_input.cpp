#include "cms_rtf/journal_input.h"
#include "cms/core.hpp"
#include "cms/metrics.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace cms_rtf {
namespace {
// Packed layout adapted from Kungfu PageHeader.h / FrameHeader.h, Apache-2.0,
// Copyright 2017 taurus.ai (see third_party/cms_protocol/Kungfu-LICENSE).
// Fixed-width fields avoid pulling the writer/paged/Boost runtime into a reader.
#pragma pack(push, 1)
struct PageHeader {
  uint8_t status; char name[30]; int16_t number;
  int64_t start_ns, close_ns; int32_t frames, last_pos; int16_t version;
  int16_t reserved_short[3]; int64_t reserved_long[9];
};
struct FrameHeader {
  uint8_t status; int16_t source; int64_t nano; int32_t length;
  uint32_t hash; int16_t type; uint8_t last; int32_t request;
  int64_t extra_nano; int32_t error;
};
#pragma pack(pop)
static_assert(sizeof(PageHeader) == 137 && sizeof(FrameHeader) == 38, "Kungfu frame v2 ABI");
constexpr size_t kPageSize = 128ULL * 1024 * 1024;
// MurmurHash2, same seed (97) and arithmetic as Kungfu Hash.hpp; memcpy avoids
// the upstream implementation's unaligned uint32_t loads.
uint32_t Hash(const uint8_t* p, size_t n) {
  const uint32_t m = 0x5bd1e995;
  uint32_t h = 97 ^ static_cast<uint32_t>(n);
  while (n >= 4) {
    uint32_t k; std::memcpy(&k, p, 4);
    k *= m; k ^= k >> 24; k *= m; h = (h * m) ^ k;
    p += 4; n -= 4;
  }
  if (n == 3) h ^= uint32_t(p[2]) << 16;
  if (n >= 2) h ^= uint32_t(p[1]) << 8;
  if (n >= 1) { h ^= p[0]; h *= m; }
  h ^= h >> 13; h *= m; h ^= h >> 15;
  return h;
}
}

struct JournalInput::Impl {
  std::filesystem::path directory;
  uint32_t day;
  bool follow;
  int fd = -1, page = 1, frames = 0;
  uint8_t* map = nullptr;
  size_t pos = sizeof(PageHeader), next_row = 0;
  uint64_t epoch = 0;
  int64_t last_nano = 0;
  JournalStats stats;
  std::vector<DtypeRow> rows;
  std::chrono::steady_clock::time_point last_inventory{};

  Impl(std::string path, uint32_t d, bool f) : directory(std::move(path)), day(d), follow(f) {
    rows.reserve(32);
  }
  ~Impl() { Unmap(); }
  void Unmap() {
    if (map) { munmap(map, kPageSize); map = nullptr; }
    if (fd >= 0) { close(fd); fd = -1; }
  }
  std::filesystem::path PagePath(int n) const {
    return directory / ("yjj.cms_l2_raw_v1." + std::to_string(n) + ".journal");
  }
  void CheckMissingPage() {
    const auto now = std::chrono::steady_clock::now();
    if (follow && now - last_inventory < std::chrono::milliseconds(250)) return;
    last_inventory = now;
    if (!std::filesystem::exists(directory)) return;
    const std::string prefix = "yjj.cms_l2_raw_v1.";
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
      const auto name = entry.path().filename().string();
      if (entry.path().extension() != ".journal") continue;
      cms::require(name.rfind(prefix, 0) == 0, "unexpected journal in input directory");
      const std::string digits = name.substr(prefix.size(), name.size() - prefix.size() - 8);
      size_t used = 0; const int n = std::stoi(digits, &used);
      cms::require(used == digits.size() && n > 0 && n < 32767, "invalid journal page number");
      cms::require(n <= page, "missing journal page " + std::to_string(page));
    }
  }
  bool OpenPage() {
    if (!map) {
      fd = open(PagePath(page).c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0) {
        cms::require(errno == ENOENT, "cannot open journal page: " + PagePath(page).string());
        CheckMissingPage(); return false;
      }
      struct stat st{};
      cms::require(fstat(fd, &st) == 0, "cannot stat journal page");
      if (st.st_size == 0 && follow) { close(fd); fd = -1; return false; }
      cms::require(st.st_size == static_cast<off_t>(kPageSize), "invalid journal page size");
      void* p = mmap(nullptr, kPageSize, PROT_READ, MAP_SHARED, fd, 0);
      cms::require(p != MAP_FAILED, "cannot mmap journal page");
      map = static_cast<uint8_t*>(p);
      madvise(map, kPageSize, MADV_SEQUENTIAL);
    }
    if (__atomic_load_n(map, __ATOMIC_ACQUIRE) == 0) return false;
    PageHeader header{}; std::memcpy(&header, map, sizeof(header));
    // Kungfu initializes frame_version immediately after publishing page status.
    if (header.version == 0 && follow) return false;
    cms::require(header.status == 1 && header.version == 2 && header.number == page &&
                 std::string(header.name, strnlen(header.name, 30)) == cms::journal_name,
                 "invalid journal page header/version");
    return true;
  }
  void Observe(const cms::Record& r, int64_t nano) {
    cms::require(r.seq == stats.records + 1, "journal record sequence mismatch");
    cms::require(r.day == day, "unexpected journal trading day");
    cms::require(stats.records || r.kind == cms::Start, "journal missing initial START");
    if (r.kind == cms::Start) {
      cms::require(!stats.sessions || r.epoch != epoch, "reused journal session epoch");
      if (stats.sessions && !stats.ended) ++stats.unclean_restarts;
      ++stats.sessions; epoch = r.epoch; stats.ended = false;
    } else {
      cms::require(!stats.ended && r.epoch == epoch, "journal epoch/END sequence mismatch");
      stats.ended = r.kind == cms::End;
    }
    ++stats.records;
    if (r.flags & cms::Incomplete) ++stats.incomplete_records;
    cms::require(nano > 0, "invalid journal timestamp");
    if (nano < last_nano) ++stats.clock_inversions;
    last_nano = nano;
    stats.source_clock_ns = std::max(stats.source_clock_ns, nano);
  }
  Result EndOrIdle() const {
    if (follow) return Result::Idle;
    cms::require(stats.records && stats.ended, "incomplete journal: missing final END");
    return Result::Eof;
  }
  Result Next(DtypeRow& row) {
    if (next_row < rows.size()) { row = rows[next_row++]; return Result::Row; }
    rows.clear(); next_row = 0;
    for (;;) {
      if (!OpenPage()) return EndOrIdle();
      cms::require(pos + sizeof(FrameHeader) <= kPageSize, "truncated journal page tail");
      const auto status = __atomic_load_n(map + pos, __ATOMIC_ACQUIRE);
      if (status == 0) return EndOrIdle();
      if (status == 2) {
        PageHeader h{}; std::memcpy(&h, map, sizeof(h));
        cms::require(h.close_ns > 0 && h.frames == frames && h.last_pos == static_cast<int>(pos),
                     "closed-page count/position mismatch");
        // Keep the finished mapping while waiting for the next page. A gap is
        // idle, never a reason to fall back to page 1 or replay previous frames.
        if (!std::filesystem::exists(PagePath(page + 1))) {
          ++page; CheckMissingPage(); --page;
          return EndOrIdle();
        }
        Unmap(); ++page; stats.page = page; frames = 0; pos = sizeof(PageHeader);
        continue;
      }
      FrameHeader h{}; std::memcpy(&h, map + pos, sizeof(h));
      cms::require(status == 1 && h.error == 0 && h.type == cms::journal_type,
                   "invalid CMS journal frame header");
      cms::require(h.length >= static_cast<int>(sizeof(h) + cms::envelope_size) &&
                   h.length <= static_cast<int>(sizeof(h) + cms::envelope_size + 65535) &&
                   pos + h.length <= kPageSize, "invalid journal frame length");
      const auto* payload = map + pos + sizeof(h);
      const size_t length = h.length - sizeof(h);
      cms::require(Hash(payload, length) == h.hash, "journal frame hash mismatch");
      auto r = cms::decode(payload, length);
      const auto nano = h.extra_nano > 0 ? h.extra_nano : h.nano;
      Observe(r, nano);
      pos += h.length; ++frames;
      DtypeRow base{};
      base.nano_timestamp = nano; base.recv_ns = r.recv_ns; base.epoch = r.epoch;
      base.record_seq = r.seq; base.recv_seq = r.recv_seq; base.trading_day = r.day;
      base.record_flags = r.flags;
      if (r.kind == cms::Data) {
        cms::require(!(r.flags & (cms::Truncated | cms::Malformed)), "malformed/truncated CMS packet");
        uint32_t index = 0;
        cms::parse_datagram(r.market, r.payload.data(), r.payload.size(), [&](const cms::Event& e) {
          const auto event_index = index++;
          if (e.action == 'H' || e.action == 'S') return;
          cms::require(e.symbol.size() == 6 && e.symbol.find_first_not_of("0123456789") == std::string::npos,
                       "invalid stock symbol");
          const auto exchange = cms::exchange_ns(e, r.day);
          cms::require(bool(exchange), "invalid exchange timestamp");
          cms::require(e.market != 2 || e.time / 1000000000ULL == day, "SZ trading day mismatch");
          auto v = base; v.exchange_ns = *exchange; v.business_seq = e.business_seq;
          v.buy_ref = e.buy; v.sell_ref = e.sell; v.exchange_time_raw = e.time;
          v.quantity_raw = e.quantity; v.value_raw = e.value; v.price_raw = e.price;
          v.feed_seq = e.feed_seq; v.event_index = event_index; v.channel = e.channel;
          v.market = e.market; v.message_type = e.type; v.action_code = e.action; v.side_raw = e.side;
          std::memcpy(v.symbol, e.symbol.data(), 6); rows.push_back(v); ++stats.rows;
        });
      }
      if (rows.empty()) rows.push_back(base);  // progress/END/heartbeat clock notice
      row = rows[next_row++]; return Result::Row;
    }
  }
};
JournalInput::JournalInput(std::string directory, uint32_t day, bool follow)
    : impl_(std::make_unique<Impl>(std::move(directory), day, follow)) {}
JournalInput::~JournalInput() = default;
JournalInput::Result JournalInput::Next(DtypeRow& row) { return impl_->Next(row); }
const JournalStats& JournalInput::stats() const { return impl_->stats; }
}

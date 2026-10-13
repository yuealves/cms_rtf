// CMS adaptation of light_rtf_v2 546aba2 replay/src/journal_replayer.cpp:275-309.
// Same steady_clock deadline = anchor + source_delta / speed, sleep_until pacing.
// Preserve CMS frame bytes/order instead of HT's three KyStd writers and sorting.
#include "cms_rtf/journal_format.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
using namespace cms_rtf::journal_format;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
int64_t Now() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}
void Require(bool ok, const std::string& what) { if (!ok) throw std::runtime_error(what); }
struct Page {
  int fd = -1;
  uint8_t* data = nullptr;
  Page(const fs::path& path, bool write) {
    fd = open(path.c_str(), write ? O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC : O_RDONLY | O_CLOEXEC, 0600);
    Require(fd >= 0, "cannot open page: " + path.string());
    struct stat st{};
    if ((write && ftruncate(fd, kPageSize)) || fstat(fd, &st) || st.st_size != static_cast<off_t>(kPageSize)) {
      close(fd); throw std::runtime_error("invalid page size: " + path.string());
    }
    void* p = mmap(nullptr, kPageSize, write ? PROT_READ | PROT_WRITE : PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) { close(fd); throw std::runtime_error("cannot mmap page"); }
    data = static_cast<uint8_t*>(p);
    if (!write) madvise(data, kPageSize, MADV_SEQUENTIAL);
  }
  Page(const Page&) = delete;
  Page& operator=(const Page&) = delete;
  ~Page() { if (data) munmap(data, kPageSize); if (fd >= 0) close(fd); }
};
}

int main(int argc, char** argv) try {
  fs::path source, dest;
  double speed = 1;
  int64_t start = 0, stop = 0;
  bool wait_ready = false;
  for (int i = 1; i < argc; ++i) {
    const std::string key = argv[i];
    if (key == "--wait-ready") { wait_ready = true; continue; }
    Require(i + 1 < argc, "missing argument value");
    const std::string value = argv[++i];
    if (key == "--source") source = value;
    else if (key == "--dest") dest = value;
    else if (key == "--speed") speed = std::stod(value);
    else if (key == "--pace-from-ns") start = std::stoll(value);
    else if (key == "--until-ns") stop = std::stoll(value);
    else throw std::runtime_error("unknown argument: " + key);
  }
  Require(!source.empty() && !dest.empty() && std::isfinite(speed) && speed > 0 && start > 0 && stop > start,
          "usage: cms_journal_replayer --source DIR --dest NEW_DIR --speed N --pace-from-ns NS --until-ns NS [--wait-ready]");
  source = fs::canonical(source); dest = fs::weakly_canonical(fs::absolute(dest));
  for (auto p = dest; !p.empty(); p = p.parent_path()) {
    Require(p != source, "destination must be separate from original journal");
    if (p == p.parent_path()) break;
  }
  fs::create_directories(dest.parent_path());
  Require(fs::create_directory(dest), "destination already exists");
  // Bounded 1-us histogram, with a final overflow bin for delays >= 1 second.
  std::vector<uint64_t> lag_hist(1000001);
  uint64_t records = 0, paced = 0, bytes = 0;
  int64_t anchor = 0, first = 0, last = 0, high = 0, lag_sum = 0, lag_max = 0;
  const auto began = Now();
  for (int page = 1; page < 32767; ++page) {
    const auto name = "yjj.cms_l2_raw_v1." + std::to_string(page) + ".journal";
    Page input(source / name, false), output(dest / name, true);
    PageHeader ph{}; std::memcpy(&ph, input.data, sizeof(ph));
    Require(ph.status == 1 && ph.number == page && ph.version == 2 &&
            std::string(ph.name, strnlen(ph.name, sizeof(ph.name))) == "cms_l2_raw_v1", "invalid source page header");
    std::memcpy(output.data + 1, input.data + 1, sizeof(ph) - 1);
    __atomic_store_n(output.data, uint8_t{1}, __ATOMIC_RELEASE);
    size_t pos = sizeof(PageHeader);
    int frames = 0;
    while (true) {
      Require(pos + sizeof(FrameHeader) <= kPageSize, "invalid source page tail");
      const auto state = input.data[pos];
      if (state == 2) {
        Require(ph.close_ns > 0 && ph.frames == frames && ph.last_pos == static_cast<int>(pos), "invalid closed page");
        __atomic_store_n(output.data + pos, uint8_t{2}, __ATOMIC_RELEASE);
        break;
      }
      Require(state == 1, "source ended before requested cutoff");
      FrameHeader h{}; std::memcpy(&h, input.data + pos, sizeof(h));
      Require(h.type == 22001 && !h.error && h.length >= static_cast<int>(sizeof(h) + 96) &&
              h.length <= static_cast<int>(sizeof(h) + 96 + 65535) && pos + h.length <= kPageSize, "invalid source frame");
      const int64_t nano = h.extra_nano > 0 ? h.extra_nano : h.nano;
      Require(nano > 0, "invalid source clock");
      if (nano > stop) {
        Require(paced > 0, "no records in pacing interval");
        const auto elapsed = Now() - anchor;
        auto percentile = [&](double q) {
          const auto target = static_cast<uint64_t>(q * (paced - 1));
          uint64_t count = 0;
          for (size_t us = 0; us < lag_hist.size(); ++us) {
            count += lag_hist[us]; if (count > target) return us / 1000.0;
          }
          return 1000.0;
        };
        std::cout << std::setprecision(17)
          << "{\"producer\":\"cpp\",\"records\":" << records << ",\"paced_records\":" << paced
          << ",\"bytes\":" << bytes << ",\"pages\":" << page << ",\"speed_requested\":" << speed
          << ",\"start_source_ns\":" << start << ",\"last_source_ns\":" << last
          << ",\"first_paced_source_ns\":" << first << ",\"elapsed_paced_seconds\":" << elapsed / 1e9
          << ",\"source_seconds\":" << (last - start) / 1e9 << ",\"effective_speed\":" << double(last - start) / elapsed
          << ",\"warmup_seconds\":" << (anchor - began) / 1e9 << ",\"anchor_steady_ns\":" << anchor
          << ",\"scheduler_lag_ms\":{\"mean\":" << double(lag_sum) / paced / 1e6
          << ",\"p50\":" << percentile(.5) << ",\"p99\":" << percentile(.99)
          << ",\"max\":" << lag_max / 1e6 << ",\"over_1s\":" << lag_hist.back()
          << "},\"stopped_before_source_ns\":" << nano << ",\"prefix_only\":true,\"synthesized_end\":false}\n";
        return 0;
      }
      if (nano >= start && !anchor) {
        if (wait_ready) {
          std::cout << "{\"ready_records\":" << records << "}" << std::endl;
          std::string line;
          Require(bool(std::getline(std::cin, line)) && line == "go", "missing consumer readiness acknowledgement");
        }
        anchor = Now(); first = nano;
      }
      int64_t deadline = 0;
      if (anchor) {
        high = std::max(high, nano);
        deadline = anchor + static_cast<int64_t>((high - start) / speed);
        std::this_thread::sleep_until(Clock::time_point(std::chrono::nanoseconds(deadline)));
      }
      std::memcpy(output.data + pos + 1, input.data + pos + 1, h.length - 1);
      __atomic_store_n(output.data + pos, uint8_t{1}, __ATOMIC_RELEASE);
      if (anchor) {
        const auto lag = std::max<int64_t>(0, Now() - deadline);
        ++lag_hist[std::min<size_t>(lag / 1000, lag_hist.size() - 1)];
        lag_sum += lag; lag_max = std::max(lag_max, lag); ++paced;
      }
      ++records; ++frames; bytes += h.length; last = nano; pos += h.length;
    }
  }
  throw std::runtime_error("page limit exceeded");
} catch (const std::exception& ex) {
  std::cerr << "journal replay: " << ex.what() << '\n';
  return 1;
}

// Native equivalent of aggregate.py. Formula source: light_rtf v2
// src/factors/index_500ms/aggregate.cpp (546aba2). Keep symbol order, separate
// multiply/add, Float32 publication and Float64 rolling sums without subtraction.
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <unistd.h>

#include "cms_rtf/catalog.h"
#include "cms_rtf/events.h"
#include "cms_rtf/aggregate.h"
#include "cms_rtf/record_blocks.h"

namespace {
using namespace cms_rtf;
using namespace cms_rtf::aggregation;
using Clock = std::chrono::steady_clock;
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
double Seconds(Clock::time_point start) { return std::chrono::duration<double>(Clock::now() - start).count(); }
template<class T> void Read(std::istream& in, T& v) { if (!in.read(reinterpret_cast<char*>(&v), sizeof(v))) throw std::runtime_error("truncated reference/seed file"); }
// Bounded buffered readers, not a full-day mmap: consumed records don't remain
// in the process RSS. Worker files are emitted in ascending symbol order.
template<class T> class Reader {
 public:
  explicit Reader(const std::string& path) : path_(path), buffer_(4096) {
    if (std::filesystem::file_size(path) % sizeof(T))
      throw std::runtime_error("truncated records: " + path);
    file_.reset(std::fopen(path.c_str(), "rb"));
    if (!file_) throw std::runtime_error("cannot open " + path);
    Advance();
  }
  bool Has() const { return pos_ < count_; }
  const T& Current() const { return buffer_[pos_]; }
  void Advance() {
    if (Has()) { previous_ = Current().symbol; ++pos_; }
    if (pos_ == count_) {
      const size_t bytes = std::fread(buffer_.data(), 1, buffer_.size() * sizeof(T), file_.get());
      if (std::ferror(file_.get()) || bytes % sizeof(T))
        throw std::runtime_error("unreadable/truncated records: " + path_);
      count_ = bytes / sizeof(T);
      pos_ = 0;
    }
    if (Has() && Current().symbol < previous_)
      throw std::runtime_error("worker records must be sorted by symbol: " + path_);
  }
 private:
  struct Close { void operator()(FILE* f) const { std::fclose(f); } };
  std::string path_;
  std::unique_ptr<FILE, Close> file_;
  std::vector<T> buffer_;
  size_t pos_ = 0, count_ = 0;
  int32_t previous_ = -1;
};

struct InputPath { std::string path; bool blocks = false; };
template<class T> class SymbolReader {
 public:
  explicit SymbolReader(const InputPath& input) {
    if (input.blocks) blocks_ = std::make_unique<BlockReader<T>>(input.path);
    else legacy_ = std::make_unique<Reader<T>>(input.path);
  }
  bool Has() const { return blocks_ ? blocks_->Has() : legacy_->Has(); }
  int32_t symbol() const { return blocks_ ? blocks_->symbol() : legacy_->Current().symbol; }
  void ReadSymbol(std::vector<T>& records) {
    if (blocks_) { blocks_->ReadSymbol(records); return; }
    records.clear();
    const auto sym = symbol();
    do {
      records.push_back(legacy_->Current());
      legacy_->Advance();
    } while (Has() && symbol() == sym);
  }
 private:
  std::unique_ptr<Reader<T>> legacy_;
  std::unique_ptr<BlockReader<T>> blocks_;
};

template<class T, class Fn> uint64_t ForSymbols(const std::vector<InputPath>& paths, Fn fn) {
  std::vector<std::unique_ptr<SymbolReader<T>>> readers;
  for (const auto& path : paths) readers.push_back(std::make_unique<SymbolReader<T>>(path));
  std::vector<T> records;
  uint64_t count = 0;
  while (true) {
    SymbolReader<T>* next = nullptr;
    // Stable ties preserve the Python concatenation/file order for repeated groups.
    for (auto& r : readers)
      if (r->Has() && (!next || r->symbol() < next->symbol())) next = r.get();
    if (!next) break;
    const int32_t symbol = next->symbol();
    next->ReadSymbol(records);
    count += records.size();
    fn(symbol, records);
  }
  return count;
}

void AddBook(int32_t symbol, const std::vector<BookRec>& records, const Reference& ref,
             const std::map<int32_t, double>& seeds, const std::vector<int32_t>& grid,
             Accumulators& out) {
  const auto weights = ref.weights.find(symbol);
  if (weights == ref.weights.end()) return;
  if (!std::is_sorted(records.begin(), records.end(),
                      [](const auto& a, const auto& b) { return a.ep_ms < b.ep_ms; }))
    throw std::runtime_error("book endpoints must be ascending");
  std::vector<const BookRec*> selected(grid.size());
  std::vector<double> returns(grid.size(), kNaN);
  double previous = seeds.count(symbol) ? seeds.at(symbol) : kNaN;
  size_t pos = 0;
  const BookRec* current = nullptr;
  for (size_t e = 0; e < grid.size(); ++e) {
    while (pos < records.size() && records[pos].ep_ms <= grid[e]) current = &records[pos++];
    selected[e] = current;
    const double mid = current ? current->mid : kNaN;
    if (std::isfinite(mid) && std::isfinite(previous))
      returns[e] = mid == previous && mid > 0 ? 0.0 : std::log(mid) - std::log(previous);
    previous = mid;  // Invalid previous midpoint must not fall back to an earlier valid one.
  }
  for (int idx = 0; idx < kNumIndices; ++idx) {
    const double w = weights->second[idx];
    if (w <= 0) continue;
    for (size_t e = 0; e < grid.size(); ++e) {
      const BookRec* r = selected[e];
      if (!r || !std::isfinite(r->mid) || r->act_ms < 0) continue;
      auto& a = out[idx][e];
      AddBookValue(r, returns[e], grid[e], w, a);
    }
  }
}

void AddFlow(int32_t symbol, const std::vector<FlowRec>& records, const Reference& ref,
             const std::vector<int32_t>& grid, Accumulators& out) {
  const auto weights = ref.weights.find(symbol);
  if (weights == ref.weights.end()) return;
  for (const auto& r : records) {
    const auto ep = std::lower_bound(grid.begin(), grid.end(), r.ep_ms);
    if (ep == grid.end() || *ep != r.ep_ms) throw std::runtime_error("flow endpoint outside grid");
    const size_t e = ep - grid.begin();
    for (int idx = 0; idx < kNumIndices; ++idx) {
      const double w = weights->second[idx];
      if (w <= 0) continue;
      auto& a = out[idx][e];
      AddFlowValue(r, w, a);
    }
  }
}

int Run(int argc, char** argv) {
  std::string reference, day, output;
  std::vector<InputPath> books, flows;
  std::vector<std::string> seed_paths;
  if (argc % 2 != 1) throw std::runtime_error("each option requires a value");
  for (int i = 1; i < argc; i += 2) {
    const std::string key = argv[i], value = argv[i + 1];
    if (key == "--ref") reference = value;
    else if (key == "--day") day = value;
    else if (key == "--out") output = value;
    else if (key == "--book") books.push_back({value, false});
    else if (key == "--flow") flows.push_back({value, false});
    else if (key == "--book-blocks") books.push_back({value, true});
    else if (key == "--flow-blocks") flows.push_back({value, true});
    else if (key == "--seed") seed_paths.push_back(value);
    else throw std::runtime_error("unknown argument: " + key);
  }
  if (reference.empty() || output.empty() || books.empty() || flows.empty() || seed_paths.empty())
    throw std::runtime_error("usage: --ref F --day YYYYMMDD --out F --book F --flow F --seed F (file options repeatable)");
  const int64_t day0 = DayStart(day);
  const auto started = Clock::now();
  const Reference ref = ReadReference(reference);
  const auto grid = Grid();
  std::map<int32_t, double> seeds;
  for (const auto& path : seed_paths) {
    const auto size = std::filesystem::file_size(path);
    if (size % 12) throw std::runtime_error("truncated seeds: " + path);
    std::ifstream in(path, std::ios::binary);
    for (uint64_t pos = 0; pos < size; pos += 12) {
      int32_t symbol; double mid;
      Read(in, symbol); Read(in, mid);
      seeds[symbol] = mid;
    }
  }
  Accumulators acc;
  for (auto& index : acc) index.resize(grid.size());
  auto t = Clock::now();
  const auto nbook = ForSymbols<BookRec>(books, [&](int32_t s, const auto& r) { AddBook(s, r, ref, seeds, grid, acc); });
  const double book_s = Seconds(t);
  t = Clock::now();
  const auto nflow = ForSymbols<FlowRec>(flows, [&](int32_t s, const auto& r) { AddFlow(s, r, ref, grid, acc); });
  const double flow_s = Seconds(t);
  t = Clock::now();
  RollingFinalizer finalizer;
  ArrowOutput output_stream(output, day0);
  double finalize_s = 0;
  for (int e = 0; e < kNumEndpoints; ++e) {
    std::array<Endpoint, kNumIndices> window;
    for (int i = 0; i < kNumIndices; ++i) window[i] = acc[i][e];
    const auto finalize_started = Clock::now();
    const auto row = finalizer.Publish(e, window, ref);
    finalize_s += Seconds(finalize_started);
    output_stream.Append(grid[e], row);
  }
  output_stream.Commit();
  const double write_s = Seconds(t) - finalize_s;
  std::cout << "{\"rows\": " << kNumEndpoints * kNumIndices << ", \"book_records\": " << nbook
            << ", \"flow_records\": " << nflow << ", \"book_seconds\": " << book_s
            << ", \"flow_seconds\": " << flow_s << ", \"finalize_seconds\": " << finalize_s
            << ", \"arrow_seconds\": " << write_s << ", \"total_seconds\": " << Seconds(started) << "}\n";
  return 0;
}
}  // namespace

int main(int argc, char** argv) {
  try { return Run(argc, argv); }
  catch (const std::exception& e) { std::cerr << "aggregate: " << e.what() << '\n'; return 1; }
}

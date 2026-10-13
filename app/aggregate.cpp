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

namespace {
using namespace cms_rtf;
using Clock = std::chrono::steady_clock;
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr double kMinCoverage = 0.8;  // upstream aggregate.cpp
constexpr int kNumIndices = 4;
constexpr int kNumBases = 21;
constexpr int kRollingLen = 10;

void Check(const arrow::Status& s) {
  if (!s.ok()) throw std::runtime_error(s.ToString());
}
template<class T> T Value(arrow::Result<T> r) {
  if (!r.ok()) throw std::runtime_error(r.status().ToString());
  return std::move(*r);
}
double Seconds(Clock::time_point start) {
  return std::chrono::duration<double>(Clock::now() - start).count();
}
template<class T> void Read(std::istream& in, T& v) {
  if (!in.read(reinterpret_cast<char*>(&v), sizeof(v)))
    throw std::runtime_error("truncated reference/seed file");
}

struct Reference {
  std::array<double, kNumIndices> official;
  std::map<int32_t, std::array<double, kNumIndices>> weights;
};
Reference ReadReference(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  char magic[8];
  if (!in.read(magic, 8) || std::memcmp(magic, "CMSAG01\0", 8))
    throw std::runtime_error("invalid aggregation reference version");
  uint32_t count;
  Read(in, count);
  if (count > 1000000 || std::filesystem::file_size(path) != 44ULL + count * 36ULL)
    throw std::runtime_error("invalid aggregation reference length");
  Reference ref;
  for (double& v : ref.official) {
    Read(in, v);
    if (!std::isfinite(v) || v < 0) throw std::runtime_error("invalid official weight");
  }
  int32_t previous = -1;
  for (uint32_t i = 0; i < count; ++i) {
    int32_t symbol;
    Read(in, symbol);
    if (symbol <= previous || symbol >= 1000000)
      throw std::runtime_error("reference symbols must be unique and ascending");
    previous = symbol;
    auto& w = ref.weights[symbol];
    for (double& v : w) {
      Read(in, v);
      if (!std::isfinite(v) || v < 0) throw std::runtime_error("invalid member weight");
    }
  }
  return ref;
}

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

template<class T, class Fn> uint64_t ForSymbols(const std::vector<std::string>& paths, Fn fn) {
  std::vector<std::unique_ptr<Reader<T>>> readers;
  for (const auto& path : paths) readers.push_back(std::make_unique<Reader<T>>(path));
  std::vector<T> records;
  uint64_t count = 0;
  while (true) {
    Reader<T>* next = nullptr;
    // Stable ties preserve the Python concatenation/file order for repeated groups.
    for (auto& r : readers)
      if (r->Has() && (!next || r->Current().symbol < next->Current().symbol)) next = r.get();
    if (!next) break;
    const int32_t symbol = next->Current().symbol;
    records.clear();
    do {
      records.push_back(next->Current());
      next->Advance();
    } while (next->Has() && next->Current().symbol == symbol);
    count += records.size();
    fn(symbol, records);
  }
  return count;
}

struct Endpoint {
  double num[32]{}, den[32]{};
  double valid_weight = 0, age = 0, fresh = 0;
  double flow[kNumBases]{}, active[3]{};
};
using Accumulators = std::array<std::vector<Endpoint>, kNumIndices>;
using FactorRow = std::array<float, 106>;

std::vector<int32_t> Grid() {
  std::vector<int32_t> grid;
  for (int t = kAmOpenMs; t <= kAmCloseMs; t += kGridMs) grid.push_back(t);
  for (int t = kPmOpenMs + kGridMs; t <= kPmCloseMs; t += kGridMs) grid.push_back(t);
  return grid;
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
      for (int c = 0; c < 31; ++c) {
        if (std::isfinite(r->feat[c])) {
          a.num[c] += w * r->feat[c];
          a.den[c] += w;
        }
      }
      if (std::isfinite(returns[e])) {
        a.num[31] += w * returns[e];
        a.den[31] += w;
      }
      a.valid_weight += w;
      const double age = (static_cast<int64_t>(grid[e]) - r->act_ms) / 1000.0;
      a.age += w * age;
      if (age < 0.5) a.fresh += w;
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
    const double* b = r.bases;
    const bool active[3] = {((b[0] + b[1]) + b[2]) + b[3] > 0,
                            ((b[9] + b[10]) + b[11]) + b[12] > 0,
                            ((b[13] + b[14]) + b[15]) + b[16] > 0};
    for (int idx = 0; idx < kNumIndices; ++idx) {
      const double w = weights->second[idx];
      if (w <= 0) continue;
      auto& a = out[idx][e];
      for (int c = 0; c < kNumBases; ++c) a.flow[c] += w * b[c];
      for (int c = 0; c < 3; ++c) if (active[c]) a.active[c] += w;
    }
  }
}

double Imbalance(double buy, double sell) {
  const double den = buy + sell;
  return den > 0 ? std::clamp((buy - sell) / den, -1.0, 1.0) : 0.0;
}
double Ratio(double num, double den) {
  return den > 0 ? std::clamp(num / den, 0.0, 1.0) : 0.0;
}
void Derived500(const double* b, const double* active, float* out) {
  const double values[] = {
    Imbalance(b[0], b[1]), Imbalance(b[2], b[3]), Imbalance(b[4], b[5]),
    Ratio(b[6], b[4] + b[5]), Ratio(b[7], b[4] + b[5]), Ratio(b[8], b[0] + b[1]), active[0],
    Imbalance(b[9], b[10]), Imbalance(b[11], b[12]), active[1],
    Imbalance(b[13], b[14]), Imbalance(b[15], b[16]), Imbalance(b[17], b[18]),
    Ratio(b[19], b[17] + b[18]), Ratio(b[20], b[17] + b[18]), active[2]};
  std::copy(std::begin(values), std::end(values), out);
}
void Derived5s(const double* b, float* out) {
  const double values[] = {
    Imbalance(b[0], b[1]), Imbalance(b[2], b[3]), Imbalance(b[4], b[5]),
    Ratio(b[6], b[4] + b[5]), Ratio(b[7], b[4] + b[5]),
    Imbalance(b[9], b[10]), Imbalance(b[11], b[12]),
    Imbalance(b[13], b[14]), Imbalance(b[15], b[16]), Imbalance(b[17], b[18]),
    Ratio(b[19], b[17] + b[18]), Ratio(b[20], b[17] + b[18]), Ratio(b[8], b[0] + b[1])};
  std::copy(std::begin(values), std::end(values), out);
}

std::array<std::vector<FactorRow>, kNumIndices> Finalize(const Accumulators& acc,
                                                      const Reference& ref) {
  std::array<std::vector<FactorRow>, kNumIndices> out;
  for (int idx = 0; idx < kNumIndices; ++idx) {
    out[idx].resize(kNumEndpoints);
    const double official = ref.official[idx];
    for (int e = 0; e < kNumEndpoints; ++e) {
      const auto& a = acc[idx][e];
      auto& row = out[idx][e];
      for (int c = 0; c < 32; ++c)
        row[c] = a.den[c] > 0 && a.den[c] / official >= kMinCoverage ? a.num[c] / a.den[c] : kNaN;
      const double coverage = official > 0 ? std::clamp(a.valid_weight / official, 0.0, 1.0) : 0;
      row[32] = coverage;
      row[33] = a.valid_weight > 0 && coverage >= kMinCoverage ? a.age / a.valid_weight : kNaN;
      row[34] = official > 0 ? std::clamp(a.fresh / official, 0.0, 1.0) : 0;
      double flow[kNumBases], active[3];
      for (int c = 0; c < kNumBases; ++c) row[35 + c] = flow[c] = a.flow[c] / official;
      for (int c = 0; c < 3; ++c) active[c] = a.active[c] / official;
      Derived500(flow, active, row.data() + 56);
      double rolling[kNumBases]{};
      const int session_start = e < 14400 ? 0 : 14400;
      // Chronological Float64 sum of the last <=10 published Float32 values,
      // matching NumPy's existing window order, without subtractive residuals.
      for (int t = std::max(session_start, e - kRollingLen + 1); t <= e; ++t)
        for (int c = 0; c < kNumBases; ++c) rolling[c] += out[idx][t][35 + c];
      for (int c = 0; c < kNumBases; ++c) row[72 + c] = rolling[c];
      Derived5s(rolling, row.data() + 93);
    }
  }
  return out;
}

int64_t DayStart(const std::string& day) {
  if (day.size() != 8 || day.find_first_not_of("0123456789") != std::string::npos)
    throw std::runtime_error("day must be YYYYMMDD");
  std::tm tm{};
  const int year = std::stoi(day.substr(0, 4)), month = std::stoi(day.substr(4, 2));
  const int date = std::stoi(day.substr(6, 2));
  tm.tm_year = year - 1900; tm.tm_mon = month - 1; tm.tm_mday = date;
  const auto seconds = timegm(&tm);
  if (tm.tm_year != year - 1900 || tm.tm_mon != month - 1 || tm.tm_mday != date)
    throw std::runtime_error("invalid day");
  return static_cast<int64_t>(seconds) * 1000000;  // naive Beijing wall-clock date
}

void WriteArrow(const std::string& path, int64_t day0, const std::vector<int32_t>& grid,
                const std::array<std::vector<FactorRow>, kNumIndices>& rows) {
  std::vector<std::shared_ptr<arrow::Field>> fields = {
    arrow::field("datetime", arrow::timestamp(arrow::TimeUnit::MICRO)), arrow::field("code", arrow::utf8())};
  for (const char* name : kColumnNames) fields.push_back(arrow::field(name, arrow::float32()));
  const auto schema = arrow::schema(fields);
  auto file = Value(arrow::io::FileOutputStream::Open(path));
  auto writer = Value(arrow::ipc::MakeStreamWriter(file, schema));
  constexpr int kBatchEndpoints = 1024;
  for (int lo = 0; lo < kNumEndpoints; lo += kBatchEndpoints) {
    const int hi = std::min(lo + kBatchEndpoints, kNumEndpoints);
    const int n = (hi - lo) * kNumIndices;
    arrow::TimestampBuilder datetime(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::StringBuilder code;
    Check(datetime.Reserve(n)); Check(code.Reserve(n));
    for (int e = lo; e < hi; ++e) for (int idx = 0; idx < kNumIndices; ++idx) {
      Check(datetime.Append(day0 + static_cast<int64_t>(grid[e]) * 1000));
      Check(code.Append(kIndexCodes[idx]));
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays = {Value(datetime.Finish()), Value(code.Finish())};
    for (int c = 0; c < 106; ++c) {
      arrow::FloatBuilder values;
      Check(values.Reserve(n));
      for (int e = lo; e < hi; ++e) for (int idx = 0; idx < kNumIndices; ++idx) {
        const float v = rows[idx][e][c];
        // Match PyArrow from_pandas=True: NaN is null; infinity stays a value.
        if (std::isnan(v)) Check(values.AppendNull()); else Check(values.Append(v));
      }
      arrays.push_back(Value(values.Finish()));
    }
    Check(writer->WriteRecordBatch(*arrow::RecordBatch::Make(schema, n, arrays)));
  }
  Check(writer->Close());
  Check(file->Close());
}

int Run(int argc, char** argv) {
  std::string reference, day, output;
  std::vector<std::string> books, flows, seed_paths;
  if (argc % 2 != 1) throw std::runtime_error("each option requires a value");
  for (int i = 1; i < argc; i += 2) {
    const std::string key = argv[i], value = argv[i + 1];
    if (key == "--ref") reference = value;
    else if (key == "--day") day = value;
    else if (key == "--out") output = value;
    else if (key == "--book") books.push_back(value);
    else if (key == "--flow") flows.push_back(value);
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
  auto rows = Finalize(acc, ref);
  const double finalize_s = Seconds(t);
  t = Clock::now();
  // A failed write must not replace a previously accepted output.
  const std::string temporary = output + ".tmp." + std::to_string(getpid());
  try {
    WriteArrow(temporary, day0, grid, rows);
    std::filesystem::rename(temporary, output);
  } catch (...) {
    std::error_code ignored;
    std::filesystem::remove(temporary, ignored);
    throw;
  }
  const double write_s = Seconds(t);
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

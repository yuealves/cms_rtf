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
#include <fcntl.h>

#include "cms_rtf/catalog.h"
#include "cms_rtf/events.h"
#include "cms_rtf/aggregate.h"

namespace cms_rtf::aggregation {
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr double kMinCoverage = 0.8;
void Check(const arrow::Status& s) {
  if (!s.ok()) throw std::runtime_error(s.ToString());
}
template<class T> T Value(arrow::Result<T> r) {
  if (!r.ok()) throw std::runtime_error(r.status().ToString());
  return std::move(*r);
}
template<class T> void Read(std::istream& in, T& v) {
  if (!in.read(reinterpret_cast<char*>(&v), sizeof(v)))
    throw std::runtime_error("truncated reference/seed file");
}

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

std::vector<int32_t> Grid() {
  std::vector<int32_t> grid;
  for (int t = kAmOpenMs; t <= kAmCloseMs; t += kGridMs) grid.push_back(t);
  for (int t = kPmOpenMs + kGridMs; t <= kPmCloseMs; t += kGridMs) grid.push_back(t);
  return grid;
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

void AddBookValue(const BookRec* r, double ret, int32_t ep, double w, Endpoint& a) {
  if (!r || !std::isfinite(r->mid) || r->act_ms < 0) return;
  for (int c = 0; c < 31; ++c) if (std::isfinite(r->feat[c])) {
    a.num[c] += w * r->feat[c]; a.den[c] += w;
  }
  if (std::isfinite(ret)) { a.num[31] += w * ret; a.den[31] += w; }
  a.valid_weight += w;
  const double age = (static_cast<int64_t>(ep) - r->act_ms) / 1000.0;
  a.age += w * age;
  if (age < 0.5) a.fresh += w;
}
void AddFlowValue(const FlowRec& r, double w, Endpoint& a) {
  const auto* b = r.bases;
  const bool active[] = {((b[0] + b[1]) + b[2]) + b[3] > 0,
                        ((b[9] + b[10]) + b[11]) + b[12] > 0,
                        ((b[13] + b[14]) + b[15]) + b[16] > 0};
  for (int c = 0; c < kNumBases; ++c) a.flow[c] += w * b[c];
  for (int c = 0; c < 3; ++c) if (active[c]) a.active[c] += w;
}
WindowRows RollingFinalizer::Publish(int e, const std::array<Endpoint, kNumIndices>& acc,
                                    const Reference& ref) {
  WindowRows out;
  for (int idx = 0; idx < kNumIndices; ++idx) {
    const auto& a = acc[idx];
    auto& row = out[idx];
    const double official = ref.official[idx];
    for (int c = 0; c < 32; ++c)
      row[c] = a.den[c] > 0 && a.den[c] / official >= kMinCoverage ? a.num[c] / a.den[c] : kNaN;
    const double coverage = official > 0 ? std::clamp(a.valid_weight / official, 0.0, 1.0) : 0;
    row[32] = coverage;
    row[33] = a.valid_weight > 0 && coverage >= kMinCoverage ? a.age / a.valid_weight : kNaN;
    row[34] = official > 0 ? std::clamp(a.fresh / official, 0.0, 1.0) : 0;
    double flow[kNumBases], active[3];
    for (int c = 0; c < kNumBases; ++c) {
      row[35 + c] = flow[c] = a.flow[c] / official;
      ring_[idx][e % kRollingLen][c] = row[35 + c];
    }
    for (int c = 0; c < 3; ++c) active[c] = a.active[c] / official;
    Derived500(flow, active, row.data() + 56);
    double rolling[kNumBases]{};
    const int session_start = e < 14400 ? 0 : 14400;
    for (int t = std::max(session_start, e - kRollingLen + 1); t <= e; ++t)
      for (int c = 0; c < kNumBases; ++c) rolling[c] += ring_[idx][t % kRollingLen][c];
    for (int c = 0; c < kNumBases; ++c) row[72 + c] = rolling[c];
    Derived5s(rolling, row.data() + 93);
  }
  return out;
}

struct ArrowOutput::Impl {
  std::string destination, temporary;
  int64_t day0;
  uint64_t count = 0;
  bool committed = false;
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::io::FileOutputStream> file;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  std::vector<int32_t> endpoints;
  std::vector<WindowRows> rows;
  bool live;
  std::function<void(const std::vector<int32_t>&)> flush_observer;
  Impl(const std::string& path, int64_t day, bool publish_live) : destination(path),
      temporary(publish_live ? path : path + ".tmp." + std::to_string(getpid())), day0(day), live(publish_live) {
    std::vector<std::shared_ptr<arrow::Field>> fields = {
      arrow::field("datetime", arrow::timestamp(arrow::TimeUnit::MICRO)), arrow::field("code", arrow::utf8())};
    for (const char* name : kColumnNames) fields.push_back(arrow::field(name, arrow::float32()));
    schema = arrow::schema(fields);
    if (live) {
      const int fd = ::open(temporary.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0644);
      if (fd < 0) throw std::runtime_error("live output must be a new writable path: " + temporary);
      auto opened = arrow::io::FileOutputStream::Open(fd);
      if (!opened.ok()) { ::close(fd); throw std::runtime_error(opened.status().ToString()); }
      file = *opened;
    } else file = Value(arrow::io::FileOutputStream::Open(temporary));
    writer = Value(arrow::ipc::MakeStreamWriter(file, schema));
    endpoints.reserve(1024); rows.reserve(1024);
  }
  ~Impl() {
    if (!committed) {
      if (writer) (void)writer->Close();
      if (file) (void)file->Close();
      std::error_code ignored;
      if (!live) std::filesystem::remove(temporary, ignored);
    }
  }
};
ArrowOutput::ArrowOutput(const std::string& path, int64_t day0, bool live)
    : impl_(std::make_unique<Impl>(path, day0, live)) {}
ArrowOutput::~ArrowOutput() = default;
void ArrowOutput::SetFlushObserver(std::function<void(const std::vector<int32_t>&)> observer) {
  impl_->flush_observer = std::move(observer);
}
uint64_t ArrowOutput::rows() const { return impl_->count; }
void ArrowOutput::Append(int32_t ep, const WindowRows& rows) {
  impl_->endpoints.push_back(ep); impl_->rows.push_back(rows); impl_->count += kNumIndices;
  if (impl_->rows.size() == 1024) Flush();
}
void ArrowOutput::Flush() {
  auto& p = *impl_;
  if (p.rows.empty()) return;
  const int n = p.rows.size() * kNumIndices;
  arrow::TimestampBuilder datetime(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
  arrow::StringBuilder code;
  Check(datetime.Reserve(n)); Check(code.Reserve(n));
  for (size_t e = 0; e < p.rows.size(); ++e) for (int idx = 0; idx < kNumIndices; ++idx) {
    Check(datetime.Append(p.day0 + static_cast<int64_t>(p.endpoints[e]) * 1000));
    Check(code.Append(kIndexCodes[idx]));
  }
  std::vector<std::shared_ptr<arrow::Array>> arrays = {Value(datetime.Finish()), Value(code.Finish())};
  for (int c = 0; c < 106; ++c) {
    arrow::FloatBuilder values;
    Check(values.Reserve(n));
    for (const auto& window : p.rows) for (const auto& row : window) {
      const float v = row[c];
      if (std::isnan(v)) Check(values.AppendNull()); else Check(values.Append(v));
    }
    arrays.push_back(Value(values.Finish()));
  }
  Check(p.writer->WriteRecordBatch(*arrow::RecordBatch::Make(p.schema, n, arrays)));
  Check(p.file->Flush());
  if (p.flush_observer) p.flush_observer(p.endpoints);
  p.endpoints.clear(); p.rows.clear();
}
void ArrowOutput::Commit() {
  Flush();
  Check(impl_->writer->Close()); Check(impl_->file->Close());
  if (!impl_->live) std::filesystem::rename(impl_->temporary, impl_->destination);
  impl_->committed = true;
}
}  // namespace cms_rtf::aggregation

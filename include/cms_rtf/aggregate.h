#pragma once
#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "cms_rtf/events.h"
namespace cms_rtf::aggregation {
constexpr int kNumIndices = 4;
constexpr int kNumBases = 21;
constexpr int kRollingLen = 10;
struct Reference {
  std::array<double, kNumIndices> official;
  std::map<int32_t, std::array<double, kNumIndices>> weights;
};
Reference ReadReference(const std::string& path);
struct Endpoint {
  double num[32]{}, den[32]{};
  double valid_weight = 0, age = 0, fresh = 0;
  double flow[kNumBases]{}, active[3]{};
};
using Accumulators = std::array<std::vector<Endpoint>, kNumIndices>;
using FactorRow = std::array<float, 106>;
using WindowRows = std::array<FactorRow, kNumIndices>;
std::vector<int32_t> Grid();
int64_t DayStart(const std::string& day);
// Each caller adds stocks in ascending symbol order; never reduce worker sums.
void AddBookValue(const BookRec* r, double ret, int32_t ep, double weight, Endpoint& out);
void AddFlowValue(const FlowRec& r, double weight, Endpoint& out);
class RollingFinalizer {
 public:
  WindowRows Publish(int e, const std::array<Endpoint, kNumIndices>& acc, const Reference& ref);
 private:
  std::array<std::array<std::array<float, kNumBases>, kRollingLen>, kNumIndices> ring_{};
};
// The temporary is the final Arrow stream itself, not per-stock intermediate data.
// Commit atomically publishes it only after the whole input has succeeded.
class ArrowOutput {
 public:
  ArrowOutput(const std::string& path, int64_t day0, bool live = false);
  ~ArrowOutput();
  void Append(int32_t ep, const WindowRows& rows);
  void Flush();
  void Commit();
  uint64_t rows() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace cms_rtf::aggregation

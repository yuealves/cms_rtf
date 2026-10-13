#include "cms_rtf/latency.h"
#include "cms_rtf/close_schedule.h"
#include "cms_rtf/runner.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

int main() {
  char path[] = "/tmp/cms-latency-XXXXXX";
  assert(mkdtemp(path));
  {
    cms_rtf::LatencyLog log(path);
    log.BeginClose(34200500000000LL, 100000000);
    log.Written(103012346);
    log.BeginClose(41460000000000LL, 104000000); // includes a no-row session cutoff
    log.Written(104050000);
    std::ifstream f(std::string(path) + "/latency.log");
    std::string line;
    std::getline(f, line); assert(line == "09:30:00.500 3.01235");
    std::getline(f, line); assert(line == "11:31:00.000 0.05");
    assert(!std::getline(f, line));
    bool refused = false;
    try { cms_rtf::LatencyLog duplicate(path); } catch (...) { refused = true; }
    assert(refused);
  }
  std::filesystem::remove_all(path);

  using namespace cms_rtf;
  const auto grid = BuildGrid();
  CloseSchedule schedule(grid);
  std::vector<std::pair<int64_t, bool>> calls;
  auto close = [&](int64_t ns, bool timed) { calls.emplace_back(ns, timed); };
  schedule.Idle(34201000000000LL, close); assert(calls.empty()); // not anchored
  schedule.Frame(34200500000000LL, close); assert(calls.empty()); // right closed
  schedule.Frame(34200500000001LL, close); assert(calls.size() == 1);
  schedule.Frame(34200500000000LL, close); assert(calls.size() == 1); // inversion
  schedule.Idle(34201000000000LL, close); assert(calls.size() == 1);
  schedule.Idle(34201000000001LL, close); assert(calls.size() == 2);
  schedule.Frame(41460000000000LL, close);
  assert(calls.size() == 14400); // timer does not fire at exact frame time
  schedule.Idle(41460000000000LL, close);
  assert(calls.size() == 14401 && calls.back().second);
  schedule.Idle(46799999999999LL, close);
  assert(calls.size() == 14401); // no lunch polling
  schedule.Frame(54060000000001LL, close);
  // Regular tail wins ties at 15:01; explicit timer follows it.
  assert(calls[calls.size()-2] == std::make_pair(int64_t(54060000000000LL), false));
  assert(calls.back() == std::make_pair(int64_t(54060000000000LL), true));
  const auto size = calls.size();
  schedule.Idle(55000000000000LL, close); assert(calls.size() == size); // idle cannot create tails

  CloseSchedule eof(grid);
  calls.clear(); eof.Frame(34200200000000LL, close); eof.Finish(close);
  assert(calls.size() == 28801); // remaining local cutoffs + final actual snapshot
  for (const auto& c : calls) assert(!c.second); // EOF never invents session timers
  assert(calls.back().first == 34200200000000LL);
}

#include "cms_rtf/latency.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

int main() {
  char path[] = "/tmp/cms-latency-XXXXXX";
  assert(mkdtemp(path));
  {
    cms_rtf::LatencyLog log(path, "journal_follow_wall");
    const int ep = cms_rtf::kAmOpenMs;
    log.Observe(ep - 400, 0, 1000000000, 100000000);
    log.Observe(ep, 0, 1002000000, 101000000); // right boundary stays in this window
    log.Observe(ep - 100, 0, 1001000000, 103000000); // receive clock inverted
    log.Observe(ep - 1, 1, 9000000000, 200000000); // late event must not overwrite
    log.Observe(cms_rtf::kPmOpenMs, 0, 9000000000, 200000000); // outside session
    log.BeginClose("watermark", "tail", 104000000);
    log.Written({ep, ep + 500}, 107000000, 1012000000);
    std::ifstream f(std::string(path) + "/latency.csv");
    std::string line; std::getline(f, line); std::getline(f, line);
    // close=3ms; last read=4ms; latest gateway receive=10ms; 3 eligible events.
    assert(line == "09:30:00.500,exch,journal_follow_wall,tail,watermark,1,1,3.000000,4.000000,10.000000,1002000000,103000000,1012000000,107000000,104000000,3,0");
    std::getline(f, line);
    assert(line.find(",3.000000,,,0,0,") != std::string::npos); // empty window is not zero latency
    log.Observe(ep + 1001, 2, 2000000000, 110000000);
    log.BeginClose("eof", "catchup", 111000000);
    log.Written({ep + 1500}, 112000000, 1999000000); // signed wall clock anomaly retained
    std::getline(f, line);
    assert(line.find(",1.000000,2.000000,-1.000000,") != std::string::npos);
  }
  std::filesystem::remove_all(path); // this test's own temporary logs only
}

#pragma once
#include "cms/core.hpp"
#include <array>
#include <optional>

namespace cms {
// CLOCK_REALTIME receive time minus exchange time. SH resolution: 10 ms; SZ: 1 ms.
std::optional<int64_t> exchange_ns(const Event& event, uint32_t trading_day);
class SecondStats {
    struct Cell { uint64_t count=0, invalid_time=0; std::optional<int64_t> last; };
    std::array<Cell,4> cells{};
    int64_t second;
    bool partial=true;
    uint64_t clock_steps=0;
    std::function<void(const std::string&)> sink;
    void emit(bool final, const std::string& quality);
public:
    SecondStats(int64_t now, std::function<void(const std::string&)> output);
    bool due(int64_t now)const{return now/1000000000!=second;}
    void advance(int64_t now, const std::string& quality);
    void observe(const Event& event, uint32_t day, int64_t receive_ns);
    void finish(const std::string& quality);
};
}

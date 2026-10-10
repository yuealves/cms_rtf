// 流量状态机单元测试：SH 增量口径、SZ 补价、桶推进/封口、大小单分层
#include <cassert>
#include <cmath>
#include <cstdio>
#include <map>
#include <vector>

#include "cms_rtf/flow.h"

using namespace cms_rtf;

static EventRec MkOrder(int32_t sym, int64_t id, int8_t side, int8_t type,
                        int64_t px_tick, int64_t vol, int32_t t_ms, int32_t ch,
                        int32_t biz) {
  EventRec ev{};
  ev.symbol = sym;
  ev.kind = 0;
  ev.side = side;
  ev.order_type = type;
  ev.px_tick = px_tick;
  ev.volume = vol;
  ev.time_ms = t_ms;
  ev.channel = ch;
  ev.biz_index = biz;
  ev.ref1 = id;
  return ev;
}

static EventRec MkTrade(int32_t sym, int64_t buy_ref, int64_t sell_ref, int8_t side,
                        int8_t ttype, int64_t px_tick, int64_t vol, int32_t t_ms,
                        int32_t ch, int32_t biz) {
  EventRec ev{};
  ev.symbol = sym;
  ev.kind = 1;
  ev.side = side;
  ev.trade_type = ttype;
  ev.px_tick = px_tick;
  ev.volume = vol;
  ev.time_ms = t_ms;
  ev.channel = ch;
  ev.biz_index = biz;
  ev.ref1 = buy_ref;
  ev.ref2 = sell_ref;
  return ev;
}

struct Sink {
  std::map<int32_t, std::vector<double>> buckets;
  void push(int32_t ep, const double* bases) {
    buckets[ep] = std::vector<double>(bases, bases + kNumBases);
  }
};

int main() {
  // ---- SH：A + 主动成交碎片同桶聚合，封口结算金额（桶末已知价） ----
  {
    Sink sink;
    SymbolFlow f(200000.0, 50000.0);  // xl=20 万, m=5 万
    auto emit = [&](int32_t ep, const double* b) { sink.push(ep, b); };
    const int32_t t0 = 34'200'100;  // 09:30:00.100 → 桶 09:30:00.500
    // 委托 1：买 A 100 股 @10.00（09:30:00.100）
    f.OnEvent(MkOrder(600000, 1, kSideBuy, kShOrderAdd, 100000, 100, t0, 0, 1), emit);
    // 同桶内同一委托的主动成交碎片：40 @10.00、60 @10.01（09:30:00.200/.300）
    f.OnEvent(MkTrade(600000, 1, 9001, kSideBuy, 0, 100000, 40, 34'200'200, 0, 2), emit);
    f.OnEvent(MkTrade(600000, 1, 9002, kSideBuy, 0, 100100, 60, 34'200'300, 0, 3), emit);
    // 推进到下一桶的事件封口 09:30:00.500
    f.OnEvent(MkOrder(600000, 2, kSideSell, kShOrderAdd, 101000, 50, 34'200'700, 0, 4), emit);
    assert(sink.buckets.size() == 1);
    const auto& b = sink.buckets.begin()->second;
    assert(sink.buckets.begin()->first == 34'200'500);
    assert(b[0] == 1 && b[1] == 1);             // buy/sell count 各 1
    assert(b[2] == 100 + 100 && b[3] == 50);    // volume：A 100 + 主动 T 100；卖 50
    // money：碎片 100 股 × 桶末已知价 max(10.00,10.01)=10.01；卖 50×10.10
    assert(std::abs(b[4] - 100 * 10.01) < 1e-9);
    assert(std::abs(b[5] - 50 * 10.10) < 1e-9);
    assert(b[8] == 2);  // resolved count（SH count 即 resolved）
    // 大小单：已知累计量 200 股 × 10.01 = 2002 < 5 万 → small；卖 50×10.10=505 也 small
    assert(b[6] == 0.0);
    assert(std::abs(b[7] - (200 * 10.01 + 50 * 10.10)) < 1e-9);
    std::printf("flow sh basic ok\n");
  }

  // ---- SZ 市价单补价：严格前序对手方向成交价 ----
  {
    Sink sink;
    SymbolFlow f(1e12, -1.0);  // 关掉大小单分类
    auto emit = [&](int32_t ep, const double* b) { sink.push(ep, b); };
    const int32_t ch = 2011;
    // 对手方向（卖主动）成交 @20.00，先于市价买单
    f.OnEvent(MkTrade(1, 500, 600, 0, kSzTradeFill, 200000, 100, 34'200'050, ch, 10), emit);
    // 市价买单（'1'）：补价 = 20.00 × 1.001
    f.OnEvent(MkOrder(1, 700, kSideBuy, kSzOrderMarketCounterparty, 0, 200, 34'200'100, ch, 11), emit);
    // 限价卖单 @20.05
    f.OnEvent(MkOrder(1, 701, kSideSell, kSzOrderLimit, 200500, 50, 34'200'200, ch, 12), emit);
    // 下一桶事件封口
    f.OnEvent(MkOrder(1, 702, kSideBuy, kSzOrderLimit, 199900, 10, 34'200'700, ch, 13), emit);
    assert(sink.buckets.size() == 1);
    const auto& b = sink.buckets.begin()->second;
    assert(b[0] == 1 && b[2] == 200);
    assert(std::abs(b[4] - 200 * 20.00 * 1.001) < 1e-6);
    assert(b[8] == 2);  // 市价单（有补价）+ 限价卖都 resolved
    std::printf("flow sz backfill ok\n");
  }

  // ---- 午休与收盘封口 ----
  {
    Sink sink;
    SymbolFlow f(1e12, -1.0);
    auto emit = [&](int32_t ep, const double* b) { sink.push(ep, b); };
    f.OnEvent(MkOrder(1, 1, kSideBuy, kSzOrderLimit, 100000, 10, 41'399'900, 1, 1), emit);  // 11:29:59.900
    f.OnEvent(MkOrder(1, 2, kSideBuy, kSzOrderLimit, 100000, 10, 41'700'000, 1, 2), emit);  // 11:35 午休
    f.OnEvent(MkOrder(1, 3, kSideBuy, kSzOrderLimit, 100000, 10, 46'800'600, 1, 3), emit);  // 13:00:00.600
    f.OnEvent(MkOrder(1, 4, kSideBuy, kSzOrderLimit, 100000, 10, 54'000'000, 1, 4), emit);  // 15:00:00.000
    f.Finish(emit);
    // 桶：11:30:00.000（AM 尾）、13:00:01.000（PM 首）、15:00:00.000（收盘）；午休事件不产桶
    assert(sink.buckets.count(41'400'000) == 1);
    assert(sink.buckets.count(46'801'000) == 1);
    assert(sink.buckets.count(54'000'000) == 1);
    assert(sink.buckets.size() == 3);
    std::printf("flow session seals ok\n");
  }
  std::printf("all flow tests ok\n");
  return 0;
}

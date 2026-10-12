// cms_index500ms_mt：多线程批处理/实盘同构的指数 500ms 因子引擎。
//
//   [exporter DTYPE 流] → reader 线程（顺序读 + 映射）→ symbol hash 分发
//   → N 个 worker 线程（各自 symbol 子集的每股状态机）→ per-worker 记录文件
//
// 线程模型照华泰 light_rtf（dispatcher/SymbolWorker/固定顺序聚合）；
// 聚合在 Python 侧按 symbol 升序汇总 ⇒ 输出与 worker 数无关（位级一致）。
//
// 输入：cms_journal_export 的 128B DTYPE 行流（文件；头部 CMSPQ01\0 自动跳过）。
// 输出：work_dir/w{i}/{book,flow,seeds}.bin + 健康汇总 JSON（stdout）。
#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cms_rtf/engine.h"
#include "cms_rtf/events.h"
#include "cms_rtf/runner.h"

namespace {

using namespace cms_rtf;

// 与 cms_market_gateway/tools/archive_journal.py 的 DTYPE / cms_rtf journal_events
// JOURNAL_DTYPE 逐字节一致（128B）
struct DtypeRow {
  int64_t nano_timestamp, recv_ns, exchange_ns;
  uint64_t epoch, record_seq, recv_seq, business_seq, buy_ref, sell_ref,
      exchange_time_raw, quantity_raw, value_raw;
  uint32_t price_raw, feed_seq, trading_day, event_index;
  uint16_t channel;
  uint8_t market, message_type, action_code, side_raw, record_flags, _pad;
  char symbol[8];
};
static_assert(sizeof(DtypeRow) == 128, "DTYPE row layout changed");

inline int32_t parse_symbol(const char* s) {
  // S8 NUL 填充定长字符串
  char buf[9];
  std::memcpy(buf, s, 8);
  buf[8] = '\0';
  return std::atoi(buf);
}

inline int32_t hhmmssmmm_to_ms(int64_t v) {
  const int64_t hh = v / 10000000, mm = (v / 100000) % 100, ss = (v / 1000) % 100;
  return static_cast<int32_t>((hh * 3600 + mm * 60 + ss) * 1000 + v % 1000);
}

// DTYPE 行 → canonical EventRec（规则与 cms_rtf/python/cms_rtf/journal_events.py 一致）
EventRec map_row(const DtypeRow& r) {
  EventRec ev{};
  ev.symbol = parse_symbol(r.symbol);
  const bool is_sh = r.market == 1;
  ev.channel = r.channel;
  ev.biz_index = static_cast<int32_t>(r.business_seq);
  const int64_t hhmmssmmm = is_sh ? static_cast<int64_t>(r.exchange_time_raw) * 10
                                  : static_cast<int64_t>(r.exchange_time_raw) % 1000000000;
  ev.time_ms = hhmmssmmm_to_ms(hhmmssmmm);
  ev.px_tick = static_cast<int64_t>(r.price_raw) * (is_sh ? 10 : 1);
  ev.volume = static_cast<int64_t>(r.quantity_raw / (is_sh ? 1000 : 100));

  const bool is_trade = r.action_code == 'T';
  const bool is_add = r.action_code == 'A';
  const bool is_del = r.action_code == 'D';
  if (is_sh) {
    ev.side = r.side_raw == 0 ? kSideBuy : (r.side_raw == 1 ? kSideSell : kSideNeutral);
  } else {
    ev.side = r.side_raw == 49 ? kSideBuy : kSideSell;
  }
  if (is_trade || (is_del && !is_sh)) {
    ev.kind = 1;
    ev.trade_type = is_sh ? 0 : (is_del ? kSzTradeWithdraw : kSzTradeFill);
    ev.ref1 = static_cast<int64_t>(r.buy_ref);
    ev.ref2 = static_cast<int64_t>(r.sell_ref);
    if (is_del && !is_sh) {
      ev.side = r.buy_ref > 0 ? kSideBuy : kSideSell;  // SZ 撤单方向=非零引用位置
    }
    if (!is_sh && !is_del) {
      ev.side = 0;  // SZ 成交 side=0，下游按引用推断
    }
  } else {
    ev.kind = 0;
    ev.order_type = is_sh ? (is_add ? kShOrderAdd : kShOrderDelete)
                          : static_cast<int8_t>(r.value_raw & 0xff);  // SZ '1'/'2'/'U'
    ev.ref1 = static_cast<int64_t>(ev.side == kSideBuy ? r.buy_ref : r.sell_ref);
    ev.ref2 = 0;
  }
  return ev;
}

struct SymbolBuffers {           // 每股独立的输出缓冲（保证落盘按 symbol 连续）
  std::vector<BookRec> book;
  std::vector<FlowRec> flow;
};

struct WorkerCtx {
  SpscRing<EventRec, 1 << 16> ring;
  std::thread thread;
  std::unordered_map<int32_t, SymbolRunner*> states;
  std::unordered_map<int32_t, SymbolBuffers> bufs;
  BookHealth health;
  int64_t nonmono = 0;
  int64_t events = 0;
};

}  // namespace

int main(int argc, char** argv) {
  std::string input_path, thresh_path, work_dir;
  int workers = 8;
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string k = argv[i], v = argv[i + 1];
    if (k == "--input") input_path = v;
    else if (k == "--thresh") thresh_path = v;
    else if (k == "--work-dir") work_dir = v;
    else if (k == "--workers") workers = std::stoi(v);
    else { std::cerr << "unknown arg " << k << "\n"; return 2; }
  }
  if (input_path.empty() || thresh_path.empty() || work_dir.empty() ||
      workers < 1 || workers > 64) {
    std::cerr << "usage: --input F --thresh T --work-dir D [--workers N]\n";
    return 2;
  }

  std::unordered_map<int32_t, Thresh> thresh;
  {
    std::ifstream in(thresh_path, std::ios::binary);
    int32_t sym;
    Thresh t;
    while (in.read(reinterpret_cast<char*>(&sym), 4) &&
           in.read(reinterpret_cast<char*>(&t.xl), 8) &&
           in.read(reinterpret_cast<char*>(&t.m), 8)) {
      thresh.emplace(sym, t);
    }
  }

  const auto grid = BuildGrid();
  std::vector<std::unique_ptr<WorkerCtx>> ctxs(workers);
  std::atomic<bool> input_done{false};
  std::atomic<int64_t> spin_waits{0};

  for (int i = 0; i < workers; ++i) {
    auto ctx = std::make_unique<WorkerCtx>();
    ctx->thread = std::thread([&, i, ctx = ctx.get()]() {
      EventRec ev;
      auto finish_all = [&]() {
        // 按 symbol 升序连续落盘：聚合层的读法依赖组内连续
        std::ofstream book_out(work_dir + "/w" + std::to_string(i) + ".book.bin",
                               std::ios::binary);
        std::ofstream flow_out(work_dir + "/w" + std::to_string(i) + ".flow.bin",
                               std::ios::binary);
        std::ofstream seeds_out(work_dir + "/w" + std::to_string(i) + ".seeds.bin",
                                std::ios::binary);
        std::vector<int32_t> syms;
        syms.reserve(ctx->states.size());
        for (auto& [sym, runner] : ctx->states) {
          syms.push_back(sym);
        }
        std::sort(syms.begin(), syms.end());
        for (int32_t sym : syms) {
          SymbolRunner* runner = ctx->states[sym];
          runner->Finish();
          seeds_out.write(reinterpret_cast<const char*>(&sym), sizeof(sym));
          const double sm = runner->seed_mid();
          seeds_out.write(reinterpret_cast<const char*>(&sm), sizeof(sm));
          ctx->health.over_deduct += runner->health().over_deduct;
          ctx->health.d_unknown += runner->health().d_unknown;
          ctx->health.unknown_trade_ref += runner->health().unknown_trade_ref;
          ctx->health.late_passive_ref += runner->health().late_passive_ref;
          ctx->health.agg_skip += runner->health().agg_skip;
          ctx->health.crossed_book += runner->health().crossed_book;
          ctx->health.immediate_fill += runner->health().immediate_fill;
          ctx->nonmono += runner->non_monotonic();
          auto& buf = ctx->bufs[sym];
          book_out.write(reinterpret_cast<const char*>(buf.book.data()),
                         static_cast<std::streamsize>(buf.book.size() * sizeof(BookRec)));
          flow_out.write(reinterpret_cast<const char*>(buf.flow.data()),
                         static_cast<std::streamsize>(buf.flow.size() * sizeof(FlowRec)));
          delete runner;
        }
        ctx->states.clear();
        ctx->bufs.clear();
      };
      while (true) {
        if (ctx->ring.pop(ev)) {
          auto it = ctx->states.find(ev.symbol);
          if (it == ctx->states.end()) {
            const auto th = thresh.find(ev.symbol);
            auto& buf = ctx->bufs[ev.symbol];
            it = ctx->states
                     .emplace(ev.symbol,
                              new SymbolRunner(ev.symbol,
                                               th != thresh.end() ? th->second.xl : 0.0,
                                               th != thresh.end() ? th->second.m : 0.0,
                                               grid, &buf.book, &buf.flow))
                     .first;
          }
          it->second->OnEvent(ev);
          ++ctx->events;
        } else if (input_done.load(std::memory_order_acquire)) {
          break;
        } else {
          std::this_thread::yield();
        }
      }
      finish_all();
    });
    ctxs[i] = std::move(ctx);
  }

  // reader 线程：顺序读 + 映射 + hash 分发（背压：ring 满则自旋）
  FILE* in = std::fopen(input_path.c_str(), "rb");
  if (!in) {
    std::cerr << "cannot open " << input_path << "\n";
    return 1;
  }
  {
    char magic[8];
    if (std::fread(magic, 1, 8, in) != 8 || std::memcmp(magic, "CMSPQ01\0", 8) != 0) {
      std::fseek(in, 0, SEEK_SET);  // 无头部则从头读
    }
  }
  std::vector<DtypeRow> rows(1 << 16);
  int64_t n_events = 0;
  size_t got;
  while ((got = std::fread(rows.data(), sizeof(DtypeRow), rows.size(), in)) > 0) {
    for (size_t j = 0; j < got; ++j) {
      EventRec ev = map_row(rows[j]);
      if (thresh.find(ev.symbol) == thresh.end()) {
        continue;  // 非成分股不进引擎（与 codec 的成分股过滤一致）
      }
      const uint32_t mixed = static_cast<uint32_t>(ev.symbol) * 2654435761u;
      WorkerCtx& ctx = *ctxs[mixed % workers];
      while (!ctx.ring.push(ev)) {
        ++spin_waits;
        std::this_thread::yield();
      }
      ++n_events;
    }
  }
  std::fclose(in);
  input_done.store(true, std::memory_order_release);

  BookHealth total;
  int64_t nonmono = 0;
  for (auto& ctx : ctxs) {
    ctx->thread.join();
    total.over_deduct += ctx->health.over_deduct;
    total.d_unknown += ctx->health.d_unknown;
    total.unknown_trade_ref += ctx->health.unknown_trade_ref;
    total.late_passive_ref += ctx->health.late_passive_ref;
    total.agg_skip += ctx->health.agg_skip;
    total.crossed_book += ctx->health.crossed_book;
    total.immediate_fill += ctx->health.immediate_fill;
    nonmono += ctx->nonmono;
  }
  std::printf("{\"events\": %" PRId64 ", \"workers\": %d, \"spin_waits\": %" PRId64
              ", \"over_deduct\": %" PRId64 ", \"d_unknown\": %" PRId64
              ", \"unknown_trade_ref\": %" PRId64 ", \"late_passive_ref\": %" PRId64
              ", \"agg_skip\": %" PRId64 ", \"crossed_book\": %" PRId64
              ", \"immediate_fill\": %" PRId64 ", \"sz_non_monotonic\": %" PRId64 "}\n",
              n_events, workers, spin_waits.load(), total.over_deduct, total.d_unknown,
              total.unknown_trade_ref, total.late_passive_ref, total.agg_skip,
              total.crossed_book, total.immediate_fill, nonmono);
  return 0;
}

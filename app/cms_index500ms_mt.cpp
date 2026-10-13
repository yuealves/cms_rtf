// cms_index500ms_mt：多线程批处理/实盘同构的指数 500ms 因子引擎。
//
//   [exporter DTYPE 流] → reader 线程（顺序读 + 映射）→ symbol hash 分发
//   → N 个 worker（每股状态机）→ 内存窗口聚合 → 最终 Arrow（stream 模式）
//
// 线程模型照华泰 light_rtf（dispatcher/SymbolWorker/固定顺序聚合）；
// 聚合在 C++ 侧按 symbol 升序汇总 ⇒ 输出与 worker 数无关（位级一致）。
//
// 输入：cms_journal_export 的 128B DTYPE 行流（文件；头部 CMSPQ01\0 自动跳过）。
// stream 输出：单个最终 Arrow；blocks/legacy 显式保留离线记录文件作对照。
#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <atomic>
#include <cinttypes>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sys/resource.h>
#include <unistd.h>

#include "cms_rtf/engine.h"
#include "cms_rtf/events.h"
#include "cms_rtf/runner.h"
#include "cms_rtf/record_blocks.h"
#include "cms_rtf/window_records.h"
#ifdef CMS_RTF_HAS_ARROW
#include "cms_rtf/aggregate.h"
#endif
#include <cmath>
#include <condition_variable>
#include <mutex>
#include <poll.h>
#include <csignal>
#include "cms_rtf/dtype_row.h"
#include "cms_rtf/journal_input.h"
#include "cms_rtf/latency.h"

namespace {

using namespace cms_rtf;
volatile std::sig_atomic_t stopping = 0;
void Stop(int) { stopping = 1; }

// 与 cms_market_gateway/tools/archive_journal.py 的 DTYPE / cms_rtf journal_events
// JOURNAL_DTYPE 逐字节一致（128B）

inline int32_t parse_symbol(const char* s) {
  if (s[6] == 0) {
    int32_t value = 0;
    int i = 0;
    for (; i < 6 && s[i] >= '0' && s[i] <= '9'; ++i)
      value = value * 10 + (s[i] - '0');
    if (i == 6) return value;
  }
  // Preserve the headerless legacy input parser for other S8 strings.
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
EventRec map_row(const DtypeRow& r, int32_t symbol) {
  EventRec ev{};
  ev.symbol = symbol;
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

struct SymbolBuffers : RecordSink {
  std::vector<BookRec> book;
  std::vector<FlowRec> flow;
  BlockWriter<BookRec>* books = nullptr;
  BlockWriter<FlowRec>* flows = nullptr;

  void BookRange(BookRec record, const std::vector<int32_t>& grid, int begin, int end) override {
    if (books) {
      // Aggregate already carries the latest record forward to each endpoint.
      // A quiet span needs one capture, not repeated 280-byte copies. Keep act_ms
      // and midpoint unchanged: age and the return chain are evaluated on grid.
      if (begin < end) { record.ep_ms = grid[begin]; Book(record); }
    } else {
      RecordSink::BookRange(record, grid, begin, end);
    }
  }

  void Book(const BookRec& record) override {
    if (books && book.capacity() == 0) book.reserve(kOutputBlockRecords);
    book.push_back(record);
    if (books && book.size() == kOutputBlockRecords) { books->Append(book.data(), book.size()); book.clear(); }
  }
  void Flow(const FlowRec& record) override {
    if (flows && flow.capacity() == 0) flow.reserve(kOutputBlockRecords);
    flow.push_back(record);
    if (flows && flow.size() == kOutputBlockRecords) { flows->Append(flow.data(), flow.size()); flow.clear(); }
  }
  void Flush() {
    if (books) { books->Append(book.data(), book.size()); book.clear(); }
    if (flows) { flows->Append(flow.data(), flow.size()); flow.clear(); }
  }
};

struct WorkerCtx {
  SpscRing<EventRec, 1 << 16> ring;
  std::thread thread;
  std::unordered_map<int32_t, SymbolRunner*> states;
  std::unordered_map<int32_t, SymbolBuffers> bufs;
  std::unordered_map<int32_t, std::unique_ptr<WindowRecords>> windows;
  std::unique_ptr<PendingBudget> pending_budget;
  std::atomic<int> closed_target{0};
  std::exception_ptr error;
  BookHealth health;
  int64_t nonmono = 0;
  int64_t events = 0;
  double event_seconds = 0, close_seconds = 0;
  uint64_t output_capacity_at_eof = 0, sh_identities = 0, identity_capacity = 0, live_orders = 0;
};

}  // namespace

int Run(int argc, char** argv) {
  std::string input_path, thresh_path, work_dir;
  std::string output_mode = "blocks";
  int workers = 8, window_batch = 64, pending_mib = 256;
  std::string reference_path, day, output;
  std::string journal_path, journal_mode = "follow", journal_clock = "wall";
  std::string latency_mode = "auto";
  bool batch_specified = false;
  if (argc % 2 != 1) throw std::runtime_error("each option requires a value");
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string k = argv[i], v = argv[i + 1];
    if (k == "--input") input_path = v;
    else if (k == "--thresh") thresh_path = v;
    else if (k == "--work-dir") work_dir = v;
    else if (k == "--workers") workers = std::stoi(v);
    else if (k == "--output-mode") output_mode = v;
    else if (k == "--ref") reference_path = v;
    else if (k == "--day") day = v;
    else if (k == "--out") output = v;
    else if (k == "--window-batch") { window_batch = std::stoi(v); batch_specified = true; }
    else if (k == "--pending-mib") pending_mib = std::stoi(v);
    else if (k == "--journal") journal_path = v;
    else if (k == "--journal-mode") journal_mode = v;
    else if (k == "--journal-clock") journal_clock = v;
    else if (k == "--latency") latency_mode = v;
    else { std::cerr << "unknown arg " << k << "\n"; return 2; }
  }
  const bool journal_input = !journal_path.empty();
  const bool follow = journal_input && journal_mode == "follow";
  if (journal_input && !batch_specified) window_batch = 1;
  if (journal_mode != "follow" && journal_mode != "replay") throw std::runtime_error("invalid journal-mode");
  if (journal_clock != "source" && journal_clock != "wall") throw std::runtime_error("invalid journal-clock");
  if (journal_input && (!input_path.empty() || output_mode != "stream" || window_batch != 1))
    throw std::runtime_error("journal requires stream output, window-batch 1 and no --input");
  if ((!journal_input && input_path.empty()) || thresh_path.empty() || work_dir.empty() ||
      workers < 1 || workers > 64 || window_batch < 1 || window_batch > kNumEndpoints || pending_mib < 1 ||
      (output_mode != "blocks" && output_mode != "legacy" && output_mode != "stream")) {
    std::cerr << "usage: --input F --thresh T --work-dir D [--workers N] [--output-mode stream|blocks|legacy] [--ref F --day YYYYMMDD --out F] [--window-batch N] [--pending-mib N]\n";
    return 2;
  }

  const bool streaming = output_mode == "stream";
  if (latency_mode != "auto" && latency_mode != "on" && latency_mode != "off")
    throw std::runtime_error("latency must be auto, on or off");
  const bool track_latency = latency_mode == "on" || (latency_mode == "auto" && journal_input);
  if (track_latency && !streaming) throw std::runtime_error("latency requires stream output");
  std::unique_ptr<LatencyLog> latency;
#ifdef CMS_RTF_HAS_ARROW
  std::unique_ptr<aggregation::Reference> reference;
  std::unique_ptr<aggregation::ArrowOutput> arrow;
  aggregation::RollingFinalizer finalizer;
  if (streaming) {
    if (reference_path.empty() || day.empty() || output.empty())
      throw std::runtime_error("stream output requires --ref, --day and --out");
    reference = std::make_unique<aggregation::Reference>(aggregation::ReadReference(reference_path));
    arrow = std::make_unique<aggregation::ArrowOutput>(output, aggregation::DayStart(day), follow);
    if (track_latency) {
      const std::string mode = follow ? "journal_follow_" + journal_clock :
                              (journal_input ? "journal_replay" : "dtype_replay");
      auto directory = std::filesystem::path(output).parent_path();
      latency = std::make_unique<LatencyLog>(directory.empty() ? "." : directory.string(), mode);
      arrow->SetFlushObserver([&](const std::vector<int32_t>& endpoints) {
        const auto done_ns = SteadyNs();
        const auto wall_ns = WallNs();
        latency->Written(endpoints, done_ns, wall_ns);
      });
    }
  }
#else
  if (streaming) throw std::runtime_error("rebuild with CMS_RTF_BUILD_AGGREGATE=ON for stream output");
#endif
  std::unordered_map<int32_t, Thresh> thresh;
  {
    std::ifstream in(thresh_path, std::ios::binary);
    if (!in) { std::cerr << "cannot open thresholds\n"; return 1; }
    if (std::filesystem::file_size(thresh_path) % 20 != 0) {
      std::cerr << "truncated thresholds\n"; return 1;
    }
    int32_t sym;
    Thresh t;
    while (in.read(reinterpret_cast<char*>(&sym), 4) &&
           in.read(reinterpret_cast<char*>(&t.xl), 8) &&
           in.read(reinterpret_cast<char*>(&t.m), 8)) {
      thresh.emplace(sym, t);
    }
  }

#ifdef CMS_RTF_HAS_ARROW
  if (streaming) for (const auto& [symbol, weights] : reference->weights) {
    (void)weights;
    if (!thresh.count(symbol)) throw std::runtime_error("reference member missing thresholds");
  }
#endif
  if (!std::filesystem::is_directory(work_dir)) {
    std::cerr << "work directory must exist\n"; return 1;
  }
  FILE* in = journal_input ? nullptr : (input_path == "-" ? stdin : std::fopen(input_path.c_str(), "rb"));
  if (!journal_input && !in) { std::cerr << "cannot open " << input_path << "\n"; return 1; }
  // Avoid stdio reading ahead into records while parsing the header. The event
  // loop uses read(2), which returns available pipe bytes without waiting for an
  // entire 8 MiB batch (important for a continuously running journal producer).
  if (in) std::setvbuf(in, nullptr, _IONBF, 0);
  std::array<char, 8> prefix{};
  const size_t prefix_size = in ? std::fread(prefix.data(), 1, prefix.size(), in) : 0;
  const bool has_magic = prefix_size == 8 && std::memcmp(prefix.data(), "CMSPQ01\0", 8) == 0;
  if (input_path == "-" && !has_magic) {
    std::cerr << "stdin requires CMSPQ01 header\n"; return 1;
  }
  std::unique_ptr<JournalInput> journal;
  int64_t journal_day0_ns = 0;
  if (journal_input) {
#ifdef CMS_RTF_HAS_ARROW
    // Arrow stores Beijing wall time without a timezone; journal clocks are UTC.
    journal_day0_ns = (aggregation::DayStart(day) - 8LL * 3600 * 1000000) * 1000;
#endif
    journal = std::make_unique<JournalInput>(journal_path, std::stoul(day), follow, track_latency);
    std::signal(SIGINT, Stop); std::signal(SIGTERM, Stop);
  }
  std::vector<int8_t> dispatch(1000000, -1);
  for (const auto& item : thresh) {
    if (item.first < 0 || item.first >= static_cast<int32_t>(dispatch.size())) {
      std::cerr << "threshold symbol outside six-digit universe\n"; return 1;
    }
#ifdef CMS_RTF_HAS_ARROW
    if (streaming && !reference->weights.count(item.first)) continue;
#endif
    dispatch[item.first] = (static_cast<uint32_t>(item.first) * 2654435761u) % workers;
  }
  const auto started = std::chrono::steady_clock::now();
  const auto grid = BuildGrid();
  std::vector<std::unique_ptr<WorkerCtx>> ctxs(workers);
  std::atomic<bool> input_done{false};
  std::atomic<bool> worker_failed{false};
  std::atomic<int> resumed_target{0};
  std::mutex barrier_mu;
  std::condition_variable closed_cv, resumed_cv;
  auto fail_workers = [&] {
    { std::lock_guard<std::mutex> lock(barrier_mu); worker_failed.store(true); }
    closed_cv.notify_all(); resumed_cv.notify_all();
  };
  PendingPool pending_pool(static_cast<size_t>(pending_mib) * 1024 * 1024);
  int64_t spin_waits = 0;
  constexpr size_t kBatchSize = 256;

  for (int i = 0; i < workers; ++i) {
    auto ctx = std::make_unique<WorkerCtx>();
    ctx->pending_budget = std::make_unique<PendingBudget>(&pending_pool);
    ctx->thread = std::thread([&, i, ctx = ctx.get()]() {
      try {
      std::array<EventRec, kBatchSize> batch;
      unsigned idle_polls = 0;
      const std::string prefix = work_dir + "/w" + std::to_string(i);
      std::unique_ptr<BlockWriter<BookRec>> books;
      std::unique_ptr<BlockWriter<FlowRec>> flows;
      if (output_mode == "blocks") {
        books = std::make_unique<BlockWriter<BookRec>>(prefix + ".book.cmsb");
        flows = std::make_unique<BlockWriter<FlowRec>>(prefix + ".flow.cmsb");
      }
      auto finish_all = [&]() {
        if (streaming) {
          for (auto& [symbol, runner] : ctx->states) {
            const auto& window = *ctx->windows.at(symbol);
            ctx->output_capacity_at_eof += window.books.capacity() * sizeof(BookRec) + window.flows.capacity() * sizeof(FlowRec);
            ctx->sh_identities += runner->sh_order_identities();
            ctx->identity_capacity += runner->sh_identity_capacity_bytes();
            ctx->live_orders += runner->live_orders();
            const auto& h = runner->health();
            ctx->health.over_deduct += h.over_deduct;
            ctx->health.d_unknown += h.d_unknown;
            ctx->health.unknown_trade_ref += h.unknown_trade_ref;
            ctx->health.late_passive_ref += h.late_passive_ref;
            ctx->health.agg_skip += h.agg_skip;
            ctx->health.crossed_book += h.crossed_book;
            ctx->health.immediate_fill += h.immediate_fill;
            ctx->nonmono += runner->non_monotonic();
            delete runner;
          }
          ctx->states.clear();
          return;
        }
        // Legacy output remains available for byte-for-byte acceptance. Block
        // output has already been written during OnEvent; only tails/index remain.
        std::ofstream book_out, flow_out;
        if (!books) { book_out.open(prefix + ".book.bin", std::ios::binary); flow_out.open(prefix + ".flow.bin", std::ios::binary); }
        std::ofstream seeds_out(prefix + ".seeds.bin", std::ios::binary);
        book_out.exceptions(std::ios::failbit | std::ios::badbit);
        flow_out.exceptions(std::ios::failbit | std::ios::badbit);
        seeds_out.exceptions(std::ios::failbit | std::ios::badbit);
        std::vector<int32_t> syms;
        syms.reserve(ctx->states.size());
        for (auto& [sym, runner] : ctx->states) {
          syms.push_back(sym);
        }
        std::sort(syms.begin(), syms.end());
        for (int32_t sym : syms) {
          SymbolRunner* runner = ctx->states[sym];
          auto& buf = ctx->bufs[sym];
          ctx->output_capacity_at_eof += buf.book.capacity() * sizeof(BookRec) + buf.flow.capacity() * sizeof(FlowRec);
          ctx->sh_identities += runner->sh_order_identities();
          ctx->identity_capacity += runner->sh_identity_capacity_bytes();
          ctx->live_orders += runner->live_orders();
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
          if (books) { buf.Flush(); }
          else {
          book_out.write(reinterpret_cast<const char*>(buf.book.data()),
                         static_cast<std::streamsize>(buf.book.size() * sizeof(BookRec)));
          flow_out.write(reinterpret_cast<const char*>(buf.flow.data()),
                         static_cast<std::streamsize>(buf.flow.size() * sizeof(FlowRec)));
          }
          delete runner;
          ctx->bufs.erase(sym);
        }
        if (books) { books->Close(); flows->Close(); }
        else { book_out.close(); flow_out.close(); }
        seeds_out.close();
        ctx->states.clear();
        ctx->bufs.clear();
      };
      while (!worker_failed.load(std::memory_order_relaxed)) {
        size_t count = ctx->ring.pop_batch(batch.data(), batch.size());
        if (!count && input_done.load(std::memory_order_acquire)) {
          // Acquire completion before the final drain. The first empty check
          // may have raced with the producer's last publication.
          count = ctx->ring.pop_batch(batch.data(), batch.size());
          if (!count) break;
        }
        if (!count) {
          if (streaming && ++idle_polls > 8) std::this_thread::sleep_for(std::chrono::microseconds(25));
          else std::this_thread::yield();
          continue;
        }
        idle_polls = 0;
        const auto batch_started = std::chrono::steady_clock::now();
        double marker_seconds = 0;
        for (size_t k = 0; k < count; ++k) {
          const EventRec& ev = batch[k];
          if (ev.symbol < 0) {
            const auto marker_started = std::chrono::steady_clock::now();
            for (auto& [symbol, runner] : ctx->states) {
              if (ev.symbol == -2) runner->Finish(); else runner->CloseThrough(ev.time_ms);
              ctx->windows.at(symbol)->seed_mid = runner->seed_mid();
            }
            ctx->close_seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - marker_started).count();
            std::unique_lock<std::mutex> lock(barrier_mu);
            ctx->closed_target.store(ev.time_ms, std::memory_order_release);
            closed_cv.notify_one();
            resumed_cv.wait(lock, [&] { return resumed_target.load() >= ev.time_ms || worker_failed.load(); });
            marker_seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - marker_started).count();
            continue;
          }
          auto it = ctx->states.find(ev.symbol);
          if (it == ctx->states.end()) {
            const auto th = thresh.find(ev.symbol);
            RecordSink* sink;
            if (streaming) {
              auto window = std::make_unique<WindowRecords>(ctx->pending_budget.get());
              sink = window.get();
              ctx->windows.emplace(ev.symbol, std::move(window));
            } else {
              auto& buf = ctx->bufs[ev.symbol];
              buf.books = books.get(); buf.flows = flows.get();
              sink = &buf;
            }
            it = ctx->states
                     .emplace(ev.symbol,
                              new SymbolRunner(ev.symbol,
                                               th != thresh.end() ? th->second.xl : 0.0,
                                               th != thresh.end() ? th->second.m : 0.0,
                                               grid, sink))
                     .first;
            if (journal_input) it->second->EnableLive(ctx->closed_target.load(std::memory_order_relaxed));
          }
          it->second->OnEvent(ev);
          ++ctx->events;
        }
        ctx->event_seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - batch_started).count() - marker_seconds;
      }
      if (!worker_failed.load()) finish_all();
      } catch (...) {
        ctx->error = std::current_exception();
        fail_workers();
      }
    });
    ctxs[i] = std::move(ctx);
  }

  // Filter before mapping; publish bounded batches to amortize cache-line traffic.
  std::vector<std::array<EventRec, kBatchSize>> pending(workers);
  std::vector<size_t> pending_size(workers, 0);
  auto flush = [&](int i) {
    size_t sent = 0;
    while (sent < pending_size[i] && !worker_failed.load(std::memory_order_relaxed)) {
      const size_t n = ctxs[i]->ring.push_batch(pending[i].data() + sent,
                                                pending_size[i] - sent);
      sent += n;
      if (!n) { ++spin_waits; std::this_thread::yield(); }
    }
    pending_size[i] = 0;
  };
  int emitted = 0;
  bool tail_phase = false;
  double merge_seconds = 0, barrier_seconds = 0, read_seconds = 0, map_seconds = 0;
  auto close_windows = [&](int end, bool eof, const char* reason) {
    if (end <= emitted || worker_failed.load()) return;
    const auto barrier_started = std::chrono::steady_clock::now();
    if (latency) latency->BeginClose(reason,
        follow && journal_clock == "wall" ? (tail_phase ? "tail" : "catchup") : "replay",
        std::chrono::duration_cast<std::chrono::nanoseconds>(barrier_started.time_since_epoch()).count());
    for (int i = 0; i < workers; ++i) {
      flush(i);
      EventRec marker{};
      marker.symbol = eof ? -2 : -1;
      marker.time_ms = end;
      while (!ctxs[i]->ring.push_batch(&marker, 1) && !worker_failed.load()) std::this_thread::yield();
    }
    {
      std::unique_lock<std::mutex> lock(barrier_mu);
      closed_cv.wait(lock, [&] {
        if (worker_failed.load()) return true;
        for (auto& ctx : ctxs) if (ctx->closed_target.load() < end) return false;
        return true;
      });
    }
    if (worker_failed.load()) return;
    barrier_seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - barrier_started).count();
#ifdef CMS_RTF_HAS_ARROW
    const auto merge_started = std::chrono::steady_clock::now();
    struct Member { const std::array<double, 4>* weights; WindowRecords* records; };
    std::vector<Member> members;
    for (const auto& [symbol, weights] : reference->weights) {
      auto& windows = ctxs[dispatch[symbol]]->windows;
      const auto found = windows.find(symbol);
      if (found != windows.end()) members.push_back({&weights, found->second.get()});
    }
    for (int e = emitted; e < end; ++e) {
      std::array<aggregation::Endpoint, 4> acc{};
      for (const auto& member : members) {
        auto& r = *member.records;
        while (r.book_pos < r.books.size() && r.books[r.book_pos].ep_ms <= grid[e]) {
          r.current = r.books[r.book_pos++]; r.has_current = true;
        }
        const BookRec* book = r.has_current ? &r.current : nullptr;
        const double mid = book ? book->mid : std::numeric_limits<double>::quiet_NaN();
        if (e == 0) r.previous_mid = r.seed_mid;
        double ret = std::numeric_limits<double>::quiet_NaN();
        if (std::isfinite(mid) && std::isfinite(r.previous_mid))
          ret = mid == r.previous_mid && mid > 0 ? 0.0 : std::log(mid) - std::log(r.previous_mid);
        r.previous_mid = mid;
        for (int idx = 0; idx < 4; ++idx) {
          const double w = (*member.weights)[idx];
          if (w > 0) aggregation::AddBookValue(book, ret, grid[e], w, acc[idx]);
        }
        while (r.flow_pos < r.flows.size() && r.flows[r.flow_pos].ep_ms <= grid[e]) {
          const auto& flow = r.flows[r.flow_pos++];
          if (flow.ep_ms != grid[e]) throw std::runtime_error("flow behind emitted window");
          for (int idx = 0; idx < 4; ++idx) {
            const double w = (*member.weights)[idx];
            if (w > 0) aggregation::AddFlowValue(flow, w, acc[idx]);
          }
        }
      }
      arrow->Append(grid[e], finalizer.Publish(e, acc, *reference));
    }
    // Flush to the single final-format Arrow stream even while stdin stays open.
    arrow->Flush();
    for (auto& ctx : ctxs) {
      for (auto& [symbol, window] : ctx->windows) { (void)symbol; window->Consumed(); }
      ctx->pending_budget->Reclaim();
    }
    merge_seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - merge_started).count();
#endif
    emitted = end;
    { std::lock_guard<std::mutex> lock(barrier_mu); resumed_target.store(end, std::memory_order_release); }
    resumed_cv.notify_all();
  };
  std::vector<int32_t> channel_time(2 * 65536, -1);
  std::vector<int> channels;
  std::string input_failure;
  std::vector<DtypeRow> rows(journal_input ? 4096 : (1 << 16));
  std::vector<int64_t> read_stamps(track_latency ? rows.size() : 0);
  int64_t n_events = 0, input_rows = 0, late_events = 0;
  size_t carry = has_magic ? 0 : prefix_size;
  if (carry) std::memcpy(rows.data(), prefix.data(), carry);
  bool input_error = false;
  bool reader_caught_up = false;
  int64_t source_clock_ns = 0, next_clock_poll_ns = 0;
  int32_t clock_ms = -1;
  auto last_status = std::chrono::steady_clock::time_point{};
  auto write_status = [&](const char* state, bool force = false) {
    if (!journal) return;
    const auto now = std::chrono::steady_clock::now();
    if (!force && now - last_status < std::chrono::seconds(1)) return;
    last_status = now;
    const auto& js = journal->stats();
    const auto path = std::filesystem::path(work_dir) / "live-status.json";
    const auto tmp = path.string() + ".tmp";
    std::ofstream out(tmp);
    out.exceptions(std::ios::badbit | std::ios::failbit);
    struct rusage usage{}; getrusage(RUSAGE_SELF, &usage);
    std::error_code ignored;
    const auto bytes = std::filesystem::file_size(output, ignored);
    out << "{\"status\":\"" << state << "\",\"day\":\"" << day
        << "\",\"pid\":" << getpid() << ",\"emitted_endpoints\":" << emitted
        << ",\"last_endpoint_ms\":" << (emitted ? grid[emitted - 1] : -1)
        << ",\"clock_ms\":" << clock_ms << ",\"source_clock_ns\":" << source_clock_ns
        << ",\"records\":" << js.records << ",\"input_rows\":" << input_rows
        << ",\"constituent_events\":" << n_events << ",\"late_events\":" << late_events
        << ",\"sessions\":" << js.sessions << ",\"unclean_restarts\":" << js.unclean_restarts
        << ",\"input_incomplete\":" << ((js.incomplete_records || js.sessions > 1) ? "true" : "false")
        << ",\"journal_ended\":" << (js.ended ? "true" : "false")
        << ",\"clock_inversions\":" << js.clock_inversions << ",\"page\":" << js.page
        << ",\"caught_up\":" << (reader_caught_up ? "true" : "false")
        << ",\"peak_rss_kib\":" << usage.ru_maxrss
        << ",\"arrow_bytes\":" << (ignored ? 0 : bytes)
        << ",\"latency_enabled\":" << (latency ? "true" : "false")
        << ",\"full_grid\":" << (emitted == kNumEndpoints ? "true" : "false") << "}\n";
    out.close(); std::filesystem::rename(tmp, path);
  };
  auto advance_clock = [&](int64_t ns, bool force = false) {
    if (ns < journal_day0_ns || ns >= journal_day0_ns + 86400000000000LL)
      throw std::runtime_error("journal source clock is outside its trading day");
    source_clock_ns = std::max(source_clock_ns, ns);
    if (!force && source_clock_ns < next_clock_poll_ns) return;
    next_clock_poll_ns = (source_clock_ns / 500000000 + 1) * 500000000;
    clock_ms = static_cast<int32_t>((source_clock_ns - journal_day0_ns) / 1000000);
    int32_t watermark = -1;
    if (!channels.empty()) {
      watermark = kPmCloseMs + 1;
      for (const int c : channels) watermark = std::min(watermark, channel_time[c]);
    }
    int end = std::lower_bound(grid.begin(), grid.end(), watermark) - grid.begin();
    // Retain CMS closing batches until the explicit Huatai +60s session timer.
    // Source time drives replay; idle wall time is permitted only after the
    // reader has drained all currently committed frames in follow mode.
    if ((watermark >= kAmCloseMs && watermark <= kPmOpenMs) || watermark >= kPmCloseMs)
      end = emitted;
    const char* reason = "watermark";
    if (clock_ms >= kAmCloseMs + 60000 && end < kNumEndpoints / 2 && emitted < kNumEndpoints / 2) {
      end = kNumEndpoints / 2; reason = "am_timer";
    }
    if (clock_ms >= kPmCloseMs + 60000 && emitted < kNumEndpoints) {
      end = kNumEndpoints; reason = "pm_timer";
    }
    if (end > emitted) close_windows(end, false, reason);
  };
  try {
  write_status("running", true);
  while (!worker_failed.load(std::memory_order_relaxed) && !stopping) {
    if (!journal_input && streaming && input_path == "-") {
      pollfd descriptor{fileno(in), POLLIN, 0};
      const int ready = ::poll(&descriptor, 1, 100);
      if (ready < 0 && errno != EINTR) { input_error = true; break; }
      if (ready <= 0) continue;
    }
    size_t got = 0, total = 0, bytes = 0;
    bool eof = false, idle = false;
    int64_t idle_sample_ns = 0;
    const auto read_started = std::chrono::steady_clock::now();
    if (journal) {
      while (got < rows.size()) {
        idle_sample_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        const auto result = journal->Next(rows[got]);
        if (result == JournalInput::Result::Idle) { idle = true; break; }
        if (result == JournalInput::Result::Eof) { eof = true; break; }
        if (latency) read_stamps[got] = journal->read_steady_ns();
        ++got;
      }
      reader_caught_up = idle || eof;
    } else {
      ssize_t received;
      do {
        received = ::read(fileno(in), reinterpret_cast<char*>(rows.data()) + carry,
                          rows.size() * sizeof(DtypeRow) - carry);
      } while (received < 0 && errno == EINTR);
      if (received < 0) { input_error = true; break; }
      bytes = static_cast<size_t>(received); total = bytes + carry;
      got = total / sizeof(DtypeRow); input_rows += got;
      if (latency) std::fill_n(read_stamps.begin(), got, SteadyNs());
    }
    read_seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - read_started).count();
    const auto map_started = std::chrono::steady_clock::now();
    for (size_t j = 0; j < got; ++j) {
      if (journal) {
        advance_clock(rows[j].nano_timestamp);
        if (!rows[j].action_code) continue;
        ++input_rows;
      }
      const int32_t sym = parse_symbol(rows[j].symbol);
      if (sym < 0 || sym >= static_cast<int32_t>(dispatch.size())) continue;
      const int worker = dispatch[sym];
      if (worker < 0) continue;
      const EventRec ev = map_row(rows[j], sym);
      if (latency) latency->Observe(ev.time_ms, emitted, rows[j].recv_ns, read_stamps[j]);
      if (streaming) {
        if (emitted && ev.time_ms <= grid[emitted - 1]) {
          if (journal) ++late_events;
          else throw std::runtime_error("late event behind published watermark: symbol=" + std::to_string(sym) +
            " event_ms=" + std::to_string(ev.time_ms) + " closed_ms=" + std::to_string(grid[emitted - 1]));
        }
        const int channel = (IsSh(sym) ? 65536 : 0) + static_cast<uint16_t>(ev.channel);
        if (channel_time[channel] < 0) channels.push_back(channel);
        channel_time[channel] = std::max(channel_time[channel], ev.time_ms);
      }
      pending[worker][pending_size[worker]++] = ev;
      if (pending_size[worker] == kBatchSize) flush(worker);
      ++n_events;
    }
    for (int i = 0; i < workers; ++i) flush(i);
    map_seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - map_started).count();
    if (journal) {
      if (idle && follow && journal_clock == "wall") {
        const int64_t wall_day = (idle_sample_ns - journal_day0_ns) / 1000000;
        if (wall_day >= 0 && wall_day < 86400000) {
          if (journal->stats().records) advance_clock(idle_sample_ns);
        }
        else throw std::runtime_error("follow wall clock must match --day; use journal-mode replay for history");
        // Gateway normally emits END at 15:30. Do not treat a midday END or an
        // idle interval as day completion. Stop at 15:31; expose missing END.
        if (wall_day >= 55860000) {
          if (journal->stats().records && !journal->stats().ended)
            throw std::runtime_error("daily stop reached without gateway END");
          eof = true;
        }
      }
      write_status("running");
      if (idle) tail_phase = true;
      if (eof) { if (source_clock_ns) advance_clock(source_clock_ns, true); break; }
      if (idle) std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    carry = total % sizeof(DtypeRow);
    if (carry) std::memmove(rows.data(), reinterpret_cast<char*>(rows.data()) + got * sizeof(DtypeRow), carry);
    if (!bytes) { input_error = std::ferror(in) || carry != 0; break; }
    if (streaming && !channels.empty()) {
      int32_t watermark = kPmCloseMs + 1;
      for (const auto channel : channels) watermark = std::min(watermark, channel_time[channel]);
      const int end = std::lower_bound(grid.begin(), grid.end(), watermark) - grid.begin();
      const bool closing_batch = (watermark >= kAmCloseMs && watermark <= kPmOpenMs) || watermark >= kPmCloseMs;
      if (!closing_batch && end - emitted >= window_batch) close_windows(end, false, "watermark");
    }
  }
  if (input_error) fail_workers();
  if (streaming && !input_error && !journal_input) close_windows(kNumEndpoints, true, "eof");
  } catch (const std::exception& e) {
    input_failure = e.what();
    fail_workers();
  }
  if (in && in != stdin) std::fclose(in);
  const auto dispatched = std::chrono::steady_clock::now();
  input_done.store(true, std::memory_order_release);

  BookHealth total;
  int64_t nonmono = 0;
  uint64_t output_capacity = 0, sh_identities = 0, identity_capacity = 0, live_orders = 0;
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
    output_capacity += ctx->output_capacity_at_eof;
    sh_identities += ctx->sh_identities;
    identity_capacity += ctx->identity_capacity;
    live_orders += ctx->live_orders;
  }
  if (!input_failure.empty()) std::cerr << input_failure << "\n";
  if (input_error) std::cerr << "truncated or unreadable DTYPE stream\n";
  if (worker_failed.load()) {
    if (streaming) {
      int slowest = -1;
      for (int c : channels) if (slowest < 0 || channel_time[c] < channel_time[slowest]) slowest = c;
      std::cerr << "stream stopped: input_rows=" << input_rows << " emitted_endpoints=" << emitted;
      if (slowest >= 0) std::cerr << " watermark_channel=" << slowest << " watermark_ms=" << channel_time[slowest];
      std::cerr << '\n';
    }
    for (auto& ctx : ctxs) if (ctx->error) {
      try { std::rethrow_exception(ctx->error); }
      catch (const std::exception& e) { std::cerr << "worker failed: " << e.what() << "\n"; }
    }
    write_status("failed", true);
    return 1;
  }
  if (input_error) { std::cerr << "truncated or unreadable DTYPE stream\n"; return 1; }
#ifdef CMS_RTF_HAS_ARROW
  if (streaming) arrow->Commit();
  write_status(stopping ? "stopped" : "completed", true);
#endif
  size_t pending_peak = 0;
  double max_worker_event_seconds = 0, max_worker_close_seconds = 0;
  for (const auto& ctx : ctxs) pending_peak += ctx->pending_budget->peak();
  for (const auto& ctx : ctxs) {
    max_worker_event_seconds = std::max(max_worker_event_seconds, ctx->event_seconds);
    max_worker_close_seconds = std::max(max_worker_close_seconds, ctx->close_seconds);
  }
  const auto finished = std::chrono::steady_clock::now();
  const double dispatch_s = std::chrono::duration<double>(dispatched - started).count();
  const double total_s = std::chrono::duration<double>(finished - started).count();
  struct rusage usage{};
  getrusage(RUSAGE_SELF, &usage);
  std::printf("{\"read_seconds\": %.6f, \"map_seconds\": %.6f, \"barrier_seconds\": %.6f, "
              "\"max_worker_event_seconds\": %.6f, \"max_worker_close_seconds\": %.6f, "
              "\"emitted_endpoints\": %d, \"merge_seconds\": %.6f, \"sum_worker_peak_pending_payload_bytes\": %zu, \"output_mode\": \"%s\", \"peak_rss_kib\": %ld, "
              "\"output_capacity_at_eof_bytes\": %" PRIu64 ", \"sh_identities_at_eof\": %" PRIu64 ", "
              "\"sh_identity_capacity_at_eof_bytes\": %" PRIu64 ", \"live_orders_at_eof\": %" PRIu64 ", "
              "\"input_rows\": %" PRId64 ", \"dispatch_seconds\": %.6f, "
              "\"total_seconds\": %.6f, \"events\": %" PRId64 ", \"workers\": %d, \"spin_waits\": %" PRId64
              ", \"over_deduct\": %" PRId64 ", \"d_unknown\": %" PRId64
              ", \"unknown_trade_ref\": %" PRId64 ", \"late_passive_ref\": %" PRId64
              ", \"agg_skip\": %" PRId64 ", \"crossed_book\": %" PRId64
              ", \"immediate_fill\": %" PRId64 ", \"sz_non_monotonic\": %" PRId64 "}\n",
              read_seconds, map_seconds, barrier_seconds, max_worker_event_seconds, max_worker_close_seconds,
              emitted, merge_seconds, pending_peak, output_mode.c_str(), usage.ru_maxrss, output_capacity, sh_identities, identity_capacity, live_orders,
              input_rows, dispatch_s, total_s, n_events, workers, spin_waits, total.over_deduct, total.d_unknown,
              total.unknown_trade_ref, total.late_passive_ref, total.agg_skip,
              total.crossed_book, total.immediate_fill, nonmono);
  return 0;
}

int main(int argc, char** argv) {
  try { return Run(argc, argv); }
  catch (const std::exception& e) { std::cerr << "cms_index500ms_mt: " << e.what() << '\n'; return 1; }
}

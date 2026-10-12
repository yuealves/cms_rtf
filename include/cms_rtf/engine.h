#pragma once

// cms_rtf 多线程引擎（华泰 light_rtf 线程模型）：
//   单 reader 线程顺序读事件流 → 按 symbol hash 分发到 N 个 worker 线程
//   → 每 worker 独立驱动其 symbol 子集的每股状态机（runner.h）。
//
// 与 light_rtf 的对应：reader = dispatcher 帧循环；SPSC ring = 两层 ring 的
// 接收侧；worker = SymbolWorker（每股状态机顺序执行）。批处理 EOF 收口；
// 实时运行时的通道水位关窗是后续事项（docs/design.md）。
//
// 正确性不变量（与 light_rtf 一致，聚合层保证）：
//   - 每股事件序 = 到达序（同一 symbol 恒入同一 worker，FIFO ring 保序）
//   - 聚合按固定 symbol 升序逐项相加 ⇒ 输出与 worker 数无关（位级一致）
//   - ring 满则 reader 自旋等活（背压），不丢事件、不扩容

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

namespace cms_rtf {

// 单生产者单消费者有界环形队列（2 的幂容量；满了 push 返回 false 由调用方自旋）
template <typename T, size_t kCapPow2>
class SpscRing {
 public:
  SpscRing() : buf_(kCapPow2) { static_assert((kCapPow2 & (kCapPow2 - 1)) == 0); }

  bool push(const T& v) {
    const size_t h = head_.load(std::memory_order_relaxed);
    const size_t next = (h + 1) & (kCapPow2 - 1);
    if (next == tail_.load(std::memory_order_acquire)) {
      return false;
    }
    buf_[h] = v;
    head_.store(next, std::memory_order_release);
    return true;
  }

  bool pop(T& v) {
    const size_t t = tail_.load(std::memory_order_relaxed);
    if (t == head_.load(std::memory_order_acquire)) {
      return false;
    }
    v = buf_[t];
    tail_.store((t + 1) & (kCapPow2 - 1), std::memory_order_release);
    return true;
  }

 private:
  std::vector<T> buf_;
  alignas(64) std::atomic<size_t> head_{0};
  alignas(64) std::atomic<size_t> tail_{0};
};

}  // namespace cms_rtf

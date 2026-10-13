#include <array>
#include <cstdlib>
#include <iostream>
#include "cms_rtf/engine.h"

int main() {
  cms_rtf::SpscRing<uint64_t, 64> ring;
  constexpr uint64_t count = 1000003;
  std::thread producer([&] {
    std::array<uint64_t, 37> batch;
    uint64_t next = 0;
    while (next < count) {
      const auto n = std::min<uint64_t>(batch.size(), count - next);
      for (size_t i = 0; i < n; ++i) batch[i] = next + i;
      next += ring.push_batch(batch.data(), n);
    }
  });
  std::array<uint64_t, 29> batch;
  uint64_t expected = 0;
  while (expected < count) {
    const auto n = ring.pop_batch(batch.data(), batch.size());
    for (size_t i = 0; i < n; ++i) {
      if (batch[i] != expected++) { std::cerr << "FIFO mismatch\n"; std::exit(1); }
    }
  }
  producer.join();
  if (ring.pop_batch(batch.data(), batch.size()) != 0) return 1;
  std::cout << "batch FIFO wraparound/backpressure passed\n";
}

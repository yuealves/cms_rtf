#include <cstdlib>
#include <iostream>
#include <random>
#include <unordered_map>
#include "cms_rtf/identity_table.h"

struct State { double volume = 0; double price = 0; bool counted = false; };
void Check(bool ok) { if (!ok) std::abort(); }

int main() {
  cms_rtf::IdentityTable<State> dense;
  std::unordered_map<int64_t, State> reference;
  State* stable = &dense[0];
  stable->volume = 19;
  reference[0].volume = 19;
  std::mt19937_64 rng(428);
  for (int i = 0; i < 100000; ++i) {
    const int64_t key = static_cast<int64_t>(rng());
    auto& a = dense[key];
    auto& b = reference[key];
    a.volume += i; b.volume += i;
    a.price = b.price = i * 0.25;
    a.counted = b.counted = true;
    Check(&dense[0] == stable);
  }
  dense[INT64_MIN].price = reference[INT64_MIN].price = 1;
  dense[INT64_MAX].price = reference[INT64_MAX].price = 2;
  Check(dense.size() == reference.size());
  for (const auto& [key, b] : reference) {
    const auto& a = dense[key];
    Check(a.volume == b.volume && a.price == b.price && a.counted == b.counted);
  }
  Check(dense.size() == reference.size());
  std::cout << "identity values survive collisions/rehash with stable references; bytes="
            << dense.capacity_bytes() << '\n';
}

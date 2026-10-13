#pragma once
#include <cstddef>
#include <cstdint>

namespace cms_rtf::journal_format {
// Kungfu PageHeader.h / FrameHeader.h, Apache-2.0, Copyright 2017 taurus.ai.
// See third_party/cms_protocol/Kungfu-LICENSE. Reader and replay share this ABI.
#pragma pack(push, 1)
struct PageHeader {
  uint8_t status; char name[30]; int16_t number;
  int64_t start_ns, close_ns; int32_t frames, last_pos; int16_t version;
  int16_t reserved_short[3]; int64_t reserved_long[9];
};
struct FrameHeader {
  uint8_t status; int16_t source; int64_t nano; int32_t length;
  uint32_t hash; int16_t type; uint8_t last; int32_t request;
  int64_t extra_nano; int32_t error;
};
#pragma pack(pop)
static_assert(sizeof(PageHeader) == 137 && sizeof(FrameHeader) == 38, "Kungfu frame v2 ABI");
constexpr size_t kPageSize = 128ULL * 1024 * 1024;
}  // namespace cms_rtf::journal_format

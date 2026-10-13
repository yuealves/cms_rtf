#pragma once
#include <cstdint>

namespace cms_rtf {
// Existing CMSPQ01/parquet export ABI; journal control notices remain internal
// to JournalInput and are never serialized into the CMSPQ01 interface.
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
}

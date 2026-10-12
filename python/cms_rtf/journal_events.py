"""journal（cms_l2_raw_v1）→ 内核 canonical 事件流。

链路：cms_journal_export（C++ 校验+解析，stdout 出 128B DTYPE 行）→ 本模块
numpy 向量化映射为 EventRec（与 codec.py 的 v1 parquet 口径同规则——
DTYPE 就是 v1 的原始字段源）。

用于"parquet→journal→replay→因子"链路的最后一段：因子引擎直接消费回放
journal，不再倒回 parquet。
"""
import numpy as np

from .codec import EVENT_DTYPE, SIDE_BUY, SIDE_SELL, SIDE_NEUTRAL, KIND_ORDER, KIND_TRADE, \
    _new_part, _hhmmssmmm_to_ms

# 与 cms_market_gateway/tools/archive_journal.py 的 DTYPE 逐字节一致
JOURNAL_DTYPE = np.dtype([
    ('nano_timestamp', '<i8'), ('recv_ns', '<i8'), ('exchange_ns', '<i8'),
    ('epoch', '<u8'), ('record_seq', '<u8'), ('recv_seq', '<u8'),
    ('business_seq', '<u8'), ('buy_ref', '<u8'), ('sell_ref', '<u8'),
    ('exchange_time_raw', '<u8'), ('quantity_raw', '<u8'), ('value_raw', '<u8'),
    ('price_raw', '<u4'), ('feed_seq', '<u4'), ('trading_day', '<u4'),
    ('event_index', '<u4'), ('channel', '<u2'), ('market', 'u1'),
    ('message_type', 'u1'), ('action_code', 'u1'), ('side_raw', 'u1'),
    ('record_flags', 'u1'), ('_padding', 'u1'), ('symbol', 'S8'),
])
assert JOURNAL_DTYPE.itemsize == 128


def rows_to_events(rows, constituents):
    """DTYPE 行 → 内核事件列（字段规则与 v1 codec 相同）。"""
    sym_str = np.char.rstrip(rows['symbol'].astype(str), '\x00')
    sym = sym_str.astype(np.int32)
    keep = np.isin(sym, constituents)
    if not keep.any():
        return None
    sym = sym[keep]
    n = len(sym)
    is_sh = rows['market'][keep] == 1
    is_trade = rows['action_code'][keep] == ord('T')
    side_raw = rows['side_raw'][keep]

    out = _new_part(n)
    out['symbol'] = sym
    raw_t = rows['exchange_time_raw'][keep].astype(np.int64)
    hhmmssmmm = np.where(is_sh, raw_t * 10, raw_t % 1000000000)
    out['time_ms'] = _hhmmssmmm_to_ms(hhmmssmmm).astype(np.int32)
    out['channel'] = rows['channel'][keep].astype(np.int32)
    out['biz_index'] = rows['business_seq'][keep].astype(np.int32)
    out['px_tick'] = rows['price_raw'][keep].astype(np.int64) * np.where(is_sh, 10, 1)
    out['volume'] = rows['quantity_raw'][keep] // np.where(is_sh, 1000, 100)
    out['record_seq'] = rows['record_seq'][keep]
    out['event_index'] = rows['event_index'][keep]

    side = np.where(is_sh, np.where(side_raw == 0, SIDE_BUY,
                                    np.where(side_raw == 1, SIDE_SELL, SIDE_NEUTRAL)),
                    np.where(side_raw == 49, SIDE_BUY, SIDE_SELL)).astype(np.uint8)
    side[is_trade & ~is_sh] = 0  # SZ 成交 side=0（下游 buy/sell 引用推断）
    out['side'] = side

    is_add = rows['action_code'][keep] == ord('A')
    is_del = rows['action_code'][keep] == ord('D')
    out['kind'] = np.where(is_trade | (is_del & ~is_sh), KIND_TRADE, KIND_ORDER).astype(np.uint8)
    out['order_type'] = np.where(is_sh & is_add, 65, np.where(is_sh & is_del, 68, 0)).astype(np.uint8)
    sz_order = ~is_sh & is_add
    out['order_type'][sz_order] = rows['value_raw'][keep][sz_order].astype(np.uint8)  # '1'/'2'/'U'
    out['trade_type'] = np.where(is_sh, 0, np.where(is_del, 52, 70)).astype(np.uint8)
    out['trade_type'][is_trade & is_sh] = 0

    buy_ref = rows['buy_ref'][keep]
    sell_ref = rows['sell_ref'][keep]
    out['ref1'] = np.where(out['kind'] == KIND_TRADE, buy_ref,
                           np.where(side == SIDE_BUY, buy_ref, sell_ref))
    out['ref2'] = np.where(out['kind'] == KIND_TRADE, sell_ref, 0)
    # SZ 撤单方向：非零引用位置
    sz_wd = is_del & ~is_sh
    out['side'][sz_wd] = np.where(buy_ref[sz_wd] > 0, SIDE_BUY, SIDE_SELL)
    return out

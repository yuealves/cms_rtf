"""CMS parquet → 内核 canonical 事件流（events.bin / thresh.bin）。

字段映射（2026-10-09 实盘数据实证，见 docs/validation/20261009-field-mapping.md）：
  SH：action A/D（message_type 62）；side_raw 0=买/1=卖，本单 id = 买方侧 buy_ref /
      卖方侧 sell_ref；A 的 quantity = 残余 Balance（与 light_rtf 簿内核同口径）。
  SH 成交：side_raw 0=B/1=S/2=N（集合竞价）；buy_ref/sell_ref 双引用恒非零。
  SZ：action A（message_type 23）：side_raw 49('1')=买/50('2')=卖，id 同理按侧取引用；
      sz_order_type_raw 49='1'/50='2'/85='U'；business_seq 为 channel 内严格序。
      撤单 action D（message_type 24）：无 side，由非零引用位置给方向（buy_ref→买）。
  SZ 成交：side_raw 恒 0（下游按 buy_ref>sell_ref 启发式推断，与 light_rtf 一致）。

行序：先按 (symbol, record_seq, event_index) 稳定排序——record_seq 是全日 journal 序，
event_index 是报文内位置；每股组内即 journal 到达序。
"""
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pathlib import Path

# 与 include/cms_rtf/events.h 的 EventRec 逐字节一致
EVENT_DTYPE = np.dtype([
    ('symbol', '<i4'), ('time_ms', '<i4'), ('channel', '<i4'), ('biz_index', '<i4'),
    ('ref1', '<i8'), ('ref2', '<i8'), ('px_tick', '<i8'), ('volume', '<i8'),
    ('kind', '<u1'), ('side', '<u1'), ('order_type', '<u1'), ('trade_type', '<u1'),
    ('pad', '<u1', (4,)),
], align=True)
assert EVENT_DTYPE.itemsize == 56

SIDE_BUY, SIDE_SELL, SIDE_NEUTRAL = 66, 83, 78
KIND_ORDER, KIND_TRADE = 0, 1

_ORDER_COLS = ['symbol', 'exchange', 'action', 'side_raw', 'buy_ref', 'sell_ref',
               'price_e4', 'quantity', 'time_hhmmssmmm', 'channel', 'business_seq',
               'record_seq', 'event_index', 'sz_order_type_raw']
_TRADE_COLS = ['symbol', 'exchange', 'side_raw', 'buy_ref', 'sell_ref', 'price_e4',
               'quantity', 'time_hhmmssmmm', 'channel', 'business_seq', 'record_seq',
               'event_index']


def _np(col):
    # pyarrow 11: ChunkedArray.to_numpy 不收 zero_copy_only；先合并
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()
    return col.to_numpy(zero_copy_only=False)


def _i64(t, name):
    return _np(t.column(name)).astype(np.int64)


def _i32(t, name):
    return _np(t.column(name)).astype(np.int32)


def _hhmmssmmm_to_ms(v):
    hh, mm = v // 10000000, (v // 100000) % 100
    ss, ms = (v // 1000) % 100, v % 1000
    return ((hh * 60 + mm) * 60 + ss) * 1000 + ms


def _symbol_i32(t):
    return _np(pc.cast(t.column('symbol'), pa.int32()).combine_chunks())


def _new_part(n):
    return {name: np.zeros(n, dtype=EVENT_DTYPE[name]) for name in EVENT_DTYPE.names
            if name != 'pad'} | {'record_seq': None, 'event_index': None}


def _order_events(t, constituents):
    sym = _symbol_i32(t)
    keep = np.isin(sym, constituents)
    if not keep.any():
        return None
    sym = sym[keep]
    n = len(sym)
    is_sh = _np(pc.equal(t.column('exchange'), 'SH').combine_chunks())[keep]
    is_add = _np(pc.equal(t.column('action'), 'A').combine_chunks())[keep]
    side_raw = _i64(t, 'side_raw')[keep]
    buy_ref = _i64(t, 'buy_ref')[keep]
    sell_ref = _i64(t, 'sell_ref')[keep]

    side_buy = np.where(is_sh, side_raw == 0, side_raw == 49)
    side = np.where(side_buy, SIDE_BUY, SIDE_SELL).astype(np.uint8)

    out = _new_part(n)
    out['symbol'] = sym
    out['time_ms'] = _hhmmssmmm_to_ms(_i64(t, 'time_hhmmssmmm')[keep])
    out['channel'] = _i32(t, 'channel')[keep]
    out['biz_index'] = _i32(t, 'business_seq')[keep]
    out['px_tick'] = _i64(t, 'price_e4')[keep]
    out['volume'] = _i64(t, 'quantity')[keep]
    out['side'] = side
    out['record_seq'] = _i64(t, 'record_seq')[keep]
    out['event_index'] = _i64(t, 'event_index')[keep]

    own_ref = np.where(side_buy, buy_ref, sell_ref)
    sh_a = is_sh & is_add
    sh_d = is_sh & ~is_add
    sz_a = ~is_sh & is_add
    sz_d = ~is_sh & ~is_add
    out['kind'][sz_d] = KIND_TRADE
    out['order_type'][sh_a] = 65   # 'A'
    out['order_type'][sh_d] = 68   # 'D'
    sz_type = _np(pc.fill_null(t.column('sz_order_type_raw'), 0).combine_chunks() if isinstance(pc.fill_null(t.column('sz_order_type_raw'), 0), pa.ChunkedArray) else pc.fill_null(t.column('sz_order_type_raw'), 0))
    out['order_type'][sz_a] = sz_type[keep][sz_a].astype(np.uint8)
    out['trade_type'][sz_d] = 52   # '4'
    out['ref1'] = own_ref
    out['ref1'][sz_d] = buy_ref[sz_d]
    out['ref2'][sz_d] = sell_ref[sz_d]
    # SZ 撤单方向：非零引用位置（实证 100% 与委托侧一致）
    out['side'][sz_d] = np.where(buy_ref[sz_d] > 0, SIDE_BUY, SIDE_SELL)
    return out


def _trade_events(t, constituents):
    sym = _symbol_i32(t)
    keep = np.isin(sym, constituents)
    if not keep.any():
        return None
    sym = sym[keep]
    n = len(sym)
    is_sh = _np(pc.equal(t.column('exchange'), 'SH').combine_chunks())[keep]
    side_raw = _i64(t, 'side_raw')[keep]
    side = np.where(is_sh & (side_raw == 0), SIDE_BUY,
                    np.where(is_sh & (side_raw == 1), SIDE_SELL,
                             np.where(is_sh, SIDE_NEUTRAL, 0))).astype(np.uint8)
    out = _new_part(n)
    out['symbol'] = sym
    out['time_ms'] = _hhmmssmmm_to_ms(_i64(t, 'time_hhmmssmmm')[keep])
    out['channel'] = _i32(t, 'channel')[keep]
    out['biz_index'] = _i32(t, 'business_seq')[keep]
    out['ref1'] = _i64(t, 'buy_ref')[keep]
    out['ref2'] = _i64(t, 'sell_ref')[keep]
    out['px_tick'] = _i64(t, 'price_e4')[keep]
    out['volume'] = _i64(t, 'quantity')[keep]
    out['kind'] = np.ones(n, np.uint8)  # KIND_TRADE
    out['side'] = side
    out['trade_type'] = np.where(is_sh, 0, 70)  # SZ: 'F'
    out['record_seq'] = _i64(t, 'record_seq')[keep]
    out['event_index'] = _i64(t, 'event_index')[keep]
    return out


def write_events(data_root, day, constituents, out_events, out_thresh, dyn):
    """扫描当天 parquet，写 events.bin（按 symbol 分组、组内 journal 序）与 thresh.bin。

    自动按分片 schema 分派 v1/v2（v2 = KyStd 兼容列，见 codec_v2）。"""
    from .codec_v2 import detect_schema_version, read_day_events_v2
    constituents = np.asarray(constituents, dtype=np.int32)
    day_dir = Path(data_root) / day
    schema = detect_schema_version(day_dir)
    if schema == 'cms-parquet-v2':
        parts = read_day_events_v2(data_root, day, constituents)
        n_shards = len(list(day_dir.glob(f'{day}_*_data_*.parquet')))
    else:
        order_shards = sorted(day_dir.glob(f'{day}_order_data_*.parquet'))
        trade_shards = sorted(day_dir.glob(f'{day}_trade_data_*.parquet'))
        if not order_shards or not trade_shards:
            raise RuntimeError(f'no parquet shards under {day_dir}')
        parts = []
        for shard in order_shards:
            part = _order_events(pq.read_table(shard, columns=_ORDER_COLS), constituents)
            if part is not None:
                parts.append(part)
        for shard in trade_shards:
            part = _trade_events(pq.read_table(shard, columns=_TRADE_COLS), constituents)
            if part is not None:
                parts.append(part)
        n_shards = len(order_shards) + len(trade_shards)

    n = sum(len(p['symbol']) for p in parts)
    events = np.zeros(n, dtype=EVENT_DTYPE)
    record_seq = np.concatenate([p['record_seq'] for p in parts])
    event_index = np.concatenate([p['event_index'] for p in parts])
    for name in EVENT_DTYPE.names:
        if name != 'pad':
            events[name] = np.concatenate([p[name] for p in parts])
    order = np.lexsort((event_index, record_seq, events['symbol']))
    events[...] = events[order]
    events.tofile(out_events)

    thresh = np.zeros(len(constituents), dtype=[('symbol', '<i4'), ('xl', '<f8'), ('m', '<f8')])
    for i, s in enumerate(constituents):
        thresh[i] = (s, dyn[int(s)][1], dyn[int(s)][2])
    thresh.tofile(out_thresh)
    return dict(events=int(n), symbols=int(len(constituents)), shards=n_shards,
                schema=schema)

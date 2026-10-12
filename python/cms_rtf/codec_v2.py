"""cms-parquet-v2（KyStd 兼容列）→ 内核 canonical 事件流。

与 codec.py（v1）的差异：v2 的列已经是 KyStd 编码域，映射接近直通：
  order：FunctionCode 1/2 → side B/S；OrderKind 2/10 → SH 'A'/'D'；
         OrderKind 1/2/3 → SZ '1'/'2'/'U'；本单 id = OrderOriNo（SH）/OrderNumber（SZ）。
  trade：FunctionCode 0/1 → fill/withdraw；BSFlag 1/2/0 → SH B/S/N；
         SZ fill side=0（下游推断）；SZ 撤单方向 = 非零引用位置。
字段语义实证见 cms_market_gateway/docs/design-parquet-v2.md 与本项目
docs/validation/20261009-field-mapping.md。
"""
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pathlib import Path

from .codec import EVENT_DTYPE, SIDE_BUY, SIDE_SELL, SIDE_NEUTRAL, KIND_ORDER, KIND_TRADE, \
    _new_part, _hhmmssmmm_to_ms

_ORDER_V2 = ['Symbol', 'BizIndex', 'Channel', 'FunctionCode', 'OrderKind', 'OrderNumber',
             'OrderOriNo', 'Price', 'Time', 'Volume', 'recv_ns', 'record_seq',
             'event_index', 'sh_already_traded_quantity', 'exchange']
_TRADE_V2 = ['Symbol', 'AskOrder', 'BidOrder', 'BSFlag', 'BizIndex', 'Channel',
             'FunctionCode', 'Index', 'Price', 'Time', 'Volume', 'record_seq',
             'event_index', 'exchange']


def _np(t, name):
    col = t.column(name)
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()
    return col.to_numpy(zero_copy_only=False)


def _fill0(t, name):
    col = pc.fill_null(t.column(name), 0)
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()
    return col.to_numpy(zero_copy_only=False)


def detect_schema_version(day_dir):
    """从任一分片的 schema metadata 判定 v1/v2。"""
    day_dir = Path(day_dir)
    for f in sorted(day_dir.glob('*_order_data_*.parquet')):
        meta = pq.read_schema(f).metadata or {}
        if b'cms_schema' in meta:
            return meta[b'cms_schema'].decode()
    raise RuntimeError(f'no parquet shards under {day_dir}')


def _order_events_v2(t, constituents):
    sym = _np(t, 'Symbol').astype(np.int32)
    keep = np.isin(sym, constituents)
    if not keep.any():
        return None
    sym = sym[keep]
    n = len(sym)
    is_sh = pc.equal(t.column('exchange'), 'SH').combine_chunks().to_numpy(zero_copy_only=False)[keep]
    fc = _np(t, 'FunctionCode')[keep]
    okind = _np(t, 'OrderKind')[keep]
    biz = _np(t, 'BizIndex')[keep]

    out = _new_part(n)
    out['symbol'] = sym
    out['time_ms'] = _hhmmssmmm_to_ms(_np(t, 'Time')[keep].astype(np.int64)).astype(np.int32)
    out['channel'] = _np(t, 'Channel')[keep].astype(np.int32)
    out['biz_index'] = biz.astype(np.int32)
    out['px_tick'] = _np(t, 'Price')[keep]
    out['volume'] = _np(t, 'Volume')[keep].astype(np.int64)
    out['side'] = np.where(fc == 1, SIDE_BUY, SIDE_SELL).astype(np.uint8)
    out['record_seq'] = _np(t, 'record_seq')[keep]
    out['event_index'] = _np(t, 'event_index')[keep]

    # 本单 id：SH 取 OrderOriNo；SZ 取 OrderNumber
    ori = _np(t, 'OrderOriNo')[keep]
    out['ref1'] = np.where(is_sh, ori, biz)
    out['kind'] = np.zeros(n, np.uint8)  # KIND_ORDER
    out['order_type'] = np.where(is_sh, np.where(okind == 2, 65, 68),
                                 np.where(okind == 1, 49, np.where(okind == 2, 50, 85))).astype(np.uint8)
    return out


def _trade_events_v2(t, constituents):
    sym = _np(t, 'Symbol').astype(np.int32)
    keep = np.isin(sym, constituents)
    if not keep.any():
        return None
    sym = sym[keep]
    n = len(sym)
    is_sh = pc.equal(t.column('exchange'), 'SH').combine_chunks().to_numpy(zero_copy_only=False)[keep]
    fc = _np(t, 'FunctionCode')[keep]
    bs = _np(t, 'BSFlag')[keep]
    bid = _np(t, 'BidOrder')[keep]
    ask = _np(t, 'AskOrder')[keep]

    out = _new_part(n)
    out['symbol'] = sym
    out['time_ms'] = _hhmmssmmm_to_ms(_np(t, 'Time')[keep].astype(np.int64)).astype(np.int32)
    out['channel'] = _np(t, 'Channel')[keep].astype(np.int32)
    out['biz_index'] = _np(t, 'BizIndex')[keep].astype(np.int32)
    out['ref1'] = bid
    out['ref2'] = ask
    out['px_tick'] = _np(t, 'Price')[keep]
    out['volume'] = _np(t, 'Volume')[keep].astype(np.int64)
    out['record_seq'] = _np(t, 'record_seq')[keep]
    out['event_index'] = _np(t, 'event_index')[keep]
    out['kind'] = np.ones(n, np.uint8)  # KIND_TRADE
    out['trade_type'] = np.where(is_sh, 0, np.where(fc == 1, 52, 70)).astype(np.uint8)  # SH=0；SZ '4'/'F'
    # side：SH 按 BSFlag；SZ fill 恒 0；SZ 撤单按非零引用位置
    sz_wd_buy = (~is_sh) & (fc == 1) & (bid > 0)
    side = np.where(is_sh, np.where(bs == 1, SIDE_BUY, np.where(bs == 2, SIDE_SELL, SIDE_NEUTRAL)),
                    np.where(fc == 1, np.where(bid > 0, SIDE_BUY, SIDE_SELL), 0)).astype(np.uint8)
    out['side'] = side
    return out


def read_day_events_v2(data_root, day, constituents):
    """读 v2 日目录 →（按 symbol 分组、组内 journal 序）事件列字典列表。"""
    constituents = np.asarray(constituents, dtype=np.int32)
    day_dir = Path(data_root) / day
    parts = []
    for shard in sorted(day_dir.glob(f'{day}_order_data_*.parquet')):
        part = _order_events_v2(pq.read_table(shard, columns=_ORDER_V2), constituents)
        if part is not None:
            parts.append(part)
    for shard in sorted(day_dir.glob(f'{day}_trade_data_*.parquet')):
        part = _trade_events_v2(pq.read_table(shard, columns=_TRADE_V2), constituents)
        if part is not None:
            parts.append(part)
    return parts

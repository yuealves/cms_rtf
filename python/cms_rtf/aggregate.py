"""内核输出 → 指数级 106 列因子 → Arrow IPC stream。

聚合语义（移植 light_rtf v2 index_500ms/aggregate.cpp + worker.cpp 的批处理等价）：
  - 簿 35 列：每列独立"有效权重"分母（valid_book && 该列有限）；列覆盖率 ≥80% 才输出；
    coverage 恒输出；age 仅在 coverage ≥80% 时输出；fresh_weight 恒输出。
  - 流量 21 列：官方全权重和分母（含不活跃股）；空桶精确零（查无记录=零）。
  - 派生 16/13 列：imb0（0 分母→0，clip [-1,1]）、ratio0（clip [0,1]）；active_weight 直出。
  - 5s 环：聚合基础量 Float32 量化后入环，最近 ≤10 个 Float64 求和（滑窗无减法），
    午休后重启（session 按端点 > 11:30 判定）。
  - mid 收益链：每股 prev_mid = 上一端点捕获 mid（NaN 不回退）；首端点前值 =
    09:30:00 前最后捕获 mid（seeds.bin）。
"""
from pathlib import Path

import numpy as np
import pyarrow as pa

from .catalog import COLUMN_NAMES, INDEX_CODES, endpoint_schedule_ms

BOOK_REC = np.dtype([
    ('symbol', '<i4'), ('ep_ms', '<i4'), ('feat', '<f8', (31,)), ('mid', '<f8'),
    ('act_ms', '<i4'), ('valid', '<u1'), ('pad', '<u1', (7,)),
], align=True)
assert BOOK_REC.itemsize == 280

FLOW_REC = np.dtype([
    ('symbol', '<i4'), ('ep_ms', '<i4'), ('bases', '<f8', (21,)),
], align=True)
assert FLOW_REC.itemsize == 176

SEED_REC = np.dtype([('symbol', '<i4'), ('seed_mid', '<f8')])  # 内核写 12B 无对齐
assert SEED_REC.itemsize == 12

K_MIN_COVERAGE = 0.8
GRID_MS = 500


def imb0(buy, sell):
    d = buy + sell
    return np.where(d > 0, np.clip((buy - sell) / np.where(d > 0, d, 1), -1.0, 1.0), 0.0)


def ratio0(num, den):
    return np.where(den > 0, np.clip(num / np.where(den > 0, den, 1), 0.0, 1.0), 0.0)


def flow_derived_500ms(b, act3):
    """b: [n,21] double；act3: [n,3]。返回 [n,16] float32。"""
    out = np.stack([
        imb0(b[:, 0], b[:, 1]), imb0(b[:, 2], b[:, 3]), imb0(b[:, 4], b[:, 5]),
        ratio0(b[:, 6], b[:, 4] + b[:, 5]), ratio0(b[:, 7], b[:, 4] + b[:, 5]),
        ratio0(b[:, 8], b[:, 0] + b[:, 1]), act3[:, 0],
        imb0(b[:, 9], b[:, 10]), imb0(b[:, 11], b[:, 12]), act3[:, 1],
        imb0(b[:, 13], b[:, 14]), imb0(b[:, 15], b[:, 16]), imb0(b[:, 17], b[:, 18]),
        ratio0(b[:, 19], b[:, 17] + b[:, 18]), ratio0(b[:, 20], b[:, 17] + b[:, 18]),
        act3[:, 2],
    ], axis=1)
    return out.astype(np.float32)


def flow_derived_5s(b):
    """b: [n,21] double（5s 滚动和）。返回 [n,13] float32。"""
    out = np.stack([
        imb0(b[:, 0], b[:, 1]), imb0(b[:, 2], b[:, 3]), imb0(b[:, 4], b[:, 5]),
        ratio0(b[:, 6], b[:, 4] + b[:, 5]), ratio0(b[:, 7], b[:, 4] + b[:, 5]),
        imb0(b[:, 9], b[:, 10]), imb0(b[:, 11], b[:, 12]),
        imb0(b[:, 13], b[:, 14]), imb0(b[:, 15], b[:, 16]), imb0(b[:, 17], b[:, 18]),
        ratio0(b[:, 19], b[:, 17] + b[:, 18]), ratio0(b[:, 20], b[:, 17] + b[:, 18]),
        ratio0(b[:, 8], b[:, 0] + b[:, 1]),
    ], axis=1)
    return out.astype(np.float32)


def rolling_sum_session(agg21_f32, grid):
    """[n,21] f32 → 同形状 f64 滚动和（窗口=最近 ≤10 个已发布，session 重启，无减法）。"""
    from numpy.lib.stride_tricks import sliding_window_view
    out = np.zeros(agg21_f32.shape, dtype=np.float64)
    session_break = 14400  # AM 端点数
    for lo, hi in ((0, session_break), (session_break, len(grid))):
        seg = agg21_f32[lo:hi]
        pad = np.zeros((9, seg.shape[1]), dtype=np.float32)
        padded = np.concatenate([pad, seg])
        win = sliding_window_view(padded, 10, axis=0)  # [n, 21, 10]
        out[lo:hi] = win.sum(axis=2, dtype=np.float64)
    return out


def aggregate(book_path, flow_path, seeds_path, ref, day):
    """返回 (datetimes_us[28800], rows[4][28800, 106] float32)。"""
    grid = endpoint_schedule_ms()
    n_ep = len(grid)
    official = ref['official']  # [4]

    num = np.zeros((4, n_ep, 32))
    den = np.zeros((4, n_ep, 32))
    valid_w = np.zeros((4, n_ep))
    age_sum = np.zeros((4, n_ep))
    fresh_w = np.zeros((4, n_ep))
    agg21 = np.zeros((4, n_ep, 21))
    act3 = np.zeros((4, n_ep, 3))

    # symbol → [(index_ord, weight)]
    sym_idx = {}
    for r, s in enumerate(ref['symbols']):
        entries = [(i, ref['weights'][i, r]) for i in range(4) if ref['weights'][i, r] > 0]
        if entries:
            sym_idx[int(s)] = entries

    seeds_arr = np.fromfile(seeds_path, dtype=SEED_REC)
    seeds = dict(zip(seeds_arr['symbol'].astype(np.int32), seeds_arr['seed_mid']))

    book = np.memmap(book_path, dtype=BOOK_REC, mode='r')
    flow = (np.memmap(flow_path, dtype=FLOW_REC, mode='r')
            if Path(flow_path).stat().st_size else np.zeros(0, dtype=FLOW_REC))

    # ---- 簿：逐股展开到端点网格 ----
    bounds = np.flatnonzero(np.r_[True, book['symbol'][1:] != book['symbol'][:-1], True])
    for lo, hi in zip(bounds[:-1], bounds[1:]):
        recs = book[lo:hi]
        sym = int(recs['symbol'][0])
        entries = sym_idx.get(sym)
        if not entries:
            continue
        ep_idx = np.searchsorted(grid, recs['ep_ms'])
        # 端点 e 的捕获 = 最后一条 ep ≤ e 的记录
        sel = np.searchsorted(recs['ep_ms'], grid, side='right') - 1
        present = sel >= 0
        sel_clip = np.clip(sel, 0, len(recs) - 1)
        feat31 = np.where(present[:, None], recs['feat'][sel_clip], np.nan)
        mid = np.where(present, recs['mid'][sel_clip], np.nan)
        act = np.where(present, recs['act_ms'][sel_clip], -1)
        valid_book = present & np.isfinite(mid) & (act >= 0)
        # mid 收益链（前值含 NaN 不回退；首端点前值 = seed）
        seed = seeds.get(sym, np.nan)
        prev = np.concatenate([[seed], mid[:-1]])
        both = np.isfinite(mid) & np.isfinite(prev)
        ret = np.where(both, np.log(np.where(both, mid, 1.0)) - np.log(np.where(both, prev, 1.0)), np.nan)
        feat32 = np.concatenate([feat31, ret[:, None]], axis=1)
        colmask = valid_book[:, None] & np.isfinite(feat32)
        age = np.where(act >= 0, (grid - act) / 1000.0, np.nan)
        for i, w in entries:
            num[i] += w * np.where(colmask, feat32, 0.0)
            den[i] += w * colmask
            valid_w[i] += w * valid_book
            age_ok = valid_book & np.isfinite(age)
            age_sum[i] += w * np.where(age_ok, age, 0.0)
            fresh_w[i] += w * (age_ok & (age < 0.5))

    # ---- 流量：逐股散布进指数 ----
    fbounds = np.flatnonzero(np.r_[True, flow['symbol'][1:] != flow['symbol'][:-1], True])
    for lo, hi in zip(fbounds[:-1], fbounds[1:]):
        recs = flow[lo:hi]
        if not len(recs):
            continue
        sym = int(recs['symbol'][0])
        entries = sym_idx.get(sym)
        if not entries:
            continue
        ep_idx = np.searchsorted(grid, recs['ep_ms'])
        bases = recs['bases']
        a_order = bases[:, 0:4].sum(axis=1) > 0
        a_cancel = bases[:, 9:13].sum(axis=1) > 0
        a_trade = bases[:, 13:17].sum(axis=1) > 0
        for i, w in entries:
            np.add.at(agg21[i], ep_idx, w * bases)
            np.add.at(act3[i, :, 0], ep_idx, w * a_order)
            np.add.at(act3[i, :, 1], ep_idx, w * a_cancel)
            np.add.at(act3[i, :, 2], ep_idx, w * a_trade)

    # ---- 逐指数出列 ----
    all_rows = []
    for i in range(4):
        off = official[i]
        num_i, den_i = num[i], den[i]
        out35 = np.full((n_ep, 35), np.nan)
        cover_ok = den_i / off >= K_MIN_COVERAGE
        good = (den_i > 0) & cover_ok
        out35[:, :32] = np.where(good, num_i / np.where(good, den_i, 1.0), np.nan)
        coverage = np.clip(valid_w[i] / off, 0.0, 1.0) if off > 0 else np.zeros(n_ep)
        out35[:, 32] = coverage
        age_ok = (valid_w[i] > 0) & (coverage >= K_MIN_COVERAGE)
        out35[:, 33] = np.where(age_ok,
                                age_sum[i] / np.where(valid_w[i] > 0, valid_w[i], 1.0), np.nan)
        out35[:, 34] = np.clip(fresh_w[i] / off, 0.0, 1.0) if off > 0 else np.zeros(n_ep)

        agg = agg21[i] / off
        act = act3[i] / off
        agg_f32 = agg.astype(np.float32)
        der16 = flow_derived_500ms(agg, act)
        roll = rolling_sum_session(agg_f32, grid)
        roll_f32 = roll.astype(np.float32)
        der13 = flow_derived_5s(roll)

        rows = np.concatenate([
            out35.astype(np.float32), agg_f32, der16, roll_f32, der13,
        ], axis=1)
        assert rows.shape == (n_ep, 106)
        all_rows.append(rows)
    return grid, all_rows


def write_arrow(grid, all_rows, day, out_path):
    """index_500ms_exch.arrow：datetime(timestamp[us] 无时区北京) + code + 106×float32，
    行序 = 端点序 × 4 指数固定序；NaN→null。"""
    n_ep = len(grid)
    day0 = np.datetime64(f'{day[:4]}-{day[4:6]}-{day[6:]}T00:00:00', 'us')
    dt_col = np.repeat(day0 + grid.astype('timedelta64[ms]').astype('timedelta64[us]'), 4)
    code_col = np.tile(np.array(INDEX_CODES), n_ep)
    values = np.concatenate(all_rows, axis=0).reshape(4, n_ep, 106)
    # [n_ep, 4, 106]：端点优先
    values = np.transpose(values, (1, 0, 2)).reshape(n_ep * 4, 106)

    dt_arr = pa.array(dt_col)  # datetime64[us] → timestamp[us]
    if dt_arr.type != pa.timestamp('us'):
        dt_arr = dt_arr.cast(pa.timestamp('us'))
    cols = [dt_arr, pa.array(code_col)]
    for c in range(106):
        cols.append(pa.array(values[:, c], type=pa.float32(), from_pandas=True))
    schema = pa.schema([('datetime', pa.timestamp('us')), ('code', pa.string())] +
                       [(name, pa.float32()) for name in COLUMN_NAMES])
    table = pa.Table.from_arrays(cols, schema=schema)
    with pa.ipc.new_stream(out_path, schema) as writer:
        writer.write_table(table)
    return table.num_rows

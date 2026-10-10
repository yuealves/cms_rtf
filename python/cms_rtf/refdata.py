"""参考数据加载：指数权重（≤ 当天回退）+ 动态大小单阈值 CSV → dyn tier。

公式与同事 online_simu DYNAMIC_PARAMS / light_rtf ref_data_loader.cpp 逐字一致：
  tier = max(floor, min(avg_daily_money*dyn_ratio, min(circ_cap*1e8*cap_ratio, abs_max)))
空字段 = 该约束缺席；0 是有效值（拉回 floor）。fail-closed：缺文件/缺成分阈值直接报错。
"""
import csv
import datetime
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from .catalog import INDEX_CODES, INDEX_WEIGHT_DIRS

# (floor, dyn_ratio, cap_ratio, abs_max)
DYN_XXL = (500000.0, 0.0020, 0.0005, 5000000.0)
DYN_XL = (100000.0, 0.0005, 0.0001, 1000000.0)
DYN_M = (20000.0, 0.0001, 0.00005, 200000.0)


def _dyn_tier(p, avg_money, circ_cap_yi):
    ceiling = p[3]
    if circ_cap_yi is not None:
        ceiling = min(circ_cap_yi * 1e8 * p[2], p[3])
    candidate = ceiling
    if avg_money is not None:
        candidate = min(avg_money * p[1], ceiling)
    return max(p[0], candidate)


def load_thresholds(path):
    """threshold_YYYYMMDD.csv → {symbol: (xxl, xl, m)}"""
    out = {}
    with open(path, newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        if 'avg_daily_money' not in header:
            raise RuntimeError(f'threshold csv header mismatch: {path}')
        for row in reader:
            if not row:
                continue
            sym = int(row[2])
            avg_money = float(row[3]) if row[3] else None
            circ_cap = float(row[4]) if row[4] else None
            out[sym] = (_dyn_tier(DYN_XXL, avg_money, circ_cap),
                        _dyn_tier(DYN_XL, avg_money, circ_cap),
                        _dyn_tier(DYN_M, avg_money, circ_cap))
    return out


def latest_threshold_csv(directory, day):
    """directory 下 ≤ day 的最新 threshold_YYYYMMDD.csv；没有则 fail-closed。"""
    directory = Path(directory)
    best = None
    for p in directory.glob('threshold_*.csv'):
        d = p.name[10:18]
        if len(d) == 8 and d.isdigit() and d <= day and (best is None or d > best):
            best = d
    if best is None:
        raise RuntimeError(f'no threshold csv ≤ {day} in {directory}')
    return directory / f'threshold_{best}.csv'


def latest_weights_file(index_dir, day):
    """指数目录里 ≤ day 的最新 YYYY-MM-DD.parquet；没有则 fail-closed。"""
    best = None
    for p in Path(index_dir).glob('????-??-??.parquet'):
        d = p.name[:10]
        if d <= day and (best is None or d > best):
            best = d
    if best is None:
        raise RuntimeError(f'no weights file ≤ {day} in {index_dir}')
    return Path(index_dir) / f'{best}.parquet'


def load_weights(weights_dir, day):
    """返回 (symbols 升序 int32, weights[4, n] float64, members: 4 × symbol 列表)。"""
    dash_day = f'{day[:4]}-{day[4:6]}-{day[6:]}'
    weight_map = {}  # (index_ord, sym) -> w
    for dir_name, index_code in INDEX_WEIGHT_DIRS.items():
        path = latest_weights_file(Path(weights_dir) / dir_name, dash_day)
        tab = pq.read_table(path)
        codes = tab.column('stock_code').to_pylist()
        weights = tab.column('weight').to_pylist()
        ord_idx = INDEX_CODES.index(index_code)
        for sc, w in zip(codes, weights):
            if w is None or not np.isfinite(w) or w <= 0:
                continue
            sym = int(sc[:6])
            weight_map[(ord_idx, sym)] = float(w)
    symbols = np.array(sorted({s for _, s in weight_map}), dtype=np.int32)
    row_of = {s: i for i, s in enumerate(symbols)}
    weights = np.zeros((4, len(symbols)))
    for (ord_idx, sym), w in weight_map.items():
        weights[ord_idx, row_of[sym]] = w
    members = [symbols[weights[i] > 0] for i in range(4)]
    return symbols, weights, members


def build_ref_tables(weights_dir, threshold_dir, day):
    """加载并校验全部参考数据。返回 dict(symbols, weights, members, dyn, official)。"""
    symbols, weights, members = load_weights(weights_dir, day)
    dyn = load_thresholds(latest_threshold_csv(threshold_dir, day))
    missing = [int(s) for s in symbols if int(s) not in dyn]
    if missing:
        raise RuntimeError(f'missing liquidity thresholds for {len(missing)} symbols: {missing[:5]}...')
    official = weights.sum(axis=1)
    return dict(symbols=symbols, weights=weights, members=members, dyn=dyn,
                official=official)

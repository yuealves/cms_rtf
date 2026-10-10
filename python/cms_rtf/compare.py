"""与参考 arrow 的对拍报告：键集合、null 掩码、逐列数值差异、相关/中位数偏差。

定位："基本一致"量化——不同行情源（CMS vs HT 网关）不可能位级一致，
报告按列给出：null 匹配率、非空交集的 max/mean 绝对差、Pearson 相关、
相对误差中位数、符号一致率。支持 JSON 导出。
"""
import json

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from .catalog import COLUMN_NAMES


def _load(path):
    with ipc.open_stream(path) as reader:
        t = reader.read_all()
    return t


def compare(mine_path, ref_path):
    mine = _load(mine_path)
    ref = _load(ref_path)
    report = {'mine_rows': mine.num_rows, 'ref_rows': ref.num_rows, 'columns': {}}

    key_mine = list(zip(mine.column('datetime').to_pylist(), mine.column('code').to_pylist()))
    key_ref = list(zip(ref.column('datetime').to_pylist(), ref.column('code').to_pylist()))
    report['keys_equal'] = key_mine == key_ref
    if key_mine != key_ref:
        sm, sr = set(key_mine), set(key_ref)
        report['keys_only_mine'] = len(sm - sr)
        report['keys_only_ref'] = len(sr - sm)

    take = None
    if not report['keys_equal']:
        order_ref = {k: i for i, k in enumerate(key_ref)}
        take = pa.array([order_ref[k] for k in key_mine if k in order_ref])

    exact_cols = 0
    close_cols = 0
    for name in COLUMN_NAMES:
        a = mine.column(name)
        b = ref.column(name) if take is None else ref.column(name).take(take)
        entry = _column_report(a, b)
        report['columns'][name] = entry
        if entry['exact_match']:
            exact_cols += 1
        elif entry['corr'] is not None and entry['corr'] > 0.99:
            close_cols += 1
    report['summary'] = dict(exact_cols=exact_cols, close_cols=close_cols,
                             total=len(COLUMN_NAMES),
                             keys_equal=report['keys_equal'])
    return report


def _np(col):
    import pyarrow as pa
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()
    return col.to_numpy(zero_copy_only=False)


def _column_report(a, b):
    an = _np(a)
    bn = _np(b).astype(np.float32)
    a_null = np.isnan(an)
    b_null = np.isnan(bn)
    n = len(an)
    both = ~a_null & ~b_null
    entry = {
        'nulls_mine': int(a_null.sum()),
        'nulls_ref': int(b_null.sum()),
        'null_mask_match': float((a_null == b_null).mean()),
    }
    if not both.any():
        entry.update(corr=None, max_abs=None, mean_abs=None, exact_match=bool(a_null.sum() == b_null.sum()))
        return entry
    x, y = an[both].astype(np.float64), bn[both].astype(np.float64)
    diff = np.abs(x - y)
    entry['max_abs'] = float(diff.max())
    entry['mean_abs'] = float(diff.mean())
    entry['exact'] = float((x == y).mean())
    entry['exact_match'] = bool((x == y).all() and a_null.sum() == b_null.sum())
    sx, sy = x.std(), y.std()
    entry['corr'] = float(np.corrcoef(x, y)[0, 1]) if sx > 0 and sy > 0 else None
    denom = np.abs(y)
    rel = np.where(denom > 1e-12, diff / np.where(denom > 1e-12, denom, 1.0), diff)
    entry['rel_median'] = float(np.median(rel))
    return entry


def write_report(report, path):
    with open(path, 'w') as f:
        json.dump(report, f, indent=1, ensure_ascii=False)


def print_summary(report, top=20):
    print(f"rows: mine={report['mine_rows']} ref={report['ref_rows']} "
          f"keys_equal={report['keys_equal']}")
    rows = []
    for name, e in report['columns'].items():
        rows.append((name, e.get('null_mask_match', 1.0), e.get('exact', 0.0),
                     e.get('corr'), e.get('max_abs'), e.get('rel_median')))
    rows.sort(key=lambda r: (r[3] is not None and -(r[3]), -(r[2] or 0)))
    print(f"{'column':34s} {'null%':>7s} {'exact%':>7s} {'corr':>8s} {'max_abs':>12s} {'rel_med':>9s}")
    for name, nm, ex, corr, mx, rel in rows[:top]:
        print(f"{name:34s} {nm:7.4f} {ex:7.4f} "
              f"{('%8.5f' % corr) if corr is not None else '     —  '}"
              f"{('%12.4g' % mx) if mx is not None else '          —'}"
              f"{('%9.3g' % rel) if rel is not None else '        —'}")

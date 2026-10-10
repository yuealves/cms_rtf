"""聚合层单元测试：覆盖门、有效权重分母、派生列、5s 滚动 session 重启、mid 收益链。

纯 stdlib unittest（本机无 pytest）：PYTHONPATH=python python3 -m unittest discover -s tests/python
"""
import tempfile
import unittest
from pathlib import Path

import numpy as np

from cms_rtf import aggregate as agg
from cms_rtf.catalog import endpoint_schedule_ms


def approx(a, b, tol=1e-6):
    return abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def _write(path, dtype, rows):
    arr = np.array(rows, dtype=dtype)
    arr.tofile(path)
    return path


def _ref(symbols, weights_row):
    symbols = np.array(symbols, dtype=np.int32)
    weights = np.zeros((4, len(symbols)))
    weights[0] = weights_row
    members = [symbols[weights[i] > 0] for i in range(4)]
    return dict(symbols=symbols, weights=weights, members=members,
                dyn={int(s): (1e12, 1e12, -1.0) for s in symbols},
                official=weights.sum(axis=1))


class PureFnTest(unittest.TestCase):
    def test_imb0_ratio0(self):
        self.assertTrue(approx(agg.imb0(np.array([2.0]), np.array([1.0]))[0], 1 / 3))
        self.assertEqual(agg.imb0(np.array([0.0]), np.array([0.0]))[0], 0.0)  # 0 分母 → 0
        self.assertEqual(agg.ratio0(np.array([1.0]), np.array([0.0]))[0], 0.0)
        self.assertEqual(agg.ratio0(np.array([2.0]), np.array([1.0]))[0], 1.0)  # clip

    def test_rolling_session_reset(self):
        grid = agg.endpoint_schedule_ms()
        x = np.ones((len(grid), 21), dtype=np.float32)
        roll = agg.rolling_sum_session(x, grid)
        self.assertEqual(roll[0, 0], 1.0)       # 首端点部分窗口
        self.assertEqual(roll[8, 0], 9.0)
        self.assertEqual(roll[9, 0], 10.0)
        self.assertEqual(roll[14399, 0], 10.0)
        self.assertEqual(roll[14400, 0], 1.0)   # 午后首端点重启（不混入上午）
        self.assertEqual(roll[14409, 0], 10.0)
        x[5] = 0                                # 空窗精确零
        roll = agg.rolling_sum_session(x, grid)
        self.assertEqual(roll[14, 0], 9.0)

    def test_flow_derived(self):
        b = np.zeros((1, 21))
        b[0, 0], b[0, 1] = 3.0, 1.0     # order buy/sell count
        b[0, 4], b[0, 5] = 900.0, 100.0
        b[0, 6] = 500.0
        d = agg.flow_derived_500ms(b, np.array([[0.7, 0.0, 1.0]]))
        self.assertTrue(approx(d[0, 0], 0.5))    # count imbalance
        self.assertTrue(approx(d[0, 2], 0.8))    # money imbalance
        self.assertTrue(approx(d[0, 3], 0.5))    # large share
        self.assertTrue(approx(d[0, 6], 0.7))    # active weight 直出
        d5 = agg.flow_derived_5s(b)
        self.assertEqual(d5.shape, (1, 13))
        self.assertTrue(approx(d5[0, 12], 0.0))  # resolved_fraction: 0/4 → 0


class AggregateEndToEndTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='cms-rtf-test-')
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.grid = agg.endpoint_schedule_ms()

    def test_minimal(self):
        """2 只成分股（各占 000016 一半权重）的最小端到端聚合。"""
        grid, n = self.grid, len(self.grid)
        ref = _ref([1, 600000], [0.5, 0.5])
        feat1 = np.full(31, np.nan)
        feat1[0] = 10.0    # spread_bps
        feat1[9] = 1000.0  # bid_depth_total
        book_rows = []
        for e in range(n):
            book_rows.append((600000, int(grid[e]), tuple(feat1 * 2), 20.0,
                              int(grid[e]) - 100, 1, (0,) * 7))
            if grid[e] <= 41_400_000:  # 000001 只上午有效
                book_rows.append((1, int(grid[e]), tuple(feat1), 10.0,
                                  int(grid[e]) - 200, 1, (0,) * 7))
        book_rows.sort(key=lambda r: r[0])
        _write(self.dir / 'book.bin', agg.BOOK_REC, book_rows)
        _write(self.dir / 'seeds.bin', agg.SEED_REC, [(600000, 19.0), (1, 9.0)])
        bases = np.zeros(21)
        bases[0] = 2
        bases[17] = 1000.0
        _write(self.dir / 'flow.bin', agg.FLOW_REC, [(600000, int(grid[0]), tuple(bases))])

        _, all_rows = agg.aggregate(self.dir / 'book.bin', self.dir / 'flow.bin',
                                    self.dir / 'seeds.bin', ref, '20261009')
        rows = all_rows[0]  # 000016
        # spread：上午 (2*10*0.5 + 10*0.5)/1.0 = 15；下午 000001 簿沿用上午最后捕获 → 仍 15
        self.assertTrue(approx(rows[0, 0], 15.0))
        pm0 = 14400
        self.assertTrue(approx(rows[pm0, 0], 15.0))
        self.assertTrue(approx(rows[pm0, 32], 1.0))       # 沿用簿 → 覆盖度不变
        self.assertTrue(approx(rows[pm0, 9], 1500.0))     # (2000+1000)/2
        # fresh_weight：000001 的 act 停在上午 → age 超 0.5s → fresh 只剩 600000
        self.assertTrue(approx(rows[pm0, 34], 0.5))
        # mid 收益首端点：0.5*(log(20/19) + log(10/9))
        self.assertTrue(approx(rows[0, 31],
                               0.5 * (np.log(20 / 19) + np.log(10 / 9)), 1e-5))
        self.assertTrue(approx(rows[1, 31], 0.0))         # mid 不变 → 0
        self.assertTrue(approx(rows[0, 35], 1.0))         # order_buy_count = 0.5*2/1.0
        self.assertTrue(approx(rows[0, 52], 500.0))       # trade_buy_money = 35+17
        self.assertTrue(approx(rows[0, 62], 0.5))         # order_active_weight
        self.assertTrue(approx(rows[1, 72], 1.0))         # 5s 环 = 前两桶之和
        self.assertTrue(approx(rows[10, 72], 0.0))        # 滚出窗口

    def test_coverage_gate(self):
        """有效权重 < 80% 官方权重时簿特征列输出 NaN，coverage/fresh 仍输出。"""
        grid = self.grid
        ref = _ref([1, 600000], [0.5, 0.5])
        feat = np.zeros(31)
        _write(self.dir / 'book.bin', agg.BOOK_REC,
               [(600000, int(grid[0]), tuple(feat), 20.0, int(grid[0]) - 100, 1, (0,) * 7)])
        _write(self.dir / 'seeds.bin', agg.SEED_REC, [(600000, 19.0)])
        _write(self.dir / 'flow.bin', agg.FLOW_REC, [])
        _, all_rows = agg.aggregate(self.dir / 'book.bin', self.dir / 'flow.bin',
                                    self.dir / 'seeds.bin', ref, '20261009')
        rows = all_rows[0]
        self.assertTrue(np.isnan(rows[0, 0]))         # spread_bps 被覆盖门拦截
        self.assertTrue(approx(rows[0, 32], 0.5))     # coverage 恒输出
        self.assertTrue(np.isnan(rows[0, 33]))        # age 同被拦
        self.assertTrue(approx(rows[0, 34], 0.5))     # fresh：600000 age=0.1s<0.5 → 0.5


if __name__ == '__main__':
    unittest.main()

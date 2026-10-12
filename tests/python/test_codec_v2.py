"""codec_v2（KyStd 列直读）单测。"""
import unittest

import numpy as np
import pyarrow as pa

from cms_rtf.codec_v2 import _order_events_v2, _trade_events_v2
from cms_rtf.codec import KIND_ORDER, KIND_TRADE, SIDE_BUY, SIDE_SELL, SIDE_NEUTRAL


def v2_order_table(rows):
    cols = dict(
        Symbol=pa.array([r['Symbol'] for r in rows], type=pa.int32()),
        BizIndex=pa.array([r['BizIndex'] for r in rows], type=pa.int32()),
        Channel=pa.array([r.get('Channel', 1004) for r in rows], type=pa.int64()),
        FunctionCode=pa.array([r['FunctionCode'] for r in rows], type=pa.int8()),
        OrderKind=pa.array([r['OrderKind'] for r in rows], type=pa.int8()),
        OrderNumber=pa.array([r['OrderNumber'] for r in rows], type=pa.int32()),
        OrderOriNo=pa.array([r.get('OrderOriNo', 0) for r in rows], type=pa.int32()),
        Price=pa.array([r['Price'] for r in rows], type=pa.int64()),
        Time=pa.array([r['Time'] for r in rows], type=pa.int32()),
        Volume=pa.array([r['Volume'] for r in rows], type=pa.int32()),
        recv_ns=pa.array([r.get('recv_ns', 0) for r in rows], type=pa.int64()),
        record_seq=pa.array([r['record_seq'] for r in rows], type=pa.uint64()),
        event_index=pa.array([r.get('event_index', 0) for r in rows], type=pa.uint32()),
        sh_already_traded_quantity=pa.array([None] * len(rows), type=pa.uint64()),
        exchange=pa.array([r['exchange'] for r in rows]),
    )
    return pa.table(cols)


def v2_trade_table(rows):
    cols = dict(
        Symbol=pa.array([r['Symbol'] for r in rows], type=pa.int32()),
        AskOrder=pa.array([r.get('AskOrder', 0) for r in rows], type=pa.int32()),
        BidOrder=pa.array([r.get('BidOrder', 0) for r in rows], type=pa.int32()),
        BSFlag=pa.array([r.get('BSFlag', 0) for r in rows], type=pa.int8()),
        BizIndex=pa.array([r['BizIndex'] for r in rows], type=pa.int32()),
        Channel=pa.array([r.get('Channel', 1004) for r in rows], type=pa.int32()),
        FunctionCode=pa.array([r.get('FunctionCode', 0) for r in rows], type=pa.int8()),
        Index=pa.array([r['BizIndex'] for r in rows], type=pa.int32()),
        Price=pa.array([r['Price'] for r in rows], type=pa.int64()),
        Time=pa.array([r['Time'] for r in rows], type=pa.int32()),
        Volume=pa.array([r['Volume'] for r in rows], type=pa.int32()),
        record_seq=pa.array([r['record_seq'] for r in rows], type=pa.uint64()),
        event_index=pa.array([r.get('event_index', 0) for r in rows], type=pa.uint32()),
        exchange=pa.array([r['exchange'] for r in rows]),
    )
    return pa.table(cols)


class CodecV2Test(unittest.TestCase):
    def test_sh_order_add(self):
        t = v2_order_table([dict(Symbol=600519, exchange='SH', FunctionCode=1, OrderKind=2,
                                 OrderNumber=100, OrderOriNo=4242, BizIndex=100,
                                 Price=1263000, Time=93000500, Volume=500, record_seq=1)])
        out = _order_events_v2(t, np.array([600519], dtype=np.int32))
        ev = out
        self.assertEqual(ev['order_type'][0], 65)      # 'A'
        self.assertEqual(ev['side'][0], SIDE_BUY)
        self.assertEqual(ev['ref1'][0], 4242)          # OrderOriNo
        self.assertEqual(ev['kind'][0], KIND_ORDER)
        # Time 是 HHMMSSmmm 整数，内核要午夜起毫秒
        self.assertEqual(ev['time_ms'][0], 34200500)
        self.assertEqual(ev['px_tick'][0], 1263000)     # 价格进 px_tick

    def test_sh_delete(self):
        t = v2_order_table([dict(Symbol=600519, exchange='SH', FunctionCode=2, OrderKind=10,
                                 OrderNumber=101, OrderOriNo=4242, BizIndex=101,
                                 Price=1263000, Time=93000600, Volume=200, record_seq=2)])
        out = _order_events_v2(t, np.array([600519], dtype=np.int32))
        self.assertEqual(out['order_type'][0], 68)     # 'D'
        self.assertEqual(out['side'][0], SIDE_SELL)

    def test_sz_market_u(self):
        t = v2_order_table([dict(Symbol=1, exchange='SZ', FunctionCode=2, OrderKind=3,
                                 OrderNumber=777, BizIndex=777,
                                 Price=0, Time=93000700, Volume=100, record_seq=3)])
        out = _order_events_v2(t, np.array([1], dtype=np.int32))
        self.assertEqual(out['order_type'][0], 85)     # 'U'
        self.assertEqual(out['side'][0], SIDE_SELL)
        self.assertEqual(out['ref1'][0], 777)          # SZ 用 OrderNumber

    def test_sh_trade_side(self):
        rows = [dict(Symbol=600519, exchange='SH', BSFlag=1, BidOrder=10, AskOrder=20,
                     BizIndex=1, Price=1263000, Time=93000800, Volume=100, record_seq=4),
                dict(Symbol=600519, exchange='SH', BSFlag=2, BidOrder=10, AskOrder=21,
                     BizIndex=2, Price=1263000, Time=93000810, Volume=100, record_seq=5),
                dict(Symbol=600519, exchange='SH', BSFlag=0, BidOrder=10, AskOrder=22,
                     BizIndex=3, Price=1263000, Time=93000820, Volume=100, record_seq=6)]
        out = _trade_events_v2(v2_trade_table(rows), np.array([600519], dtype=np.int32))
        self.assertEqual(out['side'].tolist(), [SIDE_BUY, SIDE_SELL, SIDE_NEUTRAL])
        self.assertEqual(out['trade_type'][0], 0)      # SH 成交恒 0

    def test_sz_withdraw_side_from_ref_position(self):
        rows = [dict(Symbol=1, exchange='SZ', FunctionCode=1, BidOrder=0, AskOrder=888,
                     BizIndex=9, Price=0, Time=93000900, Volume=100, record_seq=7),
                dict(Symbol=1, exchange='SZ', FunctionCode=1, BidOrder=777, AskOrder=0,
                     BizIndex=10, Price=0, Time=93000910, Volume=100, record_seq=8)]
        out = _trade_events_v2(v2_trade_table(rows), np.array([1], dtype=np.int32))
        self.assertEqual(out['trade_type'][0], 52)             # '4'
        self.assertEqual(out['side'].tolist(), [SIDE_SELL, SIDE_BUY])


if __name__ == '__main__':
    unittest.main()

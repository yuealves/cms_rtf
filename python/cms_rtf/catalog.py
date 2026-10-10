# index_500ms 因子 106 列目录：顺序与 light_rtf v2 catalog.h 逐字一致（对拍契约，禁止修改）。

COLUMN_NAMES = [
    "spread_bps", "log_spread", "weighted_spread_5",
    "obi_1", "obi_3", "obi_5", "obi_10", "obi_money_5", "obi_gradient",
    "bid_depth_total", "ask_depth_total", "total_depth",
    "bid_concentration", "ask_concentration", "bid_near_share", "ask_near_share",
    "depth_ratio", "bid_slope", "ask_slope", "slope_imbalance",
    "bid_depth_cv", "ask_depth_cv", "bid_range_bps", "ask_range_bps",
    "book_symmetry", "micro_price_premium", "weighted_mid_premium",
    "book_pressure_imb", "total_order_volume_imb",
    "price_impact_buy_5", "price_impact_sell_5",
    "mid_log_return_500ms", "snapshot_coverage", "snapshot_age_seconds",
    "snapshot_fresh_weight_500ms",
    "order_buy_count_500ms", "order_sell_count_500ms",
    "order_buy_volume_500ms", "order_sell_volume_500ms",
    "order_buy_money_500ms", "order_sell_money_500ms",
    "order_large_money_500ms", "order_small_money_500ms",
    "order_resolved_count_500ms",
    "cancel_buy_count_500ms", "cancel_sell_count_500ms",
    "cancel_buy_volume_500ms", "cancel_sell_volume_500ms",
    "trade_buy_count_500ms", "trade_sell_count_500ms",
    "trade_buy_volume_500ms", "trade_sell_volume_500ms",
    "trade_buy_money_500ms", "trade_sell_money_500ms",
    "trade_large_money_500ms", "trade_small_money_500ms",
    "order_count_imbalance_500ms", "order_volume_imbalance_500ms",
    "order_money_imbalance_500ms", "order_large_money_share_500ms",
    "order_small_money_share_500ms", "order_resolved_fraction_500ms",
    "order_active_weight_500ms",
    "cancel_count_imbalance_500ms", "cancel_volume_imbalance_500ms",
    "cancel_active_weight_500ms",
    "trade_count_imbalance_500ms", "trade_volume_imbalance_500ms",
    "trade_money_imbalance_500ms", "trade_large_money_share_500ms",
    "trade_small_money_share_500ms", "trade_active_weight_500ms",
    "order_buy_count_5s", "order_sell_count_5s",
    "order_buy_volume_5s", "order_sell_volume_5s",
    "order_buy_money_5s", "order_sell_money_5s",
    "order_large_money_5s", "order_small_money_5s", "order_resolved_count_5s",
    "cancel_buy_count_5s", "cancel_sell_count_5s",
    "cancel_buy_volume_5s", "cancel_sell_volume_5s",
    "trade_buy_count_5s", "trade_sell_count_5s",
    "trade_buy_volume_5s", "trade_sell_volume_5s",
    "trade_buy_money_5s", "trade_sell_money_5s",
    "trade_large_money_5s", "trade_small_money_5s",
    "order_count_imbalance_5s", "order_volume_imbalance_5s",
    "order_money_imbalance_5s", "order_large_money_share_5s",
    "order_small_money_share_5s",
    "cancel_count_imbalance_5s", "cancel_volume_imbalance_5s",
    "trade_count_imbalance_5s", "trade_volume_imbalance_5s",
    "trade_money_imbalance_5s", "trade_large_money_share_5s",
    "trade_small_money_share_5s", "order_resolved_fraction_5s",
]

assert len(COLUMN_NAMES) == 106

# 4 个指数（固定顺序，= 输出行内 code 顺序）
INDEX_CODES = ["000016.XSHG", "000300.XSHG", "000905.XSHG", "000852.XSHG"]
INDEX_WEIGHT_DIRS = {"csi_50": "000016.XSHG", "csi_300": "000300.XSHG",
                     "csi_500": "000905.XSHG", "csi_1000": "000852.XSHG"}

GRID_MS = 500
AM_OPEN_MS = 34_200_500
AM_CLOSE_MS = 41_400_000
PM_OPEN_MS = 46_800_000
PM_CLOSE_MS = 54_000_000


def endpoint_schedule_ms():
    """28,800 个右闭端点：[09:30:00.500, 11:30:00] ∪ [13:00:00.500, 15:00:00]。"""
    import numpy as np
    am = np.arange(AM_OPEN_MS, AM_CLOSE_MS + 1, GRID_MS, dtype=np.int32)
    pm = np.arange(PM_OPEN_MS + GRID_MS, PM_CLOSE_MS + 1, GRID_MS, dtype=np.int32)
    return np.concatenate([am, pm])

import asyncio
from async_all_candles import get_all_candles
import argparse
from backend_response_structure import AlertResult
from history_data_structure import History
import numpy as np

def detect_peaks(prices):
    peaks = np.zeros(len(prices), dtype=int)

    for i in range(2, len(prices) - 2):
        window = prices[i - 2:i + 3]
        center = prices[i]

        if center == max(window) and center > window[1] and center > window[3]:
            peaks[i] = 1  # Local max
        elif center == min(window) and center < window[1] and center < window[3]:
            peaks[i] = -1  # Local min

    return peaks

def is_on_horizontal_level(alert, history):
    tf = alert[1]
    radius_pct = 0.5 * 0.01
    required_peaks = alert[2]

    df = history[tf].sort_values(["instrument_id", "start_time"])
    df = df.groupby("instrument_id").tail(100).reset_index(drop=True)

    result = set()

    for instrument_id, group in df.groupby("instrument_id"):
        closes = group["close"].values

        peaks = detect_peaks(closes)

        last_close = closes[-1]
        lower_bound = last_close * (1 - radius_pct)
        upper_bound = last_close * (1 + radius_pct)

        max_mask = (peaks == 1) & (closes >= lower_bound) & (closes <= upper_bound)
        min_mask = (peaks == -1) & (closes >= lower_bound) & (closes <= upper_bound)

        if max_mask.sum() >= required_peaks or min_mask.sum() >= required_peaks:
            result.add(instrument_id)

    return result

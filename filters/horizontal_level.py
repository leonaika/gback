import numpy as np


def process_horizontal_level(horizontal_level_alerts, alerts_users_map, local_history, local_normal_trading_set):
    for alert in horizontal_level_alerts:
        result = is_on_horizontal_level(alert, local_history)
        alert_id = alert[0]
        if alerts_users_map[alert_id].seen:
            alerts_users_map[alert_id].instruments &= result
        else:
            alerts_users_map[alert_id].instruments = result
            alerts_users_map[alert_id].seen = True
        alerts_users_map[alert_id].instruments &= local_normal_trading_set


def detect_peaks(open_prices, close_prices):
    highs = np.maximum(open_prices, close_prices)
    lows = np.minimum(open_prices, close_prices)
    peaks = np.zeros(len(open_prices), dtype=int)

    for i in range(2, len(open_prices) - 2):
        high_window = highs[i - 2:i + 3]
        low_window = lows[i - 2:i + 3]
        center_high = highs[i]
        center_low = lows[i]

        # Local maximum condition
        if (center_high == max(high_window) and 
            center_high > high_window[1] and 
            center_high > high_window[3]):
            peaks[i] = 1

        # Local minimum condition
        elif (center_low == min(low_window) and 
              center_low < low_window[1] and 
              center_low < low_window[3]):
            peaks[i] = -1

    return peaks



def is_on_horizontal_level(alert, history):
    tf = alert[1]
    required_peaks = alert[2]

    df = history[tf].sort_values(["instrument_id", "start_time"])
    df = df.groupby("instrument_id").tail(100).reset_index(drop=True)

    result = set()

    for instrument_id, group in df.groupby("instrument_id"):
        closes = group["close"].values
        opens = group["open"].values
        peaks = detect_peaks(opens, closes)
        lower, upper = group['low'].quantile(0.05), group['high'].quantile(0.95)
        radius = (upper - lower) / 30

        last_close = closes[-1]
        lower_bound = last_close - radius
        upper_bound = last_close + radius

        # Masks for peaks within the band
        candle_highs = np.maximum(group["open"].values, group["close"].values)
        candle_lows = np.minimum(group["open"].values, group["close"].values)

        max_mask = (peaks == 1) & (candle_highs >= lower_bound) & (candle_highs <= upper_bound)
        min_mask = (peaks == -1) & (candle_lows >= lower_bound) & (candle_lows <= upper_bound)

        tops = False
        btms = False

        if max_mask.sum() >= required_peaks and (lower_bound <= last_close <= upper_bound):
            first_peak_idx = np.argmax(max_mask)  # Index of first True in max_mask
            if not any(closes[first_peak_idx:] > upper_bound):
                tops = True

        if min_mask.sum() >= required_peaks and (lower_bound <= last_close <= upper_bound):
            first_trough_idx = np.argmax(min_mask)  # Index of first True in min_mask
            if not any(closes[first_trough_idx:] < lower_bound):
                btms = True

        if tops or btms:
            result.add(instrument_id)
    return result

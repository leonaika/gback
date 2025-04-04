import asyncio
from async_all_candles import get_all_candles
import argparse
from backend_response_structure import AlertResult
import numpy as np
import os
from history_data_structure import History


def change_of_price(alert, history):
    tf = alert[1]
    threshold = alert[2]
    df = history[tf].sort_values(["instrument_id", "start_time"])

    df["return"] = df.groupby("instrument_id")["close"].pct_change()

    df["std_return_30"] = (
        df.groupby("instrument_id")["return"]
        .transform(lambda x: x.shift(1).rolling(30).std())
    )

    latest = df.groupby("instrument_id").tail(1)
    latest = latest[latest["std_return_30"].notnull() & (latest["std_return_30"] > 0)]
    latest["volatility_ratio"] = latest["return"].abs() / latest["std_return_30"]
    result = latest[latest["volatility_ratio"] > threshold]["instrument_id"]

    return set(result)

import asyncio
from async_all_candles import get_all_candles
import argparse
from backend_response_structure import AlertResult
import os
from tinkoff.invest import AsyncClient, CandleInterval
from tinkoff.invest.schemas import CandleSource
from history_data_structure import History
import typing
import numpy as np


def abnormal_volume(alert, history):
    threshold = 0.6
    tf = alert[1]
    df = history[tf].sort_values(["instrument_id", "start_time"])

    df["mean_volume_30"] = (
        df.groupby("instrument_id")["volume"]
        .transform(lambda x: x.shift(1).rolling(30).mean())
    )

    latest = df.groupby("instrument_id").tail(1)
    latest = latest[latest["mean_volume_30"].notnull() & (latest["mean_volume_30"] > 0)]
    latest["volume_ratio"] = latest["volume"] / latest["mean_volume_30"]
    result = latest[latest["volume_ratio"] > threshold]["instrument_id"]

    return set(result)

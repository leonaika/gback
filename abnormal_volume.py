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


def abnormal_volume(
    candle_type, instruments, number_of_candles, multiplier, token, history
) -> AlertResult:

    alert_result = AlertResult(0, [], candle_type, "abnormal_volume")  # why candle_type?

    for instrument in instruments:

        relevant_history = np.array([])
        if len(history[instrument][candle_type]):

            relevant_history = history[instrument][candle_type]["volume"][
                -number_of_candles:
            ]
        average_volume = np.mean(relevant_history)

        current_volume = asyncio.run(
            get_all_candles(instrument, 0, candle_type, True, token)
        )[0][
            3
        ]  # check that len(candles) > 0

        result = current_volume / average_volume >= multiplier

        if result:
            alert_result.instruments.append(instrument)

    return alert_result

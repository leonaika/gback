import asyncio
from async_all_candles import get_all_candles
import argparse
from backend_response_structure import AlertResult
import numpy as np
import os
from history_data_structure import History


TOKEN = os.getenv("TOKEN")


def change_of_price(
    candle_type, instruments, number_of_candles, rate_to_std, token, history
) -> AlertResult:

    alert_result = AlertResult(0, [], candle_type, "change_of_price")

    for instrument in instruments:

        relevant_history = np.array([])
        if len(history[instrument][candle_type]):

            relevant_history = history[instrument][candle_type]["high_price"][
                -number_of_candles:
            ]

        std_price_high = np.std(relevant_history)

        current_price_high = asyncio.run(
            get_all_candles(instrument, 0, candle_type, True, token)
        )[0][6]

        result = bool(current_price_high / std_price_high > rate_to_std)

        if result:
            alert_result.instruments.append(instrument)

    return alert_result

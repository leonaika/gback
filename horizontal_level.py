import asyncio
from async_all_candles import get_all_candles
import argparse
from backend_response_structure import AlertResult
from history_data_structure import History
import numpy as np


def find_peaks(candles, num_of_neighbours):

    peaks_indices = []

    for candle_ind in range(num_of_neighbours, len(candles) - num_of_neighbours):

        if_lower = True

        for left_neighbour_ind in range(
            candle_ind - 1, candle_ind - num_of_neighbours - 1, -1
        ):

            if candles[left_neighbour_ind] >= candles[candle_ind]:
                if_lower = False
                break

        for right_neighbour_ind in range(
            candle_ind + 1, candle_ind + num_of_neighbours + 1
        ):

            if candles[right_neighbour_ind] >= candles[candle_ind]:
                if_lower = False
                break

        if if_lower:
            peaks_indices.append(candles[candle_ind])

    return peaks_indices


def is_on_horizontal_level(
    candle_type,
    instruments,
    min_peaks_number,
    num_of_neighbours,
    price_radius,
    token,
    history,
) -> set[str]:

    results = set()

    for instrument in instruments:

        relevant_history = np.array([])
        if len(history[instrument][candle_type]):

            relevant_history = history[instrument][candle_type]["close_price"][-50:]

        current_price_close = asyncio.run(
            get_all_candles(instrument, 0, candle_type, True, token)
        )[0][5]

        peaks = find_peaks(relevant_history, num_of_neighbours)

        lower_bound, upper_bound = (
            current_price_close - current_price_close * (1 + price_radius / 100),
            current_price_close + price_radius * (1 - price_radius / 100),
        )

        peaks_in_neighboorhood = sum(
            1 if lower_bound <= peak <= upper_bound else 0 for peak in peaks
        )

        result = peaks_in_neighboorhood >= min_peaks_number

        if result:
            results.add(instrument)

    return results

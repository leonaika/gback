import asyncio
import argparse
import os
from datetime import timedelta

from tinkoff.invest import AsyncClient, CandleInterval
from tinkoff.invest.schemas import CandleSource
from tinkoff.invest.utils import now

import typing

TOKEN = os.getenv("TOKEN")


def parse_arguments_candles():
    parser = argparse.ArgumentParser(description="Parser of candle arguments")

    parser.add_argument("-timeframe", type=int, help="Timeframe", required=False)
    parser.add_argument(
        "-instrument_id", type=str, help="Instrument ID", required=False
    )
    parser.add_argument("-candle_type", type=str, help="Candle type", required=False)
    parser.add_argument(
        "-last_candle_only",
        type=int,
        help="If only the last candle is needed",
        required=False,
    )
    args = parser.parse_args()
    return args


async def get_all_candles(
    instrument_id, timeframe, candle_type, last_candle_only, token
) -> list[tuple[str, ...]]:

    async with AsyncClient(token) as client:

        delta = 0
        if candle_type == "5min":
            candle_interval = CandleInterval.CANDLE_INTERVAL_5_MIN
            delta = 5
        elif candle_type == "15min":
            candle_interval = CandleInterval.CANDLE_INTERVAL_15_MIN
            delta = 15
        elif candle_type == "1h":
            candle_interval = CandleInterval.CANDLE_INTERVAL_HOUR
            delta = 60
        elif candle_type == "4h":
            candle_interval = CandleInterval.CANDLE_INTERVAL_4_HOUR
            delta = 240
        elif candle_type == "1d":
            candle_interval = CandleInterval.CANDLE_INTERVAL_DAY
            delta = 1440

        if last_candle_only:
            timeframe = delta

        candles = []

        async for candle in client.get_all_candles(
            instrument_id=instrument_id,
            from_=now() - timedelta(minutes=timeframe),
            interval=candle_interval,
            candle_source_type=CandleSource.CANDLE_SOURCE_EXCHANGE,
        ):
            candles.append(
                (
                    instrument_id,
                    candle_type,
                    candle.time,
                    candle.volume,
                    candle.open.units + candle.open.nano / 100000000,
                    candle.close.units + candle.close.nano / 100000000,
                    candle.high.units + candle.high.nano / 100000000,
                    candle.low.units + candle.low.nano / 100000000,
                )
            )

        if last_candle_only:
            return candles[-1:]

    return candles


def main():
    args = parse_arguments_candles()
    candles = asyncio.run(
        get_all_candles(
            args.instrument_id,
            args.timeframe,
            args.candle_type,
            args.last_candle_only,
            TOKEN,
        )
    )
    return candles


if __name__ == "__main__":
    main()

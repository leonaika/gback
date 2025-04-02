import asyncio
from async_all_candles import get_all_candles
import numpy as np
import os
from tinkoff.invest import Client, AsyncClient, CandleInterval
from tqdm import tqdm


HOST = os.getenv("HOST")
TOKEN = os.getenv("TOKEN")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")


class History:

    def __init__(
        self, instruments, candle_timeframes, token, dbname, user, password, host
    ):

        self.instruments = instruments
        self.candle_timeframes = candle_timeframes
        self.history = {}
        self.token = token
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.last_candle = {}

        for instrument in self.instruments:

            self.history[instrument] = {}
            self.last_candle[instrument] = {}

            for candle_timeframe in self.candle_timeframes:

                self.history[instrument][candle_timeframe] = {}
                self.last_candle[instrument][candle_timeframe] = {}

    def __getitem__(self, instrument):
        return self.history[instrument]

    def create_local_history(
        self,
        instrument: str,
        candle_timeframe: str,
    ):

        time_line = 0

        if candle_timeframe == "5min":
            time_line = 5 * 300
        elif candle_timeframe == "15min":
            time_line = 15 * 200
        elif candle_timeframe == "1h":
            time_line = 60 * 100
        elif candle_timeframe == "4h":
            time_line = 240 * 100
        elif candle_timeframe == "1d":
            time_line = 1440 * 100

        a = asyncio.run(
            get_all_candles(instrument, time_line, candle_timeframe, False, self.token)
        )

        candles = np.array(a)

        if candles.size > 0:
            candles = candles[:, 2:]

        for candle in candles:
            self.history[instrument][candle_timeframe]["date_time"] = candles[:, 0]
            self.history[instrument][candle_timeframe]["volume"] = candles[:, 1]
            self.history[instrument][candle_timeframe]["open_price"] = candles[:, 2]
            self.history[instrument][candle_timeframe]["close_price"] = candles[:, 3]
            self.history[instrument][candle_timeframe]["high_price"] = candles[:, 4]
            self.history[instrument][candle_timeframe]["low_price"] = candles[:, 5]

    def create_history(self) -> None:

        for instrument in tqdm(self.instruments):
            tqdm.write(instrument)

            for candle_timeframe in self.candle_timeframes:

                self.create_local_history(
                    instrument,
                    candle_timeframe,
                )

                self.last_candle[instrument][candle_timeframe]["date_time"] = (
                    self.history[instrument][candle_timeframe]["date_time"][-1]
                )

                self.last_candle[instrument][candle_timeframe]["volume"] = self.history[
                    instrument
                ][candle_timeframe]["volume"][-1]

                self.last_candle[instrument][candle_timeframe]["open_price"] = (
                    self.history[instrument][candle_timeframe]["open_price"][-1]
                )

                self.last_candle[instrument][candle_timeframe]["close_price"] = (
                    self.history[instrument][candle_timeframe]["close_price"][-1]
                )

                self.last_candle[instrument][candle_timeframe]["high_price"] = (
                    self.history[instrument][candle_timeframe]["high_price"][-1]
                )

                self.last_candle[instrument][candle_timeframe]["low_price"] = (
                    self.history[instrument][candle_timeframe]["low_price"][-1]
                )

    def update_history(
        self,
        date_time,
        instrument: str,
        candle_timeframe: str,
        volume,
        open_price,
        close_price,
        high_price,
        low_price,
    ) -> None:

        if self.last_candle[instrument][candle_timeframe]["date_time"] == date_time:
            return

        self.history[instrument][candle_timeframe]["volume"] = np.append(
            self.history[instrument][candle_timeframe]["volume"],
            self.last_candle[instrument][candle_timeframe]["volume"],
        )
        self.history[instrument][candle_timeframe]["volume"] = self.history[instrument][
            candle_timeframe
        ]["volume"][1:]

        self.history[instrument][candle_timeframe]["open_price"] = np.append(
            self.history[instrument][candle_timeframe]["open_price"],
            self.last_candle[instrument][candle_timeframe]["open_price"],
        )
        self.history[instrument][candle_timeframe]["open_price"] = self.history[
            instrument
        ][candle_timeframe]["open_price"][1:]

        self.history[instrument][candle_timeframe]["close_price"] = np.append(
            self.history[instrument][candle_timeframe]["close_price"],
            self.last_candle[instrument][candle_timeframe]["close_price"],
        )
        self.history[instrument][candle_timeframe]["close_price"] = self.history[
            instrument
        ][candle_timeframe]["close_price"][1:]

        self.history[instrument][candle_timeframe]["high_price"] = np.append(
            self.history[instrument][candle_timeframe]["high_price"],
            self.last_candle[instrument][candle_timeframe]["high_price"],
        )
        self.history[instrument][candle_timeframe]["high_price"] = self.history[
            instrument
        ][candle_timeframe]["high_price"][1:]

        self.history[instrument][candle_timeframe]["low_price"] = np.append(
            self.history[instrument][candle_timeframe]["low_price"],
            self.last_candle[instrument][candle_timeframe]["low_price"],
        )
        self.history[instrument][candle_timeframe]["low_price"] = self.history[
            instrument
        ][candle_timeframe]["low_price"][1:]

        self.last_candle[instrument][candle_timeframe]["date_time"] = date_time
        self.last_candle[instrument][candle_timeframe]["volume"] = volume
        self.last_candle[instrument][candle_timeframe]["open_price"] = open_price
        self.last_candle[instrument][candle_timeframe]["close_price"] = close_price
        self.last_candle[instrument][candle_timeframe]["high_pricee"] = high_price
        self.last_candle[instrument][candle_timeframe]["low_price"] = low_price

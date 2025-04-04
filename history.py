import pandas as pd
from tinkoff.invest import Client, CandleInterval
from tinkoff.invest.utils import now
from datetime import timedelta, datetime, timezone
import pytz
import os
from dotenv import load_dotenv
from tqdm import tqdm
from tinkoff.invest import AsyncClient

load_dotenv()

TOKEN = os.getenv('TOKEN')

HISTORY_LIMIT_DAYS = {
    "5min": 1,
    "15min": 3,
    "1h": 5,
    "4h": 10,
    "1d": 30,
}

INTERVAL_MAPPING = {
    "5min": CandleInterval.CANDLE_INTERVAL_5_MIN,
    "15min": CandleInterval.CANDLE_INTERVAL_15_MIN,
    "1h": CandleInterval.CANDLE_INTERVAL_HOUR,
    "4h": CandleInterval.CANDLE_INTERVAL_4_HOUR,
    "1d": CandleInterval.CANDLE_INTERVAL_DAY,
}

TIMEFRAME_MAP = {
    "5min": (CandleInterval.CANDLE_INTERVAL_5_MIN, timedelta(minutes=5)),
    "15min": (CandleInterval.CANDLE_INTERVAL_15_MIN, timedelta(minutes=15)),
    "1h": (CandleInterval.CANDLE_INTERVAL_HOUR, timedelta(hours=1)),
    "4h": (CandleInterval.CANDLE_INTERVAL_HOUR, timedelta(hours=4)),
    "1d": (CandleInterval.CANDLE_INTERVAL_DAY, timedelta(days=1)),
}

last_update_time = None

def get_history(instruments):
    timeframes = ["5min", "15min", "1h", "4h", "1d"]

    dfs = {}

    with Client(TOKEN) as client:
        for tf in tqdm(timeframes):
            interval = INTERVAL_MAPPING[tf]
            days_back = HISTORY_LIMIT_DAYS[tf]
            end_time = now()
            start_time = end_time - timedelta(days=days_back)

            records = []

            for instrument_id in instruments:
                candles = client.market_data.get_candles(
                    figi=instrument_id,
                    from_=start_time,
                    to=end_time,
                    interval=interval
                ).candles

                for candle in candles:
                    records.append({
                        "instrument_id": instrument_id,
                        "start_time": candle.time,
                        "open": candle.open.units + candle.open.nano / 1e9,
                        "high": candle.high.units + candle.high.nano / 1e9,
                        "low": candle.low.units + candle.low.nano / 1e9,
                        "close": candle.close.units + candle.close.nano / 1e9,
                        "volume": candle.volume
                    })

            df = pd.DataFrame(records)
            dfs[tf] = df

    return dfs["5min"], dfs["15min"], dfs["1h"], dfs["4h"], dfs["1d"]


async def update_history(history: dict):
    global last_update_time
    tz = timezone.utc
    now = datetime.now(tz)

    if last_update_time and now.minute // 5 == last_update_time.minute // 5:
        return history
    
    last_update_time = now

    async with AsyncClient(TOKEN) as client:
        for tf, df in history.items():
            candle_interval, step = TIMEFRAME_MAP[tf]
            updated_frames = []

            for instrument_id, group in df.groupby("instrument_id"):
                group = group.sort_values("start_time")
                latest_time = group["start_time"].max()

                start_time = latest_time + step
                end_time = now

                if start_time >= end_time:
                    continue  # nothing to update

                response = await client.market_data.get_candles(
                    figi=instrument_id,
                    from_=start_time,
                    to=end_time,
                    interval=candle_interval
                )

                candles = response.candles
                new_records = []
                for candle in candles:
                    new_records.append({
                        "instrument_id": instrument_id,
                        "start_time": candle.time.astimezone(tz),
                        "open": candle.open.units + candle.open.nano / 1e9,
                        "high": candle.high.units + candle.high.nano / 1e9,
                        "low": candle.low.units + candle.low.nano / 1e9,
                        "close": candle.close.units + candle.close.nano / 1e9,
                        "volume": candle.volume
                    })

                if new_records:
                    new_df = pd.DataFrame(new_records)
                    updated_frames.append(new_df)

            if updated_frames:
                updated_df = pd.concat([df] + updated_frames).drop_duplicates(
                    subset=["instrument_id", "start_time"]
                ).sort_values(["instrument_id", "start_time"])
                history[tf] = updated_df.reset_index(drop=True)
    return history

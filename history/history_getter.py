import os
import asyncio
import pandas as pd
from datetime import timedelta, datetime, timezone
from tinkoff.invest import Client, CandleInterval, AsyncClient
from tinkoff.invest.utils import now
from dotenv import load_dotenv
from tqdm import tqdm
from time import time, sleep

load_dotenv()
TOKEN = os.getenv("TOKEN")

# Timeframe settings
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
    "4h": (CandleInterval.CANDLE_INTERVAL_4_HOUR, timedelta(hours=4)),
    "1d": (CandleInterval.CANDLE_INTERVAL_DAY, timedelta(days=1)),
}

# Shared state
history = {}  # stores latest history
history_lock = None
last_update_time = None

async def init_history_globals():
    global history_lock
    history_lock = asyncio.Lock()

# Sync function to fetch full initial history
def get_history(instruments):
    dfs = {}
    timeframes = list(HISTORY_LIMIT_DAYS.keys())

    with Client(TOKEN) as client:
        for tf in tqdm(timeframes):
            interval = INTERVAL_MAPPING[tf]
            end_time = now()
            start_time = end_time - timedelta(days=HISTORY_LIMIT_DAYS[tf])
            records = []

            for instrument_id in instruments:
                candles = client.market_data.get_candles(
                    figi=instrument_id,
                    from_=start_time,
                    to=end_time,
                    interval=interval
                ).candles
                sleep(2)

                for candle in candles:
                    records.append({
                        "instrument_id": instrument_id,
                        "start_time": candle.time.replace(tzinfo=timezone.utc),
                        "open": candle.open.units + candle.open.nano / 1e9,
                        "high": candle.high.units + candle.high.nano / 1e9,
                        "low": candle.low.units + candle.low.nano / 1e9,
                        "close": candle.close.units + candle.close.nano / 1e9,
                        "volume": candle.volume
                    })

            df = pd.DataFrame(records)
            dfs[tf] = df

    return dfs["5min"], dfs["15min"], dfs["1h"], dfs["4h"], dfs["1d"]


# Async update function — updates existing history in memory
async def update_history(history: dict):
    global last_update_time
    now_utc = datetime.now(timezone.utc)

    async with AsyncClient(TOKEN) as client:
        for tf, df in history.items():
            t0 = time()
            candle_interval, step = TIMEFRAME_MAP[tf]
            updated_frames = []

            for instrument_id, group in df.groupby("instrument_id"):
                group = group.sort_values("start_time")
                latest_time = group["start_time"].max()

                start_time = latest_time + step
                end_time = now_utc

                if start_time >= end_time:
                    continue  # up to date

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
                        "start_time": candle.time.replace(tzinfo=timezone.utc),
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
                async with history_lock:
                    history[tf] = updated_df.reset_index(drop=True)
            print(f"------------Update history {tf}:", round(time() - t0, 2))
    return history


# Background updater loop
async def periodic_history_updater():
    global history

    while True:
        try:
            updated = await update_history(history)
            history = updated
            await asyncio.sleep(5)
        except:
            print('API cooldown')
            await asyncio.sleep(30)

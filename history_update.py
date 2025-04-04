from dotenv import load_dotenv
import os
import pg8000
from tinkoff.invest import Client, CandleInterval
from datetime import datetime, timedelta
import time
from tqdm import tqdm
from zoneinfo import ZoneInfo


load_dotenv()


HOST = os.getenv("HOST")
TOKEN = os.getenv("TOKEN")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")


conn = pg8000.connect(
            database=DATABASE,
            user=DB_USER,
            password=PASSWORD,
            host=HOST,
            ssl_context=True,
        )

cur = conn.cursor()

# Define the number of historical candles to keep for each timeframe
HISTORY_LIMIT = {
    "5min": 2000,
    "15min": 1500,
    "1h": 1000,
    "4h": 800,
    "1d": 500,
}

HISTORY_LIMIT_DAYS = {
    "5min": 20,
    "15min": 40,
    "1h": 60,
    "4h": 120,
    "1d": 300,
}

# Mapping timeframe strings to Tinkoff API CandleInterval
TIMEFRAME_MAPPING = {
    "5min": CandleInterval.CANDLE_INTERVAL_5_MIN,
    "15min": CandleInterval.CANDLE_INTERVAL_15_MIN,
    "1h": CandleInterval.CANDLE_INTERVAL_HOUR,
    "4h": CandleInterval.CANDLE_INTERVAL_4_HOUR,
    "1d": CandleInterval.CANDLE_INTERVAL_DAY,
}

moscow_tz = ZoneInfo("Europe/Moscow")


def update_history(instruments: list):
    """Updates missing candles in the database for given instruments."""

    with Client(TOKEN) as client:
        for instrument_id in tqdm(instruments):

            for timeframe, candle_interval in TIMEFRAME_MAPPING.items():
                # Get the latest candle from the database for this instrument & timeframe
                query = """SELECT start_time FROM candles_data_stock 
                           WHERE instrument_id = %s AND timeframe = %s 
                           ORDER BY start_time DESC LIMIT 1"""
                cur.execute(query, (instrument_id, timeframe))
                latest_candle = cur.fetchone()

                # Determine the start time for fetching new data
                if latest_candle:
                    start_time = (latest_candle[0] + timedelta(minutes=5 if timeframe == "5min" else
                                                                15 if timeframe == "15min" else
                                                                60 if timeframe == "1h" else
                                                                240 if timeframe == "4h" else
                                                                1440)).replace(tzinfo=moscow_tz)
                else:
                    start_time = datetime.now(tz=moscow_tz) - timedelta(days=HISTORY_LIMIT_DAYS[timeframe])  # Fetch full history

                end_time = datetime.now(tz=moscow_tz)

                # Fetch missing candles
                candles = client.get_all_candles(
                    figi=instrument_id,
                    from_=start_time,
                    to=end_time,
                    interval=candle_interval
                )

                candles = list(candles)[-HISTORY_LIMIT[timeframe]:]

                # Insert new candles into the database
                for candle in candles:
                    query = """INSERT INTO candles_data_stock (instrument_id, start_time, timeframe, open, high, low, close, volume)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT DO NOTHING"""
                    cur.execute(query, (instrument_id, candle.time, timeframe,
                                        candle.open.units + candle.open.nano / 1e9,
                                        candle.high.units + candle.high.nano / 1e9,
                                        candle.low.units + candle.low.nano / 1e9,
                                        candle.close.units + candle.close.nano / 1e9,
                                        candle.volume))

                # Keep only the latest N candles to limit storage
                query = f"""DELETE FROM candles_data_stock 
                            WHERE instrument_id = %s AND timeframe = %s 
                            AND start_time NOT IN (
                                SELECT start_time FROM candles_data_stock 
                                WHERE instrument_id = %s AND timeframe = %s 
                                ORDER BY start_time DESC LIMIT %s
                            )"""
                cur.execute(query, (instrument_id, timeframe, instrument_id, timeframe, HISTORY_LIMIT[timeframe]))

                conn.commit()

                time.sleep(0.2)

query = """SELECT instrument_id FROM instrument_id_ticker"""
cur.execute(query)
result = cur.fetchall()
instr_list = [i[0] for i in result]
while True:
    try:
        update_history(instr_list)
    except:
        time.sleep(120)

from abnormal_volume import abnormal_volume
import asyncio
from async_all_candles import get_all_candles
import argparse
from change_of_price import change_of_price
from check_time_msc import check_time
import datetime
from history_data_structure import History
from horizontal_level import is_on_horizontal_level
import os
import pg8000
import requests
import time
from tinkoff.invest import Client, AsyncClient, CandleInterval
import zoneinfo


HOST = os.getenv("HOST")
TOKEN = os.getenv("TOKEN")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")


def main():

    if (
        check_time()
    ):  # проверяю, что биржа работает через время работы мск биржи, feel free to change

        all_instruments = []
        with Client(TOKEN) as client:
            r = client.instruments.shares()
            for instrument in r.instruments:
                if instrument.class_code == "TQBR":
                    all_instruments.append(instrument)

        # all_instruments = ["BBG004730N88"]       #если просто потестить, то можешь вот эту строчку юзать вместо предыдщущих
        timeframes = ["5min", "15min", "1h", "4h", "1d"]
        history = History(
            all_instruments,
            timeframes,
            TOKEN,
            DATABASE,
            DB_USER,
            PASSWORD,
            HOST,
        )
        history.create_history()

    while True:

        if not check_time():
            continue

        time.sleep(10)

        conn = pg8000.connect(
            database=DATABASE,
            user=DB_USER,
            password=PASSWORD,
            host=HOST,
            ssl_context=True,
        )

        cur = conn.cursor()

        cur.execute(
            """SELECT user_id, alert_id
                  FROM alerts
                  ;""",
        )

        users_alerts = cur.fetchall()

        alerts_users_map = {}

        for user, alert in users_alerts:
            alerts_users_map[alert] = user

        cur.execute(
            """SELECT alert_id, timeframe, instrument_id, multiplier
                    FROM filter_abnormal_volume
            ;""",
        )

        abnormal_volume_alerts = cur.fetchall()

        cur.execute(
            """SELECT alert_id, timeframe, instrument_id, rate_to_std
                  FROM filter_price_change
            ;""",
        )

        price_change_alerts = cur.fetchall()

        cur.execute(
            """SELECT alert_id, timeframe, instrument_id, min_peaks_count
                  FROM filter_horizontal_level
            ;""",
        )

        horizontal_level_alerts = cur.fetchall()

        results = {}

        for abnormal_volume_alert in abnormal_volume_alerts:
            result = abnormal_volume(
                abnormal_volume_alert[1],
                all_instruments,
                30,
                abnormal_volume_alert[2],
                TOKEN,
                history,
            )
            if result:
                results[abnormal_volume_alert[0]] = result

        for price_change_alert in price_change_alerts:
            result = change_of_price(
                price_change_alert[1],
                all_instruments,
                30,
                price_change_alert[2],
                TOKEN,
                history,
            )
            if result:
                results[price_change_alert[0]] = result

        for horizontal_level_alert in horizontal_level_alerts:
            result = is_on_horizontal_level(
                horizontal_level_alert[1],
                all_instruments,
                horizontal_level_alert[2],
                3,  # num_of_neigbours (one side)
                5,  # price radius (percent)
                TOKEN,
                history,
            )
            if result:
                results[horizontal_level_alert[0]] = result

        for alert in results:
            results[alert].user_id = alerts_users_map[alert]

        for instument in all_instruments:
            for timeframe in timeframes:
                candle = asyncio.run(
                    get_all_candles(instument, 0, timeframe, True, TOKEN)
                )[0]

                history.update_history(
                    candle[2],
                    instument,
                    timeframe,
                    candle[3],
                    candle[4],
                    candle[5],
                    candle[6],
                    candle[7],
                )

        for alert in results:
            result = results[alert]
            if len(result.instruments):
                url = "http://0.0.0.0:8000/send-message/"
                data = {
                    "user_id": result.user_id,
                    "instruments": result.instruments,
                    "timeframe": result.timeframe,
                    "alert_name": result.alert_name,
                }
                response = requests.post(url, json=data)


if __name__ == "__main__":
    main()

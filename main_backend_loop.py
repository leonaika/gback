from abnormal_volume import abnormal_volume
import asyncio
from async_all_candles import get_all_candles
import argparse
from backend_response_structure import AlertResult
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

        all_instruments = [
            "BBG004730N88"
        ]  # если просто потестить, то можешь вот эту строчку юзать вместо предыдщущих
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
            """SELECT alert_name, alert_id
                  FROM alerts
                  ;""",
        )

        alerts_name_id = cur.fetchall()

        alerts_users_map = {}

        for alert_name, alert_id in alerts_name_id:
            alerts_users_map[alert_id] = AlertResult(alert_id, set(), alert_name, False)

        cur.execute(
            """SELECT alert_id, high_volume_tf
                    FROM filter_high_volume
            ;""",
        )

        abnormal_volume_alerts = cur.fetchall()

        cur.execute(
            """SELECT alert_id, high_volatility_tf, high_volatility_ret_std
                  FROM filter_high_volatility
            ;""",
        )

        price_change_alerts = cur.fetchall()

        cur.execute(
            """SELECT alert_id, horizontal_level_tf, horizontal_level_peaks
                  FROM filter_horizontal_level
            ;""",
        )

        horizontal_level_alerts = cur.fetchall()

        # results = {}

        for abnormal_volume_alert in abnormal_volume_alerts:
            result = abnormal_volume(
                abnormal_volume_alert[1],
                all_instruments,
                30,
                2,
                TOKEN,
                history,
            )
            if alerts_users_map[abnormal_volume_alert[0]].seen:
                alerts_users_map[abnormal_volume_alert[0]].instruments &= result
            else:
                alerts_users_map[abnormal_volume_alert[0]].instruments = result
                alerts_users_map[abnormal_volume_alert[0]].seen = True

        for price_change_alert in price_change_alerts:
            result = change_of_price(
                price_change_alert[1],
                all_instruments,
                30,
                price_change_alert[2],
                TOKEN,
                history,
            )
            if alerts_users_map[abnormal_volume_alert[0]].seen:
                alerts_users_map[abnormal_volume_alert[0]].instruments &= result
            else:
                alerts_users_map[abnormal_volume_alert[0]].instruments = result
                alerts_users_map[abnormal_volume_alert[0]].seen = True

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
            if alerts_users_map[abnormal_volume_alert[0]].seen:
                alerts_users_map[abnormal_volume_alert[0]].instruments &= result
            else:
                alerts_users_map[abnormal_volume_alert[0]].instruments = result
                alerts_users_map[abnormal_volume_alert[0]].seen = True

        # for alert in results:
        #     results[alert].user_id = alerts_users_map[alert]

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

        for alert in alerts_users_map:
            result = alerts_users_map[alert]
            if len(result.instruments):
                url = "http://0.0.0.0:8000/send-message/"
                data = {
                    "alert_id": result.alert_id,
                    "instruments": list(result.instruments),
                    "alert_name": result.alert_name,
                }
                response = requests.post(url, json=data)


if __name__ == "__main__":
    main()

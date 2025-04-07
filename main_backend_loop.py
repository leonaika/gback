from abnormal_volume import abnormal_volume
import asyncio
from change_of_price import change_of_price
from check_time_msc import check_time
import time
from tinkoff.invest import Client
import requests
import os
import pg8000
from instruments_blacklist import instruments_blacklist
import history
from backend_response_structure import AlertResult
from horizontal_level import is_on_horizontal_level

HOST = os.getenv("HOST")
TOKEN = os.getenv("TOKEN")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")


async def main():
    await history.init_history_globals()

    if True:
        # Fetch all instruments (this can be done here or in a separate task)
        all_instruments = []
        with Client(TOKEN) as client:
            r = client.instruments.shares()
            for instrument in r.instruments:
                if (instrument.class_code == "TQBR") and (
                    instrument.figi not in instruments_blacklist
                ):
                    all_instruments.append(instrument.figi)

        # all_instruments = [
        #     "BBG004730N88",
        #     "BBG004731032"
        # ]  # test

        timeframes = ["5min", "15min", "1h", "4h", "1d"]
        hist_dfs = history.get_history(all_instruments)
        local_history = dict(zip(timeframes, hist_dfs))

        async with history.history_lock:
            history.history.clear()
            history.history.update(local_history)

    # Start the background task for updating history
    asyncio.create_task(history.periodic_history_updater())

    while True:
        print("sleeping...")
        await asyncio.sleep(10)

        # Database connection setup
        conn = pg8000.connect(
            database=DATABASE,
            user=DB_USER,
            password=PASSWORD,
            host=HOST,
            ssl_context=True,
        )

        cur = conn.cursor()

        cur.execute("""SELECT alert_name, alert_id FROM alerts;""")
        alerts_name_id = cur.fetchall()
        alerts_users_map = {}

        t0 = time.time()
        for alert_id, alert_name in alerts_name_id:
            alerts_users_map[alert_id] = AlertResult(alert_id, set(), alert_name, False)

        cur.execute("""SELECT alert_id, high_volume_tf FROM filter_high_volume;""")
        abnormal_volume_alerts = cur.fetchall()

        cur.execute(
            """SELECT alert_id, high_volatility_tf, high_volatility_ret_std FROM filter_high_volatility;"""
        )
        price_change_alerts = cur.fetchall()

        cur.execute(
            """SELECT alert_id, horizontal_level_tf, horizontal_level_peaks FROM filter_horizontal_level;"""
        )
        horizontal_level_alerts = cur.fetchall()

        print("Get all alerts data:", round(time.time() - t0, 2))

        # Use the imported global history, locked for thread safety
        async with history.history_lock:
            local_history = history.history.copy()  # Make a copy of the current history

        # Process each type of alert (abnormal volume, price change, horizontal level)
        t0 = time.time()
        for abnormal_volume_alert in abnormal_volume_alerts:
            result = abnormal_volume(abnormal_volume_alert, local_history)
            alert_id = abnormal_volume_alert[0]
            if alerts_users_map[alert_id].seen:
                alerts_users_map[alert_id].instruments &= result
            else:
                alerts_users_map[alert_id].instruments = result
                alerts_users_map[alert_id].seen = True
        print("Filter high volume:", round(time.time() - t0, 2))

        t0 = time.time()
        for price_change_alert in price_change_alerts:
            result = change_of_price(price_change_alert, local_history)
            alert_id = price_change_alert[0]
            if alerts_users_map[alert_id].seen:
                alerts_users_map[alert_id].instruments &= result
            else:
                alerts_users_map[alert_id].instruments = result
                alerts_users_map[alert_id].seen = True
        print("Filter high volatility:", round(time.time() - t0, 2))

        t0 = time.time()
        for horizontal_level_alert in horizontal_level_alerts:
            result = is_on_horizontal_level(horizontal_level_alert, local_history)
            alert_id = horizontal_level_alert[0]
            if alerts_users_map[alert_id].seen:
                alerts_users_map[alert_id].instruments &= result
            else:
                alerts_users_map[alert_id].instruments = result
                alerts_users_map[alert_id].seen = True
        print("Filter horizontal level:", round(time.time() - t0, 2))

        t0 = time.time()
        # Send alerts to front
        for alert in alerts_users_map:
            result = alerts_users_map[alert]
            if len(result.instruments):
                url = "http://0.0.0.0:8080/send-message/"
                data = {
                    "alert_id": result.alert_id,
                    "instruments": list(result.instruments),
                    "alert_name": result.alert_name,
                }
                try:
                    response = requests.post(url, json=data)
                except:
                    print("Failed to send alerts to front")
        print("Send alerts to front:", round(time.time() - t0, 2))


if __name__ == "__main__":
    asyncio.run(main())

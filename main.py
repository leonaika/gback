from filters.high_volume import process_high_volume_filter
import asyncio
from filters.high_volatility import process_high_volatility_filter
import time
from tinkoff.invest import Client
import requests
import os
import pg8000
from instruments_blacklist import instruments_blacklist
import history.history_getter as history_getter
from filters.horizontal_level import process_horizontal_level_filter
import alerts_getter as alget

HOST = os.getenv("HOST")
TOKEN = os.getenv("TOKEN")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")


async def main():
    await history_getter.init_history_globals()

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
        hist_dfs = history_getter.get_history(all_instruments)
        local_history = dict(zip(timeframes, hist_dfs))

        async with history_getter.history_lock:
            history_getter.history.clear()
            history_getter.history.update(local_history)

    # Start the background task for updating history
    asyncio.create_task(history_getter.periodic_history_updater())

    print('running infinite loop')
    while True:
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

        alerts_users_map = await alget.get_alerts_users_map(cur)
        high_volume_alerts = await alget.get_high_volume_alerts(cur)
        high_volatility_alerts = await alget.get_high_volatility_alerts(cur)
        horizontal_level_alerts = await alget.get_horizontal_level_alerts(cur)

        # Use the imported global history, locked for thread safety
        async with history_getter.history_lock:
            local_history = history_getter.history.copy()  # Make a copy of the current history

        process_high_volume_filter(high_volume_alerts, alerts_users_map, local_history)
        process_high_volatility_filter(high_volatility_alerts, alerts_users_map, local_history)
        process_horizontal_level_filter(horizontal_level_alerts, alerts_users_map, local_history)

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


if __name__ == "__main__":
    asyncio.run(main())

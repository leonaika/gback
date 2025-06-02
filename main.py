from filters.high_volume import process_high_volume
from filters.high_volatility import process_high_volatility
from filters.horizontal_level import process_horizontal_level
from filters.rsi import process_rsi
import asyncio
import time
from tinkoff.invest import Client
import requests
import os
import pg8000
from instruments_blacklist import instruments_blacklist
import history.history_getter as history_getter
import history.trading_status as trading_status
import alerts_getter as alget

HOST = os.getenv("HOST")
TOKEN = os.getenv("TOKEN")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")


async def main():
    await history_getter.init_history_globals()
    await trading_status.init_trading_status_globals()

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
        
        local_normal_trading_set = trading_status.get_initial_trading_status(all_instruments)
        async with trading_status.trading_status_lock:
            trading_status.normal_trading_set.clear()
            trading_status.normal_trading_set.update(local_normal_trading_set)

    # Start the background task for updating history and trading status
    asyncio.create_task(history_getter.periodic_history_updater())
    asyncio.create_task(trading_status.periodic_trading_status_updater(all_instruments))

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
        rsi_alerts = await alget.get_rsi_alerts(cur)

        # Use the imported global history, locked for thread safety
        async with history_getter.history_lock:
            local_history = history_getter.history.copy()
        
        async with trading_status.trading_status_lock:
            local_normal_trading_set = trading_status.normal_trading_set.copy()

        process_high_volume(high_volume_alerts, alerts_users_map, local_history, local_normal_trading_set)
        process_high_volatility(high_volatility_alerts, alerts_users_map, local_history, local_normal_trading_set)
        process_horizontal_level(horizontal_level_alerts, alerts_users_map, local_history, local_normal_trading_set)
        process_rsi(rsi_alerts, alerts_users_map, local_history, local_normal_trading_set)

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

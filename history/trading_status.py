from tinkoff.invest import Client, InstrumentIdType
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()
TOKEN = os.getenv('TOKEN')

normal_trading_set = set()
trading_status_lock = None

async def init_trading_status_globals():
    global trading_status_lock
    trading_status_lock = asyncio.Lock()

def get_initial_trading_status(instruments):
    initial_trading_status = {}
    for instrument_id in instruments:
        with Client(TOKEN) as client:
            status = client.market_data.get_trading_status(instrument_id=instrument_id)
        initial_trading_status[instrument_id] = str(status.trading_status)
    return initial_trading_status

async def update_trading_status(instruments: list):
    for instrument_id in instruments:
        with Client(TOKEN) as client:
            status = client.market_data.get_trading_status(instrument_id=instrument_id)
        async with trading_status_lock:
            if status.trading_status == 'SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING':
                normal_trading_set.add(instrument_id)
    return normal_trading_set


# Background updater loop
async def periodic_trading_status_updater(instruments: list):
    global normal_trading_set

    while True:
        try:
            updated = await update_trading_status(instruments)
            normal_trading_set = updated
            await asyncio.sleep(60)
        except:
            await asyncio.sleep(60)

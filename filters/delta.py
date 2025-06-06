import asyncio
import datetime
from tinkoff.invest import AsyncClient, Client, TradeDirection, SecurityTradingStatus
from tinkoff.invest.utils import now
import os
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv(
    "TOKEN_TRADES"
)  # important: requires TOKEN_TRADES secret variable to exist in .env

delta_lock = None
time_from_global = now().replace(hour=0, minute=0, second=0, microsecond=0)


async def init_delta_globals():
    global delta_lock
    delta_lock = asyncio.Lock()


async def get_local_delta(cur, conn, instruments, time_from, time_to):

    for instrument_id in instruments:

        with AsyncClient(TOKEN) as client:

            status = client.market_data.get_trading_status(instrument_id=instrument_id)

            if (
                status.trading_status
                == SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING
            ):

                trades = await client.market_data.get_last_trades(
                    figi=instrument_id, from_=time_from, to=time_to
                )
                realized_bids_volume = sum(
                    trade.quantity * (trade.price.units + trade.price.nano / 1e9)
                    for trade in trades
                    if trade.direction == TradeDirection.TRADE_DIRECTION_BUY
                )

                realized_asks_volume = sum(
                    trade.quantity * (trade.price.units + trade.price.nano / 1e9)
                    for trade in trades
                    if trade.direction == TradeDirection.TRADE_DIRECTION_SELL
                )

                realized_delta = realized_bids_volume - realized_asks_volume

                async with delta_lock:
                    cur.executemany(
                        """
                            INSERT INTO local_delta (instrument_id, date_time_start, date_time_end, delta) VALUES (%s, %s, %s, %s); 
                            """,
                        # important: requires local_delta table to exist
                        [
                            (instrument_id, time_from, time_to, realized_delta),
                        ],
                    )
                    conn.commit()


# Background updater loop
async def delta_bd_updater(cur, conn, instruments):
    global time_from_global
    # time_from_global = now().replace(hour=0, minute=0, second=0, microsecond=0)

    while True:
        try:
            time_end = time_from_global + datetime.timedelta(minutes=5)
            if time_from_global == now().replace(
                hour=0, minute=0, second=0, microsecond=0
            ):
                time_end = now()

            updated = await get_local_delta(
                cur,
                conn,
                instruments,
                time_from_global,
                time_end,
            )
            time_from_global += datetime.timedelta(minutes=5)
            await asyncio.sleep(5)
        except:
            await asyncio.sleep(5)

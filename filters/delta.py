from tinkoff.invest import Client, TradeDirection
from tinkoff.invest.utils import now


def get_local_delta(cur, conn, token, instruments, time_from, time_to):

    realized_delta = 0
    with Client(token) as client:

        for instrument_id in instruments:

            status = client.market_data.get_trading_status(instrument_id=instrument_id)

            if (
                status.trading_status
                == "SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING"
            ):

                trades = client.market_data.get_last_trades(
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

                cur.executemany(
                    """
                        INSERT INTO local_delta (instrument_id, date_time_start, date_time_end, delta) VALUES (%s, %s, %s, %s);
                        """,
                    [
                        (instrument_id, time_from, time_to, realized_delta),
                    ],
                )
                conn.commit()

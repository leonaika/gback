def process_rsi(rsi_alerts, alerts_users_map, local_history, local_normal_trading_set):
    for alert in rsi_alerts:
        result = rsi(alert, local_history)
        alert_id = alert[0]
        if alerts_users_map[alert_id].seen:
            alerts_users_map[alert_id].instruments &= result
        else:
            alerts_users_map[alert_id].instruments = result
            alerts_users_map[alert_id].seen = True
        alerts_users_map[alert_id].instruments &= local_normal_trading_set

def rsi(alert, history):
    tf = alert[1]
    rsi_min = alert[2]
    rsi_max = alert[3]

    df = history[tf].sort_values(["instrument_id", "start_time"]).copy()

    df["close_diff"] = df.groupby("instrument_id")["close"].diff()
    df["gain"] = df["close_diff"].where(df["close_diff"] > 0, 0.0)
    df["loss"] = -df["close_diff"].where(df["close_diff"] < 0, 0.0)

    window = 14

    df["avg_gain"] = df.groupby("instrument_id")["gain"].transform(
        lambda x: x.ewm(alpha=1/window, adjust=False).mean()
    )
    df["avg_loss"] = df.groupby("instrument_id")["loss"].transform(
        lambda x: x.ewm(alpha=1/window, adjust=False).mean()
    )

    df["rs"] = df["avg_gain"] / df["avg_loss"]
    df["rsi"] = 100 - (100 / (1 + df["rs"]))

    latest = df.groupby("instrument_id").tail(1)
    latest = latest[latest["rsi"].notnull()]

    result = latest[
        (latest["rsi"] > rsi_max) | (latest["rsi"] < rsi_min)
    ]["instrument_id"]

    return set(result)

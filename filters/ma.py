def process_ma(ma_alerts, alerts_users_map, local_history, local_normal_trading_set):
    for alert in ma_alerts:
        result = ma(alert, local_history)
        alert_id = alert[0]
        if alerts_users_map[alert_id].seen:
            alerts_users_map[alert_id].instruments &= result
        else:
            alerts_users_map[alert_id].instruments = result
            alerts_users_map[alert_id].seen = True
        alerts_users_map[alert_id].instruments &= local_normal_trading_set

def ma(alert, history):
    tf = alert[1]
    ma_type = alert[2].lower()
    ma_length = alert[3]

    df = history[tf].sort_values(["instrument_id", "start_time"]).copy()

    if ma_type == "sma":
        df["ma"] = df.groupby("instrument_id")["close"].transform(
            lambda x: x.rolling(ma_length).mean()
        )
    elif ma_type == "ema":
        df["ma"] = df.groupby("instrument_id")["close"].transform(
            lambda x: x.ewm(span=ma_length, adjust=False).mean()
        )
    else:
        raise ValueError(f"Unsupported MA type: {ma_type}")

    proximity_map = df.groupby("instrument_id")[["instrument_id", "high", "low"]].apply(
        lambda group: (group["high"].quantile(0.95) - group["low"].quantile(0.05)) / 40).to_dict()



    df["proximity"] = df["instrument_id"].map(proximity_map)

    latest = df.groupby("instrument_id").tail(1).copy()

    lower = latest["ma"] - latest["proximity"]
    upper = latest["ma"] + latest["proximity"]
    matched = latest[(latest["close"] >= lower) & (latest["close"] <= upper)]

    return set(matched["instrument_id"])

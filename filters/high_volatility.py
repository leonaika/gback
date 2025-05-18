def process_high_volatility(high_volatility_alerts, alerts_users_map, local_history):
    for alert in high_volatility_alerts:
        result = change_of_price(alert, local_history)
        alert_id = alert[0]
        if alerts_users_map[alert_id].seen:
            alerts_users_map[alert_id].instruments &= result
        else:
            alerts_users_map[alert_id].instruments = result
            alerts_users_map[alert_id].seen = True


def change_of_price(alert, history):
    tf = alert[1]
    threshold = alert[2]
    df = history[tf].sort_values(["instrument_id", "start_time"])

    df["return"] = df.groupby("instrument_id")["close"].pct_change()

    df["std_return_30"] = (
        df.groupby("instrument_id")["return"]
        .transform(lambda x: x.shift(1).rolling(30).std())
    )

    latest = df.groupby("instrument_id").tail(1)
    latest = latest[latest["std_return_30"].notnull() & (latest["std_return_30"] > 0)]
    latest["volatility_ratio"] = latest["return"].abs() / latest["std_return_30"]
    result = latest[latest["volatility_ratio"] > threshold]["instrument_id"]

    return set(result)

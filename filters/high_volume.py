def process_high_volume(high_volume_alerts, alerts_users_map, local_history, local_normal_trading_set):
    for alert in high_volume_alerts:
            result = abnormal_volume(alert, local_history)
            alert_id = alert[0]
            if alerts_users_map[alert_id].seen:
                alerts_users_map[alert_id].instruments &= result
            else:
                alerts_users_map[alert_id].instruments = result
                alerts_users_map[alert_id].seen = True
            alerts_users_map[alert_id].instruments &= local_normal_trading_set


def abnormal_volume(alert, history):
    threshold = 2.5
    tf = alert[1]
    df = history[tf].sort_values(["instrument_id", "start_time"])

    df["mean_volume_30"] = (
        df.groupby("instrument_id")["volume"]
        .transform(lambda x: x.shift(1).rolling(30).mean())
    )

    latest = df.groupby("instrument_id").tail(1)
    latest = latest[latest["mean_volume_30"].notnull() & (latest["mean_volume_30"] > 0)]
    latest["volume_ratio"] = latest["volume"] / latest["mean_volume_30"]
    result = latest[latest["volume_ratio"] > threshold]["instrument_id"]

    return set(result)

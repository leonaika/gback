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

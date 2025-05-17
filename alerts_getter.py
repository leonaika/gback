from response.backend_response_structure import AlertResult


async def get_alerts_users_map(cur):
    cur.execute("""SELECT alert_id, alert_name FROM alerts;""")
    alerts_name_id = cur.fetchall()
    alerts_users_map = {}

    for alert_id, alert_name in alerts_name_id:
        alerts_users_map[alert_id] = AlertResult(alert_id, set(), alert_name, False)
    return alerts_users_map

async def get_high_volume_alerts(cur):
    cur.execute("""SELECT alert_id, high_volume_tf FROM filter_high_volume;""")
    abnormal_volume_alerts = cur.fetchall()
    return abnormal_volume_alerts

async def get_high_volatility_alerts(cur):
    cur.execute(
        """SELECT alert_id, high_volatility_tf, high_volatility_ret_std FROM filter_high_volatility;"""
    )
    price_change_alerts = cur.fetchall()
    return price_change_alerts

async def get_horizontal_level_alerts(cur):
    cur.execute(
        """SELECT alert_id, horizontal_level_tf, horizontal_level_peaks FROM filter_horizontal_level;"""
    )
    horizontal_level_alerts = cur.fetchall()
    return horizontal_level_alerts

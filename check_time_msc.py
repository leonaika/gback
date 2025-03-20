import datetime
import zoneinfo


def check_time() -> bool:

    zone = zoneinfo.ZoneInfo("Europe/Moscow")
    day_of_week_msc = datetime.datetime.now(zone).weekday()
    time_msc = (
        datetime.datetime.now(zone).hour + datetime.datetime.now(zone).minute / 60
    )

    if 0 <= day_of_week_msc <= 4:
        if 9 + 50 / 60 < time_msc < 19:
            return True

    return False

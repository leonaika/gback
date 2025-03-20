from dataclasses import dataclass


@dataclass
class AlertResult:

    user_id: int
    instruments: list[str]
    timeframe: str
    alert_name: str

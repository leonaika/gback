from dataclasses import dataclass


@dataclass
class AlertResult:

    user_id: int # change to alert_id
    instruments: list[str]
    timeframe: str # not needed
    alert_name: str

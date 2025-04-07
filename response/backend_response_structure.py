from dataclasses import dataclass


@dataclass
class AlertResult:

    alert_id: int
    instruments: set[str]
    alert_name: str
    seen: bool

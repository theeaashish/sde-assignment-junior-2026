from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

IST = timezone(timedelta(hours=5, minutes=30))


def parse_ist(value: str | datetime) -> datetime:
    dt = datetime.fromisoformat(value) if isinstance(value, str) else value
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(IST)


def format_ist(dt: datetime) -> str:
    return parse_ist(dt).isoformat(timespec="microseconds")


class Clock(Protocol):
    def now(self) -> datetime: ...

    def sim_to_real_seconds(self, sim_seconds: float) -> float: ...


@dataclass(frozen=True)
class SimClock:
    sim_epoch: datetime
    real_epoch: float
    speed: float

    @classmethod
    def from_clock_payload(cls, payload: dict[str, Any]) -> SimClock:
        return cls(
            sim_epoch=parse_ist(payload["sim_epoch"]),
            real_epoch=float(payload["real_epoch"]),
            speed=float(payload["speed"]),
        )

    def now(self) -> datetime:
        elapsed_real = time.time() - self.real_epoch
        return self.sim_epoch + timedelta(seconds=elapsed_real * self.speed)

    def sim_to_real_seconds(self, sim_seconds: float) -> float:
        if self.speed <= 0:
            raise ValueError("clock speed must be positive")
        return max(0.0, sim_seconds / self.speed)


class FakeClock:
    def __init__(self, current_time: str | datetime, speed: float = 600.0) -> None:
        self.current_time = parse_ist(current_time)
        self.speed = speed

    def now(self) -> datetime:
        return self.current_time

    def advance(self, *, minutes: float = 0.0, seconds: float = 0.0) -> datetime:
        self.current_time += timedelta(minutes=minutes, seconds=seconds)
        return self.current_time

    def set_time(self, value: str | datetime) -> datetime:
        self.current_time = parse_ist(value)
        return self.current_time

    def sim_to_real_seconds(self, sim_seconds: float) -> float:
        if self.speed <= 0:
            raise ValueError("clock speed must be positive")
        return max(0.0, sim_seconds / self.speed)

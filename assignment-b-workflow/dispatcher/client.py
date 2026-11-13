from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from dispatcher.clock import SimClock
from dispatcher.models import CallAttempt


@dataclass(frozen=True)
class CallAccepted:
    call_sid: str
    state: str
    result: dict[str, Any] | None


@dataclass(frozen=True)
class CallTransientError:
    pass


@dataclass(frozen=True)
class CallAmbiguousError:
    pass


@dataclass(frozen=True)
class CallRejectedError:
    status_code: int
    detail: str


@dataclass(frozen=True)
class CallFound:
    call_sid: str
    state: str
    result: dict[str, Any] | None


@dataclass(frozen=True)
class CallNotFound:
    pass


PlaceResult = CallAccepted | CallTransientError | CallAmbiguousError | CallRejectedError
CallLookupResult = CallFound | CallNotFound


class CarrierClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8099") -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=5.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch_clock(self, retries: int = 20, delay: float = 0.25) -> SimClock:
        last_error: httpx.HTTPError | None = None
        for attempt in range(retries):
            try:
                response = await self._client.get("/v1/clock")
                response.raise_for_status()
                return SimClock.from_clock_payload(response.json())
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt + 1 < retries:
                    await asyncio.sleep(delay)
        if last_error is None:
            raise ValueError("retries must be at least 1")
        raise last_error

    async def place_call(self, attempt: CallAttempt, callback_url: str) -> PlaceResult:
        payload = {
            "request_id": attempt.request_id,
            "campaign_id": attempt.campaign_id,
            "lead_id": attempt.lead_id,
            "phone": attempt.phone,
            "callback_url": callback_url,
        }
        try:
            response = await self._client.post("/v1/calls", json=payload)
        except httpx.HTTPError:
            return CallAmbiguousError()

        if response.status_code in (200, 201):
            data = response.json()
            return CallAccepted(
                call_sid=data["call_sid"],
                state=data["state"],
                result=data.get("result"),
            )
        if response.status_code == 503:
            return CallTransientError()
        if response.status_code == 504:
            return CallAmbiguousError()
        return CallRejectedError(response.status_code, response.text)

    async def lookup_call(self, request_id: str) -> CallLookupResult:
        response = await self._client.get(
            "/v1/calls", params={"request_id": request_id}
        )
        if response.status_code == 404:
            return CallNotFound()
        if response.status_code != 200:
            response.raise_for_status()
            raise RuntimeError(f"unexpected carrier status: {response.status_code}")

        data = response.json()
        return CallFound(
            call_sid=data["call_sid"],
            state=data["state"],
            result=data.get("result"),
        )

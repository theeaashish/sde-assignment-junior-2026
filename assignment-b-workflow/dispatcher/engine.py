from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from dispatcher.client import (
    CallAccepted,
    CallAmbiguousError,
    CallNotFound,
    CallTransientError,
    CarrierClient,
)
from dispatcher.clock import Clock, parse_ist
from dispatcher.db import Database
from dispatcher.io_files import export_final_state_csv
from dispatcher.models import AttemptState, CallAttempt
from dispatcher.transitions import (
    abort_reserved_attempt,
    apply_call_outcome,
    mark_attempt_accepted,
    reserve_next_lead,
)

logger = logging.getLogger(__name__)


@dataclass
class DispatcherEngine:
    db: Database
    client: CarrierClient
    clock: Clock
    callback_url: str
    output_path: Path
    wakeup_event: asyncio.Event = field(default_factory=asyncio.Event)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    in_flight_tasks: set[asyncio.Task[None]] = field(default_factory=set)
    active_dispatch_ids: set[str] = field(default_factory=set)

    async def startup_recover(self) -> None:
        in_flight = await asyncio.to_thread(self.db.get_in_flight_attempts)
        for attempt in in_flight:
            await self._reconcile_attempt(attempt)

    async def _reconcile_attempt(self, attempt: CallAttempt) -> None:
        try:
            lookup = await self.client.lookup_call(attempt.request_id)
        except httpx.HTTPError as exc:
            logger.warning("carrier lookup failed for %s: %s", attempt.request_id, exc)
            return

        if isinstance(lookup, CallNotFound):
            if attempt.state is AttemptState.RESERVING:
                await asyncio.to_thread(
                    abort_reserved_attempt, self.db, attempt.request_id
                )
                self.wakeup_event.set()
            return

        if lookup.state == "ended" and lookup.result is not None:
            result = lookup.result
            await asyncio.to_thread(
                apply_call_outcome,
                self.db,
                request_id=attempt.request_id,
                call_sid=lookup.call_sid,
                status=result["status"],
                disposition=result.get("disposition"),
                ended_at=result["ended_at"],
                callback_at=result.get("callback_at"),
            )
            self.wakeup_event.set()
        else:
            await asyncio.to_thread(
                mark_attempt_accepted,
                self.db,
                attempt.request_id,
                lookup.call_sid,
            )

    async def _dispatch_attempt(self, attempt: CallAttempt) -> None:
        self.active_dispatch_ids.add(attempt.request_id)
        try:
            result = await self.client.place_call(attempt, self.callback_url)

            if isinstance(result, CallAccepted):
                if result.state == "ended" and result.result is not None:
                    payload = result.result
                    await asyncio.to_thread(
                        apply_call_outcome,
                        self.db,
                        request_id=attempt.request_id,
                        call_sid=result.call_sid,
                        status=payload["status"],
                        disposition=payload.get("disposition"),
                        ended_at=payload["ended_at"],
                        callback_at=payload.get("callback_at"),
                    )
                else:
                    await asyncio.to_thread(
                        mark_attempt_accepted,
                        self.db,
                        attempt.request_id,
                        result.call_sid,
                    )
                self.wakeup_event.set()
            elif isinstance(result, CallTransientError):
                await asyncio.to_thread(
                    abort_reserved_attempt, self.db, attempt.request_id
                )
                await asyncio.sleep(0.02)
                self.wakeup_event.set()
            elif isinstance(result, CallAmbiguousError):
                await self._reconcile_attempt(attempt)
                self.wakeup_event.set()
            else:
                logger.error(
                    "carrier rejected call %s: status=%s detail=%s",
                    attempt.request_id,
                    result.status_code,
                    result.detail,
                )
                await asyncio.to_thread(
                    abort_reserved_attempt, self.db, attempt.request_id
                )
                await asyncio.sleep(0.05)
                self.wakeup_event.set()
        finally:
            self.active_dispatch_ids.discard(attempt.request_id)

    async def scheduler_loop(self) -> None:
        while not self.stop_event.is_set():
            self.wakeup_event.clear()
            remaining = await asyncio.to_thread(self.db.count_non_final_leads)
            if remaining == 0:
                if self.in_flight_tasks:
                    await asyncio.gather(
                        *list(self.in_flight_tasks), return_exceptions=True
                    )
                await asyncio.to_thread(
                    export_final_state_csv, self.db, self.output_path
                )
                self.stop_event.set()
                return

            while True:
                reserved = await asyncio.to_thread(
                    reserve_next_lead, self.db, self.clock.now()
                )
                if reserved is None:
                    break
                _, attempt = reserved
                task = asyncio.create_task(self._dispatch_attempt(attempt))
                self.in_flight_tasks.add(task)
                task.add_done_callback(self.in_flight_tasks.discard)

            try:
                await asyncio.wait_for(self.wakeup_event.wait(), timeout=0.02)
            except TimeoutError:
                pass

    async def reconciler_loop(self) -> None:
        while not self.stop_event.is_set():
            in_flight = await asyncio.to_thread(self.db.get_in_flight_attempts)
            sim_now = self.clock.now()
            for attempt in in_flight:
                if (
                    attempt.state is AttemptState.RESERVING
                    and attempt.request_id in self.active_dispatch_ids
                ):
                    continue
                age = (sim_now - parse_ist(attempt.created_sim_at)).total_seconds()
                threshold = 15.0 if attempt.state is AttemptState.RESERVING else 60.0
                if (
                    attempt.state in (AttemptState.RESERVING, AttemptState.ACTIVE)
                    and age >= threshold
                ):
                    await self._reconcile_attempt(attempt)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=0.15)
            except TimeoutError:
                pass

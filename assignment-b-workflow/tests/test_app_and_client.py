from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from dispatcher.app import create_app
from dispatcher.client import (
    CallAccepted,
    CallAmbiguousError,
    CallFound,
    CallLookupResult,
    CallNotFound,
    CallRejectedError,
    CallTransientError,
    CarrierClient,
    PlaceResult,
)
from dispatcher.clock import FakeClock, format_ist
from dispatcher.db import Database
from dispatcher.engine import DispatcherEngine
from dispatcher.io_files import load_fixtures
from dispatcher.models import AttemptState, CallAttempt, LeadState
from dispatcher.transitions import mark_attempt_accepted, reserve_next_lead
from fastapi.testclient import TestClient

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "campaigns.json"
SIM_START = "2026-11-12T17:30:00+05:30"
ATTEMPT = CallAttempt(
    request_id="req-1",
    campaign_id="c1",
    lead_id="l1",
    tenant_id="t1",
    phone="+919000000001",
    created_sim_at=SIM_START,
)


class LookupStub:
    def __init__(self, outcome: CallLookupResult) -> None:
        self.outcome = outcome

    async def lookup_call(self, request_id: str) -> CallLookupResult:
        return self.outcome


def make_engine(
    db: Database, client: object, clock: FakeClock, output_path: Path
) -> DispatcherEngine:
    return DispatcherEngine(
        db=db,
        client=cast(CarrierClient, client),
        clock=clock,
        callback_url="http://127.0.0.1:8200/webhooks/telephony",
        output_path=output_path,
    )


def sign(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


@pytest.fixture()
def db(tmp_path: Path) -> Iterator[Database]:
    database = Database(tmp_path / "dispatcher.db")
    load_fixtures(database, FIXTURES)
    try:
        yield database
    finally:
        database.close()


def test_webhook_verifies_signature_and_is_idempotent(
    db: Database, tmp_path: Path
) -> None:
    clock = FakeClock(SIM_START)
    engine = make_engine(db, LookupStub(CallNotFound()), clock, tmp_path / "out.csv")
    reserved = reserve_next_lead(db, clock.now())
    assert reserved is not None
    lead, attempt = reserved
    assert mark_attempt_accepted(db, attempt.request_id, "CA-webhook")

    payload = {
        "event": "call.completed",
        "call_sid": "CA-webhook",
        "request_id": attempt.request_id,
        "status": "completed",
        "disposition": "interested",
        "ended_at": format_ist(clock.now()),
        "callback_at": None,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {"X-AU-Signature": sign(body, "whsec_au_assignment_local")}

    with TestClient(create_app(db, engine)) as api:
        missing = api.post("/webhooks/telephony", content=body)
        invalid = api.post(
            "/webhooks/telephony",
            content=body,
            headers={"X-AU-Signature": "0" * 64},
        )
        accepted = api.post("/webhooks/telephony", content=body, headers=headers)
        duplicate = api.post("/webhooks/telephony", content=body, headers=headers)

    assert missing.status_code == 401
    assert missing.json() == {"detail": "invalid_signature"}
    assert invalid.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json() == {"ok": True}
    assert duplicate.status_code == 200
    assert duplicate.json() == {"ok": True}

    stored_lead = db.get_lead(lead.campaign_id, lead.lead_id)
    stored_attempt = db.get_call_attempt(attempt.request_id)
    assert stored_lead is not None
    assert stored_lead.state is LeadState.COMPLETED
    assert stored_lead.attempts == 1
    assert stored_lead.last_disposition == "interested"
    assert stored_attempt is not None
    assert stored_attempt.state is AttemptState.ENDED
    assert engine.wakeup_event.is_set()


def test_status_reports_progress_metrics(db: Database, tmp_path: Path) -> None:
    clock = FakeClock(SIM_START)
    engine = make_engine(db, LookupStub(CallNotFound()), clock, tmp_path / "out.csv")
    reserved = reserve_next_lead(db, clock.now())
    assert reserved is not None
    lead, attempt = reserved
    assert mark_attempt_accepted(db, attempt.request_id, "CA-status")

    with db.write_tx() as conn:
        conn.execute(
            "INSERT INTO dnd_phones "
            "(phone, triggered_by_campaign, triggered_by_lead, recorded_at) "
            "VALUES (?, ?, ?, ?)",
            ("+919000000099", lead.campaign_id, lead.lead_id, SIM_START),
        )

    with TestClient(create_app(db, engine)) as api:
        response = api.get("/status")

    assert response.status_code == 200
    body = response.json()
    assert body["sim_now"] == format_ist(clock.now())
    assert body["non_final_leads"] == 200
    assert body["active_calls_by_tenant"] == {lead.tenant_id: 1}
    assert body["leads_by_campaign_and_state"][lead.campaign_id]["ACTIVE"] == 1
    assert (
        sum(
            sum(states.values())
            for states in body["leads_by_campaign_and_state"].values()
        )
        == 200
    )
    assert body["dnd_phones_count"] == 1


async def place_with(response: httpx.Response) -> PlaceResult:
    carrier = CarrierClient("http://carrier.test")
    await carrier.aclose()
    carrier._client = httpx.AsyncClient(
        base_url="http://carrier.test",
        transport=httpx.MockTransport(lambda request: response),
    )
    try:
        return await carrier.place_call(
            ATTEMPT, "http://dispatcher.test/webhooks/telephony"
        )
    finally:
        await carrier.aclose()


async def lookup_with(response: httpx.Response) -> CallLookupResult:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/calls"
        assert request.url.params["request_id"] == "req-1"
        return response

    carrier = CarrierClient("http://carrier.test")
    await carrier.aclose()
    carrier._client = httpx.AsyncClient(
        base_url="http://carrier.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        return await carrier.lookup_call("req-1")
    finally:
        await carrier.aclose()


def test_carrier_client_maps_place_call_outcomes() -> None:
    accepted = asyncio.run(
        place_with(
            httpx.Response(
                201, json={"call_sid": "CA-201", "state": "active", "result": None}
            )
        )
    )
    assert accepted == CallAccepted("CA-201", "active", None)

    result: dict[str, Any] = {
        "status": "completed",
        "disposition": "interested",
        "ended_at": SIM_START,
        "callback_at": None,
    }
    replayed = asyncio.run(
        place_with(
            httpx.Response(
                200, json={"call_sid": "CA-200", "state": "ended", "result": result}
            )
        )
    )
    assert replayed == CallAccepted("CA-200", "ended", result)

    transient = asyncio.run(place_with(httpx.Response(503, json={"error": "busy"})))
    assert isinstance(transient, CallTransientError)

    ambiguous = asyncio.run(place_with(httpx.Response(504, json={"error": "timeout"})))
    assert isinstance(ambiguous, CallAmbiguousError)

    rejected = asyncio.run(place_with(httpx.Response(422, text="outside_window")))
    assert isinstance(rejected, CallRejectedError)
    assert rejected.status_code == 422
    assert rejected.detail == "outside_window"


def test_carrier_client_maps_lookup_outcomes() -> None:
    found = asyncio.run(
        lookup_with(
            httpx.Response(
                200,
                json={
                    "call_sid": "CA-found",
                    "state": "ended",
                    "result": {"status": "busy"},
                },
            )
        )
    )
    assert found == CallFound("CA-found", "ended", {"status": "busy"})

    missing = asyncio.run(lookup_with(httpx.Response(404, json={"error": "not_found"})))
    assert isinstance(missing, CallNotFound)


def test_startup_recover_aborts_crashed_reservation_not_found(
    db: Database, tmp_path: Path
) -> None:
    clock = FakeClock(SIM_START)
    engine = make_engine(db, LookupStub(CallNotFound()), clock, tmp_path / "out.csv")
    reserved = reserve_next_lead(db, clock.now())
    assert reserved is not None
    lead, attempt = reserved

    asyncio.run(engine.startup_recover())

    reverted = db.get_lead(lead.campaign_id, lead.lead_id)
    stored_attempt = db.get_call_attempt(attempt.request_id)
    assert reverted is not None
    assert reverted.state is LeadState.FRESH
    assert reverted.attempts == 0
    assert reverted.active_request_id is None
    assert stored_attempt is not None
    assert stored_attempt.state is AttemptState.ABORTED
    assert engine.wakeup_event.is_set()


def test_startup_recover_resolves_crashed_reservation_found_ended(
    db: Database, tmp_path: Path
) -> None:
    clock = FakeClock(SIM_START)
    result = {
        "status": "no_answer",
        "disposition": None,
        "ended_at": format_ist(clock.now()),
        "callback_at": None,
    }
    engine = make_engine(
        db,
        LookupStub(CallFound("CA-recovered", "ended", result)),
        clock,
        tmp_path / "out.csv",
    )
    reserved = reserve_next_lead(db, clock.now())
    assert reserved is not None
    lead, attempt = reserved

    asyncio.run(engine.startup_recover())

    recovered = db.get_lead(lead.campaign_id, lead.lead_id)
    stored_attempt = db.get_call_attempt(attempt.request_id)
    assert recovered is not None
    assert recovered.state is LeadState.WAITING_RETRY
    assert recovered.attempts == 1
    assert recovered.active_request_id is None
    assert stored_attempt is not None
    assert stored_attempt.state is AttemptState.ENDED
    assert stored_attempt.call_sid == "CA-recovered"
    assert engine.wakeup_event.is_set()

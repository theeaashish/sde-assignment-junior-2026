from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from dispatcher.clock import IST, FakeClock, format_ist
from dispatcher.db import Database
from dispatcher.io_files import export_final_state_csv
from dispatcher.models import AttemptState, CallAttempt, Lead, LeadState
from dispatcher.transitions import (
    abort_reserved_attempt,
    apply_call_outcome,
    mark_attempt_accepted,
    reserve_next_lead,
)

SIM_NOW = datetime(2026, 11, 12, 10, 0, tzinfo=IST)
ALL_DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def make_db(tmp_path: Path) -> Database:
    db = Database(tmp_path / "dispatcher.db")
    with db.write_tx() as conn:
        conn.execute(
            """
            INSERT INTO config (
                id, sim_start, regulatory_start, regulatory_end,
                window_grace_seconds, retry_no_answer_minutes,
                retry_busy_minutes, retry_failed_minutes, webhook_secret
            ) VALUES (1, ?, '09:00', '21:00', 120, 60, 60, 15, 'test-secret')
            """,
            (format_ist(SIM_NOW),),
        )
    return db


def add_tenant(db: Database, tenant_id: str, max_concurrent_calls: int) -> None:
    with db.write_tx() as conn:
        conn.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES (?, ?)",
            (tenant_id, max_concurrent_calls),
        )


def add_campaign(
    db: Database,
    campaign_id: str,
    tenant_id: str,
    *,
    window_start: str = "08:00",
    window_end: str = "22:00",
    window_days: tuple[str, ...] = ALL_DAYS,
    max_attempts: int = 3,
) -> None:
    with db.write_tx() as conn:
        conn.execute(
            """
            INSERT INTO campaigns (
                campaign_id, tenant_id, window_start, window_end,
                window_days_json, max_attempts, leads_file
            ) VALUES (?, ?, ?, ?, ?, ?, 'leads.csv')
            """,
            (
                campaign_id,
                tenant_id,
                window_start,
                window_end,
                json.dumps(window_days),
                max_attempts,
            ),
        )


def add_lead(
    db: Database,
    campaign_id: str,
    lead_id: str,
    tenant_id: str,
    phone: str,
    *,
    state: LeadState = LeadState.FRESH,
    priority: int = 3,
    attempts: int = 0,
    dispatch_seq: int = 0,
    next_eligible_at: str | None = None,
    last_status: str = "",
    last_disposition: str = "",
) -> None:
    with db.write_tx() as conn:
        conn.execute(
            """
            INSERT INTO leads (
                campaign_id, lead_id, tenant_id, phone, state, priority,
                attempts, dispatch_seq, next_eligible_at, last_status,
                last_disposition
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                campaign_id,
                lead_id,
                tenant_id,
                phone,
                state.value,
                priority,
                attempts,
                dispatch_seq,
                next_eligible_at,
                last_status,
                last_disposition,
            ),
        )


def reserve(db: Database, clock: FakeClock) -> tuple[Lead, CallAttempt]:
    reserved = reserve_next_lead(db, clock.now())
    assert reserved is not None
    return reserved


def test_tenant_capacity_blocks_reservations_across_campaigns(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 1)
    add_campaign(db, "c1", "t1")
    add_campaign(db, "c2", "t1")
    add_lead(db, "c1", "l1", "t1", "+919000000001")
    add_lead(db, "c2", "l2", "t1", "+919000000002")
    clock = FakeClock(SIM_NOW)

    first_lead, first_attempt = reserve(db, clock)
    assert first_lead.campaign_id == "c1"
    assert reserve_next_lead(db, clock.now()) is None

    assert mark_attempt_accepted(db, first_attempt.request_id, "CA-1")
    clock.advance(minutes=5)
    assert apply_call_outcome(
        db,
        request_id=first_attempt.request_id,
        status="completed",
        disposition="interested",
        ended_at=clock.now(),
    )
    second_lead, _ = reserve(db, clock)
    assert second_lead.campaign_id == "c2"
    db.close()


def test_shared_phone_blocks_reservation_until_call_ends(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 5)
    add_tenant(db, "t2", 5)
    add_campaign(db, "c1", "t1")
    add_campaign(db, "c2", "t2")
    phone = "+919000000003"
    add_lead(db, "c1", "l1", "t1", phone)
    add_lead(db, "c2", "l2", "t2", phone)
    clock = FakeClock(SIM_NOW)

    first_lead, first_attempt = reserve(db, clock)
    assert first_lead.campaign_id == "c1"
    assert reserve_next_lead(db, clock.now()) is None

    assert mark_attempt_accepted(db, first_attempt.request_id, "CA-2")
    clock.advance(minutes=5)
    assert apply_call_outcome(
        db,
        request_id=first_attempt.request_id,
        status="completed",
        disposition="interested",
        ended_at=clock.now(),
    )
    second_lead, _ = reserve(db, clock)
    assert second_lead.campaign_id == "c2"
    assert second_lead.phone == phone
    db.close()


def test_priority_orders_callback_then_retry_then_fresh(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 10)
    add_campaign(db, "c1", "t1")
    due_at = format_ist(SIM_NOW - timedelta(minutes=5))
    add_lead(
        db,
        "c1",
        "callback",
        "t1",
        "+919000000011",
        state=LeadState.WAITING_CALLBACK,
        priority=1,
        attempts=1,
        next_eligible_at=due_at,
    )
    add_lead(
        db,
        "c1",
        "retry",
        "t1",
        "+919000000012",
        state=LeadState.WAITING_RETRY,
        priority=2,
        attempts=1,
        next_eligible_at=due_at,
    )
    add_lead(db, "c1", "fresh", "t1", "+919000000013")
    clock = FakeClock(SIM_NOW)

    ordered = [reserve(db, clock)[0].lead_id for _ in range(3)]

    assert ordered == ["callback", "retry", "fresh"]
    db.close()


def test_full_tenant_does_not_block_another_tenant(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t_a", 1)
    add_tenant(db, "t_b", 1)
    add_campaign(db, "c_a", "t_a")
    add_campaign(db, "c_b", "t_b")
    add_lead(db, "c_a", "l1", "t_a", "+919000000021")
    add_lead(db, "c_a", "l2", "t_a", "+919000000022")
    add_lead(db, "c_b", "l1", "t_b", "+919000000023")
    clock = FakeClock(SIM_NOW)

    assert reserve(db, clock)[0].campaign_id == "c_a"
    second_lead, _ = reserve(db, clock)

    assert second_lead.campaign_id == "c_b"
    db.close()


def test_abort_reverts_lead_and_next_reservation_uses_new_sequence(
    tmp_path: Path,
) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 1)
    add_campaign(db, "c1", "t1")
    add_lead(db, "c1", "l1", "t1", "+919000000031")
    clock = FakeClock(SIM_NOW)

    first_lead, first_attempt = reserve(db, clock)
    assert first_attempt.request_id == "c1:l1:seq_1"
    assert first_lead.dispatch_seq == 1

    assert abort_reserved_attempt(db, first_attempt.request_id)
    assert abort_reserved_attempt(db, first_attempt.request_id) is False

    reverted = db.get_lead("c1", "l1")
    assert reverted is not None
    assert reverted.state is LeadState.FRESH
    assert reverted.prior_state is None
    assert reverted.attempts == 0
    assert reverted.active_request_id is None
    assert reverted.dispatch_seq == 1

    aborted_attempt = db.get_call_attempt(first_attempt.request_id)
    assert aborted_attempt is not None
    assert aborted_attempt.state is AttemptState.ABORTED

    second_lead, second_attempt = reserve(db, clock)
    assert second_attempt.request_id == "c1:l1:seq_2"
    assert second_lead.dispatch_seq == 2
    assert second_lead.state is LeadState.RESERVING
    db.close()


def test_aborted_attempt_outcome_cannot_clear_newer_reservation(
    tmp_path: Path,
) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 1)
    add_campaign(db, "c1", "t1")
    add_lead(db, "c1", "l1", "t1", "+919000000032")
    clock = FakeClock(SIM_NOW)

    _, first_attempt = reserve(db, clock)
    assert abort_reserved_attempt(db, first_attempt.request_id)
    _, second_attempt = reserve(db, clock)

    assert (
        apply_call_outcome(
            db,
            request_id=first_attempt.request_id,
            status="no_answer",
            disposition=None,
            ended_at=clock.now(),
        )
        is False
    )

    lead = db.get_lead("c1", "l1")
    assert lead is not None
    assert lead.state is LeadState.RESERVING
    assert lead.active_request_id == second_attempt.request_id
    assert lead.attempts == 0
    assert lead.dispatch_seq == 2

    aborted_attempt = db.get_call_attempt(first_attempt.request_id)
    assert aborted_attempt is not None
    assert aborted_attempt.state is AttemptState.ABORTED
    db.close()


def test_webhook_before_201_makes_late_accept_a_noop(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 1)
    add_campaign(db, "c1", "t1")
    add_lead(db, "c1", "l1", "t1", "+919000000041")
    clock = FakeClock(SIM_NOW)

    _, attempt = reserve(db, clock)
    clock.advance(minutes=2)
    assert apply_call_outcome(
        db,
        request_id=attempt.request_id,
        status="no_answer",
        disposition=None,
        ended_at=clock.now(),
    )

    assert mark_attempt_accepted(db, attempt.request_id, "CA-late") is False

    lead = db.get_lead("c1", "l1")
    assert lead is not None
    assert lead.attempts == 1
    assert lead.state is LeadState.WAITING_RETRY
    assert lead.active_request_id is None

    ended_attempt = db.get_call_attempt(attempt.request_id)
    assert ended_attempt is not None
    assert ended_attempt.state is AttemptState.ENDED
    assert ended_attempt.accepted == 1
    db.close()


def test_duplicate_webhooks_do_not_double_count_attempts(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 1)
    add_campaign(db, "c1", "t1")
    add_lead(db, "c1", "l1", "t1", "+919000000051")
    clock = FakeClock(SIM_NOW)

    _, attempt = reserve(db, clock)
    assert mark_attempt_accepted(db, attempt.request_id, "CA-duplicate")
    clock.advance(minutes=5)
    outcome = {
        "status": "completed",
        "disposition": "interested",
        "ended_at": clock.now(),
    }

    assert apply_call_outcome(db, request_id=attempt.request_id, **outcome)
    assert apply_call_outcome(db, request_id=attempt.request_id, **outcome) is False
    assert apply_call_outcome(db, call_sid="CA-duplicate", **outcome) is False

    lead = db.get_lead("c1", "l1")
    assert lead is not None
    assert lead.attempts == 1
    assert lead.state is LeadState.COMPLETED
    db.close()


def test_replayed_accepted_response_does_not_increment_attempts_again(
    tmp_path: Path,
) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 1)
    add_campaign(db, "c1", "t1")
    add_lead(db, "c1", "l1", "t1", "+919000000052")
    clock = FakeClock(SIM_NOW)

    _, attempt = reserve(db, clock)
    assert mark_attempt_accepted(db, attempt.request_id, "CA-replay")
    assert mark_attempt_accepted(db, attempt.request_id, "CA-replay")

    lead = db.get_lead("c1", "l1")
    assert lead is not None
    assert lead.attempts == 1
    assert lead.state is LeadState.ACTIVE

    active_attempt = db.get_call_attempt(attempt.request_id)
    assert active_attempt is not None
    assert active_attempt.state is AttemptState.ACTIVE
    assert active_attempt.accepted == 1
    db.close()


def test_global_dnd_preserves_final_leads_and_blocks_waiting_lead(
    tmp_path: Path,
) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 10)
    phone = "+919000000099"
    for campaign_id in ("c1", "c2", "c3", "c4"):
        add_campaign(db, campaign_id, "t1")

    add_lead(
        db,
        "c1",
        "l1",
        "t1",
        phone,
        state=LeadState.COMPLETED,
        attempts=3,
        last_status="completed",
        last_disposition="interested",
    )
    add_lead(
        db,
        "c2",
        "l2",
        "t1",
        phone,
        state=LeadState.EXHAUSTED,
        attempts=3,
        last_status="no_answer",
    )
    add_lead(
        db,
        "c3",
        "l3",
        "t1",
        phone,
        state=LeadState.WAITING_RETRY,
        priority=2,
        attempts=1,
        next_eligible_at=format_ist(SIM_NOW + timedelta(hours=1)),
        last_status="no_answer",
    )
    add_lead(db, "c4", "l4", "t1", phone)
    clock = FakeClock(SIM_NOW)

    _, attempt = reserve(db, clock)
    assert mark_attempt_accepted(db, attempt.request_id, "CA-dnd")
    assert apply_call_outcome(
        db,
        request_id=attempt.request_id,
        status="completed",
        disposition="dnd",
        ended_at=clock.now(),
    )

    completed = db.get_lead("c1", "l1")
    exhausted = db.get_lead("c2", "l2")
    waiting = db.get_lead("c3", "l3")
    assert completed is not None and exhausted is not None and waiting is not None

    assert completed.state is LeadState.COMPLETED
    assert completed.attempts == 3
    assert completed.last_status == "completed"
    assert completed.last_disposition == "interested"

    assert exhausted.state is LeadState.EXHAUSTED
    assert exhausted.attempts == 3
    assert exhausted.last_status == "no_answer"

    assert waiting.state is LeadState.DND_BLOCKED
    assert waiting.attempts == 1
    assert waiting.last_status == "no_answer"
    assert waiting.prior_state is None
    assert waiting.next_eligible_at is None
    assert waiting.active_request_id is None
    assert waiting.active_call_sid is None
    assert db.is_phone_dnd(phone)
    db.close()


def test_dnd_cascade_leaves_in_flight_rows_untouched(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 10)
    add_campaign(db, "c1", "t1")
    add_campaign(db, "c2", "t1")
    phone = "+919000000098"
    add_lead(db, "c1", "l1", "t1", phone)
    add_lead(db, "c2", "l2", "t1", phone)
    clock = FakeClock(SIM_NOW)

    _, attempt = reserve(db, clock)
    assert mark_attempt_accepted(db, attempt.request_id, "CA-dnd-inflight")
    with db.write_tx() as conn:
        conn.execute(
            """
            UPDATE leads
            SET state = 'RESERVING',
                prior_state = 'FRESH',
                active_request_id = 'c2:l2:seq_1',
                active_call_sid = 'CA-inflight'
            WHERE campaign_id = 'c2' AND lead_id = 'l2'
            """
        )

    assert apply_call_outcome(
        db,
        request_id=attempt.request_id,
        status="completed",
        disposition="dnd",
        ended_at=clock.now(),
    )

    in_flight = db.get_lead("c2", "l2")
    assert in_flight is not None
    assert in_flight.state is LeadState.RESERVING
    assert in_flight.prior_state is LeadState.FRESH
    assert in_flight.active_request_id == "c2:l2:seq_1"
    assert in_flight.active_call_sid == "CA-inflight"
    assert db.is_phone_dnd(phone)
    db.close()


def test_export_final_state_csv_requires_final_leads_and_writes_sorted_output(
    tmp_path: Path,
) -> None:
    db = make_db(tmp_path)
    add_tenant(db, "t1", 10)
    add_campaign(db, "c_a", "t1")
    add_campaign(db, "c_b", "t1")
    add_lead(
        db,
        "c_b",
        "l2",
        "t1",
        "+919000000062",
        state=LeadState.COMPLETED,
        attempts=2,
        last_status="completed",
        last_disposition="interested",
    )
    add_lead(
        db,
        "c_a",
        "l1",
        "t1",
        "+919000000061",
        state=LeadState.DND_BLOCKED,
    )
    add_lead(
        db,
        "c_a",
        "l0",
        "t1",
        "+919000000060",
        state=LeadState.EXHAUSTED,
        attempts=3,
        last_status="no_answer",
    )
    add_lead(db, "c_b", "l3", "t1", "+919000000063")
    output = tmp_path / "final_state.csv"

    with pytest.raises(RuntimeError, match="non-final"):
        export_final_state_csv(db, output)
    assert not output.exists()

    with db.write_tx() as conn:
        conn.execute(
            """
            UPDATE leads
            SET state = 'COMPLETED',
                attempts = 1,
                last_status = 'completed',
                last_disposition = 'not_interested'
            WHERE campaign_id = 'c_b' AND lead_id = 'l3'
            """
        )

    export_final_state_csv(db, output)

    with output.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows == [
        [
            "campaign_id",
            "lead_id",
            "final_state",
            "attempts",
            "last_status",
            "last_disposition",
        ],
        ["c_a", "l0", "exhausted", "3", "no_answer", ""],
        ["c_a", "l1", "dnd_blocked", "0", "", ""],
        ["c_b", "l2", "completed", "2", "completed", "interested"],
        ["c_b", "l3", "completed", "1", "completed", "not_interested"],
    ]
    assert not list(tmp_path.glob(".final_state.csv.*"))
    db.close()

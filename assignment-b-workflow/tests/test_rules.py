from __future__ import annotations

from datetime import UTC, datetime, time, timedelta

import pytest
from dispatcher.clock import IST, FakeClock, SimClock, format_ist, parse_ist
from dispatcher.models import Campaign, GlobalConfig, Lead, LeadState
from dispatcher.rules import (
    DAYS,
    TERMINAL_DISPOSITIONS,
    InvalidCallOutcomeError,
    InvalidWindowConfigError,
    candidate_sort_key,
    compute_next_eligible_at,
    effective_daily_bounds,
    evaluate_call_outcome,
    is_window_open,
    next_valid_window_open,
)

THURSDAY = datetime(2026, 11, 12, tzinfo=IST)
FRIDAY = datetime(2026, 11, 13, tzinfo=IST)
MONDAY = datetime(2026, 11, 16, tzinfo=IST)


def make_campaign(
    *,
    window_start: str = "08:00",
    window_end: str = "22:00",
    window_days: tuple[str, ...] = DAYS,
    max_attempts: int = 3,
) -> Campaign:
    return Campaign(
        campaign_id="c_test",
        tenant_id="t_test",
        window_start=window_start,
        window_end=window_end,
        window_days=window_days,
        max_attempts=max_attempts,
        leads_file="leads.csv",
    )


def make_config(
    *,
    regulatory_start: str = "09:00",
    regulatory_end: str = "21:00",
    retry_no_answer_minutes: int = 60,
    retry_busy_minutes: int = 60,
    retry_failed_minutes: int = 15,
) -> GlobalConfig:
    return GlobalConfig(
        sim_start="2026-11-12T17:30:00+05:30",
        regulatory_start=regulatory_start,
        regulatory_end=regulatory_end,
        window_grace_seconds=120,
        retry_no_answer_minutes=retry_no_answer_minutes,
        retry_busy_minutes=retry_busy_minutes,
        retry_failed_minutes=retry_failed_minutes,
        webhook_secret="test-secret",
    )


def make_lead(
    *,
    lead_id: str = "l1",
    campaign_id: str = "c_test",
    state: LeadState = LeadState.FRESH,
    priority: int = 3,
    attempts: int = 0,
    next_eligible_at: str | None = None,
) -> Lead:
    return Lead(
        campaign_id=campaign_id,
        lead_id=lead_id,
        tenant_id="t_test",
        phone="+919876543210",
        state=state,
        priority=priority,
        attempts=attempts,
        next_eligible_at=next_eligible_at,
    )


def at(base: datetime, hour: int, minute: int, second: int = 0) -> datetime:
    return base.replace(hour=hour, minute=minute, second=second, microsecond=0)


def test_parse_ist_normalizes_strings_and_datetimes() -> None:
    utc_value = "2026-11-12T12:00:00+00:00"
    parsed = parse_ist(utc_value)

    assert parsed == datetime(2026, 11, 12, 17, 30, tzinfo=IST)
    assert parsed.utcoffset() == timedelta(hours=5, minutes=30)
    assert parse_ist(datetime(2026, 11, 12, 12, 0, tzinfo=UTC)) == parsed


@pytest.mark.parametrize(
    "value",
    [
        "2026-11-12T17:30:00",
        datetime(2026, 11, 12, 17, 30),
    ],
)
def test_parse_ist_rejects_naive_datetimes(value: str | datetime) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        parse_ist(value)


def test_format_ist_uses_microseconds_and_sorts_chronologically() -> None:
    earlier = format_ist(datetime(2026, 11, 12, 17, 30, tzinfo=IST))
    later = format_ist(datetime(2026, 11, 12, 17, 30, 0, 1, tzinfo=IST))

    assert earlier == "2026-11-12T17:30:00.000000+05:30"
    assert earlier < later


def test_fake_clock_advance_and_set_time() -> None:
    clock = FakeClock("2026-11-12T17:30:00+05:30")

    assert clock.advance(minutes=10) == datetime(2026, 11, 12, 17, 40, tzinfo=IST)
    assert clock.advance(seconds=30) == datetime(2026, 11, 12, 17, 40, 30, tzinfo=IST)
    assert clock.set_time("2026-11-12T12:00:00Z") == datetime(
        2026, 11, 12, 17, 30, tzinfo=IST
    )
    assert clock.now() == datetime(2026, 11, 12, 17, 30, tzinfo=IST)


def test_sim_clock_payload_conversion_and_real_time_scaling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("dispatcher.clock.time.time", lambda: 1000.0)
    clock = SimClock.from_clock_payload(
        {
            "sim_epoch": "2026-11-12T17:30:00+05:30",
            "real_epoch": 990.0,
            "speed": 600.0,
        }
    )

    assert clock.now() == datetime(2026, 11, 12, 19, 10, tzinfo=IST)
    assert clock.sim_to_real_seconds(600) == 1.0
    assert clock.sim_to_real_seconds(-10) == 0.0


def test_sim_clock_rejects_non_positive_speed() -> None:
    clock = SimClock(sim_epoch=THURSDAY, real_epoch=0.0, speed=0.0)

    with pytest.raises(ValueError, match="positive"):
        clock.sim_to_real_seconds(60)


def test_effective_daily_bounds_clamps_campaign_to_regulatory_window() -> None:
    campaign = make_campaign(window_start="08:00", window_end="22:00")

    assert effective_daily_bounds(campaign, make_config()) == (time(9, 0), time(21, 0))


def test_effective_daily_bounds_rejects_non_overlapping_windows() -> None:
    campaign = make_campaign(window_start="21:30", window_end="22:30")

    with pytest.raises(InvalidWindowConfigError, match="do not overlap"):
        effective_daily_bounds(campaign, make_config())


@pytest.mark.parametrize("window_days", [(), ("mon", "funday")])
def test_effective_daily_bounds_rejects_invalid_days(
    window_days: tuple[str, ...],
) -> None:
    campaign = make_campaign(window_days=window_days)

    with pytest.raises(InvalidWindowConfigError):
        effective_daily_bounds(campaign, make_config())


@pytest.mark.parametrize(
    ("hour", "minute", "second", "expected"),
    [
        (8, 59, 59, False),
        (9, 0, 0, True),
        (20, 59, 44, True),
        (20, 59, 45, False),
        (21, 0, 0, False),
    ],
)
def test_window_is_half_open_and_respects_close_buffer(
    hour: int, minute: int, second: int, expected: bool
) -> None:
    sim_now = at(THURSDAY, hour, minute, second)

    assert is_window_open(sim_now, make_campaign(), make_config()) is expected


def test_window_respects_campaign_days() -> None:
    campaign = make_campaign(window_days=("mon", "tue", "wed", "thu", "fri"))

    assert is_window_open(at(THURSDAY, 12, 0), campaign, make_config()) is True
    assert is_window_open(at(FRIDAY, 12, 0), campaign, make_config()) is True
    assert (
        is_window_open(at(MONDAY - timedelta(days=1), 12, 0), campaign, make_config())
        is False
    )


def test_retry_after_close_rolls_to_next_morning() -> None:
    eligible = compute_next_eligible_at(
        "no_answer", None, at(THURSDAY, 20, 30), None, make_campaign(), make_config()
    )

    assert eligible == at(FRIDAY, 9, 0)


def test_next_window_open_rolls_from_closing_buffer() -> None:
    eligible = next_valid_window_open(
        at(THURSDAY, 20, 59, 50), make_campaign(), make_config()
    )

    assert eligible == at(FRIDAY, 9, 0)


def test_retry_rolls_over_weekend() -> None:
    campaign = make_campaign(window_days=("mon", "tue", "wed", "thu", "fri"))

    eligible = compute_next_eligible_at(
        "no_answer", None, at(FRIDAY, 20, 30), None, campaign, make_config()
    )

    assert eligible == at(MONDAY, 9, 0)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("no_answer", at(THURSDAY, 11, 0)),
        ("busy", at(THURSDAY, 11, 0)),
        ("failed", at(THURSDAY, 10, 15)),
    ],
)
def test_retry_backoff_delays(status: str, expected: datetime) -> None:
    eligible = compute_next_eligible_at(
        status, None, at(THURSDAY, 10, 0), None, make_campaign(), make_config()
    )

    assert eligible == expected


def test_callback_requested_uses_callback_at() -> None:
    callback_at = at(THURSDAY, 12, 30)

    eligible = compute_next_eligible_at(
        "completed",
        "callback_requested",
        at(THURSDAY, 11, 0),
        callback_at,
        make_campaign(),
        make_config(),
    )

    assert eligible == callback_at


def test_callback_outside_window_rolls_to_next_open() -> None:
    eligible = compute_next_eligible_at(
        "completed",
        "callback_requested",
        at(THURSDAY, 18, 0),
        at(THURSDAY, 22, 0),
        make_campaign(),
        make_config(),
    )

    assert eligible == at(FRIDAY, 9, 0)


def test_callback_requested_requires_callback_at() -> None:
    with pytest.raises(InvalidCallOutcomeError, match="callback_at"):
        compute_next_eligible_at(
            "completed",
            "callback_requested",
            at(THURSDAY, 11, 0),
            None,
            make_campaign(),
            make_config(),
        )


def test_candidate_ordering_prioritises_callbacks_then_retries_then_fresh() -> None:
    callback = make_lead(
        lead_id="callback",
        state=LeadState.WAITING_CALLBACK,
        priority=1,
        next_eligible_at="2026-11-12T12:00:00.000000+05:30",
    )
    retry = make_lead(
        lead_id="retry",
        state=LeadState.WAITING_RETRY,
        priority=2,
        next_eligible_at="2026-11-12T11:00:00.000000+05:30",
    )
    fresh = make_lead(lead_id="fresh")

    ordered = sorted([fresh, retry, callback], key=candidate_sort_key)

    assert [lead.lead_id for lead in ordered] == ["callback", "retry", "fresh"]


def test_candidate_ordering_ties_break_by_eligible_time_and_identity() -> None:
    later = make_lead(
        lead_id="later",
        priority=2,
        next_eligible_at="2026-11-12T11:00:00.000000+05:30",
    )
    earlier = make_lead(
        lead_id="earlier",
        priority=2,
        next_eligible_at="2026-11-12T10:00:00.000000+05:30",
    )
    same_time_b = make_lead(
        lead_id="b",
        campaign_id="c_b",
        priority=1,
        next_eligible_at="2026-11-12T12:00:00.000000+05:30",
    )
    same_time_a = make_lead(
        lead_id="a",
        campaign_id="c_a",
        priority=1,
        next_eligible_at="2026-11-12T12:00:00.000000+05:30",
    )

    ordered = sorted([later, earlier, same_time_b, same_time_a], key=candidate_sort_key)

    assert [lead.lead_id for lead in ordered] == ["a", "b", "earlier", "later"]


@pytest.mark.parametrize("disposition", sorted(TERMINAL_DISPOSITIONS))
def test_terminal_dispositions_complete_the_lead(disposition: str) -> None:
    lead = make_lead(attempts=0)

    decision = evaluate_call_outcome(
        lead,
        make_campaign(),
        make_config(),
        "completed",
        disposition,
        at(THURSDAY, 10, 0),
    )

    assert decision.new_state is LeadState.COMPLETED
    assert decision.new_priority == lead.priority
    assert decision.next_eligible_at is None
    assert decision.trigger_global_dnd is (disposition == "dnd")
    assert lead.attempts == 0


def test_final_attempt_terminal_outcome_beats_exhaustion() -> None:
    lead = make_lead(attempts=2)

    decision = evaluate_call_outcome(
        lead,
        make_campaign(max_attempts=3),
        make_config(),
        "completed",
        "interested",
        at(THURSDAY, 10, 0),
    )

    assert decision.new_state is LeadState.COMPLETED
    assert decision.trigger_global_dnd is False


def test_final_attempt_non_terminal_outcome_exhausts_the_lead() -> None:
    lead = make_lead(attempts=2)

    decision = evaluate_call_outcome(
        lead,
        make_campaign(max_attempts=3),
        make_config(),
        "no_answer",
        None,
        at(THURSDAY, 10, 0),
    )

    assert decision.new_state is LeadState.EXHAUSTED
    assert decision.next_eligible_at is None
    assert decision.trigger_global_dnd is False


def test_final_attempt_callback_is_exhausted_before_scheduling() -> None:
    lead = make_lead(attempts=2)

    decision = evaluate_call_outcome(
        lead,
        make_campaign(max_attempts=3),
        make_config(),
        "completed",
        "callback_requested",
        at(THURSDAY, 10, 0),
        callback_at=at(THURSDAY, 12, 0),
    )

    assert decision.new_state is LeadState.EXHAUSTED
    assert decision.next_eligible_at is None


def test_retry_outcome_waits_until_next_eligible_at() -> None:
    lead = make_lead(attempts=0)

    decision = evaluate_call_outcome(
        lead,
        make_campaign(),
        make_config(),
        "no_answer",
        None,
        at(THURSDAY, 10, 0),
    )

    assert decision.new_state is LeadState.WAITING_RETRY
    assert decision.new_priority == 2
    assert decision.next_eligible_at == format_ist(at(THURSDAY, 11, 0))
    assert decision.trigger_global_dnd is False


def test_callback_outcome_waits_until_callback_at() -> None:
    lead = make_lead(attempts=0)

    decision = evaluate_call_outcome(
        lead,
        make_campaign(),
        make_config(),
        "completed",
        "callback_requested",
        at(THURSDAY, 10, 0),
        callback_at=at(THURSDAY, 12, 30),
    )

    assert decision.new_state is LeadState.WAITING_CALLBACK
    assert decision.new_priority == 1
    assert decision.next_eligible_at == format_ist(at(THURSDAY, 12, 30))
    assert decision.trigger_global_dnd is False


@pytest.mark.parametrize(
    ("status", "disposition", "callback_at"),
    [
        ("no_answer", "interested", None),
        ("busy", "callback_requested", None),
        ("completed", None, None),
        ("completed", "unknown", None),
        ("completed", "callback_requested", None),
        ("in_progress", None, None),
    ],
)
def test_malformed_call_outcomes_are_rejected(
    status: str, disposition: str | None, callback_at: datetime | None
) -> None:
    with pytest.raises(InvalidCallOutcomeError):
        evaluate_call_outcome(
            make_lead(),
            make_campaign(),
            make_config(),
            status,
            disposition,
            at(THURSDAY, 10, 0),
            callback_at=callback_at,
        )

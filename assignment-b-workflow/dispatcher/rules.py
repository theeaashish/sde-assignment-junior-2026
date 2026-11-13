from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta

from dispatcher.clock import IST, format_ist, parse_ist
from dispatcher.models import Campaign, GlobalConfig, Lead, LeadState

DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
TERMINAL_DISPOSITIONS = frozenset(
    {"interested", "not_interested", "wrong_number", "dnd"}
)
VALID_STATUSES = frozenset({"completed", "no_answer", "busy", "failed"})
DEFAULT_CLOSE_BUFFER_SEC = 15


class InvalidWindowConfigError(ValueError):
    pass


class InvalidCallOutcomeError(ValueError):
    pass


def _parse_hh_mm(value: str) -> time:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError as exc:
        raise InvalidWindowConfigError(f"invalid HH:MM time: {value!r}") from exc


def effective_daily_bounds(
    campaign: Campaign, config: GlobalConfig
) -> tuple[time, time]:
    if not campaign.window_days:
        raise InvalidWindowConfigError("campaign window_days must not be empty")
    invalid_days = set(campaign.window_days) - set(DAYS)
    if invalid_days:
        raise InvalidWindowConfigError(f"invalid window days: {sorted(invalid_days)}")

    campaign_start = _parse_hh_mm(campaign.window_start)
    campaign_end = _parse_hh_mm(campaign.window_end)
    regulatory_start = _parse_hh_mm(config.regulatory_start)
    regulatory_end = _parse_hh_mm(config.regulatory_end)

    eff_start = max(campaign_start, regulatory_start)
    eff_end = min(campaign_end, regulatory_end)
    if eff_start >= eff_end:
        raise InvalidWindowConfigError(
            "campaign and regulatory calling windows do not overlap"
        )
    return eff_start, eff_end


def is_window_open(
    sim_now: datetime,
    campaign: Campaign,
    config: GlobalConfig,
    close_buffer_sec: int = DEFAULT_CLOSE_BUFFER_SEC,
) -> bool:
    lt = parse_ist(sim_now)
    if DAYS[lt.weekday()] not in campaign.window_days:
        return False

    start, end = effective_daily_bounds(campaign, config)
    start_dt = datetime.combine(lt.date(), start, IST)
    end_dt = datetime.combine(lt.date(), end, IST)
    return start_dt <= lt < end_dt - timedelta(seconds=close_buffer_sec)


def next_valid_window_open(
    target_time: datetime,
    campaign: Campaign,
    config: GlobalConfig,
    close_buffer_sec: int = DEFAULT_CLOSE_BUFFER_SEC,
) -> datetime:
    lt = parse_ist(target_time)
    start, end = effective_daily_bounds(campaign, config)

    for day_offset in range(8):
        day = lt.date() + timedelta(days=day_offset)
        start_dt = datetime.combine(day, start, IST)
        candidate = max(lt, start_dt)
        if candidate < datetime.combine(day, end, IST) and is_window_open(
            candidate, campaign, config, close_buffer_sec
        ):
            return candidate

    raise InvalidWindowConfigError("no valid calling window found within 8 days")


def compute_next_eligible_at(
    status: str,
    disposition: str | None,
    ended_at: datetime,
    callback_at: datetime | None,
    campaign: Campaign,
    config: GlobalConfig,
    close_buffer_sec: int = DEFAULT_CLOSE_BUFFER_SEC,
) -> datetime:
    parsed_ended_at = parse_ist(ended_at)

    if status == "no_answer":
        raw_eligible = parsed_ended_at + timedelta(
            minutes=config.retry_no_answer_minutes
        )
    elif status == "busy":
        raw_eligible = parsed_ended_at + timedelta(minutes=config.retry_busy_minutes)
    elif status == "failed":
        raw_eligible = parsed_ended_at + timedelta(minutes=config.retry_failed_minutes)
    elif status == "completed" and disposition == "callback_requested":
        if callback_at is None:
            raise InvalidCallOutcomeError(
                "callback_requested requires a callback_at timestamp"
            )
        raw_eligible = parse_ist(callback_at)
    else:
        raise InvalidCallOutcomeError(
            f"unsupported call outcome: status={status!r}, disposition={disposition!r}"
        )

    return next_valid_window_open(raw_eligible, campaign, config, close_buffer_sec)


def candidate_sort_key(lead: Lead) -> tuple[int, str, str, str]:
    return (
        lead.priority,
        lead.next_eligible_at or "",
        lead.campaign_id,
        lead.lead_id,
    )


@dataclass(frozen=True)
class OutcomeDecision:
    new_state: LeadState
    new_priority: int
    next_eligible_at: str | None
    trigger_global_dnd: bool


def evaluate_call_outcome(
    lead: Lead,
    campaign: Campaign,
    config: GlobalConfig,
    status: str,
    disposition: str | None,
    ended_at: datetime,
    callback_at: datetime | None = None,
    close_buffer_sec: int = DEFAULT_CLOSE_BUFFER_SEC,
) -> OutcomeDecision:
    if status not in VALID_STATUSES:
        raise InvalidCallOutcomeError(f"invalid call status: {status!r}")

    if status == "completed":
        if disposition not in TERMINAL_DISPOSITIONS | {"callback_requested"}:
            raise InvalidCallOutcomeError(
                f"invalid disposition for completed call: {disposition!r}"
            )
    elif disposition is not None:
        raise InvalidCallOutcomeError(
            f"disposition must be None for status {status!r}, got {disposition!r}"
        )

    new_attempts = lead.attempts + 1

    if status == "completed" and disposition in TERMINAL_DISPOSITIONS:
        return OutcomeDecision(
            new_state=LeadState.COMPLETED,
            new_priority=lead.priority,
            next_eligible_at=None,
            trigger_global_dnd=disposition == "dnd",
        )

    if new_attempts >= campaign.max_attempts:
        return OutcomeDecision(
            new_state=LeadState.EXHAUSTED,
            new_priority=lead.priority,
            next_eligible_at=None,
            trigger_global_dnd=False,
        )

    eligible = format_ist(
        compute_next_eligible_at(
            status,
            disposition,
            ended_at,
            callback_at,
            campaign,
            config,
            close_buffer_sec,
        )
    )

    if status == "completed":
        return OutcomeDecision(
            new_state=LeadState.WAITING_CALLBACK,
            new_priority=1,
            next_eligible_at=eligible,
            trigger_global_dnd=False,
        )

    return OutcomeDecision(
        new_state=LeadState.WAITING_RETRY,
        new_priority=2,
        next_eligible_at=eligible,
        trigger_global_dnd=False,
    )

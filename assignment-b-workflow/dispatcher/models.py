from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from enum import Enum


class LeadState(str, Enum):
    FRESH = "FRESH"
    WAITING_RETRY = "WAITING_RETRY"
    WAITING_CALLBACK = "WAITING_CALLBACK"
    RESERVING = "RESERVING"
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"
    EXHAUSTED = "EXHAUSTED"
    DND_BLOCKED = "DND_BLOCKED"


class AttemptState(str, Enum):
    RESERVING = "RESERVING"
    ACTIVE = "ACTIVE"
    ENDED = "ENDED"
    ABORTED = "ABORTED"


FINAL_LEAD_STATES: frozenset[LeadState] = frozenset(
    {
        LeadState.COMPLETED,
        LeadState.EXHAUSTED,
        LeadState.DND_BLOCKED,
    }
)

IN_FLIGHT_LEAD_STATES: frozenset[LeadState] = frozenset(
    {
        LeadState.RESERVING,
        LeadState.ACTIVE,
    }
)


@dataclass(frozen=True)
class GlobalConfig:
    sim_start: str
    regulatory_start: str
    regulatory_end: str
    window_grace_seconds: int
    retry_no_answer_minutes: int
    retry_busy_minutes: int
    retry_failed_minutes: int
    webhook_secret: str
    id: int = 1

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> GlobalConfig:
        return cls(
            id=row["id"],
            sim_start=row["sim_start"],
            regulatory_start=row["regulatory_start"],
            regulatory_end=row["regulatory_end"],
            window_grace_seconds=row["window_grace_seconds"],
            retry_no_answer_minutes=row["retry_no_answer_minutes"],
            retry_busy_minutes=row["retry_busy_minutes"],
            retry_failed_minutes=row["retry_failed_minutes"],
            webhook_secret=row["webhook_secret"],
        )


@dataclass(frozen=True)
class Tenant:
    tenant_id: str
    max_concurrent_calls: int

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Tenant:
        return cls(
            tenant_id=row["tenant_id"],
            max_concurrent_calls=row["max_concurrent_calls"],
        )


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    tenant_id: str
    window_start: str
    window_end: str
    window_days: tuple[str, ...]
    max_attempts: int
    leads_file: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Campaign:
        return cls(
            campaign_id=row["campaign_id"],
            tenant_id=row["tenant_id"],
            window_start=row["window_start"],
            window_end=row["window_end"],
            window_days=tuple(json.loads(row["window_days_json"])),
            max_attempts=row["max_attempts"],
            leads_file=row["leads_file"],
        )


@dataclass
class Lead:
    campaign_id: str
    lead_id: str
    tenant_id: str
    phone: str
    state: LeadState = LeadState.FRESH
    prior_state: LeadState | None = None
    priority: int = 3
    attempts: int = 0
    dispatch_seq: int = 0
    next_eligible_at: str | None = None
    active_request_id: str | None = None
    active_call_sid: str | None = None
    last_status: str = ""
    last_disposition: str = ""
    last_ended_at: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Lead:
        return cls(
            campaign_id=row["campaign_id"],
            lead_id=row["lead_id"],
            tenant_id=row["tenant_id"],
            phone=row["phone"],
            state=LeadState(row["state"]),
            prior_state=LeadState(row["prior_state"]) if row["prior_state"] else None,
            priority=row["priority"],
            attempts=row["attempts"],
            dispatch_seq=row["dispatch_seq"],
            next_eligible_at=row["next_eligible_at"],
            active_request_id=row["active_request_id"],
            active_call_sid=row["active_call_sid"],
            last_status=row["last_status"],
            last_disposition=row["last_disposition"],
            last_ended_at=row["last_ended_at"],
        )


@dataclass
class CallAttempt:
    request_id: str
    campaign_id: str
    lead_id: str
    tenant_id: str
    phone: str
    created_sim_at: str
    call_sid: str | None = None
    state: AttemptState = AttemptState.RESERVING
    accepted: int = 0
    outcome_status: str | None = None
    disposition: str | None = None
    callback_at: str | None = None
    ended_sim_at: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> CallAttempt:
        return cls(
            request_id=row["request_id"],
            call_sid=row["call_sid"],
            campaign_id=row["campaign_id"],
            lead_id=row["lead_id"],
            tenant_id=row["tenant_id"],
            phone=row["phone"],
            state=AttemptState(row["state"]),
            accepted=row["accepted"],
            outcome_status=row["outcome_status"],
            disposition=row["disposition"],
            callback_at=row["callback_at"],
            created_sim_at=row["created_sim_at"],
            ended_sim_at=row["ended_sim_at"],
        )


@dataclass(frozen=True)
class DndPhone:
    phone: str
    triggered_by_campaign: str
    triggered_by_lead: str
    recorded_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> DndPhone:
        return cls(
            phone=row["phone"],
            triggered_by_campaign=row["triggered_by_campaign"],
            triggered_by_lead=row["triggered_by_lead"],
            recorded_at=row["recorded_at"],
        )

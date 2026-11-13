from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import datetime

from dispatcher.clock import format_ist, parse_ist
from dispatcher.db import Database
from dispatcher.models import (
    FINAL_LEAD_STATES,
    AttemptState,
    CallAttempt,
    Lead,
    LeadState,
)
from dispatcher.rules import (
    DEFAULT_CLOSE_BUFFER_SEC,
    candidate_sort_key,
    evaluate_call_outcome,
    is_window_open,
)


def _reserve(
    conn: sqlite3.Connection, lead: Lead, sim_now: datetime
) -> tuple[Lead, CallAttempt]:
    new_seq = lead.dispatch_seq + 1
    request_id = f"{lead.campaign_id}:{lead.lead_id}:seq_{new_seq}"
    created_at = format_ist(sim_now)
    conn.execute(
        "UPDATE leads SET state = 'RESERVING', prior_state = ?, dispatch_seq = ?, "
        "active_request_id = ?, active_call_sid = NULL "
        "WHERE campaign_id = ? AND lead_id = ?",
        (lead.state.value, new_seq, request_id, lead.campaign_id, lead.lead_id),
    )
    conn.execute(
        "INSERT INTO call_attempts (request_id, call_sid, campaign_id, lead_id, "
        "tenant_id, phone, state, accepted, created_sim_at) "
        "VALUES (?, NULL, ?, ?, ?, ?, 'RESERVING', 0, ?)",
        (
            request_id,
            lead.campaign_id,
            lead.lead_id,
            lead.tenant_id,
            lead.phone,
            created_at,
        ),
    )
    return (
        replace(
            lead,
            state=LeadState.RESERVING,
            prior_state=lead.state,
            dispatch_seq=new_seq,
            active_request_id=request_id,
            active_call_sid=None,
        ),
        CallAttempt(
            request_id=request_id,
            campaign_id=lead.campaign_id,
            lead_id=lead.lead_id,
            tenant_id=lead.tenant_id,
            phone=lead.phone,
            created_sim_at=created_at,
        ),
    )


def reserve_next_lead(
    db: Database,
    sim_now: datetime,
    close_buffer_sec: int = DEFAULT_CLOSE_BUFFER_SEC,
) -> tuple[Lead, CallAttempt] | None:
    with db.write_tx() as conn:
        config = db.get_config()
        if config is None:
            return None
        tenants = {tenant.tenant_id: tenant for tenant in db.get_tenants()}
        campaigns = {campaign.campaign_id: campaign for campaign in db.get_campaigns()}
        active_counts = {
            row["tenant_id"]: row["count"]
            for row in conn.execute(
                "SELECT tenant_id, COUNT(*) AS count FROM leads "
                "WHERE state IN ('RESERVING', 'ACTIVE') GROUP BY tenant_id"
            ).fetchall()
        }
        active_phones = {
            row["phone"]
            for row in conn.execute(
                "SELECT DISTINCT phone FROM leads "
                "WHERE state IN ('RESERVING', 'ACTIVE')"
            ).fetchall()
        }
        leads = [
            Lead.from_row(row)
            for row in conn.execute(
                "SELECT * FROM leads "
                "WHERE state IN ('FRESH', 'WAITING_RETRY', 'WAITING_CALLBACK') "
                "AND (next_eligible_at IS NULL OR next_eligible_at <= ?) "
                "AND phone NOT IN (SELECT phone FROM dnd_phones)",
                (format_ist(sim_now),),
            ).fetchall()
        ]
        leads.sort(key=candidate_sort_key)

        for lead in leads:
            campaign = campaigns.get(lead.campaign_id)
            tenant = tenants.get(lead.tenant_id)
            if campaign is None or tenant is None:
                continue
            if lead.attempts >= campaign.max_attempts:
                continue
            if active_counts.get(lead.tenant_id, 0) >= tenant.max_concurrent_calls:
                continue
            if lead.phone in active_phones:
                continue
            if is_window_open(sim_now, campaign, config, close_buffer_sec):
                return _reserve(conn, lead, sim_now)
        return None


def mark_attempt_accepted(db: Database, request_id: str, call_sid: str) -> bool:
    with db.write_tx() as conn:
        attempt = db.get_call_attempt(request_id)
        if attempt is None:
            return False
        if attempt.state in (AttemptState.ENDED, AttemptState.ABORTED):
            return False

        conn.execute(
            "UPDATE call_attempts SET state = 'ACTIVE', call_sid = ?, accepted = 1 "
            "WHERE request_id = ?",
            (call_sid, request_id),
        )
        conn.execute(
            "UPDATE leads SET "
            "attempts = attempts + CASE WHEN ? = 0 THEN 1 ELSE 0 END, "
            "state = 'ACTIVE', active_call_sid = ? "
            "WHERE campaign_id = ? AND lead_id = ? "
            "AND active_request_id = ? AND state = 'RESERVING'",
            (
                attempt.accepted,
                call_sid,
                attempt.campaign_id,
                attempt.lead_id,
                request_id,
            ),
        )
        return True


def abort_reserved_attempt(db: Database, request_id: str) -> bool:
    with db.write_tx() as conn:
        attempt = db.get_call_attempt(request_id)
        if attempt is None:
            return False
        if attempt.accepted == 1 or attempt.state in (
            AttemptState.ENDED,
            AttemptState.ABORTED,
        ):
            return False

        conn.execute(
            "UPDATE call_attempts SET state = 'ABORTED' WHERE request_id = ?",
            (request_id,),
        )
        lead = db.get_lead(attempt.campaign_id, attempt.lead_id)
        if lead is None or lead.active_request_id != request_id:
            return True

        phone_is_dnd = conn.execute(
            "SELECT 1 FROM dnd_phones WHERE phone = ?", (lead.phone,)
        ).fetchone()
        if lead.state not in FINAL_LEAD_STATES and phone_is_dnd is not None:
            conn.execute(
                "UPDATE leads SET state = 'DND_BLOCKED', prior_state = NULL, "
                "next_eligible_at = NULL, active_request_id = NULL, "
                "active_call_sid = NULL "
                "WHERE campaign_id = ? AND lead_id = ?",
                (lead.campaign_id, lead.lead_id),
            )
        else:
            reverted_state = (
                lead.prior_state.value
                if lead.prior_state is not None
                else LeadState.FRESH.value
            )
            conn.execute(
                "UPDATE leads SET state = ?, prior_state = NULL, "
                "active_request_id = NULL, active_call_sid = NULL "
                "WHERE campaign_id = ? AND lead_id = ?",
                (reverted_state, lead.campaign_id, lead.lead_id),
            )
        return True


def _trigger_global_dnd(conn: sqlite3.Connection, lead: Lead, ended_at: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO dnd_phones "
        "(phone, triggered_by_campaign, triggered_by_lead, recorded_at) "
        "VALUES (?, ?, ?, ?)",
        (lead.phone, lead.campaign_id, lead.lead_id, ended_at),
    )
    conn.execute(
        "UPDATE leads SET state = 'DND_BLOCKED', prior_state = NULL, "
        "next_eligible_at = NULL, active_request_id = NULL, active_call_sid = NULL "
        "WHERE phone = ? AND state NOT IN "
        "('COMPLETED', 'EXHAUSTED', 'DND_BLOCKED', 'RESERVING', 'ACTIVE')",
        (lead.phone,),
    )


def apply_call_outcome(
    db: Database,
    *,
    request_id: str | None = None,
    call_sid: str | None = None,
    status: str,
    disposition: str | None,
    ended_at: str | datetime,
    callback_at: str | datetime | None = None,
    close_buffer_sec: int = DEFAULT_CLOSE_BUFFER_SEC,
) -> bool:
    with db.write_tx() as conn:
        attempt = db.get_call_attempt(request_id) if request_id is not None else None
        if attempt is None and call_sid is not None:
            attempt = db.get_call_attempt_by_sid(call_sid)
        if attempt is None or attempt.state in (
            AttemptState.ENDED,
            AttemptState.ABORTED,
        ):
            return False

        lead = db.get_lead(attempt.campaign_id, attempt.lead_id)
        campaign = db.get_campaign(attempt.campaign_id)
        config = db.get_config()
        if lead is None or campaign is None or config is None:
            return False

        ended_dt = parse_ist(ended_at)
        callback_dt = parse_ist(callback_at) if callback_at is not None else None

        # Webhooks can beat the 201 response, so an unaccepted attempt is counted here.
        if attempt.accepted == 1:
            base_attempts = max(0, lead.attempts - 1)
            final_attempts = lead.attempts
        else:
            base_attempts = lead.attempts
            final_attempts = lead.attempts + 1

        decision = evaluate_call_outcome(
            replace(lead, attempts=base_attempts),
            campaign,
            config,
            status,
            disposition,
            ended_dt,
            callback_dt,
            close_buffer_sec,
        )
        ended_str = format_ist(ended_dt)
        callback_str = format_ist(callback_dt) if callback_dt is not None else None

        conn.execute(
            "UPDATE call_attempts SET state = 'ENDED', accepted = 1, "
            "call_sid = COALESCE(?, call_sid), outcome_status = ?, disposition = ?, "
            "callback_at = ?, ended_sim_at = ? WHERE request_id = ?",
            (
                call_sid,
                status,
                disposition,
                callback_str,
                ended_str,
                attempt.request_id,
            ),
        )
        conn.execute(
            "UPDATE leads SET state = ?, prior_state = NULL, priority = ?, "
            "attempts = ?, next_eligible_at = ?, active_request_id = NULL, "
            "active_call_sid = NULL, last_status = ?, last_disposition = ?, "
            "last_ended_at = ? WHERE campaign_id = ? AND lead_id = ?",
            (
                decision.new_state.value,
                decision.new_priority,
                final_attempts,
                decision.next_eligible_at,
                status,
                disposition or "",
                ended_str,
                lead.campaign_id,
                lead.lead_id,
            ),
        )
        if decision.trigger_global_dnd:
            _trigger_global_dnd(conn, lead, ended_str)
        return True

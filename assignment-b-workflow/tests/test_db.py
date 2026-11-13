import sqlite3
from pathlib import Path

import pytest
from dispatcher.db import Database
from dispatcher.io_files import load_fixtures
from dispatcher.models import AttemptState, LeadState


def test_sqlite_pragmas(tmp_path: Path):
    """Verify WAL mode, synchronous FULL (2), foreign keys ON, and busy_timeout=5000."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    jm = db.conn.execute("PRAGMA journal_mode;").fetchone()[0]
    sync = db.conn.execute("PRAGMA synchronous;").fetchone()[0]
    fk = db.conn.execute("PRAGMA foreign_keys;").fetchone()[0]
    bt = db.conn.execute("PRAGMA busy_timeout;").fetchone()[0]

    assert jm.lower() == "wal"
    assert sync == 2  # 2 corresponds to FULL
    assert fk == 1  # foreign keys ON
    assert bt == 5000  # 5000ms busy timeout
    db.close()


def test_load_fixtures_fresh(tmp_path: Path):
    """Verify loading fixtures populates all campaigns and all 200 leads in FRESH state."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    summary = load_fixtures(db, "fixtures/campaigns.json")
    assert summary["config"] == 1
    assert summary["tenants"] == 2
    assert summary["campaigns"] == 3
    assert summary["leads"] == 200

    # Config validation
    cfg = db.get_config()
    assert cfg is not None
    assert cfg.sim_start == "2026-11-12T17:30:00+05:30"
    assert cfg.regulatory_start == "09:00"
    assert cfg.regulatory_end == "21:00"
    assert cfg.window_grace_seconds == 120
    assert cfg.retry_no_answer_minutes == 60
    assert cfg.retry_busy_minutes == 60
    assert cfg.retry_failed_minutes == 15
    assert cfg.webhook_secret == "whsec_au_assignment_local"

    # Tenants validation
    tenants = db.get_tenants()
    assert len(tenants) == 2
    tenant_map = {t.tenant_id: t for t in tenants}
    assert tenant_map["acme_insurance"].max_concurrent_calls == 4
    assert tenant_map["zen_retail"].max_concurrent_calls == 3

    # Campaigns validation
    campaigns = db.get_campaigns()
    assert len(campaigns) == 3
    camp_map = {c.campaign_id: c for c in campaigns}
    assert "c_acme_renewals" in camp_map
    assert "c_acme_feedback" in camp_map
    assert "c_zen_winback" in camp_map
    assert camp_map["c_acme_renewals"].tenant_id == "acme_insurance"
    assert camp_map["c_acme_renewals"].max_attempts == 3
    assert "sat" in camp_map["c_acme_renewals"].window_days

    # Leads validation
    leads = db.get_all_leads_sorted()
    assert len(leads) == 200
    assert db.count_non_final_leads() == 200

    for lead in leads:
        assert lead.state == LeadState.FRESH.value
        assert lead.priority == 3
        assert lead.attempts == 0
        assert lead.dispatch_seq == 0
        assert lead.prior_state is None
        assert lead.next_eligible_at is None
        assert lead.active_request_id is None
        assert lead.active_call_sid is None
        assert lead.last_status == ""
        assert lead.last_disposition == ""
        assert lead.last_ended_at is None

    # Verify individual campaign lead counts
    renewals = [l for l in leads if l.campaign_id == "c_acme_renewals"]
    feedback = [l for l in leads if l.campaign_id == "c_acme_feedback"]
    winback = [l for l in leads if l.campaign_id == "c_zen_winback"]
    assert len(renewals) == 70
    assert len(feedback) == 60
    assert len(winback) == 70

    db.close()


def test_restart_idempotency(tmp_path: Path):
    """Verify calling load_fixtures() after crash/restart does not overwrite existing leads."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    # Initial load
    load_fixtures(db, "fixtures/campaigns.json")

    # Mutate a lead to COMPLETED with attempts=2
    target_cid = "c_acme_renewals"
    target_lid = "ren_0001"
    with db.write_tx() as conn:
        conn.execute(
            """
            UPDATE leads
            SET state = ?, attempts = ?, last_status = 'completed', last_disposition = 'interested'
            WHERE campaign_id = ? AND lead_id = ?
            """,
            (LeadState.COMPLETED.value, 2, target_cid, target_lid),
        )

    # Verify mutation in DB before reload
    mutated = db.get_lead(target_cid, target_lid)
    assert mutated is not None
    assert mutated.state == LeadState.COMPLETED.value
    assert mutated.attempts == 2
    assert mutated.last_disposition == "interested"

    # Call load_fixtures() a second time (simulating crash recovery)
    summary2 = load_fixtures(db, "fixtures/campaigns.json")
    assert summary2["config"] == 0
    assert summary2["tenants"] == 0
    assert summary2["campaigns"] == 0
    assert summary2["leads"] == 0

    # Assert mutated lead is untouched
    reloaded = db.get_lead(target_cid, target_lid)
    assert reloaded is not None
    assert reloaded.state == LeadState.COMPLETED.value
    assert reloaded.attempts == 2
    assert reloaded.last_disposition == "interested"

    # Total lead count remains 200
    all_leads = db.get_all_leads_sorted()
    assert len(all_leads) == 200
    assert db.count_non_final_leads() == 199

    db.close()


def test_transaction_rollback(tmp_path: Path):
    """Verify raising an exception inside write_tx() rolls back partial writes cleanly."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    # Baseline write
    with db.write_tx() as conn:
        conn.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES (?, ?)",
            ("baseline_tenant", 5),
        )

    assert db.get_tenant("baseline_tenant") is not None

    # Transaction that fails mid-way
    with (
        pytest.raises(RuntimeError, match="simulated write failure"),
        db.write_tx() as conn,
    ):
        conn.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES (?, ?)",
            ("aborted_tenant", 10),
        )
        conn.execute(
            "UPDATE tenants SET max_concurrent_calls = 99 WHERE tenant_id = 'baseline_tenant'"
        )
        raise RuntimeError("simulated write failure")

    # Verify aborted write did not persist and baseline was not modified
    assert db.get_tenant("aborted_tenant") is None
    baseline = db.get_tenant("baseline_tenant")
    assert baseline is not None
    assert baseline.max_concurrent_calls == 5

    db.close()


def test_foreign_key_enforcement(tmp_path: Path):
    """Verify that foreign keys are enforced on table inserts."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    # Inserting campaign with nonexistent tenant_id must fail
    with pytest.raises(sqlite3.IntegrityError), db.write_tx() as conn:
        conn.execute(
            """
                INSERT INTO campaigns (
                    campaign_id, tenant_id, window_start, window_end,
                    window_days_json, max_attempts, leads_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
            (
                "c_orphan",
                "nonexistent_tenant",
                "09:00",
                "18:00",
                "[]",
                3,
                "dummy.csv",
            ),
        )

    db.close()


def test_in_flight_attempts(tmp_path: Path):
    """Verify get_in_flight_attempts() returns only RESERVING and ACTIVE attempts."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    with db.write_tx() as conn:
        conn.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES ('t1', 5)"
        )
        conn.execute(
            """
            INSERT INTO campaigns (campaign_id, tenant_id, window_start, window_end, window_days_json, max_attempts, leads_file)
            VALUES ('c1', 't1', '09:00', '18:00', '[]', 3, 'f.csv')
            """
        )
        conn.execute(
            """
            INSERT INTO call_attempts (request_id, campaign_id, lead_id, tenant_id, phone, state, created_sim_at)
            VALUES
                ('req_1', 'c1', 'l1', 't1', '+919999999901', 'RESERVING', '2026-11-12T17:30:00+05:30'),
                ('req_2', 'c1', 'l2', 't1', '+919999999902', 'ACTIVE', '2026-11-12T17:31:00+05:30'),
                ('req_3', 'c1', 'l3', 't1', '+919999999903', 'ENDED', '2026-11-12T17:32:00+05:30'),
                ('req_4', 'c1', 'l4', 't1', '+919999999904', 'ABORTED', '2026-11-12T17:33:00+05:30')
            """
        )

    in_flight = db.get_in_flight_attempts()
    assert len(in_flight) == 2
    req_ids = {a.request_id for a in in_flight}
    assert req_ids == {"req_1", "req_2"}
    states = {a.state for a in in_flight}
    assert states == {AttemptState.RESERVING.value, AttemptState.ACTIVE.value}

    db.close()


def test_dnd_phones(tmp_path: Path):
    """Verify dnd_phones table and helpers."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)

    phone = "+919876543210"
    assert not db.is_phone_dnd(phone)

    with db.write_tx() as conn:
        conn.execute(
            """
            INSERT INTO dnd_phones (phone, triggered_by_campaign, triggered_by_lead, recorded_at)
            VALUES (?, ?, ?, ?)
            """,
            (phone, "c_acme_renewals", "ren_0001", "2026-11-12T18:00:00+05:30"),
        )

    assert db.is_phone_dnd(phone)
    dnd = db.get_dnd_phone(phone)
    assert dnd is not None
    assert dnd.phone == phone
    assert dnd.triggered_by_campaign == "c_acme_renewals"
    assert dnd.triggered_by_lead == "ren_0001"

    db.close()

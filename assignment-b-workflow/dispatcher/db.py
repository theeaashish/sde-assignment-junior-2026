from __future__ import annotations

import csv
import json
import sqlite3
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Any, Self, TypeVar

from dispatcher.models import (
    CallAttempt,
    Campaign,
    DndPhone,
    GlobalConfig,
    Lead,
    Tenant,
)

T = TypeVar("T")

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS config (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        sim_start TEXT NOT NULL,
        regulatory_start TEXT NOT NULL,
        regulatory_end TEXT NOT NULL,
        window_grace_seconds INTEGER NOT NULL,
        retry_no_answer_minutes INTEGER NOT NULL,
        retry_busy_minutes INTEGER NOT NULL,
        retry_failed_minutes INTEGER NOT NULL,
        webhook_secret TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS tenants (
        tenant_id TEXT PRIMARY KEY,
        max_concurrent_calls INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS campaigns (
        campaign_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id),
        window_start TEXT NOT NULL,
        window_end TEXT NOT NULL,
        window_days_json TEXT NOT NULL,
        max_attempts INTEGER NOT NULL,
        leads_file TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS leads (
        campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id),
        lead_id TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        phone TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'FRESH',
        prior_state TEXT,
        priority INTEGER NOT NULL DEFAULT 3,
        attempts INTEGER NOT NULL DEFAULT 0,
        dispatch_seq INTEGER NOT NULL DEFAULT 0,
        next_eligible_at TEXT,
        active_request_id TEXT,
        active_call_sid TEXT,
        last_status TEXT NOT NULL DEFAULT '',
        last_disposition TEXT NOT NULL DEFAULT '',
        last_ended_at TEXT,
        PRIMARY KEY (campaign_id, lead_id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_leads_state_priority_next ON leads(state, priority, next_eligible_at);",
    "CREATE INDEX IF NOT EXISTS idx_leads_tenant_state ON leads(tenant_id, state);",
    "CREATE INDEX IF NOT EXISTS idx_leads_phone_state ON leads(phone, state);",
    """
    CREATE TABLE IF NOT EXISTS call_attempts (
        request_id TEXT PRIMARY KEY,
        call_sid TEXT,
        campaign_id TEXT NOT NULL,
        lead_id TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        phone TEXT NOT NULL,
        state TEXT NOT NULL,
        accepted INTEGER NOT NULL DEFAULT 0,
        outcome_status TEXT,
        disposition TEXT,
        callback_at TEXT,
        created_sim_at TEXT NOT NULL,
        ended_sim_at TEXT
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_call_attempts_call_sid ON call_attempts(call_sid);",
    "CREATE INDEX IF NOT EXISTS idx_call_attempts_state ON call_attempts(state);",
    """
    CREATE TABLE IF NOT EXISTS dnd_phones (
        phone TEXT PRIMARY KEY,
        triggered_by_campaign TEXT NOT NULL,
        triggered_by_lead TEXT NOT NULL,
        recorded_at TEXT NOT NULL
    );
    """,
)


class Database:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = self._connect()
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None delegates transaction boundaries to write_tx()
        conn = sqlite3.connect(
            self.db_path, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = FULL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    @contextmanager
    def write_tx(self) -> Generator[sqlite3.Connection, None, None]:
        # BEGIN IMMEDIATE acquires a write lock upfront to serialize reservations
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except Exception:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def init_schema(self) -> None:
        with self.write_tx() as conn:
            for stmt in SCHEMA_STATEMENTS:
                conn.execute(stmt)

    def load_fixtures(self, campaigns_json_path: str | Path) -> dict[str, int]:
        path = Path(campaigns_json_path)
        if not path.is_absolute() and not path.exists():
            candidate = Path("assignment-b-workflow") / path
            if candidate.exists():
                path = candidate
            else:
                candidate2 = Path(__file__).resolve().parent.parent / path
                if candidate2.exists():
                    path = candidate2
        path = path.resolve()
        base_dir = path.parent
        with path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)

        with self.write_tx() as conn:
            rw = cfg["regulatory_window"]
            rp = cfg["retry_policy"]
            cfg_count = conn.execute(
                """
                INSERT OR IGNORE INTO config (
                    id, sim_start, regulatory_start, regulatory_end,
                    window_grace_seconds, retry_no_answer_minutes, retry_busy_minutes,
                    retry_failed_minutes, webhook_secret
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cfg["sim_start"],
                    rw["start"],
                    rw["end"],
                    cfg.get("window_grace_seconds", 120),
                    rp["no_answer_minutes"],
                    rp["busy_minutes"],
                    rp["failed_minutes"],
                    cfg["webhook_secret"],
                ),
            ).rowcount

            tenant_count = sum(
                conn.execute(
                    "INSERT OR IGNORE INTO tenants (tenant_id, max_concurrent_calls) VALUES (?, ?)",
                    (t["tenant_id"], t["max_concurrent_calls"]),
                ).rowcount
                for t in cfg.get("tenants", [])
            )

            campaign_count = 0
            lead_count = 0
            for c in cfg.get("campaigns", []):
                campaign_count += conn.execute(
                    """
                    INSERT OR IGNORE INTO campaigns (
                        campaign_id, tenant_id, window_start, window_end,
                        window_days_json, max_attempts, leads_file
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        c["campaign_id"],
                        c["tenant_id"],
                        c["window"]["start"],
                        c["window"]["end"],
                        json.dumps(c["window"]["days"]),
                        c["max_attempts"],
                        c["leads_file"],
                    ),
                ).rowcount

                with (base_dir / c["leads_file"]).open("r", encoding="utf-8") as lf:
                    for row in csv.DictReader(lf):
                        lead_count += conn.execute(
                            """
                            INSERT OR IGNORE INTO leads (
                                campaign_id, lead_id, tenant_id, phone,
                                state, priority, attempts, dispatch_seq,
                                last_status, last_disposition
                            ) VALUES (?, ?, ?, ?, 'FRESH', 3, 0, 0, '', '')
                            """,
                            (
                                c["campaign_id"],
                                row["lead_id"],
                                c["tenant_id"],
                                row["phone"],
                            ),
                        ).rowcount

        return {
            "config": cfg_count,
            "tenants": tenant_count,
            "campaigns": campaign_count,
            "leads": lead_count,
        }

    def _one(
        self, sql: str, params: Sequence[Any], factory: Callable[[sqlite3.Row], T]
    ) -> T | None:
        row = self.conn.execute(sql, params).fetchone()
        return factory(row) if row is not None else None

    def _many(
        self, sql: str, params: Sequence[Any], factory: Callable[[sqlite3.Row], T]
    ) -> list[T]:
        return [factory(row) for row in self.conn.execute(sql, params).fetchall()]

    def _scalar(self, sql: str, params: Sequence[Any] = ()) -> int:
        row = self.conn.execute(sql, params).fetchone()
        return int(row[0]) if row else 0

    def get_config(self) -> GlobalConfig | None:
        return self._one("SELECT * FROM config WHERE id = 1", (), GlobalConfig.from_row)

    def get_campaigns(self) -> list[Campaign]:
        return self._many(
            "SELECT * FROM campaigns ORDER BY campaign_id", (), Campaign.from_row
        )

    def get_campaign(self, campaign_id: str) -> Campaign | None:
        return self._one(
            "SELECT * FROM campaigns WHERE campaign_id = ?",
            (campaign_id,),
            Campaign.from_row,
        )

    def get_tenants(self) -> list[Tenant]:
        return self._many(
            "SELECT * FROM tenants ORDER BY tenant_id", (), Tenant.from_row
        )

    def get_tenant(self, tenant_id: str) -> Tenant | None:
        return self._one(
            "SELECT * FROM tenants WHERE tenant_id = ?", (tenant_id,), Tenant.from_row
        )

    def get_lead(self, campaign_id: str, lead_id: str) -> Lead | None:
        return self._one(
            "SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?",
            (campaign_id, lead_id),
            Lead.from_row,
        )

    def get_all_leads_sorted(self) -> list[Lead]:
        return self._many(
            "SELECT * FROM leads ORDER BY campaign_id, lead_id", (), Lead.from_row
        )

    def count_non_final_leads(self) -> int:
        return self._scalar(
            "SELECT COUNT(*) FROM leads WHERE state NOT IN ('COMPLETED', 'EXHAUSTED', 'DND_BLOCKED')"
        )

    def get_in_flight_attempts(self) -> list[CallAttempt]:
        return self._many(
            "SELECT * FROM call_attempts WHERE state IN ('RESERVING', 'ACTIVE') ORDER BY created_sim_at",
            (),
            CallAttempt.from_row,
        )

    def get_call_attempt(self, request_id: str) -> CallAttempt | None:
        return self._one(
            "SELECT * FROM call_attempts WHERE request_id = ?",
            (request_id,),
            CallAttempt.from_row,
        )

    def get_call_attempt_by_sid(self, call_sid: str) -> CallAttempt | None:
        return self._one(
            "SELECT * FROM call_attempts WHERE call_sid = ?",
            (call_sid,),
            CallAttempt.from_row,
        )

    def is_phone_dnd(self, phone: str) -> bool:
        return self._scalar("SELECT 1 FROM dnd_phones WHERE phone = ?", (phone,)) == 1

    def get_dnd_phone(self, phone: str) -> DndPhone | None:
        return self._one(
            "SELECT * FROM dnd_phones WHERE phone = ?", (phone,), DndPhone.from_row
        )

    def get_active_tenant_call_count(self, tenant_id: str) -> int:
        return self._scalar(
            "SELECT COUNT(*) FROM leads WHERE tenant_id = ? AND state IN ('RESERVING', 'ACTIVE')",
            (tenant_id,),
        )

    def is_phone_in_use(self, phone: str) -> bool:
        return (
            self._scalar(
                "SELECT 1 FROM leads WHERE phone = ? AND state IN ('RESERVING', 'ACTIVE')",
                (phone,),
            )
            == 1
        )

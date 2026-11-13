from __future__ import annotations

import csv
import json
import os
import tempfile
from pathlib import Path

from dispatcher.db import Database


def _resolve_campaigns_path(campaigns_json_path: str | Path) -> Path:
    path = Path(campaigns_json_path)
    if not path.is_absolute() and not path.exists():
        candidate = Path("assignment-b-workflow") / path
        if candidate.exists():
            path = candidate
        else:
            module_relative = Path(__file__).resolve().parent.parent / path
            if module_relative.exists():
                path = module_relative
    return path.resolve()


def load_fixtures(db: Database, campaigns_json_path: str | Path) -> dict[str, int]:
    path = _resolve_campaigns_path(campaigns_json_path)
    base_dir = path.parent
    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    with db.write_tx() as conn:
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
        for campaign in cfg.get("campaigns", []):
            campaign_count += conn.execute(
                """
                INSERT OR IGNORE INTO campaigns (
                    campaign_id, tenant_id, window_start, window_end,
                    window_days_json, max_attempts, leads_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    campaign["campaign_id"],
                    campaign["tenant_id"],
                    campaign["window"]["start"],
                    campaign["window"]["end"],
                    json.dumps(campaign["window"]["days"]),
                    campaign["max_attempts"],
                    campaign["leads_file"],
                ),
            ).rowcount

            leads_path = base_dir / campaign["leads_file"]
            with leads_path.open("r", encoding="utf-8") as leads_file:
                for row in csv.DictReader(leads_file):
                    lead_count += conn.execute(
                        """
                        INSERT OR IGNORE INTO leads (
                            campaign_id, lead_id, tenant_id, phone,
                            state, priority, attempts, dispatch_seq,
                            last_status, last_disposition
                        ) VALUES (?, ?, ?, ?, 'FRESH', 3, 0, 0, '', '')
                        """,
                        (
                            campaign["campaign_id"],
                            row["lead_id"],
                            campaign["tenant_id"],
                            row["phone"],
                        ),
                    ).rowcount

    return {
        "config": cfg_count,
        "tenants": tenant_count,
        "campaigns": campaign_count,
        "leads": lead_count,
    }


def export_final_state_csv(db: Database, output_path: str | Path) -> None:
    if db.count_non_final_leads() > 0:
        raise RuntimeError("cannot export final state while leads remain non-final")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(
                [
                    "campaign_id",
                    "lead_id",
                    "final_state",
                    "attempts",
                    "last_status",
                    "last_disposition",
                ]
            )
            for lead in db.get_all_leads_sorted():
                writer.writerow(
                    [
                        lead.campaign_id,
                        lead.lead_id,
                        lead.state.value.lower(),
                        lead.attempts,
                        lead.last_status,
                        lead.last_disposition,
                    ]
                )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise

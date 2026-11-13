from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from dispatcher.clock import format_ist
from dispatcher.db import Database
from dispatcher.engine import DispatcherEngine
from dispatcher.transitions import apply_call_outcome


def create_app(db: Database, engine: DispatcherEngine) -> FastAPI:
    app = FastAPI(title="Durable Campaign Dispatcher")

    @app.post("/webhooks/telephony")
    async def telephony_webhook(request: Request) -> dict[str, bool]:
        raw_body = await request.body()
        signature = request.headers.get("X-AU-Signature", "")
        config = await asyncio.to_thread(db.get_config)
        if config is None:
            raise HTTPException(status_code=401, detail="invalid_signature")

        expected = hmac.new(
            config.webhook_secret.encode("utf-8"), raw_body, hashlib.sha256
        ).hexdigest()
        if not signature or not hmac.compare_digest(expected, signature):
            raise HTTPException(status_code=401, detail="invalid_signature")

        try:
            payload: dict[str, Any] = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="invalid_json") from exc

        await asyncio.to_thread(
            apply_call_outcome,
            db,
            request_id=payload.get("request_id"),
            call_sid=payload.get("call_sid"),
            status=payload["status"],
            disposition=payload.get("disposition"),
            ended_at=payload["ended_at"],
            callback_at=payload.get("callback_at"),
        )
        engine.wakeup_event.set()
        return {"ok": True}

    @app.get("/status")
    async def status() -> dict[str, Any]:
        return {
            "sim_now": format_ist(engine.clock.now()),
            "non_final_leads": await asyncio.to_thread(db.count_non_final_leads),
            "leads_by_campaign_and_state": await asyncio.to_thread(
                db.leads_by_campaign_and_state
            ),
            "active_calls_by_tenant": await asyncio.to_thread(
                db.active_calls_by_tenant
            ),
            "dnd_phones_count": await asyncio.to_thread(db.count_dnd_phones),
        }

    return app

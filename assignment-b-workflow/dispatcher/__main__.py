from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

import uvicorn

from dispatcher.app import create_app
from dispatcher.client import CarrierClient
from dispatcher.db import Database
from dispatcher.engine import DispatcherEngine
from dispatcher.io_files import load_fixtures


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Durable campaign dispatcher")
    parser.add_argument("--fixtures", default="fixtures/campaigns.json")
    parser.add_argument("--db", default="dispatcher.db")
    parser.add_argument("--output", default="final_state.csv")
    parser.add_argument("--provider-url", default="http://127.0.0.1:8099")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8200)
    return parser


async def run(args: argparse.Namespace) -> int:
    db = Database(args.db)
    client = CarrierClient(args.provider_url)
    server: uvicorn.Server | None = None
    server_task: asyncio.Task[None] | None = None
    loop_tasks: list[asyncio.Task[None]] = []

    try:
        await asyncio.to_thread(load_fixtures, db, args.fixtures)
        clock = await client.fetch_clock()
        callback_host = "127.0.0.1" if args.host == "0.0.0.0" else args.host
        engine = DispatcherEngine(
            db=db,
            client=client,
            clock=clock,
            callback_url=f"http://{callback_host}:{args.port}/webhooks/telephony",
            output_path=Path(args.output),
        )

        server = uvicorn.Server(
            uvicorn.Config(
                create_app(db, engine),
                host=args.host,
                port=args.port,
                log_level="info",
            )
        )
        server_task = asyncio.create_task(server.serve())
        while not server.started:
            if server_task.done():
                await server_task
                raise RuntimeError("uvicorn exited before startup completed")
            await asyncio.sleep(0.01)

        await engine.startup_recover()
        loop_tasks = [
            asyncio.create_task(engine.scheduler_loop()),
            asyncio.create_task(engine.reconciler_loop()),
        ]
        await engine.stop_event.wait()
    finally:
        if server is not None:
            server.should_exit = True
        if server_task is not None:
            await asyncio.gather(server_task, return_exceptions=True)
        for task in loop_tasks:
            task.cancel()
        if loop_tasks:
            await asyncio.gather(*loop_tasks, return_exceptions=True)
        await client.aclose()
        db.close()

    return 0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(run(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

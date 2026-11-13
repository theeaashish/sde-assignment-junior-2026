# Assignment B Solution: Durable Campaign Dispatcher

The complete implementation, test suite, and chaos verification harness for **Assignment B (Durable Campaign Dispatcher)** are located in [`../assignment-b-workflow/`](../assignment-b-workflow/):

- **Dispatcher Package:** [`../assignment-b-workflow/dispatcher/`](../assignment-b-workflow/dispatcher/)
  - `models.py` — Domain models & state enums (`LeadState`, `AttemptState`)
  - `clock.py` — Simulated clock (`SimClock`) & deterministic test clock (`FakeClock`)
  - `rules.py` — Pure scheduling, dual-window intersection, retry/callback math & priority rules (`R1`–`R9`)
  - `db.py` — Crash-safe SQLite WAL store with `BEGIN IMMEDIATE` & `threading.RLock`
  - `transitions.py` — Atomic reservation, acceptance latch, outcome application & global DND cascade
  - `io_files.py` — Idempotent fixture loader & atomic `final_state.csv` exporter
  - `client.py` — Async carrier HTTP client (`201`/`200`, `503`, ambiguous `504`, lookup polling)
  - `engine.py` — Startup crash recovery, event-driven scheduler loop & lost-webhook reconciler loop
  - `app.py` & `__main__.py` — FastAPI webhook receiver (`POST /webhooks/telephony`), `/status` endpoint & CLI
- **Unit & Integration Tests (68 tests):** [`../assignment-b-workflow/tests/`](../assignment-b-workflow/tests/)
- **Automated `kill -9` Chaos Harness:** [`../assignment-b-workflow/scripts/verify_chaos.py`](../assignment-b-workflow/scripts/verify_chaos.py)
- **Design & Architecture Write-Up:** [`../assignment-b-workflow/SUBMISSION.md`](../assignment-b-workflow/SUBMISSION.md)
- **AI Usage Disclosure:** [`../assignment-b-workflow/AI_USAGE.md`](../assignment-b-workflow/AI_USAGE.md)

## Quick Verification Commands

```bash
cd ../assignment-b-workflow

# 1. Run all 68 unit and integration tests
pytest tests -v

# 2. Run the automated end-to-end chaos & kill -9 verification harness
python scripts/verify_chaos.py --seed 42 --speed 1200 --crashes 3
```

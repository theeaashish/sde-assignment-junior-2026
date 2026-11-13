# AI Usage Disclosure

## Tools Used
- **Google Gemini & Antigravity IDE Agent:** Used for requirements decomposition, edge-case analysis against `mock_telephony/server.py`, modular scaffolding, and test suite generation.

---

## How AI Was Used
1. **Problem Decomposition & Failure Analysis:**
   - Used AI to audit `README.md` and `mock_telephony/server.py` and catalog every failure mode injected by the mock carrier: 5% transient `503`s, 2% pre-accept `504`s, 4% post-accept `504`s (with a 2.0-second real-time sleep), 10% duplicate webhooks, and 3% silently dropped webhooks.
2. **Modular Code Scaffolding:**
   - Generated initial module skeletons and SQLite queries across `models.py`, `clock.py`, `rules.py`, `db.py`, `io_files.py`, `transitions.py`, `client.py`, `engine.py`, and `app.py`.
3. **Test Suite & Chaos Script Drafting:**
   - Used AI to scaffold the 68 `pytest` unit/integration tests (`test_db.py`, `test_rules.py`, `test_transitions.py`, `test_app_and_client.py`) and the `scripts/verify_chaos.py` crash-recovery harness.

---

## What I Designed, Audited & Corrected
1. **Rejecting Wall-Clock Queues (Celery/Redis) in Favor of Simulated-Clock Scheduling:**
   - Early exploration considered using Celery workers, which would have failed because Celery schedules countdowns against OS wall-clock time (`time.time()`) rather than the carrier's 600×–1200× accelerated simulated clock (`/v1/clock`). I chose a single-process `asyncio` loop backed by SQLite `BEGIN IMMEDIATE` transactions synchronized directly to `SimClock`.
2. **Write-Ahead Reservation (`RESERVING`) & The One-Time `accepted` Latch:**
   - In `mock_telephony/server.py`, a `"failed"` call completes in 3–10 simulated seconds (`5–16ms` real time at 600× speed), and post-accept `504`s sleep for `2.0` real seconds before returning. As a result, result webhooks frequently arrive *while the lead is still in `RESERVING`* before `POST /v1/calls` returns.
   - To prevent double-counting `attempts` or overwriting completed leads when the late HTTP response arrives, I added the `accepted` latch (`0 -> 1`) on `call_attempts` and allowed `apply_call_outcome()` to transition leads directly from `RESERVING` while turning late `201`/`504` responses into safe no-ops.
3. **Eliminating High-Speed Reconciler Races (`--speed 1200`):**
   - At 1200× speed, 15 simulated seconds is only `12.5ms` of real time. A naive background reconciler polling `RESERVING` attempts after 15 simulated seconds could query `GET /v1/calls` while `POST /v1/calls` is still in flight over localhost, receive `404`, and prematurely abort the reservation.
   - I fixed this by tracking `active_dispatch_ids` on `DispatcherEngine` so `reconciler_loop` never aborts a reservation owned by a live in-flight coroutine, while `startup_recover()` reconciles all orphaned `RESERVING` rows after a `kill -9` crash.
4. **Refactoring Monolithic Persistence into Focused Modules:**
   - When `dispatcher/db.py` grew past 750 lines, I split it into `db.py` (connection, PRAGMAs, `threading.RLock`, read queries), `transitions.py` (atomic state machine transitions and global DND cascade), and `io_files.py` (idempotent fixture ingestion and `tempfile` + `fsync` + `os.replace` CSV export).

---

## How Correctness Was Verified
- **Deterministic Unit Tests (`pytest`):** Verified all 68 tests using `FakeClock` covering window intersections, 15-second closing buffers, overnight/weekend rollovers, priority tie-breaking (`WAITING_CALLBACK > WAITING_RETRY > FRESH`), duplicate webhook idempotency, and `completed > exhausted > dnd_blocked` precedence (`R9`).
- **Multi-Crash Chaos Testing (`scripts/verify_chaos.py`):** Executed end-to-end runs at `--speed 1200 --failure-scale 1.0` across multiple seeds (`7`, `42`, `99`) with repeated `kill -9` process kills mid-run, confirming `violation_count == 0`, `calls_active == 0`, and 200/200 exact row matches against `GET /admin/leads`.

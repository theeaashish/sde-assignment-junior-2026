"""
Automated Chaos & Crash Verification Harness for Assignment B.

1. Starts mock_telephony with configurable --speed, --seed, and --failure-scale.
2. Starts the dispatcher against a temporary SQLite WAL database.
3. Injects multiple rounds of SIGKILL (kill -9) while calls are actively in flight.
4. Restarts the dispatcher against the surviving SQLite WAL database until completion.
5. Asserts 0 violations, 0 active calls, and a 100% 1:1 diff match between
   final_state.csv and GET /admin/leads.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
WORKFLOW_DIR = HERE.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automated SIGKILL chaos verification for Assignment B"
    )
    parser.add_argument("--seed", type=int, default=42, help="Mock carrier seed")
    parser.add_argument(
        "--speed", type=float, default=1200.0, help="Simulated clock speed multiplier"
    )
    parser.add_argument(
        "--failure-scale",
        type=float,
        default=1.0,
        help="Carrier failure injection scale (1.0 = full chaos)",
    )
    parser.add_argument(
        "--crashes",
        type=int,
        default=3,
        help="Number of SIGKILL (kill -9) crash rounds to inject",
    )
    parser.add_argument("--mock-port", type=int, default=8099)
    parser.add_argument("--dispatcher-port", type=int, default=8201)
    return parser.parse_args()


def get_python_executable() -> str:
    # Check if current python has required dependencies
    try:
        import fastapi  # noqa: F401
        return sys.executable
    except ImportError:
        pass

    # Search for .venv in root or workflow directory
    candidates = [
        WORKFLOW_DIR.parent / ".venv" / "bin" / "python",
        WORKFLOW_DIR / ".venv" / "bin" / "python",
        Path.cwd() / ".venv" / "bin" / "python",
        Path.cwd().parent / ".venv" / "bin" / "python",
    ]
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)

    return sys.executable


def clean_temp_files(db_path: Path, output_path: Path) -> None:
    for base in (db_path, output_path):
        for candidate in base.parent.glob(f"{base.name}*"):
            candidate.unlink(missing_ok=True)
        for hidden in base.parent.glob(f".{base.name}*"):
            hidden.unlink(missing_ok=True)


def wait_for_mock_ready(mock_proc: subprocess.Popen[bytes], provider_url: str, timeout_sec: float = 10.0) -> None:
    deadline = time.time() + timeout_sec
    url = f"{provider_url}/v1/clock"
    while time.time() < deadline:
        if mock_proc.poll() is not None:
            err = (
                mock_proc.stderr.read().decode("utf-8", errors="replace")
                if mock_proc.stderr
                else ""
            )
            raise RuntimeError(
                f"Mock telephony server exited prematurely with code {mock_proc.returncode}:\n{err}\n"
                f"Did you install dependencies with 'pip install -r requirements.txt' or activate the virtualenv?"
            )
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.1)
    raise TimeoutError(
        f"Mock telephony server failed to respond on {url} within {timeout_sec}s"
    )


def http_get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=5.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    args = parse_args()
    provider_url = f"http://127.0.0.1:{args.mock_port}"
    tmp_dir = Path(tempfile.gettempdir())
    db_path = tmp_dir / "chaos_test.db"
    output_path = tmp_dir / "chaos_final_state.csv"

    print("=" * 70)
    print("ASSIGNMENT B: AUTOMATED CHAOS & CRASH VERIFICATION HARNESS")
    print("=" * 70)

    clean_temp_files(db_path, output_path)

    py_bin = get_python_executable()
    env = dict(os.environ)
    env["PYTHONPATH"] = str(WORKFLOW_DIR)

    mock_cmd = [
        py_bin,
        "-m",
        "mock_telephony",
        "--speed",
        str(args.speed),
        "--seed",
        str(args.seed),
        "--failure-scale",
        str(args.failure_scale),
        "--port",
        str(args.mock_port),
    ]

    dispatcher_cmd = [
        py_bin,
        "-m",
        "dispatcher",
        "--db",
        str(db_path),
        "--output",
        str(output_path),
        "--provider-url",
        provider_url,
        "--port",
        str(args.dispatcher_port),
    ]

    mock_proc: subprocess.Popen[bytes] | None = None
    disp_proc: subprocess.Popen[bytes] | None = None

    try:
        print(
            f"[1/5] Starting mock telephony server on port {args.mock_port} "
            f"(speed={args.speed}x, seed={args.seed}, failure={args.failure_scale})..."
        )
        mock_proc = subprocess.Popen(
            mock_cmd,
            cwd=str(WORKFLOW_DIR),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        wait_for_mock_ready(mock_proc, provider_url)
        print("      Mock telephony server is healthy and responding.")

        print(
            f"[2/5] Commencing {args.crashes} rounds of hard kill -9 crash injection..."
        )
        for round_idx in range(1, args.crashes + 1):
            print(
                f"      [Round {round_idx}/{args.crashes}] Spawning dispatcher (PID will be killed mid-flight)..."
            )
            disp_proc = subprocess.Popen(
                dispatcher_cmd,
                cwd=str(WORKFLOW_DIR),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            time.sleep(1.5)

            if disp_proc.poll() is None:
                pid = disp_proc.pid
                print(
                    f"      [Round {round_idx}/{args.crashes}] Injecting SIGKILL (kill -9) to PID {pid}!"
                )
                os.kill(pid, signal.SIGKILL)
                disp_proc.wait()
                print(
                    f"      [Round {round_idx}/{args.crashes}] Process killed. SQLite WAL preserved on disk."
                )
            else:
                print(
                    f"      [Round {round_idx}/{args.crashes}] Dispatcher exited early with code {disp_proc.returncode}"
                )

            time.sleep(0.3)

        print(
            "[3/5] Starting final dispatcher instance to resume from surviving SQLite WAL..."
        )
        disp_proc = subprocess.Popen(
            dispatcher_cmd,
            cwd=str(WORKFLOW_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        print(
            f"      Dispatcher running as PID {disp_proc.pid}. Waiting for all 200 leads to complete..."
        )

        _stdout, stderr = disp_proc.communicate(timeout=300)
        exit_code = disp_proc.returncode
        print(f"      Dispatcher exited with code {exit_code}.")
        if exit_code != 0:
            print(
                "STDERR from dispatcher:\n",
                stderr.decode("utf-8", errors="replace"),
            )
            raise RuntimeError(
                f"Dispatcher exited with non-zero exit code {exit_code}"
            )

        print("[4/5] Evaluating provider admin report...")
        report = http_get_json(f"{provider_url}/admin/report")
        violation_count = report.get("violation_count", -1)
        calls_active = report.get("calls_active", -1)
        calls_total = report.get("calls_total", 0)
        violations_by_type = report.get("violations_by_type", {})
        max_concurrency = report.get("max_concurrency_seen", {})
        lateness = report.get("callback_lateness_minutes", {})
        webhooks = report.get("webhooks", {})

        print(f"      Total calls placed: {calls_total}")
        print(f"      Active calls:       {calls_active}")
        print(f"      Violations:         {violation_count}")
        print(f"      Max concurrency:    {max_concurrency}")
        print(f"      Callback lateness:  {lateness}")
        print(f"      Webhook stats:      {webhooks}")

        if violation_count != 0:
            print(f"[FAIL] Expected 0 violations, found {violation_count}:")
            print(json.dumps(violations_by_type, indent=2))
            return 1

        if calls_active != 0:
            print(
                f"[FAIL] Expected 0 active calls at end of run, found {calls_active}"
            )
            return 1

        print("      PASS: 0 violations, 0 stranded active calls.")

        print(
            "[5/5] Performing exact 1:1 diff between final_state.csv and /admin/leads..."
        )
        if not output_path.exists():
            print(f"[FAIL] Output CSV not found at {output_path}")
            return 1

        with output_path.open("r", encoding="utf-8") as f:
            csv_rows = list(csv.DictReader(f))

        admin_leads = http_get_json(f"{provider_url}/admin/leads")

        if len(csv_rows) != 200 or len(admin_leads) != 200:
            print(
                f"[FAIL] Lead count mismatch: CSV={len(csv_rows)}, admin={len(admin_leads)}"
            )
            return 1

        admin_map = {(item["campaign_id"], item["lead_id"]): item for item in admin_leads}
        mismatches: list[str] = []

        for row in csv_rows:
            key = (row["campaign_id"], row["lead_id"])
            exp = admin_map.get(key)
            if exp is None:
                mismatches.append(f"Unexpected lead in CSV: {key}")
                continue

            if row["final_state"] != exp["expected_state"]:
                mismatches.append(
                    f"{key} final_state: CSV={row['final_state']!r} != Expected={exp['expected_state']!r}"
                )
            if int(row["attempts"]) != exp["attempts"]:
                mismatches.append(
                    f"{key} attempts: CSV={row['attempts']!r} != Expected={exp['attempts']!r}"
                )
            if (row["last_status"] or "") != (exp["last_status"] or ""):
                mismatches.append(
                    f"{key} last_status: CSV={row['last_status']!r} != Expected={exp['last_status']!r}"
                )
            if (row["last_disposition"] or "") != (exp["last_disposition"] or ""):
                mismatches.append(
                    f"{key} last_disposition: CSV={row['last_disposition']!r} != Expected={exp['last_disposition']!r}"
                )

        if mismatches:
            print(f"[FAIL] Found {len(mismatches)} mismatches:")
            for msg in mismatches[:15]:
                print(f"      - {msg}")
            return 1

        print(
            "      PASS: All 200 leads match expected provider outcomes identically (100% diff accuracy)."
        )
        print("=" * 70)
        print(
            "SUCCESS: ALL CHAOS & CRASH VERIFICATION CHECKS PASSED WITH 0 VIOLATIONS!"
        )
        print("=" * 70)
        return 0

    finally:
        if disp_proc is not None and disp_proc.poll() is None:
            disp_proc.kill()
        if mock_proc is not None and mock_proc.poll() is None:
            mock_proc.terminate()
            try:
                mock_proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                mock_proc.kill()
        clean_temp_files(db_path, output_path)


if __name__ == "__main__":
    raise SystemExit(main())

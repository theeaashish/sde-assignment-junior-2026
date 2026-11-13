"""Root test wrapper re-exporting assignment-b-workflow tests."""

import sys
from pathlib import Path

# Ensure assignment-b-workflow is in sys.path
ASSIGNMENT_B_DIR = Path(__file__).resolve().parent.parent / "assignment-b-workflow"
if str(ASSIGNMENT_B_DIR) not in sys.path:
    sys.path.insert(0, str(ASSIGNMENT_B_DIR))

from tests.test_db import (
    test_dnd_phones,
    test_foreign_key_enforcement,
    test_in_flight_attempts,
    test_load_fixtures_fresh,
    test_restart_idempotency,
    test_sqlite_pragmas,
    test_transaction_rollback,
)

__all__ = [
    "test_dnd_phones",
    "test_foreign_key_enforcement",
    "test_in_flight_attempts",
    "test_load_fixtures_fresh",
    "test_restart_idempotency",
    "test_sqlite_pragmas",
    "test_transaction_rollback",
]

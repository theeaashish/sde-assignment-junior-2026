import sys
from pathlib import Path

# Add assignment-b-workflow to sys.path
ASSIGNMENT_B_DIR = Path(__file__).resolve().parent.parent / "assignment-b-workflow"
if str(ASSIGNMENT_B_DIR) not in sys.path:
    sys.path.insert(0, str(ASSIGNMENT_B_DIR))

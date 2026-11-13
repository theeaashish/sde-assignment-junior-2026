"""
Root wrapper pointing to assignment-b-workflow/scripts/verify_chaos.py
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
TARGET = HERE.parent / "assignment-b-workflow" / "scripts" / "verify_chaos.py"

if __name__ == "__main__":
    sys.exit(subprocess.call([sys.executable, str(TARGET)] + sys.argv[1:]))

# __init__.py
# --- Ensure repo root is in Python path ---
import sys
from pathlib import Path

# Automatically find the repo root (assumes 'core' folder is in the root)
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


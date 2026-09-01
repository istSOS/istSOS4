import sys
from pathlib import Path

API_PATH = Path(__file__).resolve().parents[2] / "api"
sys.path.insert(0, str(API_PATH))

import sys
from pathlib import Path

# Add the v2025 directory to sys.path so tests can import transform_data
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

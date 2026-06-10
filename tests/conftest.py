"""Pytest configuration and shared fixtures."""

import sys
from pathlib import Path

# Add src to path so imports work correctly
sys.path.insert(0, str(Path(__file__).parent.parent))

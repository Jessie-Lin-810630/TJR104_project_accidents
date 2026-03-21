"""
Pytest configuration file for test discovery and path setup.

This file configures pytest to correctly resolve module imports from src/ directory.
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent  # /taiwan_traffic_accidents/
sys.path.insert(0, str(project_root))

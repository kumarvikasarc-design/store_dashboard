"""
GLOBAL PROJECT ROOT LOADER

This file ensures that all modules inside the project can import:
    from utils import ...
    from pages import ...
regardless of where they run (Dash callbacks, worker threads, etc.)

It inserts the project root into sys.path once.
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

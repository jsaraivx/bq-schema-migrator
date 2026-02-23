import sys
import os

# Ensure the project root is on sys.path so that
# migrate.py and credentials.py are importable from tests/.
sys.path.insert(0, os.path.dirname(__file__))

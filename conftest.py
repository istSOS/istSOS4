"""
This file helps run the tests by providing it a path, it is 
needed because the test directory is far away from the files 
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "api")))
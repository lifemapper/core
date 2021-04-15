"""Contains functions for validating JSON
"""
import json


# .............................................................................
def validate_json(obj_generator):
    """Validates that the response is json by trying to load it."""
    try:
        json.load(obj_generator)
        return True
    except Exception:
        return False

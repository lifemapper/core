"""Contains functions for validating JSON 
"""
import json


# .............................................................................
def validate_json(obj_generator):
    """Validates that the response is json by trying to load it
    """
    try:
        test_json = json.load(obj_generator)
        return True
    except Exception as e:
        return False

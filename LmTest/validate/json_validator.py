"""This module contains functions for validating XML


Todo:
    * Validate against schema
    * Determine if file or file-like object, then validate
    * Generalize
"""
import json
import os


# .............................................................................
def validate_json_file(json_filename):
    """Validates a JSON document by seeing if it can be loaded

    Args:
        json_filename : The file path to a JSON file to validate.
    """
    msg = 'Valid'
    valid = False
    if os.path.exists(json_filename):
        try:
            with open(json_filename) as in_json:
                _ = json.load(in_json)
            valid = True
        except Exception as e:
            msg = str(e)
    else:
        msg = 'File does not exist'

    return valid, msg

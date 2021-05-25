"""This module contains functions for validating CSV

Todo:
    * Validate against schema
    * Determine if file or file-like object, then validate
    * Generalize
"""
import csv
import os


# .............................................................................
def validate_csv_file(csv_filename, read_lines=False):
    """Validates a CSV file by seeing if it can be loaded

    Args:
        csv_filename : A file path for a CSV file to validate
        read_lines : If True, attempt to read each line for validation.
    """
    msg = 'Valid'
    valid = False
    if os.path.exists(csv_filename):
        try:
            with open(csv_filename) as in_csv:
                reader = csv.reader(in_csv)
                if read_lines:
                    for _ in reader:
                        pass
            valid = True
        except Exception as e:
            msg = str(e)
    else:
        msg = 'File does not exist'

    return valid, msg

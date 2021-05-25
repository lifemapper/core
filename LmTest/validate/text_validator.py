"""This module contains functions for validating text files

Todo:
    * Validate against something
    * Determine if file or file-like object, then validate
    * Generalize
"""
import os


# .............................................................................
def validate_text_file(text_filename, read_lines=False):
    """Validates a text file by seeing if the lines can be loaded

    Args:
        text_filename : The file path of a text file to validate.
        read_lines : If true, read each line for validation.
    """
    msg = 'Valid'
    valid = False
    if os.path.exists(text_filename):
        try:
            with open(text_filename) as in_text:
                if read_lines:
                    for line in in_text:
                        _ = str(line)
            valid = True
        except Exception as e:
            msg = str(e)
    else:
        msg = 'File does not exist'

    return valid, msg

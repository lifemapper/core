"""This module contains functions for validating a zip file

Todo:
    * Determine if file or file-like object, then validate
    * Generalize
"""
import os
import zipfile


# .............................................................................
def validate_zip_file(zip_filename):
    """Validates a zip file by seeing if it can be loaded and inspected

    Args:
        zip_filename : The file path of a zip file to validate
    """
    msg = 'Valid'
    valid = False
    if os.path.exists(zip_filename):
        try:
            with zipfile.ZipFile(zip_filename) as zip_file:
                _ = zip_file.infolist()
            valid = True
        except Exception as e:
            msg = str(e)
    else:
        msg = 'File does not exist'

    return valid, msg

"""This module contains functions for validating an Lifemapper Matrix

Todo:
    * Determine if file or file-like object, then validate
    * Generalize
"""
import os

from lmpy import Matrix


# .............................................................................
def validate_lm_matrix_file(lmmatrix_filename):
    """Validates a LM Matrix by seeing if it can be loaded

    Args:
        lmmatrix_filename : A file path for a matrix file to validate.
    """
    msg = 'Valid'
    valid = False
    if os.path.exists(lmmatrix_filename):
        try:
            _ = Matrix.load(lmmatrix_filename)
            valid = True
        except Exception as e:
            msg = str(e)
    else:
        msg = 'File does not exist'

    return valid, msg

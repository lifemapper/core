"""This module contains functions for validating a tree


Todo:
    * Determine if file or file-like object, then validate
    * Generalize
"""
import os

from lmpy import TreeWrapper

from LmCommon.common.lmconstants import LMFormat


# .............................................................................
def validate_tree_file(tree_filename, schema=None):
    """Validates a tree file by seeing if it can be loaded

    Args:
        tree_filename : The file path for a tree to be validated
        schema : Optional.  The tree schema of this tree file.  Will attempt to
            determine from file extension if not provided.
    """
    msg = 'Valid'
    valid = 'False'
    if os.path.exists(tree_filename):
        # If a schema was not provided, try to get it from the file name
        if schema is None:
            _, ext = os.path.splitext(tree_filename)
            if ext == LMFormat.NEWICK.ext:
                schema = 'newick'
            elif ext == LMFormat.NEXUS.ext:
                schema = 'nexus'
            else:
                msg = 'Extension {} did not map to a known tree format'.format(
                    ext)

        if schema is not None:
            _ = TreeWrapper.get(path=tree_filename, schema=schema)
            valid = True
    else:
        msg = 'File does not exist'
    return valid, msg

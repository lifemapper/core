# coding=utf-8
"""Module containing functions to handle data verification
"""
import hashlib
import os

from LmCommon.common.lmconstants import ENCODING

# .............................................................................
def _get_hex_hash_value(dlocation=None, content=None):
    """Returns a hexidecimal representation of the sha256sum of a datafile

    Args:
        dlocation: The file on which to compute the hash
        content: The data on which to compute the hash

    Note:
        * content is checked first, and if it exists, dlocation is ignored
    """
    hexhash = None
    if content is None:
        if dlocation and os.path.exists(dlocation):
            with open(dlocation, 'rb') as in_f:
                content = in_f.read()
        else:
            print(('Failed to hash missing file {}'.format(dlocation)))
    if content is not None:
        try:
            hashval = hashlib.sha256(content)
        except:
            content  = content.encode(ENCODING)
            try:
                hashval = hashlib.sha256(content)
            except:
                raise
        hexhash = hashval.hexdigest()
    else:
        print('Failed to hash empty content')
    return hexhash


# .............................................................................
def compute_hash(dlocation=None, content=None):
    """Computes an sha256sum on data or a datafile

    Args:
        dlocation: File on which to compute hash
        content: Data or object on which to compute hash
    """
    hexhash = _get_hex_hash_value(dlocation=dlocation, content=content)
    return hexhash


# .............................................................................
def verify_hash(verify, dlocation=None, content=None):
    """Computes an sha256sum on a datafile and compares it to the one sent

    Args:
        dlocation: The file on which to compute the hash
        verify: The hash to compare results
    """
    hexhash = _get_hex_hash_value(dlocation=dlocation, content=content)
    return hexhash == verify

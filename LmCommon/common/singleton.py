"""Module containing a singleton decorator function
"""
import hashlib
from LmCommon.common.lmconstants import ENCODING


# ............................................................................
def singleton(cls):
    """Creates a singleton for each unique set of arguments for a class
    """
    instances = {}

    def get_instance(*args, **kwargs):
        name = hashlib.md5(
            ''.join([str(args), str(kwargs)]).encode(ENCODING)).hexdigest()
        if name not in instances:
            instances[name] = cls(*args, **kwargs)
        return instances[name]

    return get_instance

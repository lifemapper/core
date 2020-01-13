# coding=utf-8
"""Module containing functions to handle unicode

todo:
    * Evalute if this is even needed with Python3
"""
from LmCommon.common.lmconstants import ENCODING

# ............................................................................
def to_unicode(item, encoding=ENCODING):
    """Convert an item to unicode if it is not already

    Args:
        item (object): The object to make into unicode
        encoding (str): The encoding of the text

    Returns:
        A unicode object
    """
    return item.encode(encoding)

# ............................................................................
def from_unicode(unicode_item, encoding=ENCODING):
    """Converts a unicode string to text for display

    Args:
        unicode_item (unicode): A unicode string to convert
        encoding (str): The encoding to use

    Returns:
        An encoded string
    """
    return unicode_item.encode(encoding)

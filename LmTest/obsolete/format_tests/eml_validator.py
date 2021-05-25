"""Contains functions for validating EML
"""
from LmCommon.common.lm_xml import deserialize, fromstring


def validate_eml(obj_generator):
    """Very simple validator for EML
    """
    try:
        # Just try to validate XML
        deserialize(fromstring(obj_generator))
        return True
    except Exception:
        return False

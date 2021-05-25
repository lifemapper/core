"""Contains functions for validating KML
"""
from LmCommon.common.lm_xml import deserialize, fromstring


def validate_kml(obj_generator):
    """Very simple validator for KML
    """
    try:
        # Just try to validate XML
        deserialize(fromstring(obj_generator))
        return True
    except Exception:
        return False

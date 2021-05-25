"""This module contains functions for validating XML

Todo:
    * Validate against schema
    * Determine if file or file-like object, then validate
    * Generalize
"""
import os

from LmCommon.common.lm_xml import deserialize, fromstring


# .............................................................................
def validate_xml_file(xml_filename):
    """Validates an XML file by seeing if it can be ready by ElementTree

    Args:
        xml_filename : The file path of an xml file to validate
    """
    msg = 'Valid'
    valid = False
    if os.path.exists(xml_filename):
        try:
            with open(xml_filename) as in_xml:
                xml_str = in_xml.read()
            _ = deserialize(fromstring(xml_str))
            valid = True
        except Exception as e:
            msg = str(e)
    else:
        msg = 'File does not exist'

    return valid, msg

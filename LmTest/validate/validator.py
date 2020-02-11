"""This module contains functions for validating files

Todo:
    * Determine if file or file-like object, then validate
    * Generalize
"""
import os

from LmCommon.common.lmconstants import LMFormat
from LmTest.validate.csv_validator import validate_csv_file
from LmTest.validate.json_validator import validate_json_file
from LmTest.validate.lm_matrix_validator import validate_lm_matrix_file
from LmTest.validate.raster_validator import validate_raster_file
from LmTest.validate.text_validator import validate_text_file
from LmTest.validate.tree_validator import validate_tree_file
from LmTest.validate.vector_validator import validate_vector_file
from LmTest.validate.xml_validator import validate_xml_file
from LmTest.validate.zip_validator import validate_zip_file


# .............................................................................
def validate_file(filename):
    """Attempts to validate a file by inspecting its file extension

    Args:
        filename : The file path to a file that should be validated
    """
    _, ext = os.path.splitext(filename)

    if ext == LMFormat.ASCII.ext:
        valid, msg = validate_raster_file(
            filename, raster_format=LMFormat.ASCII)
    elif ext == LMFormat.CSV.ext:
        valid, msg = validate_csv_file(filename)
    elif ext == LMFormat.GTIFF.ext:
        valid, msg = validate_raster_file(
            filename, raster_format=LMFormat.GTIFF)
    elif ext == LMFormat.JSON.ext:
        valid, msg = validate_json_file(filename)
    elif ext == LMFormat.MATRIX.ext:
        valid, msg = validate_lm_matrix_file(filename)
    elif ext in [LMFormat.NEWICK.ext, LMFormat.NEXUS.ext]:
        valid, msg = validate_tree_file(filename)
    elif ext == LMFormat.SHAPE.ext:
        valid, msg = validate_vector_file(filename)
    elif ext in [LMFormat.TAR_GZ.ext, LMFormat.ZIP.ext]:
        valid, msg = validate_zip_file(filename)
    elif ext == LMFormat.TXT.ext:
        valid, msg = validate_text_file(filename)
    elif ext == LMFormat.XML.ext:
        valid, msg = validate_xml_file(filename)
    else:
        valid = False
        msg = 'Unknown file extension: {}'.format(ext)

    return valid, msg

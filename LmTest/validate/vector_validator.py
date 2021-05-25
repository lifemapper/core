"""This module contains functions for validating vector files

Todo:
    * Determine if file or file-like object, then validate
    * Generalize
    * Expand to more than shapefiles
"""
import os

from osgeo import ogr

from LmCommon.common.lmconstants import LMFormat


# .............................................................................
def validate_vector_file(vector_filename):
    """Validates a vector file by seeing if it can be loaded by OGR

    Args:
        vector_filename : The file path of a vector file to validate
    """
    msg = 'Valid'
    valid = False
    if os.path.exists(vector_filename):
        try:
            drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
            dataset = drv.Open(vector_filename)
            if dataset is None:
                msg = 'Could not open {}'.format(vector_filename)
            else:
                lyr = dataset.GetLayer()
                _ = lyr.GetFeatureCount()
                valid = True
        except Exception as e:
            msg = str(e)
    else:
        msg = 'File does not exist'

    return valid, msg

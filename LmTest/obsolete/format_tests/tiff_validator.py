"""Contains functions for validating raster files
"""
import os
import random

from LmServer.base.layer import Raster
from LmServer.common.lmconstants import TEMP_PATH


def validate_tiff(obj_generator):
    """Validate a Tiff
    """
    filename = os.path.join(
        TEMP_PATH, 'tmp_{}.tif'.format(random.randint(0, 10000)))
    with open(filename, 'w') as out_file:
        for line in obj_generator:
            out_file.write(line)

    ret = Raster.test_raster(filename)
    os.remove(filename)
    return ret

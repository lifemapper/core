"""Contains functions for validating raster files 
"""
import os
import random

from LmServer.base.layer2 import Raster
from LmServer.common.lmconstants import TEMP_PATH


def validate_tiff(obj_generator):
    fn = os.path.join(TEMP_PATH, 'tmp_{}.tif'.format(random.randint(10000)))
    with open(fn, 'w') as outF:
        for line in obj_generator:
            outF.write(line)

    ret = Raster.testRaster(fn)
    os.remove(fn)
    return ret

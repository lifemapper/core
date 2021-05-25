"""Contains functions for validating shapefiles
"""
import glob
import os
import random
import shutil
import zipfile

from LmServer.base.layer import Vector
from LmServer.common.lmconstants import TEMP_PATH


# .............................................................................
def validate_shapefile(obj_generator):
    """Validate a shapefile
    """
    out_dir = os.path.join(
        TEMP_PATH, 'temp_dir_{}'.format(random.randint(0, 10000)))

    os.makedirs(out_dir)

    with zipfile.ZipFile(obj_generator) as zip_f:
        zip_f.extractall(out_dir)

    filename = glob.glob(os.path.join(out_dir, '*.shp'))[0]

    ret = Vector.test_vector(filename)

    shutil.rmtree(out_dir)
    return ret

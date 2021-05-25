"""Contains functions for validating package files
"""
import os
import random
import shutil
import zipfile

from LmServer.common.lmconstants import TEMP_PATH


# .............................................................................
def validate_package(obj_generator):
    """Validate an output package
    """
    try:
        out_dir = os.path.join(
            TEMP_PATH, 'temp_dir_{}'.format(random.randint(0, 10000)))
        os.makedirs(out_dir)

        with zipfile.ZipFile(obj_generator) as zip_f:
            zip_f.extractall(out_dir)

        shutil.rmtree(out_dir)
        return True
    except Exception:
        return False

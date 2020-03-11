"""Contains functions for validating zip files
"""
import os
import random
import shutil
import zipfile

from LmServer.common.lmconstants import TEMP_PATH


# .............................................................................
def validate_zipfile(obj_generator):
    """Validate a zip file
    """
    try:
        out_dir = os.path.join(
            TEMP_PATH, 'temp_dir_{}'.format(random.randint(0, 10000)))

        os.makedirs(out_dir)

        with zipfile.ZipFile(obj_generator) as zip_file:
            zip_file.extractall(out_dir)

        shutil.rmtree(out_dir)
        return True
    except Exception:
        return False

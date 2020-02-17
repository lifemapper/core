"""Contains functions for validating zip files 
"""
import os
import random
import shutil
import zipfile

from LmServer.common.lmconstants import TEMP_PATH


def validate_zipfile(obj_generator):

    try:
        outDir = os.path.join(TEMP_PATH, 'temp_dir_{}'.format(random.randint(10000)))

        os.makedirs(outDir)

        with zipfile.ZipFile(obj_generator) as zf:
            zf.extractall(outDir)

        shutil.rmtree(outDir)
        return True
    except Exception as e:
        return False

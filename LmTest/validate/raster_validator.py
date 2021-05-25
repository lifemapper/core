"""This module contains functions for validating raster files

Todo:
    * Determine if file or file-like object, then validate
    * Generalize
"""
import os

from osgeo import gdal

from LmCommon.common.lmconstants import LMFormat


# .............................................................................
def validate_raster_file(raster_filename, raster_format=None):
    """Validates a raster file by seeing if it can be loaded by GDAL

    Args:
        raster_filename : A file path for a raster file to validate.
        raster_format : An optional LMFormat class to use for validation.  If
            not provided, the file extension will be used to determine how to
            validate the file.
    """
    msg = 'Valid'
    valid = False
    if os.path.exists(raster_filename):
        # If a raster format was not provided, try to get it from the file
        #    extension
        if raster_format is None:
            _, ext = os.path.splitext(raster_filename)
            if ext == LMFormat.ASCII.ext:
                raster_format = LMFormat.ASCII
            elif ext == LMFormat.GTIFF.ext:
                raster_format = LMFormat.GTIFF
            else:
                msg = ('Extension {} did not map to a known raster format'
                       ).format(ext)

        if raster_format is not None:
            try:
                dataset = gdal.Open(raster_filename)
                if dataset is None:
                    msg = 'Could not open {}'.format(raster_filename)
                else:
                    _lyr = dataset.GetRasterBand(1)
                    valid = True
            except Exception as e:
                msg = str(e)
        else:
            msg = 'File does not exist'

    return valid, msg

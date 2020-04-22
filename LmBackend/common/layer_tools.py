"""Module containing compute environment layer management code

Todo:
    * Add convert tool to config
    * Use verify module
    * Skip if exists
    * Alphabetize
"""
import os
import subprocess
from time import sleep

import numpy

from osgeo import gdal

from LmCommon.common.lmconstants import (LMFormat, DEFAULT_NODATA, ENCODING)
from LmCompute.common.lmconstants import (
    CONVERT_JAVA_CMD, CONVERT_TOOL, ME_CMD)

WAIT_SECONDS = 30


# .............................................................................
def convert_and_modify_ascii_to_tiff(asc_file_name, tiff_file_name, scale=None,
                                     multiplier=None, nodata_value=127,
                                     data_type='int'):
    """Converts an ASCII file to a GeoTiff.

    Args:
        asc_file_name (str): The file name of the existing ASCII grid to
            convert.
        tiff_file_name (str): The file path for the new tiff file.
        scale (None or tuple): If provided, must be a tuple of the scale
            minimum and maximum values.
        multiplier (numeric): If provided, multiply all data values in teh grid
            by this number.
        nodata_value: The no data value to use for the new value-adjusted
            layer.
        data_type: The data type for the resulting raster.
    """
    if data_type.lower() == 'int':
        np_type = numpy.int8
        gdal_type = gdal.GDT_Byte
    else:
        raise Exception('Unknown data type')

    src_ds = gdal.Open(asc_file_name)
    band = src_ds.GetRasterBand(1)
    band.GetStatistics(0, 1)

    in_nodata_value = band.GetNoDataValue()

    data = src_ds.ReadAsArray(0, 0, src_ds.RasterXSize, src_ds.RasterYSize)

    # If scale
    if scale is not None:
        scale_min, scale_max = scale
        lyr_min = band.GetMinimum()
        lyr_max = band.GetMaximum()

        def scale_func(cell_value):
            """Function to scale layer values.
            """
            if cell_value == in_nodata_value:
                return nodata_value

            return (scale_max - scale_min) * (
                (cell_value - lyr_min) / (lyr_max - lyr_min)) + scale_min

        data = numpy.vectorize(scale_func)(data)

    # If multiply
    elif multiplier is not None:

        def multiply_func(cell_value):
            """Function to multiply layer values.
            """
            if cell_value == in_nodata_value:
                return nodata_value
            return multiplier * cell_value

        data = numpy.vectorize(multiply_func)(data)

    data = data.astype(np_type)
    driver = gdal.GetDriverByName('GTiff')
    dst_ds = driver.Create(
        tiff_file_name, src_ds.RasterXSize, src_ds.RasterYSize, 1, gdal_type)

    dst_ds.GetRasterBand(1).WriteArray(data)
    dst_ds.GetRasterBand(1).SetNoDataValue(nodata_value)
    dst_ds.GetRasterBand(1).ComputeStatistics(True)

    dst_ds.SetProjection(src_ds.GetProjection())
    dst_ds.SetGeoTransform(src_ds.GetGeoTransform())

    driver = None
    dst_ds = None
    src_ds = None


# .............................................................................
def convert_ascii_to_mxe(lyr_dir):
    """Converts a directory of ASCII files to MXEs.

    lyr_dir: A directory containing ASCII grids that should be converted.
    """
    # Run Maxent converter
    me_convert_cmd = '{0} {1} {2} -t {3} asc {3} mxe'.format(
        CONVERT_JAVA_CMD, ME_CMD, CONVERT_TOOL, lyr_dir)
    convert_proc = subprocess.Popen(me_convert_cmd, shell=True)

    while convert_proc.poll() is None:
        print('Waiting for layer conversion (asc to mxe) to finish...')
        sleep(WAIT_SECONDS)


# .............................................................................
def convert_layers_in_dir(layer_dir):
    """Converts all layers in directory from tiffs to asciis and mxes

    Args:
        layer_dir (str):The directory to traverse through looking for layers to
            convert
    """
    mxe_dirs = set([])
    for my_dir, _, files in os.walk(layer_dir):
        for file_name in files:
            tiff_file_name = os.path.join(my_dir, file_name)
            basename, ext = os.path.splitext(tiff_file_name)
            if ext.lower() == LMFormat.GTIFF.ext:
                ascii_file_name = '{}{}'.format(basename, LMFormat.ASCII.ext)
                mxe_file_name = '{}{}'.format(basename, LMFormat.MXE.ext)

                if not os.path.exists(ascii_file_name):
                    print('Converting: {}'.format(tiff_file_name))
                    convert_tiff_to_ascii(tiff_file_name, ascii_file_name)

                if not os.path.exists(mxe_file_name):
                    mxe_dirs.add(my_dir)

    for lyr_dir in mxe_dirs:
        print('Converting ASCIIs in {} to MXEs'.format(lyr_dir))
        convert_ascii_to_mxe(lyr_dir)


# .............................................................................
def convert_tiff_to_ascii(tiff_file_name, asc_file_name, header_precision=6):
    """Converts an existing GeoTIFF file into an ASCII grid.

    Args:
        tiff_file_name (str): The path to an existing GeoTIFF file
        asc_file_name (str): The output path for the new ASCII grid
        header_precision (int): The number of decimal places to keep in the
            ASCII grid headers.  Setting to None skips.

    Note:
        Headers must match exactly for Maxent so truncating them eliminates
            floating point differences

    Todo:
        Evaluate if this can all be done with GDAL.
    """
    # Use GDAL to generate ASCII Grid
    drv = gdal.GetDriverByName('AAIGrid')
    ds_in = gdal.Open(tiff_file_name)
    # Get header information from tiff file
    left_x, x_res, _, ul_y, _, y_res = ds_in.GetGeoTransform()

    left_y = ul_y + (ds_in.RasterYSize * y_res)
    cols = ds_in.RasterXSize
    rows = ds_in.RasterYSize
    # Force a NODATA value if missing from TIFF before copying to ASCII
    nodata = ds_in.GetRasterBand(1).GetNoDataValue()
    if nodata is None:
        ds_in.GetRasterBand(1).SetNoDataValue(DEFAULT_NODATA)
        nodata = DEFAULT_NODATA
    # If header precision is not None, round vlaues
    if header_precision is not None:
        left_x = round(left_x, header_precision)
        left_y = round(left_y, header_precision)
        x_res = round(x_res, header_precision)

    options = ['FORCE_CELLSIZE=True']
    drv.CreateCopy(asc_file_name, ds_in, 0, options)
    ds_in = None

    # Rewrite  ASCII header with tiff info
    output = []
    output.append('ncols    {}\n'.format(cols))
    output.append('nrows    {}\n'.format(rows))
    output.append('xllcorner    {}\n'.format(left_x))
    output.append('yllcorner    {}\n'.format(left_y))
    output.append('cellsize    {}\n'.format(x_res))
    output.append('NODATA_value    {}\n'.format(int(nodata)))
    past_header = False
    with open(asc_file_name, 'r', encoding=ENCODING) as asc_in:
        for line in asc_in:
            low_line = line.lower()
            if not past_header and any([
                    low_line.startswith(test_str) for test_str in [
                        'ncols', 'nrows', 'xllcorner', 'yllcorner', 'cellsize',
                        'dx', 'dy', 'nodata_value']]):
                pass
            else:
                past_header = True
                output.append(line)
    # Rewrite ASCII Grid
    with open(asc_file_name, 'w', encoding=ENCODING) as asc_out:
        for line in output:
            asc_out.write(line)


# .............................................................................
def process_layers_json(layer_json, sym_dir=None):
    """Process layer JSON and return file names.

    Args:
        layer_json (json): A JSON object with an entry for layers (list) and a
            mask.  Each layer should be an object with an identifier and / or
            url.
        sym_dir: If provided, symbolically link the layers in this directory.

    Note:
        Assumes that layer_json is an object with layers and mask
    """
    layers = []
    for lyr_obj in layer_json['layer']:
        layers.append(lyr_obj['path'])

    lyr_ext = os.path.splitext(layers[0])[1]

    if sym_dir is not None:
        new_layers = []
        for i, layer_i in enumerate(layers):
            new_file_name = os.path.join(
                sym_dir, "layer{}{}".format(i, lyr_ext))
            if not os.path.exists(new_file_name):
                os.symlink(layer_i, new_file_name)
            new_layers.append(new_file_name)
        return new_layers

    return layers

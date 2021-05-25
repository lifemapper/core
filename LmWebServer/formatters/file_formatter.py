"""Module containing File Formatter class and helping functions
"""
from io import BytesIO, StringIO
import os
import zipfile

import cherrypy
from lmpy import Matrix

from LmCommon.common.lmconstants import LMFormat
from LmServer.base.layer import Raster, Vector
from LmServer.legion.lm_matrix import LMMatrix


# .............................................................................
def file_formatter(filename, mode='r', stream=False, content_type=None):
    """Returns the contents of the file(s) either as a string or generator

    Args:
        filename: The file name to return or a list of files
        mode: The mode used to read the file(s)
        stream: If true, return a generator for streaming output, else return
            file contents
    """
    # Check to see if filename is a non-string iterable
    if isinstance(filename, str):
        content_flo = open(filename, mode=mode)
        ret_file_name = os.path.basename(filename)
    else:
        # Zip together before returning
        content_flo = BytesIO()
        with zipfile.ZipFile(
                content_flo, mode='w', compression=zipfile.ZIP_DEFLATED,
                allowZip64=True) as zip_f:
            for file_name in filename:
                zip_f.write(file_name, os.path.split(file_name)[1])

        ret_file_name = '{}.zip'.format(
            os.path.splitext(os.path.basename(filename[0]))[0])
        content_flo.seek(0)

    cherrypy.response.headers[
        'Content-Disposition'] = 'attachment; filename="{}"'.format(
            ret_file_name)
    if content_type is not None:
        cherrypy.response.headers['Content-Type'] = content_type

    # If we should stream the output, use the CherryPy file generator
    if stream:
        return cherrypy.lib.file_generator(content_flo)

    # Just return the content, but close the file
    cnt = content_flo.read()
    content_flo.close()
    return cnt


# .............................................................................
def csv_object_formatter(obj):
    """Attempt to return CSV for an object
    """
    if isinstance(obj, LMMatrix):
        cherrypy.response.headers[
            'Content-Disposition'] = 'attachment; filename="mtx{}.csv"'.format(
                obj.get_id())
        mtx = Matrix.load(obj.get_dlocation())
        out_stream = StringIO()
        mtx.write_csv(out_stream)
        out_stream.seek(0)
        cnt = out_stream.read()
        out_stream.close()
        return cnt

    raise Exception("Only matrix objects can be formatted as csv at this time")


# .............................................................................
def gtiff_object_formatter(obj):
    """Attempt to return a geotiff for an object if it is a raster
    """
    if isinstance(obj, Raster):
        return file_formatter(
            obj.get_dlocation(), mode='rb',
            content_type=LMFormat.GTIFF.get_mime_type())

    raise Exception("Only raster files have GeoTiff interface")


# .............................................................................
def shapefile_object_formatter(obj):
    """Attempt to format a shapefile as a file
    """
    if isinstance(obj, Vector):
        return file_formatter(
            obj.get_shapefiles(), content_type=LMFormat.SHAPE.get_mime_type())

    raise Exception("Only vector files have Shapefile interface")

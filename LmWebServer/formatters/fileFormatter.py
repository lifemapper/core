"""Module containing File Formatter class and helping functions
"""
import cherrypy
import os
from io import StringIO
import zipfile

from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.matrix import Matrix
from LmServer.base.layer2 import Raster, Vector
from LmServer.legion.lmmatrix import LMMatrix

# .............................................................................
def file_formatter(filename, readMode='r', stream=False, contentType=None):
   """
   @summary: Returns the contents of the file(s) either as a single string or
                with a generator
   @param filename: The file name to return or a list of files
   @param mode: The mode used to read the file(s)
   @param stream: If true, return a generator for streaming output, else return
                     file contents
   """
   # Check to see if filename is a non-string iterable
   if hasattr(filename, '__iter__'):
      # Zip together before returning
      contentFLO = StringIO()
      with zipfile.ZipFile(contentFLO, mode='w', 
                  compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zipF:
         for fn in filename:
            zipF.write(fn, os.path.split(fn)[1])
      
      retFilename = '{}.zip'.format(
         os.path.splitext(os.path.basename(filename[0]))[0])
      
      contentFLO.seek(0)
   else:
      contentFLO = open(filename, mode=readMode)
      retFilename = os.path.basename(filename)

   cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="{}"'.format(retFilename)
   if contentType is not None:
      cherrypy.response.headers['Content-Type'] = contentType

   # If we should stream the output, use the CherryPy file generator      
   if stream:
      return cherrypy.lib.file_generator(contentFLO)
   else:
      # Just return the content, but close the file
      cnt = contentFLO.read()
      contentFLO.close()
      return cnt

# .............................................................................
def csvObjectFormatter(obj):
   """
   @summary: Attempt to return CSV for an object
   """
   if isinstance(obj, LMMatrix):
      cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="mtx{}.csv"'.format(obj.getId())
      m = Matrix.load(obj.getDLocation())
      outStream = StringIO()
      m.writeCSV(outStream)
      outStream.seek(0)
      cnt = outStream.read()
      outStream.close()
      return cnt
   else:
      raise Exception("Only matrix objects can be formatted as csv at this time")
   
# .............................................................................
def gtiffObjectFormatter(obj):
   """
   @summary: Attempt to return a geotiff for an object if it is a raster
   """
   if isinstance(obj, Raster):
      return file_formatter(obj.getDLocation(), readMode='rb', 
                            contentType=LMFormat.GTIFF.getMimeType())
   else:
      raise Exception("Only raster files have GeoTiff interface")

# .............................................................................
def shapefileObjectFormatter(obj):
   """
   """
   if isinstance(obj, Vector):
      return file_formatter(obj.getShapefiles(), contentType=LMFormat.SHAPE.getMimeType())
   else:
      raise Exception("Only vector files have Shapefile interface")

# .............................................................................
# NOTE: This was only commented out, and not removed, in case it comes up again
#def fixFilename(fn, escapeChar='_'):
#   """
#   @summary: This function will take a filename that may include invalid 
#                characters and will escape problems
#   @param fn: The unescaped and potentially invalid file name
#   """
#   ESCAPES = [',', '}', '{', '|']
#   
#   retString = to_unicode(fn)
#   for c in ESCAPES:
#      retString = retString.replace(to_unicode(c), to_unicode(escapeChar))
#   return from_unicode(retString)
   
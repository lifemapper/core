"""
@summary: Module containing GeoTiff Formatter class and helping functions
@author: CJ Grady
@version: 1.0
@status: beta
@note: Part of the Factory pattern
@see: Formatter
@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
from cherrypy.lib import file_generator
from cStringIO import StringIO
import os
from tempfile import NamedTemporaryFile

from LmCommon.common.lmconstants import HTTPStatus
from LmServer.base.layer import Raster
from LmServer.base.lmobj import LmHTTPError

from LmWebServer.formatters.formatter import Formatter, FormatterResponse

# .............................................................................
class TiffFormatter(Formatter):
   """
   @summary: Formatter class for File output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      # Check the data format of the object
      if isinstance(self.obj, Raster):
         fn = "%s.tif" % self.obj.name
         contentType = "image/tiff"
         headers = {"Content-Disposition" : 'attachment; filename="%s"' % fn}
   
         if self.obj.dataFormat == 'GTiff':
            content = file_generator(open(self.obj.getDLocation(), 'rb'), chunkSize=32768)
         else:
            #if os.stat(self.obj.getDLocation()).st_size > 1000000000:
            #   raise LmHTTPError(503, msg="File is too large to convert")
            f = NamedTemporaryFile(suffix='.tif', delete=False)
            tmpFn = f.name
            f.close()
            
            self.obj.copyData(self.obj.getDLocation(),
                              targetDataLocation=tmpFn, format='GTiff')
            
            contentStr = StringIO()
            contentStr.writelines(open(tmpFn).readlines())
            contentStr.seek(0)
            #content = open(tmpFn).read()
            content = file_generator(contentStr, chunkSize=32768)

            os.remove(tmpFn)
      else:
         raise LmHTTPError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, 
                     "Can't return file for %s type" % str(self.obj.__class__))

      return FormatterResponse(content, contentType=contentType, filename=fn,
                               otherHeaders=headers)


"""
@summary: Module containing Shapefile Formatter class and helping functions
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
import glob
import os
from StringIO import StringIO
import uuid
import zipfile

from LmServer.base.layer import Vector
from LmServer.base.lmobj import LMError
from LmServer.rad.pamvim import PamSum
from LmServer.rad.radbucket import RADBucket
from LmServer.rad.shapegrid import ShapeGrid
from LmServer.sdm.occlayer import OccurrenceLayer

from LmWebServer.formatters.formatter import Formatter, FormatterResponse
from LmCommon.common.lmconstants import SHAPEFILE_EXTENSIONS

# .............................................................................
class ShapefileFormatter(Formatter):
   """
   @summary: Formatter class for zipped shapefile output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      if isinstance(self.obj, OccurrenceLayer):
         resp = self.obj.zipShapefiles(baseName="%s-%s" % (self.obj.displayName.replace(',', '_').replace(' ', '_'), self.obj.id))
      elif isinstance(self.obj, (ShapeGrid, Vector)):
         resp = self.obj.zipShapefiles(baseName="%s-%s" % (self.obj.name.replace(',', '_').replace(' ', '_'), self.obj.id))
      elif isinstance(self.obj, RADBucket):
         #exts = ["shp", "prj", "shx", "dbf", "qix"]
         #self.obj.populatePAMFromFile()
         #fn = "/tmp/bucket%s" % str(self.obj.id)
         fn = "/tmp/%s" % uuid.uuid4()
         success = self.obj.createLayerShapefileFromMatrix("%s.shp" % fn, isPresenceAbsence=True)
         
         tgStream = StringIO()
         zip = zipfile.ZipFile(tgStream, mode="w", 
                                  compression=zipfile.ZIP_DEFLATED, 
                                  allowZip64=True)
         
         for fName in glob.iglob("%s*" % fn):
            base, ext = os.path.splitext(fName)
            if ext in SHAPEFILE_EXTENSIONS:
               zip.write(fName, os.path.split(fName)[1])
               os.remove(fName)
         zip.close()
         tgStream.seek(0)
         resp = ''.join(tgStream.readlines())
         tgStream.close()
      elif isinstance(self.obj, PamSum):
         #exts = ["shp", "prj", "shx", "dbf", "qix"]
         from LmServer.common.log import LmPublicLogger
         from LmServer.db.peruser import Peruser
         peruser = Peruser(LmPublicLogger())
         peruser.openConnections()
         bucket = peruser.getRADBucket(self.obj.user, bucketId=self.obj._bucketId)
         peruser.closeConnections()
         fn = "/tmp/%s" % uuid.uuid4()
         success = self.obj.createLayerShapefileFromSum(bucket, "%s.shp" % fn)
         
         tgStream = StringIO()
         zip = zipfile.ZipFile(tgStream, mode="w", 
                                  compression=zipfile.ZIP_DEFLATED, 
                                  allowZip64=True)
         
         for fName in glob.iglob("%s*" % fn):
            base, ext = os.path.splitext(fName)
            if ext in SHAPEFILE_EXTENSIONS:
               zip.write(fName, os.path.split(fName)[1])
               os.remove(fName)

         zip.close()
         tgStream.seek(0)
         resp = ''.join(tgStream.readlines())
         tgStream.close()
      else:
         raise LMError("Can't return shapefile interface for %s" % self.obj.__class__)
      
      ct = "application/x-gzip"
      fn = "response.zip"
      headers = {
                 'Content-Disposition' : 'attachment; filename="%s"' % fn,
                 'Content-Encoding' : 'zip',
                 'Vary' : "*"
                }
      return FormatterResponse(resp, contentType=ct, filename=fn, otherHeaders=headers)

"""
@summary: Module containing File Formatter class and helping functions
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
import pickle
import os.path
from types import FileType, StringType

from LmCommon.common.lmconstants import HTTPStatus, OutputFormat
from LmCommon.common.unicode import fromUnicode, toUnicode

from LmServer.base.layer import Raster, Vector
from LmServer.base.lmobj import LMError, LmHTTPError
from LmServer.rad.radbucket import RADBucket
from LmServer.rad.radexperiment import RADExperiment
from LmServer.sdm.sdmexperiment import SDMExperiment
from LmServer.sdm.sdmmodel import SDMModel
from LmServer.sdm.sdmprojection import SDMProjection

from LmWebServer.formatters.formatter import Formatter, FormatterResponse

# Add more as they come up
EXTENSIONS = {
              ".asc" : "text/plain",
              ".csv" : "text/csv",
              ".gif" : "image/gif",
              ".jpg" : "image/jpeg",
              ".pckl" : "application/octet-stream",
              ".png" : "image/png",
              ".pdf" : "application/pdf",
              ".tar.gz" : "application/x-gzip",
              ".tgz" : "application/x-gzip",
              ".tif" : "image/tiff",
              ".txt" : "text/plain",
              ".xml" : "application/xml",
              ".zip" : "application/zip"
             }
# .............................................................................
class FileFormatter(Formatter):
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
      headers = {}
      if isinstance(self.obj, RADBucket):
         basename = os.path.basename(self.obj.indicesDLocation)
         contentType = EXTENSIONS['.pckl']
         content = pickle.dumps(self.obj.getAllPresenceIndices())
         #content = open(self.obj.indicesDLocation, 'rb')
         headers = {"Content-Disposition" : 'attachment; filename="%s"' % basename}
      elif isinstance(self.obj, RADExperiment):
#          if not os.path.exists(self.obj._lyridxFname):
#             self.obj.writeLayerIndices()
         basename = os.path.basename(self.obj.indicesDLocation)
         contentType = EXTENSIONS['.pckl']
         content = open(self.obj.indicesDLocation, 'rb')
         headers = {"Content-Disposition" : 'attachment; filename="%s"' % basename}
      elif isinstance(self.obj, Vector):
         basename = ''.join((os.path.split(self.obj.name)[1], '.zip'))
         contentType = "application/x-gzip"
         content = self.obj.zipShapefiles()
         headers = {"Content-Disposition" : 'attachment; filename="%s"' % basename}
      elif isinstance(self.obj, (SDMExperiment, SDMModel)):
         if self.parameters["lmformat"] == "model":
            if isinstance(self.obj, SDMExperiment):
               ruleset = self.obj.model.ruleset
            else:
               ruleset = self.obj.ruleset
               
            # Look for replicates scenario
            if not os.path.exists(ruleset):
               if self.obj.model.algorithmCode == 'ATT_MAXENT' and \
                     int(self.obj.model._algorithm.parameters['replicates']) > 1:
                  # Throw a 409 error
                  raise LmHTTPError(HTTPStatus.CONFLICT, 
                              msg="Multiple lambda files for Maxent models with replicates parameter > 1.  Use the package interface.")
               
            basename = os.path.split(ruleset)[1]
            content = open(ruleset)
            
            if ruleset.endswith('xml'):
               contentType = "application/xml"
            else:
               contentType = "text/plain"
         elif self.parameters["lmformat"] == "package":
            if isinstance(self.obj, SDMExperiment):
               packageFn = self.obj.model.getModelStatisticsFilename()
            else:
               packageFn = self.obj.getModelStatisticsFilename()
            basename = os.path.split(packageFn)[1]
            content = open(packageFn)
            contentType = "application/x-gzip"
            headers = {
                 'Content-Disposition' : 'attachment; filename="%s"' % basename,
                 'Content-Encoding' : 'zip',
                 'Vary' : "*"
            }
      elif isinstance(self.obj, StringType):
         content = open(self.obj)
         basename = os.path.split(self.obj)[1]
         contentType = self._guessContentType(basename)
      elif isinstance(self.obj, FileType):
         content = self.obj
         basename = os.path.split(self.obj.name)[1]
         contentType = self._guessContentType(basename)
         headers = {"Content-Disposition" : 'attachment; filename="%s"' % basename}
      elif isinstance(self.obj, Raster):
         if isinstance(self.obj, SDMProjection) and self.parameters["lmformat"] == "package":
            packageFn = self.obj.getProjPackageFilename()
            basename = os.path.split(packageFn)[1]
            content = open(packageFn)
            contentType = "application/x-gzip"
            headers = {
                 'Content-Disposition' : 'attachment; filename="%s"' % basename,
                 'Content-Encoding' : 'zip',
                 'Vary' : "*"
            }
         else:
            gf = self.obj.dataFormat
            ext = OutputFormat.GTIFF
            basename = "%s%s" % (self.obj.name, ext)
            contentType = EXTENSIONS[ext]
            content = open(self.obj._dlocation)
            headers = {"Content-Disposition" : 'attachment; filename="%s"' % basename}
      else:
         raise LMError("Can't return file for %s type" % str(self.obj.__class__))
      
      return FormatterResponse(content, contentType=contentType, filename=fixFilename(basename),
                               otherHeaders=headers)

   # ..................................
   def _guessContentType(self, basename):
      """
      @summary: Attempt to guess the content type from a file extension
      @param basename: The name of the file
      @return: The (guessed) mime type of the file or empty string
      @rtype: String
      """
      ext = os.path.splitext(basename)[1]
      if EXTENSIONS.has_key(ext):
         return EXTENSIONS[ext]
      else:
         return ""

# .............................................................................
def fixFilename(fn, escapeChar='_'):
   """
   @summary: This function will take a filename that may include invalid 
                characters and will escape problems
   @param fn: The unescaped and potentially invalid file name
   """
   ESCAPES = [',', '}', '{', '|']
   
   retString = toUnicode(fn)
   for c in ESCAPES:
      retString = retString.replace(toUnicode(c), toUnicode(escapeChar))
   return fromUnicode(retString)
   
"""
@summary: Module containing OGC Formatter class and helping functions
@author: CJ Grady
@version: 1.0
@status: alpha

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
@see: LmWebServer.formatters.formatter.Formatter
"""
from LmCommon.common.lmconstants import HTTPStatus

from LmServer.base.lmobj import LMError, LmHTTPError
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import MapLogger
from LmServer.db.scribe import Scribe
from LmServer.sdm.envlayer import EnvironmentalLayer
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.sdmprojection import SDMProjection

from LmWebServer.formatters.formatter import Formatter, FormatterResponse
from LmWebServer.services.ogc.sdmMapper import MapConstructor
from LmServer.common.datalocator import EarlJr

# .............................................................................
class OgcFormatter(Formatter):
   """
   @summary: Formatter class for OGC output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      @todo: Pass in mapfile name
      """
      logger = MapLogger(isDev=True)
      ogcsvr = MapConstructor(logger)
      
      logger.debug('URL named parameters: %s' % str(self.parameters))
      self.parameters['interfaces'] = ''

      self.parameters = dict(
                       [(k.lower(), self.parameters[k]) for k in self.parameters.keys()])
      #self.parameters['layers'] = "bmng,%s" % self.obj.name
      mfn = getMapfileName(self.obj)
      logger.debug("Mapfile name: %s" % mfn)
      ogcsvr.assembleMap(self.parameters, mapFilename=mfn)
      ct, content = ogcsvr.returnResponse()
      
      try:
         name = self.obj.serviceType[:-1]
      except:
         name = "items"
      
      if not self.parameters.has_key('format'):
         self.parameters['format'] = 'application/xml'
      
      if self.parameters.has_key('format'):
         fileExt = getFileExtension(self.parameters['format'])
      else:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                           msg="Required parameter 'format' was not found")
      
      fn = "%s%s.%s" % (name, self.obj.getId(), fileExt)

      return FormatterResponse(content, contentType=ct, filename=fn)

# .............................................................................
def getMapfileName(lmo):
   """
   @summary: Attempts to get the map file name of the Lifemapper object
   @param lmo: A Lifemapper object assumed to have a map file name
   """
   mapfileName = None
   # Attempt to return it off of the object if available
   try:
      mapfileName = lmo.mapFilename
   except:
      pass
   
   # If the mapfilename was None or attribute didn't exist
   if mapfileName is None:
      earlJr = EarlJr()
      if isinstance(lmo, EnvironmentalLayer):
         scribe = Scribe(MapLogger())
         scribe.openConnections()
         scns = scribe.getScenariosForLayer(lmo.getId())
         scribe.closeConnections()
         
         if len(scns) == 0:
            raise LMError("No scenarios for layer: %s" % lmo.getId())
         else:
            scnCode = scns[0].code
         
         mapfileName = earlJr.createFilename(LMFileType.SCENARIO_MAP, scenarioCode=scnCode, usr=lmo.user)
      elif isinstance(lmo, OccurrenceLayer):
         mapfileName = earlJr.createFilename(LMFileType.SDM_MAP, occsetId=lmo.getId(), usr=lmo.user)
      elif isinstance(lmo, SDMProjection):
         mapfileName = earlJr.createFilename(LMFileType.SDM_MAP, projId=lmo.getId(), usr=lmo.user)
      else:
         raise Exception, "Unknown object type"
         

   return mapfileName

# .............................................................................
def getFileExtension(frmt):
   """
   @summary: Returns a file extension for a particular format
   """
   # Add more as they come up
   EXTENSIONS = {
                 "image/x-aaigrid" : ".asc", # ASCII grid
                 "image/gif" : ".gif", # GIF image
                 "image/jpeg" : ".jpg", # JPEG image
                 "image/png" : ".png", # PNG image
                 "image/tiff" : ".tif", # GeoTiff
                 
                }
   if EXTENSIONS.has_key(frmt):
      return EXTENSIONS[frmt]
   else:
      return None

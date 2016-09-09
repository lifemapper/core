"""
@summary: Module containing EML Formatter class and helping functions
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
from LmCommon.common.lmconstants import HTTPStatus
from LmServer.base.layer import Raster, Vector
from LmServer.base.lmobj import LmHTTPError
from LmServer.base.serviceobject import ServiceObject
from LmServer.rad.radbucket import RADBucket
from LmServer.rad.radexperiment import RADExperiment
from LmServer.sdm.envlayer import EnvironmentalLayer
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.scenario import Scenario
from LmServer.sdm.sdmexperiment import SDMExperiment
from LmServer.sdm.sdmprojection import SDMProjection

from LmWebServer.formatters.formatter import Formatter, FormatterResponse
from LmWebServer.lmEml.radBucket import buildRadBucketPackage
from LmWebServer.lmEml.radExperiment import buildRadExperimentPackage
from LmWebServer.lmEml.sdmExperiment import buildSdmExperimentPackage
from LmWebServer.lmEml.sdmOccurrenceSet import buildSdmOccurrenceSetPackage
from LmWebServer.lmEml.sdmProjection import buildSdmProjectionPackage
from LmWebServer.lmEml.sdmScenario import buildSdmScenarioPackage
from LmWebServer.lmEml.serviceRaster import buildServiceRasterPackage
from LmWebServer.lmEml.serviceVector import buildServiceVectorPackage

#from LmWebServer.templates.bucketEML import bucketEML
#from LmWebServer.templates.experimentEML import experimentEML
#from LmWebServer.templates.layerEML import layerEML
#from LmWebServer.templates.occurrenceSetEML import occurrenceSetEML
#from LmWebServer.templates.projectionEML import projectionEML
#from LmWebServer.templates.radExperimentEML import radExperimentEML
#from LmWebServer.templates.scenarioEML import scenarioEML
#from LmWebServer.templates.spatialVectorEML import spatialVectorEML

emlCoordSysNamesFromEPSG = {
                            "4326" : "GCS_WGS_1984",
                           }

# .............................................................................
class EmlFormatter(Formatter):
   """
   @summary: Formatter class for EML output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      
      if isinstance(self.obj, SDMExperiment):
         cnt = buildSdmExperimentPackage(self.obj)
      elif isinstance(self.obj, EnvironmentalLayer):
         cnt = buildServiceRasterPackage(self.obj)
      elif isinstance(self.obj, OccurrenceLayer):
         cnt = buildSdmOccurrenceSetPackage(self.obj)
      elif isinstance(self.obj, SDMProjection):
         cnt = buildSdmProjectionPackage(self.obj)
      elif isinstance(self.obj, Scenario):
         cnt = buildSdmScenarioPackage(self.obj)
      elif isinstance(self.obj, ServiceObject) and isinstance(self.obj, Raster):
         cnt = buildServiceRasterPackage(self.obj)
      elif isinstance(self.obj, ServiceObject) and isinstance(self.obj, Vector):
         cnt = buildServiceVectorPackage(self.obj)
      elif isinstance(self.obj, RADBucket):
         cnt = buildRadBucketPackage(self.obj)
      elif isinstance(self.obj, RADExperiment):
         cnt = buildRadExperimentPackage(self.obj)
      else:
         raise LmHTTPError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                          "EML not available for %s" % self.obj.__class__)

      try:
         name = self.obj.serviceType[:-1]
      except:
         name = "items"
      
      ct = "application/xml"
      fn = "%s%s.eml" % (name, self.obj.getId())
      
      return FormatterResponse(cnt, contentType=ct, filename=fn)

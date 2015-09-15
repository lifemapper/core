"""
@summary: Module containing processes for SDM services
@author: CJ Grady
@version: 2.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.common.lmconstants import JobStatus
from LmCommon.common.localconstants import WEBSERVICES_ROOT

from LmServer.common.log import LmPublicLogger
from LmServer.db.peruser import Peruser

from LmWebServer.base.servicesBaseClass import WPSService
from LmWebServer.services.common.userdata import DataPoster

# =============================================================================
class SDMExperiment(WPSService):
   """
   @summary: Submits a SDM experiment for processing
   """
   identifier = "sdmexperiment"
   title = "Lifemapper Species Distribution Model Experiment"
   version = "0.1"
   abstract = ""
   processTypes = {}
   inputParameters = [
                      {
                       "minOccurs" : "1",
                       "maxOccurs" : "1",
                       "identifier" : "algorithm",
                       "title" : "Algorithm object",
                       "reference" : "%s/schemas/serviceRequest.xsd" % WEBSERVICES_ROOT,
                       "paramType" : "algorithm",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "1",
                       "maxOccurs" : "1",
                       "identifier" : "modelScenarioId",
                       "title" : "Model Climate Scenario Id",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "modelMaskId",
                       "title" : "Model Climate Scenario Mask Layer Id",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "1",
                       "maxOccurs" : "1",
                       "identifier" : "occurrenceSetId",
                       "title" : "Occurrence Set Id",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "unbounded",
                       "identifier" : "projectionScenarioId",
                       "title" : "Projection Climate Scenario Id",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "projectionMaskId",
                       "title" : "Projection Climate Scenario Mask Layer Id",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "email",
                       "title" : "Notification Email Address",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#string",
                       "paramType" : "string",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "name",
                       "title" : "Name of the experiment",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#string",
                       "paramType" : "string",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "description",
                       "title" : "Description of the experiment",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#string",
                       "paramType" : "string",
                       "defaultValue" : None
                      }
                     ]
   outputParameters = [
                       {
                        "identifier" : "url",
                        "title" : "URL to experiment",
                        "reference" : "http://www.w3.org/TR/xmlschema-2/#string",
                        "paramType" : "string"
                       },
                      ]

   # ...................................
   def execute(self):
      obj = deserialize(fromstring(self.body))
      userId = self.user
      logger = LmPublicLogger()
      with DataPoster(userId, logger) as dp:
         exp = dp.postSDMExperimentWPS(obj)[0]
      return exp
   
   # ...................................
   def getStatus(self, id):
      peruser = Peruser(LmPublicLogger())
      peruser.openConnections()
      mdl = peruser.getModel(id)
      status = mdl.status
      peruser.closeConnections()
      
      if status == JobStatus.INITIALIZE:
         statusString = "Process Accepted"
      elif status < JobStatus.COMPLETE:
         statusString = "Running"
      elif status == JobStatus.COMPLETE:
         statusString = "Process Succeeded"
      else:
         statusString = "Process Failed"
      
      creationTime = mdl.statusModTime
      percent = 0
      outputs = {
                   "url": mdl.metadataUrl
                }
      return self._executeResponse(status, statusString, creationTime, 
                                   percentComplete=percent, outputs=outputs)
   

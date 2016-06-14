"""
@summary: Module containing the top level service group
@author: CJ Grady
@version: 1.0
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
from LmServer.db.scribe import Scribe
from LmServer.common.lmconstants import DbUser
from LmServer.common.localconstants import ARCHIVE_USER, WEBSERVICES_ROOT
from LmServer.common.log import LmPublicLogger

from LmWebServer.common.lmconstants import SERVICE_MOUNTS
from LmWebServer.base.servicesBaseClass import ServiceGroup
from LmWebServer.ogc.mapService import StaticMapService
from LmWebServer.services.rad.group import RADServiceGroup
from LmWebServer.services.sdm.group import SDMServiceGroup
# SDM services needed for older Specify installations
from LmWebServer.services.sdm.experiments import SDMExpRestService
from LmWebServer.services.sdm.layers import SDMLayersRestService
from LmWebServer.services.sdm.occurrences import SDMOccurrenceSetsRestService
from LmWebServer.services.sdm.projections import SDMProjectionsRestService
from LmWebServer.services.sdm.scenarios import SDMScenariosRestService

# .............................................................................
def getUserGroupNames(othersToAdd):
   peruser = Scribe(LmPublicLogger(), dbUser=DbUser.WebService)
   peruser.openConnections()
   users = peruser.getUsers()
   uNames = [u.userid for u in users]
   names = othersToAdd
   names.extend(uNames)
   peruser.closeConnections()
   return names

# .............................................................................
class LMServiceGroup(ServiceGroup):
   """
   @summary: LMM Service group.  Houses the Lifemapper service collection
   """
   subServices = [
                   {
                    "names" : ["maps"],
                    "constructor" : StaticMapService,
                    "idParameter" : "maps"
                   },
                   {
                    "names" : SERVICE_MOUNTS["sdm"]["experiments"],
                    "constructor" : SDMExpRestService,
                    "idParameter" : "experimentId"
                   },
                   {
                    "names" : SERVICE_MOUNTS["sdm"]["layers"],
                    "constructor" : SDMLayersRestService,
                    "idParameter" : "layerId"
                   },
                   {
                    "names" : SERVICE_MOUNTS["sdm"]["occurrences"],
                    "constructor" : SDMOccurrenceSetsRestService,
                    "idParameter" : "occurrenceSetId"
                   },
                   {
                    "names" : SERVICE_MOUNTS["sdm"]["projections"],
                    "constructor" : SDMProjectionsRestService,
                    "idParameter" : "projectionId"
                   },
                  {
                   "names" : ["rad"],
                   "constructor" : RADServiceGroup,
                   "idParameter" : "rad"
                  },
                   {
                    "names" : SERVICE_MOUNTS["sdm"]["scenarios"],
                    "constructor" : SDMScenariosRestService,
                    "idParameter" : "scenarioId"
                   },
                  {
                   "names" : getUserGroupNames(["sdm"]),
                   "constructor" : SDMServiceGroup,
                   "idParameter" : "userId"
                  }
                 ]

   def __init__(self, method, conn, userId=ARCHIVE_USER, body=None, vpath=[], 
                                         parameters={}, basePath=WEBSERVICES_ROOT,
                                         ipAddress=None):
      """
      @summary: Constructor
      @param method: The HTTP method used to call the service [string]
      @param userId: (optional) The user id associated with this request 
                        [string]
      @param body: (optional) The body (payload) of the HTTP message [string]
      @param vpath: (optional) The url path in list form ex 
                       ['services', 'lm2', 'sdm', 'experiments'] [list]
      @param parameters: (optional) URL parameters for the request 
                            (after the '?') [dictionary]
      """
      self.method = method.lower()
      self.user = userId
      self.body = body
      self.conn = conn
      # Need to ensure path and parameters are lower case
      self.vpath = [str(i).lower() for i in vpath]
      self.parameters = dict(
                       [(k.lower(), parameters[k]) for k in parameters.keys()])
      self.basePath = basePath
      self.ipAddress = ipAddress
      
   
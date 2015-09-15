"""
@summary: Module containing the RAD service group class
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
from LmWebServer.base.servicesBaseClass import ServiceGroup
from LmWebServer.common.lmconstants import SERVICE_MOUNTS
from LmWebServer.services.rad.experiments import RADExpRestService
from LmWebServer.services.rad.layers import RADLayersRestService
from LmWebServer.services.rad.shapegrids import RADShapegridsRestService

# .............................................................................
class RADServiceGroup(ServiceGroup):
   """
   @summary: RAD Service group.  Houses the RAD service collection
   """
   description = """Lifemapper Range and Diversity Services"""
   identifier = "rad"
   subServices = [
                  {
                   "names" : SERVICE_MOUNTS["rad"]["experiments"],
                   "constructor" : RADExpRestService,
                   "idParameter" : "experimentId"
                  },
#                  {
#                   "names" : ["buckets", "bkts"],
#                   "constructor" : RADBucketsRestService,
#                   "idParameter" : "bucketId"
#                  },
                  {
                   "names" : SERVICE_MOUNTS["rad"]["layers"],
                   "constructor" : RADLayersRestService,
                   "idParameter" : "layerId"
                  },
#                  {
#                   "names" : ["pamsums", "pss"],
#                   "constructor" : RADPamSumsRestService,
#                   "idParameter" : "pamsumId"
#                  },
                  {
                   "names" : SERVICE_MOUNTS["rad"]["shapegrids"],
                   "constructor" : RADShapegridsRestService,
                   "idParameter" : "shapegridId"
                  }
                 ]
   
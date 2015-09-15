"""
@summary: Module containing the SDM service group class
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
from LmWebServer.services.sdm.experiments import SDMExpRestService
from LmWebServer.services.sdm.layers import SDMLayersRestService
from LmWebServer.services.sdm.occurrences import SDMOccurrenceSetsRestService
from LmWebServer.services.sdm.projections import SDMProjectionsRestService
from LmWebServer.services.sdm.scenarios import SDMScenariosRestService
from LmWebServer.services.sdm.typecodes import SDMTypeCodesRestService

# .............................................................................
class SDMServiceGroup(ServiceGroup):
   """
   @summary: SDM Service group.  Houses the SDM service collection
   """
   description = """Lifemapper Species Distribution Modeling Services"""
   identifier = "sdm"
   subServices = [
                  {
                   "names" : SERVICE_MOUNTS['sdm']['experiments'],
                   "constructor" : SDMExpRestService,
                   "idParameter" : "experimentId"
                  },
                  {
                   "names" : SERVICE_MOUNTS['sdm']["layers"],
                   "constructor" : SDMLayersRestService,
                   "idParameter" : "layerId"
                  },
                  {
                   "names" : SERVICE_MOUNTS['sdm']["occurrences"],
                   "constructor" : SDMOccurrenceSetsRestService,
                   "idParameter" : "occurrenceSetId"
                  },
                  {
                   "names" : SERVICE_MOUNTS['sdm']["projections"],
                   "constructor" : SDMProjectionsRestService,
                   "idParameter" : "projectionId"
                  },
                  {
                   "names" : SERVICE_MOUNTS['sdm']["scenarios"],
                   "constructor" : SDMScenariosRestService,
                   "idParameter" : "scenarioId"
                  },
                  {
                   "names" : SERVICE_MOUNTS['sdm']["typecodes"],
                   "constructor" : SDMTypeCodesRestService,
                   "idParameter" : "typeCodeId"
                  }
                 ]
   
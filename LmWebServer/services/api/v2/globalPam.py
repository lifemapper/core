"""
@summary: This module provides services for query and subsetting of global PAMs
@author: CJ Grady
@version: 1.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import cherrypy

from LmServer.base.atom import Atom
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.solr import queryArchiveIndex
from LmServer.common.subset import subsetGlobalPAM

from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
class GlobalPAMService(LmService):
   """
   @summary: This class is responsible for the Global PAM services.  The 
                dispatcher is responsible for calling the correct method
   """
   # ................................
   @lmFormatter
   def GET(self, algorithmCode=None, bbox=None, gridSetId=None, 
                 modelScenarioCode=None, pointMax=None, pointMin=None, 
                 public=None, prjScenCode=None, squid=None, 
                 taxonKingdom=None, taxonPhylum=None, taxonClass=None, 
                 taxonOrder=None, taxonFamily=None, taxonGenus=None, 
                 taxonSpecies=None):
      """
      @summary: A Global PAM get request will query the global PAM and return
                   entries matching the parameters, or a count of those
      """
      return self._makeSolrQuery(algorithmCode=algorithmCode, bbox=bbox, 
                                 gridSetId=gridSetId, 
                                 modelScenarioCode=modelScenarioCode, 
                                 pointMax=pointMax, pointMin=pointMin, 
                                 public=public, 
                                 projectionScenarioCode=prjScenCode, 
                                 squid=squid, taxKingdom=taxonKingdom, 
                                 taxPhylum=taxonPhylum, taxClass=taxonClass,
                                 taxOrder=taxonOrder, taxFamily=taxonFamily,
                                 taxGenus=taxonGenus, taxSpecies=taxonSpecies)
   
   # ................................
   @lmFormatter
   def POST(self, archiveName, gridSetId, algorithmCode=None, bbox=None,  
                 modelScenarioCode=None, pointMax=None, pointMin=None, 
                 public=None, prjScenCode=None, squid=None, 
                 taxonKingdom=None, taxonPhylum=None, taxonClass=None, 
                 taxonOrder=None, taxonFamily=None, taxonGenus=None, 
                 taxonSpecies=None):
      """
      @summary: A Global PAM post request will create a subset
      """
      matches = self._makeSolrQuery(algorithmCode=algorithmCode, bbox=bbox, 
                                 gridSetId=gridSetId, 
                                 modelScenarioCode=modelScenarioCode, 
                                 pointMax=pointMax, pointMin=pointMin, 
                                 public=public, 
                                 projectionScenarioCode=prjScenCode, 
                                 squid=squid, taxKingdom=taxonKingdom, 
                                 taxPhylum=taxonPhylum, taxClass=taxonClass,
                                 taxOrder=taxonOrder, taxFamily=taxonFamily,
                                 taxGenus=taxonGenus, taxSpecies=taxonSpecies)
      
      gridset = self._subsetGlobalPAM(archiveName, matches)
      cherrypy.response.status = 202
      return Atom(gridset.getId(), gridset.name, gridset.metadataUrl, 
                  gridset.modTime, epsg=gridset.epsgcode)
   
   # ................................
   def _makeSolrQuery(self, algorithmCode=None, bbox=None, gridSetId=None, 
                            modelScenarioCode=None, pointMax=None, 
                            pointMin=None, public=None, 
                            projectionScenarioCode=None, squid=None,
                            taxKingdom=None, taxPhylum=None, taxClass=None, 
                            taxOrder=None, taxFamily=None, taxGenus=None, 
                            taxSpecies=None):
      
      if public:
         userId = PUBLIC_USER
      else:
         userId = self.getUserId()

      return queryArchiveIndex(algorithmCode=algorithmCode, bbox=bbox, 
                  gridSetId=gridSetId, modelScenarioCode=modelScenarioCode, 
                  pointMax=pointMax, pointMin=pointMin,
                  projectionScenarioCode=projectionScenarioCode, squid=squid,
                  taxKingdom=taxKingdom, taxPhylum=taxPhylum, taxClass=taxClass, 
                  taxOrder=taxOrder, taxFamily=taxFamily, taxGenus=taxGenus, 
                  taxSpecies=taxSpecies, userId=userId)
   
   # ................................
   def _subsetGlobalPAM(self, archiveName, matches):
      """
      @summary: Create a subset of a global PAM and create a new grid set
      @param archiveName: The name of this new grid set
      @param matches: Solr hits to be used for subsetting
      """
      newGridSet = subsetGlobalPAM(archiveName, matches, self.getUserId(), 
                                    scribe=self.scribe)
      return newGridSet

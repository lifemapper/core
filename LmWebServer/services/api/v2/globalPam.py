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


algorithm
scenario code
spatial
user
taxon
squids
point count
grid set?


id
url
data path
 pav
 raster

"""
from ast import literal_eval
import cherrypy
import urllib2

from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# TODO: Move these somewhere
SERVER = "localhost:8983/solr/"
COLLECTION = "lmArchive"

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathGridSetId')
class GlobalPAMService(LmService):
   """
   @summary: This class is responsible for the Global PAM services.  The 
                dispatcher is responsible for calling the correct method
   """
   # ................................
   @lmFormatter
   def GET(self, algorithmCode=None, bbox=None, gridSetId=None, 
                 modelScenarioCode=None, pointMax=None, pointMin=None, 
                 public=None, projectionScenarioCode=None, squid=None):
      """
      @summary: A Global PAM get request will query the global PAM and return
                   entries matching the parameters, or a count of those
      """
      return self._makeSolrQuery(algorithmCode=algorithmCode, bbox=bbox, 
                                 gridSetId=gridSetId, 
                                 modelScenarioCode=modelScenarioCode, 
                                 pointMax=pointMax, pointMin=pointMin, 
                                 public=public, 
                                 projectionScenarioCode=projectionScenarioCode, 
                                 squid=squid)
   
   
   # ................................
   def POST(self, pathGridSetId):
      """
      @summary: A Global PAM post request will create a subset
      """
      pass
   
   # ................................
   def _makeSolrQuery(self, algorithmCode=None, bbox=None, gridSetId=None, 
                            modelScenarioCode=None, pointMax=None, 
                            pointMin=None, public=None, 
                            projectionScenarioCode=None, squid=None):
      # Build query
      queryParts = []
      
      if algorithmCode is not None:
         queryParts.append('algorithmCode:{}'.format(algorithmCode))
         
      if gridSetId is not None:
         queryParts.append('gridSetId:{}'.format(gridSetId))
      
      if pointMax is not None or pointMin is not None:
         pmax = pointMax
         pmin = pointMin
         
         if pointMax is None:
            pmax = '*'
         
         if pointMin is None:
            pmin = '*'
            
         queryParts.append('pointCount:%5B{}%20TO%20{}%5D'.format(pmin, pmax))
      
      if public:
         userId = PUBLIC_USER
      else:
         userId = self.getUserId()
      
      queryParts.append('userId:{}'.format(userId))
      
      if modelScenarioCode is not None:
         queryParts.append('modelScenarioCode:{}'.format(modelScenarioCode))
      
      if projectionScenarioCode is not None:
         queryParts.append('sdmProjScenarioCode:{}'.format(
            projectionScenarioCode))
         
      if squid is not None:
         if isinstance(squid, list):
            if len(squid) > 1:
               squidVals = '({})'.format(' '.join(squid))
            else:
               squidVals = squid[0]
         else:
            squidVals = squid
         queryParts.append('squid:{}'.format(squidVals))
               
      if bbox is not None:
         minx, miny, maxx, maxy = bbox.split(',')
         # Create query string, have to url encode brackets [, ] -> %5B, %5D
         spatialQuery = '&fq=presence:%5B{},{}%20{},{}%5D'.format(miny, minx, 
                                                                  maxy, maxx)
      else:
         spatialQuery = ''
      
      query = 'q={}{}'.format('+AND+'.join(queryParts), spatialQuery)
      
      #curl "http://localhost:8983/solr/lmArchive/select?q=*%3A*&fq=presence:%5B-90,-180%20TO%2090,180%5D&indent=true"
      
      url = 'http://{}{}/select?{}&wt=python&indent=true'.format(SERVER, 
                                                                   COLLECTION, 
                                                                   query)
      self.log.debug(url)
      res = urllib2.urlopen(url)
      resp = res.read()
      rDict = literal_eval(resp)
      
      return rDict
   
      #hits = {}
      #for h in rDict['response']['docs']:
      #   hKey = '{displayName} - {occId}'.format(displayName=h['displayName'], 
      #                                           occId=h['occurrenceSetId'])
      #   if hits.has_key(hKey):
      #      hits[hKey] = mergeHits(hits[hKey], formatHit(h))
      #   else:
      #      hits[hKey] = formatHit(h)
      ###print hits
      ## Objectify
      #hitObjs = createHitObjects(hits)
      ##f = StyledXmlFormatter(hitObjs)
      ##return unicode(f.format())
      #return formatXml(ObjectAttributeIterator("hits", hitObjs))      
   

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
from ast import literal_eval
import cherrypy
from mx.DateTime import gmt
import numpy as np
from osgeo import ogr
import urllib2

from LmCommon.compression.binaryList import decompress
from LmCommon.common.lmconstants import MatrixType, JobStatus, LMFormat,\
   ProcessType

from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import (SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS, 
                                         SOLR_SERVER)
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix

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
                 public=None, projectionScenarioCode=None, squid=None, 
                 taxKingdom=False, taxPhylum=None, taxClass=None, 
                 taxOrder=None, taxFamily=None, taxGenus=None, taxSpecies=None):
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
                                 squid=squid, taxKingdom=taxKingdom, 
                                 taxPhylum=taxPhylum, taxClass=taxClass,
                                 taxOrder=taxOrder, taxFamily=taxFamily,
                                 taxGenus=taxGenus, taxSpecies=taxSpecies)
   
   # ................................
   def POST(self, archiveName, gridSetId, algorithmCode=None, bbox=None,  
                 modelScenarioCode=None, pointMax=None, pointMin=None, 
                 public=None, projectionScenarioCode=None, squid=None, 
                 taxKingdom=False, taxPhylum=None, taxClass=None, 
                 taxOrder=None, taxFamily=None, taxGenus=None, taxSpecies=None):
      """
      @summary: A Global PAM post request will create a subset
      """
      matches = self._makeSolrQuery(algorithmCode=algorithmCode, bbox=bbox, 
                                 gridSetId=gridSetId, 
                                 modelScenarioCode=modelScenarioCode, 
                                 pointMax=pointMax, pointMin=pointMin, 
                                 public=public, 
                                 projectionScenarioCode=projectionScenarioCode, 
                                 squid=squid, taxKingdom=taxKingdom, 
                                 taxPhylum=taxPhylum, taxClass=taxClass,
                                 taxOrder=taxOrder, taxFamily=taxFamily,
                                 taxGenus=taxGenus, taxSpecies=taxSpecies)
      
      self._subsetGlobalPAM(archiveName, matches)
      cherrypy.response.status = 202
   
   # ................................
   def _makeSolrQuery(self, algorithmCode=None, bbox=None, gridSetId=None, 
                            modelScenarioCode=None, pointMax=None, 
                            pointMin=None, public=None, 
                            projectionScenarioCode=None, squid=None,
                            taxKingdom=False, taxPhylum=None, taxClass=None, 
                            taxOrder=None, taxFamily=None, taxGenus=None, 
                            taxSpecies=None):
      # Build query
      queryParts = []
      
      if algorithmCode is not None:
         queryParts.append('{}:{}'.format(SOLR_FIELDS.ALGORITHM_CODE, 
                                          algorithmCode))
         
      if gridSetId is not None:
         queryParts.append('{}:{}'.format(SOLR_FIELDS.GRIDSET_ID, gridSetId))
      
      if pointMax is not None or pointMin is not None:
         pmax = pointMax
         pmin = pointMin
         
         if pointMax is None:
            pmax = '*'
         
         if pointMin is None:
            pmin = '*'
            
         queryParts.append('{}:%5B{}%20TO%20{}%5D'.format(
            SOLR_FIELDS.POINT_COUNT, pmin, pmax))
      
      if public:
         userId = PUBLIC_USER
      else:
         userId = self.getUserId()
      
      queryParts.append('{}:{}'.format(SOLR_FIELDS.USER_ID, userId))
      
      if modelScenarioCode is not None:
         queryParts.append('{}:{}'.format(SOLR_FIELDS.MODEL_SCENARIO_CODE,
                                          modelScenarioCode))
      
      if projectionScenarioCode is not None:
         queryParts.append('{}:{}'.format(SOLR_FIELDS.PROJ_SCENARIO_CODE,
            projectionScenarioCode))
         
      if squid is not None:
         if isinstance(squid, list):
            if len(squid) > 1:
               squidVals = '({})'.format(' '.join(squid))
            else:
               squidVals = squid[0]
         else:
            squidVals = squid
         queryParts.append('{}:{}'.format(SOLR_FIELDS.SQUID, squidVals))
      
      taxonInfo = [
         (SOLR_FIELDS.TAXON_KINGDOM, taxKingdom),
         (SOLR_FIELDS.TAXON_PHYLUM, taxPhylum),
         (SOLR_FIELDS.TAXON_CLASS, taxClass),
         (SOLR_FIELDS.TAXON_ORDER, taxOrder),
         (SOLR_FIELDS.TAXON_FAMILY, taxFamily),
         (SOLR_FIELDS.TAXON_GENUS, taxGenus),
         (SOLR_FIELDS.TAXON_SPECIES, taxSpecies),
      ]
      for fld, val in taxonInfo:
         if val is not None:
            queryParts.append('{}:{}'.format(fld, val))
               
      if bbox is not None:
         minx, miny, maxx, maxy = bbox.split(',')
         # Create query string, have to url encode brackets [, ] -> %5B, %5D
         spatialQuery = '&fq={}:%5B{},{}%20{},{}%5D'.format(
            SOLR_FIELDS.PRESENCE, miny, minx, maxy, maxx)
      else:
         spatialQuery = ''
      
      query = 'q={}{}'.format('+AND+'.join(queryParts), spatialQuery)
      
      #curl "http://localhost:8983/solr/lmArchive/select?q=*%3A*&fq=presence:%5B-90,-180%20TO%2090,180%5D&indent=true"
      
      url = '{}{}/select?{}&wt=python&indent=true'.format(
         SOLR_SERVER, SOLR_ARCHIVE_COLLECTION, query)
      self.log.debug(url)
      res = urllib2.urlopen(url)
      resp = res.read()
      rDict = literal_eval(resp)
      
      return rDict['response']['docs']
   
   # ................................
   def _subsetGlobalPAM(self, archiveName, matches):
      """
      @summary: Create a subset of a global PAM and create a new grid set
      @param archiveName: The name of this new grid set
      @param matches: Solr hits to be used for subsetting
      """
      # Get metadata
      match1 = matches[0]
      origShp = self.scribe.getShapeGrid(match1[SOLR_FIELDS.SHAPEGRID_ID])
      origNrows = origShp.featureCount
      epsgCode = match1[SOLR_FIELDS.EPSG_CODE]
      origGSId = match1[SOLR_FIELDS.GRIDSET_ID]
      origGS = self.scribe.getGridset(gridsetId=origGSId, fillMatrices=True)

      # Get the row headers
      rowHeaders = getRowHeaders(origShp.getDLocation())
   
      # TODO: Subset / copy shapegrid
      myShp = origShp
   
      gsMeta = {
         ServiceObject.META_DESCRIPTION: 'Subset of Global PAM, gridset {}'.format(
            origGSId),
         ServiceObject.META_KEYWORDS: ['subset']
      }
      
      # Create a dictionary of matches by scenario
      matchesByScen = {}
      for match in matches:
         scnId = match[SOLR_FIELDS.PROJ_SCENARIO_ID]
         if matchesByScen.has_key(scnId):
            matchesByScen[scnId].append(match)
         else:
            matchesByScen[scnId] = [match]
      
      # TODO: Copy tree?
      # Create grid set
      gs = Gridset(name=archiveName, metadata=gsMeta, shapeGrid=myShp, 
                   epsgcode=epsgCode, userId=self.getUserId(), 
                   modTime=gmt().mjd, tree=origGS.tree)
      updatedGS = self.scribe.findOrInsertGridset(gs)
      
      for scnId, scnMatches in matchesByScen.iteritems():
         
         scnCode = scnMatches[0][SOLR_FIELDS.PROJ_SCENARIO_CODE]
         dateCode = altPredCode = gcmCode = None
         
         if scnMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE):
            dateCode = scnMatches[0][SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE]
         if scnMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_GCM):
            gcmCode = scnMatches[0][SOLR_FIELDS.PROJ_SCENARIO_GCM]
         if scnMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE):
            altPredCode = scnMatches[0][SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE]
         
         scnMeta = {
            ServiceObject.META_DESCRIPTION: 'Subset of grid set {}, scenario {}'.format(
               origGSId, scnId),
            ServiceObject.META_KEYWORDS: ['subset', scnCode]
         }
         
         # Assemble full matrix
         pamData = np.zeros((origNrows, len(scnMatches)), dtype=bool)
         squids = []
         
         for i in range(len(scnMatches)):
            pamData[:,i] = decompress(scnMatches[i][SOLR_FIELDS.COMPRESSED_PAV])
            squids.append(scnMatches[i][SOLR_FIELDS.SQUID])
         
         # TODO: Subset PAM data
         
         # Create object
         pamMtx = LMMatrix(pamData, matrixType=MatrixType.PAM, gcmCode=gcmCode, 
                           altpredCode=altPredCode, dateCode=dateCode, 
                           metadata=scnMeta, userId=self.getUserId(),
                           gridset=updatedGS, status=JobStatus.COMPLETE,
                           statusModTime=gmt().mjd, headers={'0' : rowHeaders,
                                                             '1' : squids})
         # Insert it into db
         updatedPamMtx = self.scribe.findOrInsertMatrix(pamMtx)
         with open(updatedPamMtx.getDLocation(), 'w') as outF:
            pamMtx.save(outF)
      
      # GRIMs
      for grim in origGS.getGRIMs():
         # TODO: Subset grim
         newGrim = LMMatrix(None, matrixType=MatrixType.GRIM, 
                            processType=ProcessType.RAD_INTERSECT, 
                            gcmCode=grim.gcmCode, altpredCode=grim.altpredCode,
                            dateCode=grim.dateCode, metadata=grim.metadata,
                            userId=self.getUserId(), gridset=gs, 
                            status=JobStatus.INITIALIZE)
         insertedGrim = self.scribe.findOrInsertMatrix(newGrim)
         insertedGrim.updateStatus(status=JobStatus.COMPLETE, modTime=gmt().mjd)
         self.scribe.updateObject(insertedGrim)
         # Save the original grim data into the new location
         with open(insertedGrim.getDLocation(), 'w') as outF:
            grim.save(outF)
         
      # BioGeo
      for bg in origGS.getBiogeographicHypotheses():
         # TODO: Subset BioGeo
         newBG = LMMatrix(None, matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                            processType=ProcessType.ENCODE_HYPOTHESES, 
                            gcmCode=bg.gcmCode, altpredCode=bg.altpredCode,
                            dateCode=bg.dateCode, metadata=bg.metadata,
                            userId=self.getUserId(), gridset=gs, 
                            status=JobStatus.INITIALIZE)
         insertedBG = self.scribe.findOrInsertMatrix(newBG)
         insertedBG.updateStatus(status=JobStatus.COMPLETE, modTime=gmt().mjd)
         self.scribe.updateObject(insertedBG)
         # Save the original grim data into the new location
         with open(insertedBG.getDLocation(), 'w') as outF:
            bg.save(outF)
         
      

# ............................................................................
def getRowHeaders(shapefileFilename):
   """
   @summary: Get a (sorted by feature id) list of row headers for a shapefile
   @todo: Move this to LmCommon or LmBackend and use with LmCompute.  This is
             a rough copy of what is now used for rasterIntersect
   """
   ogr.RegisterAll()
   drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
   ds = drv.Open(shapefileFilename)
   lyr = ds.GetLayer(0)
   
   rowHeaders = []
   
   for j in range(lyr.GetFeatureCount()):
      curFeat = lyr.GetFeature(j)
      siteIdx = curFeat.GetFID()
      x, y = curFeat.geometry().Centroid().GetPoint_2D()
      rowHeaders.append((siteIdx, x, y))
      
   return sorted(rowHeaders)

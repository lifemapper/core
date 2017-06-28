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
from mx.DateTime import gmt
import numpy as np
from osgeo import ogr

from LmCommon.compression.binaryList import decompress
from LmCommon.common.lmconstants import MatrixType, JobStatus, LMFormat,\
   ProcessType
from LmCommon.common.matrix import Matrix

from LmDbServer.boom.radme import RADCaller

from LmServer.base.atom import Atom
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import (SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS, 
                                         SOLR_SERVER)
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix

from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

from LmServer.common.solr import queryArchiveIndex


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
      updatedGS.tree = origGS.tree
      self.log.debug("Tree for gridset {} is {}".format(updatedGS.getId(), updatedGS.tree.getId()))
      updatedGS.updateModtime()
      self.scribe.updateGridset(updatedGS)
      
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
                           gridset=updatedGS, status=JobStatus.GENERAL,
                           statusModTime=gmt().mjd, headers={'0' : rowHeaders,
                                                             '1' : squids})
         # Insert it into db
         updatedPamMtx = self.scribe.findOrInsertMatrix(pamMtx)
         updatedPamMtx.updateStatus(JobStatus.COMPLETE)
         self.scribe.updateObject(updatedPamMtx)
         self.log.debug("Dlocation for updated pam: {}".format(updatedPamMtx.getDLocation()))
         with open(updatedPamMtx.getDLocation(), 'w') as outF:
            pamMtx.save(outF)
      
      # GRIMs
      for grim in origGS.getGRIMs():
         # TODO: Subset grim
         newGrim = LMMatrix(None, matrixType=MatrixType.GRIM, 
                            processType=ProcessType.RAD_INTERSECT, 
                            gcmCode=grim.gcmCode, altpredCode=grim.altpredCode,
                            dateCode=grim.dateCode, metadata=grim.mtxMetadata,
                            userId=self.getUserId(), gridset=updatedGS, 
                            status=JobStatus.INITIALIZE)
         insertedGrim = self.scribe.findOrInsertMatrix(newGrim)
         grimMetadata = grim.mtxMetadata
         grimMetadata['keywords'].append('subset')
         grimMetadata = {
            'keywords' : ['subset'],
            'description' : 'Subset of GRIM {}'.format(grim.getId())
         }
         if grim.mtxMetadata.has_key('keywords'):
            grimMetadata['keywords'].extend(grim.mtxMetadata['keywords'])
         
         insertedGrim.updateStatus(JobStatus.COMPLETE, metadata=grimMetadata)
         self.scribe.updateObject(insertedGrim)
         # Save the original grim data into the new location
         # TODO: Add read / load method for LMMatrix
         grimMtx = Matrix.load(grim.getDLocation())
         with open(insertedGrim.getDLocation(), 'w') as outF:
            grimMtx.save(outF)
         
      # BioGeo
      for bg in origGS.getBiogeographicHypotheses():
         # TODO: Subset BioGeo
         newBG = LMMatrix(None, matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                            processType=ProcessType.ENCODE_HYPOTHESES, 
                            gcmCode=bg.gcmCode, altpredCode=bg.altpredCode,
                            dateCode=bg.dateCode, metadata=bg.mtxMetadata,
                            userId=self.getUserId(), gridset=updatedGS, 
                            status=JobStatus.INITIALIZE)
         insertedBG = self.scribe.findOrInsertMatrix(newBG)
         insertedBG.updateStatus(JobStatus.COMPLETE)
         self.scribe.updateObject(insertedBG)
         # Save the original grim data into the new location
         # TODO: Add read / load method for LMMatrix
         bgMtx = Matrix.load(bg.getDLocation())
         with open(insertedBG.getDLocation(), 'w') as outF:
            bgMtx.save(outF)
      
      doMCPA = len(origGS.getBiogeographicHypotheses()) > 0 and origGS.tree is not None
      # TODO: This should be a separate service call
      rc = RADCaller(updatedGS.getId())
      rc.analyzeGrid(doCalc=True, doMCPA=doMCPA)
      rc.close()
      return updatedGS

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

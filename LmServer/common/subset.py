"""
@summary: This module provides functionality for subsetting a grid set based on
             matches from Solr
@author: CJ Grady
@version: 1.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
from mx.DateTime import gmt
import numpy as np
from osgeo import ogr

from LmCommon.common.lmconstants import (JobStatus, LMFormat, MatrixType, 
                                         ProcessType)
from LmCommon.common.matrix import Matrix
from LmCommon.compression.binaryList import decompress

from LmDbServer.boom.radme import RADCaller

from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import SOLR_FIELDS
from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree

# .............................................................................
def subsetGlobalPAM(archiveName, matches, userId, bbox=None, scribe=None):
   """
   @summary: Create a subset of a global PAM and create a new grid set
   @param archiveName: The name of this new grid set
   @param matches: Solr hits to be used for subsetting
   """
   if scribe is None:
      log = LmPublicLogger()
      scribe = BorgScribe(log)
   else:
      log = scribe.log
      
   # Get metadata
   match1 = matches[0]
   origShp = scribe.getShapeGrid(match1[SOLR_FIELDS.SHAPEGRID_ID])
   origNrows = origShp.featureCount
   epsgCode = match1[SOLR_FIELDS.EPSG_CODE]
   origGSId = match1[SOLR_FIELDS.GRIDSET_ID]
   origGS = scribe.getGridset(gridsetId=origGSId, fillMatrices=True)

   # If we should subset
   if bbox is not None and bbox != origShp.bbox:
      spatialSubset = True
      
      # Create a new shapegrid
      mySgName = 'Shapegrid {} subset {}'.format(origShp.getId(), str(bbox))
      newshp = ShapeGrid(mySgName, userId, origGS.epsgcode, 
                         origShp.cellsides, origShp.cellsize, origShp.mapUnits, 
                         bbox, status=JobStatus.INITIALIZE, 
                         statusModTime=gmt().mjd)
      myShp = scribe.findOrInsertShapeGrid(newshp)
      # Perform a cutout operation on the old shapegrid and store in new 
      #    location
      bboxWKT = 'POLYGON(({0} {1},{0} {3},{2} {3},{2} {1},{0} {1}))'.format(*bbox)

      origShp.cutout(bboxWKT, dloc=myShp.getDLocation())
      
      # Get rows to keep
      rowHeaders = getRowHeaders(myShp.getDLocation())
      origRowHeaders = getRowHeaders(origShp.getDLocation())
      
      keepSites = []
      for i in range(len(origRowHeaders)):
         if origRowHeaders[i] in rowHeaders:
            keepSites.append(i)
   else:
      if origShp.getUserId() == userId:
         myShp = origShp
      else:
         newshp = ShapeGrid('Copy of shapegrid {}'.format(origShp.getId()),
                           userId, origGS.epsgcode, origShp.cellsides, 
                           origShp.cellsize, origShp.mapUnits, origShp.bbox,
                           status=JobStatus.INITIALIZE, 
                           statusModTime=gmt().mjd)
         myShp = scribe.findOrInsertShapeGrid(newshp)
      spatialSubset = False
      # Get the row headers
      rowHeaders = getRowHeaders(origShp.getDLocation())
   
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
   
   # Create grid set
   gs = Gridset(name=archiveName, metadata=gsMeta, shapeGrid=myShp, 
                epsgcode=epsgCode, userId=userId, 
                modTime=gmt().mjd, tree=origGS.tree)
   updatedGS = scribe.findOrInsertGridset(gs)
   
   # Copy tree if necessary
   if origGS.tree.getId() != userId:
      otree = origGS.tree
      newTree = Tree('Copy of tree {}'.format(otree.getId(), 
                                              metadata=otree.treeMetadata,
                                              userId=userId, 
                                              gridsetId=updatedGS.getId()))
      insertedTree = scribe.findOrInsertTree(newTree)
      treeData = otree.read().tree
      insertedTree.setTree(treeData)
      insertedTree.writeTree()
      updatedGS.addTree(insertedTree, doRead=True)
   else:
      updatedGS.addTree(origGS.tree, doRead=True)
      
   #updatedGS.tree = origGS.tree
   log.debug("Tree for gridset {} is {}".format(updatedGS.getId(), updatedGS.tree.getId()))
   updatedGS.updateModtime(gmt().mjd)
   scribe.updateObject(updatedGS)
   
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
      pamData = np.zeros((origNrows, len(scnMatches)), dtype=int)
      squids = []
      
      for i in range(len(scnMatches)):
         pamData[:,i] = decompress(scnMatches[i][SOLR_FIELDS.COMPRESSED_PAV])
         squids.append(scnMatches[i][SOLR_FIELDS.SQUID])
      
      # Subset PAM data
      if spatialSubset:
         # Use numpy fancy indexing to cut out extra sites
         pamData = pamData[keepSites, :]
      
      # Create object
      pamMtx = LMMatrix(pamData, matrixType=MatrixType.PAM, gcmCode=gcmCode, 
                        altpredCode=altPredCode, dateCode=dateCode, 
                        metadata=scnMeta, userId=userId,
                        gridset=updatedGS, status=JobStatus.GENERAL,
                        statusModTime=gmt().mjd, headers={'0' : rowHeaders,
                                                          '1' : squids})
      # Insert it into db
      updatedPamMtx = scribe.findOrInsertMatrix(pamMtx)
      updatedPamMtx.updateStatus(JobStatus.COMPLETE)
      scribe.updateObject(updatedPamMtx)
      log.debug("Dlocation for updated pam: {}".format(updatedPamMtx.getDLocation()))
      with open(updatedPamMtx.getDLocation(), 'w') as outF:
         pamMtx.save(outF)
   
   # GRIMs
   for grim in origGS.getGRIMs():
      # TODO: Subset grim
      newGrim = LMMatrix(None, matrixType=MatrixType.GRIM, 
                         processType=ProcessType.RAD_INTERSECT, 
                         gcmCode=grim.gcmCode, altpredCode=grim.altpredCode,
                         dateCode=grim.dateCode, metadata=grim.mtxMetadata,
                         userId=userId, gridset=updatedGS, 
                         status=JobStatus.INITIALIZE)
      insertedGrim = scribe.findOrInsertMatrix(newGrim)
      grimMetadata = grim.mtxMetadata
      grimMetadata['keywords'].append('subset')
      grimMetadata = {
         'keywords' : ['subset'],
         'description' : 'Subset of GRIM {}'.format(grim.getId())
      }
      if grim.mtxMetadata.has_key('keywords'):
         grimMetadata['keywords'].extend(grim.mtxMetadata['keywords'])
      
      insertedGrim.updateStatus(JobStatus.COMPLETE, metadata=grimMetadata)
      scribe.updateObject(insertedGrim)
      # Save the original grim data into the new location
      # TODO: Add read / load method for LMMatrix
      grimMtx = Matrix.load(grim.getDLocation())
      
      # Subset
      if spatialSubset:
         grimMtx = grimMtx.slice(keepSites)
      
      with open(insertedGrim.getDLocation(), 'w') as outF:
         grimMtx.save(outF)
      
   # BioGeo
   for bg in origGS.getBiogeographicHypotheses():
      # TODO: Subset BioGeo
      newBG = LMMatrix(None, matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                         processType=ProcessType.ENCODE_HYPOTHESES, 
                         gcmCode=bg.gcmCode, altpredCode=bg.altpredCode,
                         dateCode=bg.dateCode, metadata=bg.mtxMetadata,
                         userId=userId, gridset=updatedGS, 
                         status=JobStatus.INITIALIZE)
      insertedBG = scribe.findOrInsertMatrix(newBG)
      insertedBG.updateStatus(JobStatus.COMPLETE)
      scribe.updateObject(insertedBG)
      # Save the original grim data into the new location
      # TODO: Add read / load method for LMMatrix
      bgMtx = Matrix.load(bg.getDLocation())
      
      # Subset
      if spatialSubset:
         bgMtx = bgMtx.slice(keepSites)

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

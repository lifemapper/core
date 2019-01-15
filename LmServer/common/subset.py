#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides functionality for subsetting a gridset using Solr
"""

from mx.DateTime import gmt
import os

import numpy as np
from osgeo import ogr

from LmCommon.common.lmconstants import (JobStatus, LMFormat, MatrixType, 
                                                      ProcessType)
from LmCommon.common.matrix import Matrix
from LmCommon.compression.binaryList import decompress
from LmCommon.encoding.layer_encoder import LayerEncoder
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import SOLR_FIELDS, SubsetMethod, Priority
from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.legion.processchain import MFChain

# .............................................................................
def subsetGlobalPAM(archiveName, matches, userId, bbox=None, cellSize=None,
                          scribe=None):
    """
    @summary: Create a subset of a global PAM and create a new grid set
    @param archiveName: The name of this new grid set
    @param matches: Solr hits to be used for subsetting
    """
    method = SubsetMethod.COLUMN
    
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

    # Initialize variables we'll test
    if bbox is None:
        bbox = origShp.bbox
    if cellSize is None:
        cellSize = origShp.cellsize
    
    # TODO: Add these to function so they can be specified by user
    intersectParams = {
        MatrixColumn.INTERSECT_PARAM_FILTER_STRING: None,
        MatrixColumn.INTERSECT_PARAM_VAL_NAME: 'pixel',
        MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: 1,
        MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: 255,
        MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: 25
    }
  
    origRowHeaders = getRowHeaders(origShp.getDLocation())
    
    # If bounding box, resolution, or user is different, create a new shapegrid
    if bbox != origShp.bbox or cellSize != origShp.cellsize or \
                userId != origGS.getUserId():
         
        mySgName = 'Shapegrid_{}_{}'.format(str(bbox).replace(' ', '_'), cellSize)
        newshp = ShapeGrid(mySgName, userId, origGS.epsgcode, origShp.cellsides,
                                 cellSize, origShp.mapUnits, bbox, 
                                 status=JobStatus.INITIALIZE, statusModTime=gmt().mjd)

        # Insert our new shapegrid
        myShp = scribe.findOrInsertShapeGrid(newshp)

        # Determine how to create the data files
        # ----------------
        # If the cell size is different or the bounding box is not completely 
        #     within the original, we need to reintersect
        if cellSize != origShp.cellsize or bbox[0] < origShp.bbox[0] or \
                bbox[1] < origShp.bbox[1] or bbox[2] > origShp.bbox[2] or \
                bbox[3] > origShp.bbox[3]:
            method = SubsetMethod.REINTERSECT
            
            # Write shapefile
            myShp.buildShape()
            myShp.writeShapefile(myShp.getDLocation())

        # Else, if the bounding box is different, we need to spatially subset
        elif bbox != origShp.bbox:
            method = SubsetMethod.SPATIAL
            bboxWKT = 'POLYGON(({0} {1},{0} {3},{2} {3},{2} {1},{0} {1}))'.format(
                                    *bbox)
            origShp.cutout(bboxWKT, dloc=myShp.getDLocation())
            
            rowHeaders = getRowHeaders(myShp.getDLocation())
            origRowHeaders = getRowHeaders(origShp.getDLocation())
            
            keepSites = []
            for i in range(len(origRowHeaders)):
                if origRowHeaders[i] in rowHeaders:
                    keepSites.append(i)

        # Else, we can just subset the PAM columns
        else:
            method = SubsetMethod.COLUMN
            
            rowHeaders = getRowHeaders(origShp.getDLocation())
            
            # Copy original shapegrid to new location
            origShp.writeShapefile(myShp.getDLocation())
        
    else:
        # We can use the original shapegrid
        myShp = origShp

    gsMeta = {
        ServiceObject.META_DESCRIPTION: \
                                'Subset of Global PAM, gridset {}'.format(origGSId),
        ServiceObject.META_KEYWORDS: ['subset']
    }


    # TODO: This really needs to be by original matrix (PAM) id
    #     For now, we can get close if we group by scenario id and algorithm
    #     Another option would be to hash a set of the algorithm parameter values
    #         That will split into groups of like algorithms
    
    # Create a dictionary of matches by scenario and algorithm
    matchGroups = {}
    for match in matches:
        scnId = match[SOLR_FIELDS.PROJ_SCENARIO_ID]
        algCode = match[SOLR_FIELDS.ALGORITHM_CODE]
        
        if not matchGroups.has_key(scnId):
            matchGroups[scnId] = {}
        
        if not matchGroups[scnId].has_key(algCode):
            matchGroups[scnId][algCode] = []
            
        matchGroups[scnId][algCode].append(match)

    
    # Create grid set
    gs = Gridset(name=archiveName, metadata=gsMeta, shapeGrid=myShp, 
                     epsgcode=epsgCode, userId=userId, 
                     modTime=gmt().mjd)
    updatedGS = scribe.findOrInsertGridset(gs)
    
    
    # Copy the tree if available.  It may be subsetted according to the data in
    #     the gridset and therefore should be separate
    if origGS.tree is not None:
        
        # Need to get and fill in tree
        otree = origGS.tree
        
        try:
            treeData = otree.tree
        except: # May need to be read
            try:
                otree.read()
                treeData = otree.tree
            # TODO: Remove this
            except: # Handle bad dlocation from gridset tree
                otree = scribe.getTree(treeId=otree.getId())
                otree.read()
                treeData = otree.tree
        
        if otree.name:
            tree_name = otree.name
        else:
            tree_name = otree.getId()
        newTree = Tree('Copy of {} tree at {}'.format(tree_name, gmt().mjd),
                            metadata={}, userId=userId, gridsetId=updatedGS.getId(),
                            modTime=gmt().mjd)
        newTree.setTree(treeData)
        insertedTree = scribe.findOrInsertTree(newTree)
        newTree.tree = treeData
        insertedTree.setTree(treeData)
        insertedTree.writeTree()
        updatedGS.addTree(insertedTree, doRead=True)
        log.debug('Tree for gridset {} is {}'.format(updatedGS.getId(), 
                                                                    updatedGS.tree.getId()))
        scribe.updateObject(updatedGS)
    
    # If we can reuse data from Solr index, do it
    if method in [SubsetMethod.COLUMN, SubsetMethod.SPATIAL]:
        # PAMs
        # --------
        for scnId in matchGroups.keys():
            for algCode, mtxMatches in matchGroups[scnId].iteritems():
                
                
                scnCode = mtxMatches[0][SOLR_FIELDS.PROJ_SCENARIO_CODE]
                dateCode = altPredCode = gcmCode = None
                
                if mtxMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE):
                    dateCode = mtxMatches[0][SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE]
                if mtxMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_GCM):
                    gcmCode = mtxMatches[0][SOLR_FIELDS.PROJ_SCENARIO_GCM]
                if mtxMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE):
                    altPredCode = mtxMatches[0][SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE]
                
                scnMeta = {
                    ServiceObject.META_DESCRIPTION: \
                        'Subset of grid set {}, scenario {}, algorithm {}'.format(
                                                                    origGSId, scnId, algCode),
                    ServiceObject.META_KEYWORDS: ['subset', scnCode, algCode]
                }
                
                # Assemble full matrix
                pamData = np.zeros((origNrows, len(mtxMatches)), dtype=int)
                squids = []
                
                for i in range(len(mtxMatches)):
                    pamData[:,i] = decompress(mtxMatches[i][SOLR_FIELDS.COMPRESSED_PAV])
                    squids.append(mtxMatches[i][SOLR_FIELDS.SQUID])
                
                # Create object
                # NOTE: We use original row headers even though we are going to slice
                #             them immediately after construction.  We do this to keep
                #             consistent with other matrix slicing
                pamMtx = LMMatrix(pamData, matrixType=MatrixType.PAM, gcmCode=gcmCode, 
                                        altpredCode=altPredCode, dateCode=dateCode, 
                                        metadata=scnMeta, userId=userId,
                                        gridset=updatedGS, status=JobStatus.GENERAL,
                                        statusModTime=gmt().mjd, 
                                        headers={'0' : origRowHeaders,
                                                    '1' : squids})
                
                # If we need to spatially subset, slice the matrix
                if method == SubsetMethod.SPATIAL:
                    pamMtx.slice(keepSites)
    
                # Insert it into db
                updatedPamMtx = scribe.findOrInsertMatrix(pamMtx)
                updatedPamMtx.updateStatus(JobStatus.COMPLETE)
                scribe.updateObject(updatedPamMtx)
                log.debug("Dlocation for updated pam: {}".format(updatedPamMtx.getDLocation()))
                with open(updatedPamMtx.getDLocation(), 'w') as outF:
                    pamMtx.save(outF)

        # GRIMs
        # --------
        for grim in origGS.getGRIMs():
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
            
            # If we need to spatially subset, slice the matrix
            if method == SubsetMethod.SPATIAL:
                grimMtx = grimMtx.slice(keepSites)
            
            with open(insertedGrim.getDLocation(), 'w') as outF:
                grimMtx.save(outF)
        
        # BioGeo
        # --------
        for bg in origGS.getBiogeographicHypotheses():
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
            
            # If we need to spatially subset, slice the matrix
            if method == SubsetMethod.SPATIAL:
                bgMtx = bgMtx.slice(keepSites)
    
            with open(insertedBG.getDLocation(), 'w') as outF:
                bgMtx.save(outF)
    
    else:
        # Reintersect everything
    
        # Create a new Makeflow object
        wfMeta = {
            MFChain.META_CREATED_BY: os.path.basename(__file__),
            MFChain.META_DESCRIPTION: \
                 'Subset makeflow.  Original gridset: {}, new gridset: {}'.format(
                     origGS.getId(), updatedGS.getId())
        }
        newWf = MFChain(userId, priority=Priority.REQUESTED, metadata=wfMeta,
                             status=JobStatus.GENERAL, statusModTime=gmt().mjd)
        
        myWf = scribe.insertMFChain(newWf, updatedGS.getId())
    
        # TODO : Determine if we want a different work directory
        
        workDir = myWf.getRelativeDirectory()
        
        # TODO: Make this asynchronous
        # Add shapegrid to workflow
        #myWf.addCommands(myShp.computeMe(workDir=workDir))
        myShp.buildShape()
        myShp.writeShapefile(myShp.getDLocation())
        myShp.updateStatus(JobStatus.COMPLETE)
        scribe.updateObject(myShp)
        
        # PAMs
        # --------
        for scnId in matchGroups.keys():
            for algCode, mtxMatches in matchGroups[scnId].iteritems():
                
                
                scnCode = mtxMatches[0][SOLR_FIELDS.PROJ_SCENARIO_CODE]
                dateCode = altPredCode = gcmCode = None
                
                if mtxMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE):
                    dateCode = mtxMatches[0][SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE]
                if mtxMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_GCM):
                    gcmCode = mtxMatches[0][SOLR_FIELDS.PROJ_SCENARIO_GCM]
                if mtxMatches[0].has_key(SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE):
                    altPredCode = mtxMatches[0][SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE]
                
                scnMeta = {
                    ServiceObject.META_DESCRIPTION: \
                        'Subset of grid set {}, scenario {}, algorithm {}'.format(
                                                                    origGSId, scnId, algCode),
                    ServiceObject.META_KEYWORDS: ['subset', scnCode, algCode]
                }
                
                # TODO: Get intersect parameters from Solr or user provided
                
                
                # Create PAM
                pamMtx = LMMatrix(None, matrixType=MatrixType.PAM, gcmCode=gcmCode,
                                        altpredCode=altPredCode, dateCode=dateCode,
                                        metadata=scnMeta, userId=userId,
                                        gridset=updatedGS, status=JobStatus.GENERAL,
                                        statusModTime=gmt().mjd)
                pam = scribe.findOrInsertMatrix(pamMtx)
                
                # Insert matrix columns for each match
                for i in range(len(mtxMatches)):
                    # For a while, and maybe still, it was possible that a projection
                    #     would be marked as complete when it failed an a zero-length
                    #     file would be written to the file system.  An exception is
                    #     thrown whenever those projections are access.  Catch that 
                    #     and move on
                    try:
                        prj = scribe.getSDMProject(int(mtxMatches[i][
                                                                            SOLR_FIELDS.PROJ_ID]))
                    except Exception, e:
                        prj = None
                        log.debug('Could not add projection {}: {}'.format(
                                            mtxMatches[i][SOLR_FIELDS.PROJ_ID], str(e)))
                    
                    if prj is not None:
                        prjMeta = {} # TODO: Add something here?
                        
                        tmpCol = MatrixColumn(None, pam.getId(), userId, layer=prj,
                                                     shapegrid=myShp, 
                                                     intersectParams=intersectParams,
                                                     squid=prj.squid, ident=prj.ident, 
                                                     processType=ProcessType.INTERSECT_RASTER,
                                                     metadata=prjMeta, matrixColumnId=None, 
                                                     postToSolr=True, status=JobStatus.GENERAL, 
                                                     statusModTime=gmt().mjd)
                        mtxCol = scribe.findOrInsertMatrixColumn(tmpCol)
                        
                        #log.debug('Matrix column shapegrid is: {}'.format(mtxCol.shapegrid))
                        #mtxCol.shapegrid = myShp
                    
                        # Call compute me for this intersect
                        myWf.addCommands(mtxCol.computeMe(workDir=workDir))
                    
                
                # Initialize PAM after matrix columns inserted
                pam.updateStatus(JobStatus.INITIALIZE)
                scribe.updateObject(pam)
                
        # GRIMs
        # --------
        for grim in origGS.getGRIMs():
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
                'description' : 'Reintersection of GRIM {}'.format(grim.getId())
            }
            if grim.mtxMetadata.has_key('keywords'):
                grimMetadata['keywords'].extend(grim.mtxMetadata['keywords'])
            
            # Get corresponding grim layers (scenario)
            oldGrimCols = scribe.getColumnsForMatrix(grim.getId())
            
            for oldCol in oldGrimCols:
                # TODO: Metadata
                grimLyrMeta = {}
                tmpCol = MatrixColumn(None, insertedGrim.getId(), userId, 
                                             layer=oldCol.layer, shapegrid=myShp, 
                                             intersectParams=oldCol.intersectParams,
                                             squid=oldCol.squid, ident=oldCol.ident,
                                             processType=ProcessType.INTERSECT_RASTER_GRIM,
                                             metadata=grimLyrMeta, matrixColumnId=None,
                                             postToSolr=False, status=JobStatus.GENERAL,
                                             statusModTime=gmt().mjd)
                mtxCol = scribe.findOrInsertMatrixColumn(tmpCol)
                
                # Add rules to workflow
                myWf.addCommands(mtxCol.computeMe(workDir=workDir))
            
            insertedGrim.updateStatus(JobStatus.INITIALIZE)
            scribe.updateObject(insertedGrim)
            
        # BioGeo
        # --------
        for bg in origGS.getBiogeographicHypotheses():
            newBG = LMMatrix(None, matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                                     processType=ProcessType.ENCODE_HYPOTHESES, 
                                     gcmCode=bg.gcmCode, altpredCode=bg.altpredCode,
                                     dateCode=bg.dateCode, metadata=bg.mtxMetadata,
                                     userId=userId, gridset=updatedGS, 
                                     status=JobStatus.INITIALIZE)
            insertedBG = scribe.findOrInsertMatrix(newBG)
            mtxCols = []
            oldCols = scribe.getColumnsForMatrix(bg.getId())
            
            encoder = LayerEncoder(myShp.getDLocation())
            # TODO(CJ): This should be pulled from a default config or the
            #     database or somewhere
            min_coverage = .25
            # Do this for each layer because we need to have the layer object
            #     do create a matrix column
            for oldCol in oldCols:
                lyr = oldCol.layer
                
                try:
                    valAttribute = oldCol.layer.lyrMetadata[
                                            MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
                except KeyError:
                    valAttribute = None
                
                new_cols = encoder.encode_biogeographic_hypothesis(
                     lyr.getDLocation(), lyr.name, min_coverage, 
                     event_field=valAttribute)
                
                for col in new_cols:
                    try:
                        efValue = col.split(' - ')[1]
                    except:
                        efValue = col
                        
                    if valAttribute is not None:
                        intParams = {
                            MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): valAttribute,
                            # TODO(CJ): Pick a better name if we need to do this
                            MatrixColumn.INTERSECT_PARAM_VAL_VALUE.lower(): lyr.name
                        }
                    else:
                        intParams = None
                    
                    metadata = {
                        ServiceObject.META_DESCRIPTION.lower() : 
                    'Encoded Helmert contrasts using the Lifemapper bioGeoContrasts module',
                        ServiceObject.META_TITLE.lower() : 
                    'Biogeographic hypothesis column ({})'.format(col)}
                    mc = MatrixColumn(len(mtxCols), insertedBG.getId(), userId, layer=lyr,
                                            shapegrid=myShp, intersectParams=intParams, 
                                            metadata=metadata, postToSolr=False,
                                            status=JobStatus.COMPLETE, 
                                            statusModTime=gmt().mjd)
                    updatedMC = scribe.findOrInsertMatrixColumn(mc)
                    mtxCols.append(updatedMC)
                    
            enc_mtx = encoder.get_encoded_matrix()
            # Write matrix
            # TODO(CJ): Evaluate if this is how we want to do it
            insertedBG.data = enc_mtx.data
            insertedBG.setHeaders(enc_mtx.getHeaders())
            insertedBG.write(overwrite=True)
            insertedBG.updateStatus(JobStatus.COMPLETE, modTime=gmt().mjd)
            scribe.updateObject(insertedBG)
            
        # Write workflow and update db object
        myWf.write()
        myWf.updateStatus(JobStatus.INITIALIZE)
        scribe.updateObject(myWf)
    
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

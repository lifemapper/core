#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for grid sets
"""
import json
from mx.DateTime import gmt
import os
import zipfile

import cherrypy
import dendropy

from LmCommon.common.lmconstants import (DEFAULT_TREE_SCHEMA, HTTPStatus, 
                                         JobStatus, LMFormat, MatrixType, 
                                         ProcessType)
from LmCommon.common.matrix import Matrix
from LmCommon.encoding.layer_encoder import LayerEncoder

from LmDbServer.boom.boom_collate import BoomCollate

from LmServer.base.atom import Atom
from LmServer.base.layer2 import Vector
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import ARCHIVE_PATH
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.legion.tree import Tree

from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.api.v2.matrix import MatrixService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.common.boomPost import BoomPoster
from LmWebServer.services.cpTools.lmFormat import lmFormatter

BG_REF_ID_KEY = 'identifier'
BG_REF_KEY = 'hypothesis_package_reference'
BG_REF_TYPE_KEY = 'reference_type'
EVENT_FIELD_KEY = 'event_field'
FILE_NAME_KEY = 'file_name'
HYPOTHESIS_NAME_KEY = 'hypothesis_name'
KEYWORD_KEY = 'keywords'
LAYERS_KEY = 'layers'

# .............................................................................
def summarize_object_statuses(summary):
    """Summarizes a summary

    Args:
        summary (:obj:`list` of :obj:`tuple` of :obj:`int`, :obj:`int`): A list
            of (status, count) tuples for an object type
    """
    complete = 0
    waiting = 0
    running = 0
    error = 0
    total = 0
    for status, count in summary:
        if status <= JobStatus.INITIALIZE:
            waiting += count
        elif status < JobStatus.COMPLETE:
            running += count
        elif status == JobStatus.COMPLETE:
            complete += count
        else:
            error += count
        total += count
    return (waiting, running, complete, error, total)

# .............................................................................
@cherrypy.expose
class GridsetAnalysisService(LmService):
    """This class is for the service representing gridset analyses.

    Note: 
        The dispatcher is responsible for calling the correct method.
    """
    # ................................
    # TODO: Enable DELETE? Could remove all existing analysis matrices
    # ................................
    # TODO: Enable GET?  Could this just be the outputs?
    # ................................
    @lmFormatter
    def POST(self, pathGridSetId, doMcpa=False, numPermutations=500, 
             doCalc=False, **params):
        """Adds a set of biogeographic hypotheses to the gridset
        """
        # Get gridset
        gridset = self._getGridSet(pathGridSetId)#, method=HTTPMethod.POST)

        # Check status of all matrices
        if not all(
            [mtx.status == JobStatus.COMPLETE for mtx in gridset.getMatrices()
             ]):
            raise cherrypy.HTTPError(HTTPStatus.CONFLICT, 
               ('The gridset is not ready for analysis.  ' 
               'All matrices must be complete'))

        if doMcpa:
            mcpaPossible = len(
                gridset.getBiogeographicHypotheses()
                ) > 0 and gridset.tree is not None
        if not mcpaPossible:
            raise cherrypy.HTTPError(
                HTTPStatus.CONFLICT, 
                ('The gridset must have a tree and biogeographic '
                 'hypotheses to perform MCPA'))
      
        # If everything is ready and we have analyses to run, do so
        if doMcpa or doCalc:
            bc = BoomCollate(
                gridset, do_pam_stats=doCalc, do_mcpa=doMcpa,
                num_permutations=numPermutations)
            bc.create_workflow()
            bc.close()
            
            cherrypy.response.status = HTTPStatus.ACCEPTED
            return gridset
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Must specify at least one analysis to perform')

    # ................................
    def _getGridSet(self, pathGridSetId):
        """Attempt to get a GridSet
        """
        gs = self.scribe.getGridset(gridsetId=pathGridSetId, fillMatrices=True)
        if gs is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'GridSet {} was not found'.format(pathGridSetId))
        if checkUserPermission(self.getUserId(), gs, HTTPMethod.GET):
            return gs
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN, 
                'User {} does not have permission to access GridSet {}'.format(
                    self.getUserId(), pathGridSetId))
   
    # ................................
    def _get_user_dir(self):
        """Get the user's workspace directory

        Todo:
            Change this to use something at a lower level.  This is using the
                same path construction as the getBoomPackage script
        """
        return os.path.join(
            ARCHIVE_PATH, self.getUserId(), 'uploads', 'biogeo')

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathBioGeoId')
class GridsetBioGeoService(LmService):
    """Service class for gridset biogeographic hypotheses
    """
    # ................................
    @lmFormatter
    def GET(self, pathGridSetId, pathBioGeoId=None, **params):
        """There is not a true service for limiting the biogeographic
               hypothesis matrices in a gridset, but return all when listing
        """
        gs = self._getGridSet(pathGridSetId)
        
        bgHyps = gs.getBiogeographicHypotheses()
        
        if pathBioGeoId is None:
            return bgHyps
        else:
            for bg in bgHyps:
                if bg.getId() == pathBioGeoId:
                    return bg
        
        # If not found 404...
        raise cherrypy.HTTPError(
            HTTPStatus.NOT_FOUND, 
            'Biogeographic hypothesis matrix {} not found for gridset {}'.format(
                pathBioGeoId, pathGridSetId))
        
    # ................................
    @lmFormatter
    def POST(self, pathGridSetId, **params):
        """Adds a set of biogeographic hypotheses to the gridset
        """
        # Get gridset
        gridset = self._getGridSet(pathGridSetId)

        # Process JSON
        hypothesisJson = json.loads(cherrypy.request.body.read())

        # Check reference to get file
        refObj = hypothesisJson[BG_REF_KEY]
        
        # If gridset,
        if refObj[BG_REF_TYPE_KEY].lower() == 'gridset':
            #      copy hypotheses from gridset
            try:
                refGsId = int(refObj[BG_REF_ID_KEY])
            except:
                # Probably not an integer or something
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST, 
                    'Cannot get gridset for reference identfier {}'.format(
                        refObj[BG_REF_ID_KEY]))
            refGridset = self._getGridSet(refGsId)

            # Get hypotheses from other gridset
            ret = []
            for bg in refGridset.getBiogeographicHypotheses():
                newBG = LMMatrix(
                    None, matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                    processType=ProcessType.ENCODE_HYPOTHESES, 
                    gcmCode=bg.gcmCode, altpredCode=bg.altpredCode,
                    dateCode=bg.dateCode, metadata=bg.mtxMetadata,
                    userId=gridset.getUserId(), gridset=gridset, 
                    status=JobStatus.INITIALIZE)
                insertedBG = self.scribe.findOrInsertMatrix(newBG)
                insertedBG.updateStatus(JobStatus.COMPLETE)
                self.scribe.updateObject(insertedBG)
                # Save the original grim data into the new location
                bgMtx = Matrix.load(bg.getDLocation())
                with open(insertedBG.getDLocation(), 'w') as outF:
                    bgMtx.save(outF)
                ret.append(insertedBG)
        elif refObj[BG_REF_TYPE_KEY].lower() == 'upload':
            currtime = gmt().mjd
            # Check for uploaded biogeo package
            packageName = refObj[BG_REF_ID_KEY]
            packageFilename = os.path.join(
                self._get_user_dir(), '{}{}'.format(
                    packageName, LMFormat.ZIP.ext))
            
            encoder = LayerEncoder(gridset.getShapegrid().getDLocation())
            # TODO(CJ): Pull this from config somewhere
            min_coverage = 0.25
            
            if os.path.exists(packageFilename):
                with open(packageFilename) as inF:
                    with zipfile.ZipFile(inF, allowZip64=True) as zipF:
                        # Get file names in package
                        availFiles = zipF.namelist()
                        
                        for hypLyr in refObj[LAYERS_KEY]:
                            hypFilename = hypLyr[FILE_NAME_KEY]
                            
                            # Check to see if file is in zip package
                            if hypFilename in availFiles or \
                                     '{}{}'.format(
                                         hypFilename, LMFormat.SHAPE.ext
                                         ) in availFiles:
                                if hypLyr.has_key(HYPOTHESIS_NAME_KEY):
                                    hypName = hypLyr[HYPOTHESIS_NAME_KEY]
                                else:
                                    hypName = os.path.splitext(
                                        os.path.basename(hypFilename))[0]
                                                                                        
                                if hypLyr.has_key(EVENT_FIELD_KEY):
                                    eventField = hypLyr[EVENT_FIELD_KEY]
                                    column_name = '{} - {}'.format(
                                        hypName, eventField)
                                else:
                                    eventField = None
                                    column_name = hypName
                                    
                                lyrMeta = {
                                    'name' : hypName,
                                    MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower() : eventField,
                                    ServiceObject.META_DESCRIPTION.lower() : \
                                        'Biogeographic hypothesis based on layer {}'.format(
                                            hypFilename),
                                    ServiceObject.META_KEYWORDS.lower() : [
                                        'biogeographic hypothesis'
                                    ]
                                }
                                
                                if hypLyr.has_key(KEYWORD_KEY):
                                    lyrMeta[ServiceObject.META_KEYWORDS.lower()
                                            ].extend(hypLyr[KEYWORD_KEY])
                                    
                                lyr = Vector(
                                    hypName, gridset.getUserId(), gridset.epsg, 
                                    dlocation=None, metadata=lyrMeta, 
                                    dataFormat=LMFormat.SHAPE.driver,
                                    valAttribute=eventField, modTime=currtime)
                                updatedLyr = self.scribe.findOrInsertLayer(lyr)
                                
                                # Get dlocation
                                # Loop through files to write all matching 
                                #    (ext) to out location
                                baseOut = os.path.splitext(
                                    updatedLyr.getDLocation())[0]
                                
                                for ext in LMFormat.SHAPE.getExtensions():
                                    zFn = '{}{}'.format(hypFilename, ext)
                                    outFn = '{}{}'.format(baseOut, ext)
                                    if zFn in availFiles:
                                        zipF.extract(zFn, outFn)
                                        
                                
                                # Add it to the list of files to be encoded
                                encoder.encode_biogeographic_hypothesis(
                                    updatedLyr.getDLocation(), column_name, 
                                    min_coverage, event_field=eventField)
                            else:
                                raise cherrypy.HTTPError(
                                    HTTPStatus.BAD_REQUEST, 
                                    '{} missing from package'.format(
                                        hypFilename))
                        
                # Create biogeo matrix
                # Add the matrix to contain biogeo hypotheses layer intersections
                meta = {
                    ServiceObject.META_DESCRIPTION.lower(): 
                        'Biogeographic Hypotheses from package {}'.format(
                            packageName),
                    ServiceObject.META_KEYWORDS.lower(): [
                        'biogeographic hypotheses'
                    ]
                }
                
                tmpMtx = LMMatrix(
                    None, matrixType=MatrixType.BIOGEO_HYPOTHESES,
                    processType=ProcessType.ENCODE_HYPOTHESES,
                    userId=self.usr, gridset=gridset, metadata=meta,
                    status=JobStatus.INITIALIZE, statusModTime=currtime)
                bgMtx = self.scribe.findOrInsertMatrix(tmpMtx)
                
                # Encode the hypotheses
                encMtx = encoder.get_encoded_matrix()
                with open(bgMtx.dlocation, 'w') as outF:
                    encMtx.save(outF)
                
                # We'll return the newly inserted biogeo matrix
                ret = [bgMtx]
            else:
                raise cherrypy.HTTPError(
                    HTTPStatus.NOT_FOUND,
                    'Biogeography package: {} was not found'.format(
                        packageName))
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Bad request.  Cannot add hypotheses with reference type: {}'.format(
                      refObj[BG_REF_TYPE_KEY]))
        
        # Return resulting list of matrices
        return ret

    # ................................
    def _getGridSet(self, pathGridSetId):
        """Attempts to get a GridSet
        """
        gs = self.scribe.getGridset(gridsetId=pathGridSetId, fillMatrices=True)
        if gs is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'GridSet {} was not found'.format(pathGridSetId))
        if checkUserPermission(self.getUserId(), gs, HTTPMethod.GET):
            return gs
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to access GridSet {}'.format(
                    self.getUserId(), pathGridSetId))
    
    # ................................
    def _get_user_dir(self):
        """
        @summary: Get the user's workspace directory
        @todo: Change this to use something at a lower level.  This is using the
                     same path construction as the getBoomPackage script
        """
        return os.path.join(ARCHIVE_PATH, self.getUserId(), 'uploads', 'biogeo')
    
# .............................................................................
@cherrypy.expose
class GridsetProgressService(LmService):
    """Service class for gridset progress
    """
    # ................................
    @lmFormatter
    def GET(self, pathGridSetId, detail=False, **params):
        """Get progress for a gridset
        """
        return ('gridset', pathGridSetId, detail)
    
# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathTreeId')
class GridsetTreeService(LmService):
    """
    @summary: This class is for the service representing a grid set tree.  The 
                     dispatcher is responsible for calling the correct method.
    """
    # ................................
    def DELETE(self, pathTreeId):
        """
        @summary: Attempts to delete a tree
        @param pathTreeId: The id of the tree to delete
        """
        tree = self.scribe.getTree(treeId=pathTreeId)

        if tree is None:
            raise cherrypy.HTTPError(404, "Tree {} not found".format(pathTreeId))
        
        # If allowed to, delete
        if checkUserPermission(self.getUserId(), tree, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(tree)
            if success:
                cherrypy.response.status = 204
                return 
            else:
                # TODO: How can this happen?  Make sure we catch those cases and 
                #             respond appropriately.  We don't want 500 errors
                raise cherrypy.HTTPError(500, 
                                "Failed to delete tree")
        else:
            raise cherrypy.HTTPError(403, 
                      "User does not have permission to delete this tree")

    # ................................
    @lmFormatter
    def GET(self, pathGridSetId, pathTreeId=None, includeCSV=None, 
            includeSDMs=None, **params):
        """
        @summary: At this time, there is no listing service for gridset trees.
                         For now, we won't even take a tree id parameter and instead
                         will just return the gridset's tree object
        """
        gs = self._getGridSet(pathGridSetId)
        return gs.tree
        
    # ................................
    @lmFormatter
    def POST(self, pathGridSetId, pathTreeId=None, name=None,
             treeSchema=DEFAULT_TREE_SCHEMA, **params):
        """
        @summary: Posts a new tree and adds it to the gridset
        """
        if pathTreeId is not None:
            tree = self.scribe.getTree(treeId=pathTreeId)
            if tree is None:
                raise cherrypy.HTTPError(
                    HTTPStatus.NOT_FOUND,
                    'Tree {} was not found'.format(pathTreeId))
            if checkUserPermission(self.getUserId(), tree, HTTPMethod.GET):
                pass
            else:
                # Raise exception if user does not have permission
                raise cherrypy.HTTPError(
                    HTTPStatus.FORBIDDEN,
                    'User {} does not have permission to access tree {}'.format(
                                self.getUserId(), pathTreeId))
        else:
            if name is None:
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST, 
                    'Must provide name for tree')
            tree = dendropy.Tree.get(file=cherrypy.request.body, 
                                     schema=treeSchema)
            newTree = Tree(name, userId=self.getUserId())
            updatedTree = self.scribe.findOrInsertTree(newTree)
            updatedTree.setTree(tree)
            updatedTree.writeTree()
            updatedTree.modTime = gmt().mjd
            self.scribe.updateObject(updatedTree)
            
        gridset = self._getGridSet(pathGridSetId)
        gridset.addTree(tree)
        gridset.updateModtime(gmt().mjd)
        self.scribe.updateObject(gridset)
        
        return updatedTree

    # ................................
    def _getGridSet(self, pathGridSetId):
        """
        @summary: Attempt to get a GridSet
        """
        gs = self.scribe.getGridset(gridsetId=pathGridSetId, fillMatrices=True)
        if gs is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'GridSet {} was not found'.format(pathGridSetId))
        if checkUserPermission(self.getUserId(), gs, HTTPMethod.GET):
            return gs
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to access GridSet {}'.format(
                    self.getUserId(), pathGridSetId))
    
# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathGridSetId')
class GridsetService(LmService):
    """
    @summary: This class is for the grid set service.  The dispatcher is 
                     responsible for calling the correct method.
    """
    analysis = GridsetAnalysisService()
    biogeo = GridsetBioGeoService()
    matrix = MatrixService()
    progress = GridsetProgressService()
    tree = GridsetTreeService()
    
    # ................................
    def DELETE(self, pathGridSetId):
        """
        @summary: Attempts to delete a grid set
        @param pathGridSetId: The id of the grid set to delete
        """
        gs = self.scribe.getGridset(gridsetId=pathGridSetId)

        if gs is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, "Grid set not found")
        
        # If allowed to, delete
        if checkUserPermission(self.getUserId(), gs, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(gs)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return 
            else:
                # TODO: How can this happen?  Make sure we catch those cases and 
                #             respond appropriately.  We don't want 500 errors
                raise cherrypy.HTTPError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    "Failed to delete grid set")
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                "User does not have permission to delete this grid set")

    # ................................
    @lmFormatter
    def GET(self, pathGridSetId=None, afterTime=None, beforeTime=None, 
              epsgCode=None, limit=100, metaString=None, offset=0, urlUser=None, 
              shapegridId=None, **params):
        """
        @summary: Performs a GET request.  If a grid set id is provided,
                         attempt to return that item.  If not, return a list of 
                         grid sets that match the provided parameters
        """
        if pathGridSetId is None:
            return self._listGridSets(
                self.getUserId(urlUser=urlUser), afterTime=afterTime, 
                beforeTime=beforeTime, epsgCode=epsgCode, limit=limit, 
                metaString=metaString, offset=offset, shapegridId=shapegridId)
        elif pathGridSetId.lower() == 'count':
            return self._countGridSets(
                self.getUserId(urlUser=urlUser), afterTime=afterTime, 
                beforeTime=beforeTime, epsgCode=epsgCode, 
                metaString=metaString, shapegridId=shapegridId)
        else:
            return self._getGridSet(pathGridSetId)
        
    # ................................
    def HEAD(self, pathGridSetId=None):
        if pathGridSetId is not None:
            mf_summary = self.scribe.summarizeMFChainsForGridset(pathGridSetId)
            (waiting_mfs, running_mfs, complete_mfs, error_mfs, total_mfs
             ) = summarize_object_statuses(mf_summary)
            if waiting_mfs + running_mfs == 0:
                cherrypy.response.status = HTTPStatus.OK
            else:
                cherrypy.response.status = HTTPStatus.ACCEPTED
        else:
            cherrypy.response.status = HTTPStatus.OK
        
    
    # ................................
    def POST(self, **params):
        """
        @summary: Posts a new grid set
        """
        gridsetData = json.loads(cherrypy.request.body.read())
        
        usr = self.scribe.findUser(self.getUserId())
        
        bp = BoomPoster(usr.userid, usr.email, gridsetData, self.scribe)
        gridset = bp.init_boom()

        # TODO: What do we return?
        cherrypy.response.status = HTTPStatus.ACCEPTED
        return Atom(gridset.getId(), gridset.name, gridset.metadataUrl, 
                        gridset.modTime, epsg=gridset.epsgcode)
        
    # ................................
    def _countGridSets(self, userId, afterTime=None, beforeTime=None, 
                       epsgCode=None, metaString=None, shapegridId=None):
        """
        @summary: Count GridSet objects matching the specified criteria
        @param userId: The user to count GridSets for.  Note that this may not 
                                be the same user logged into the system
        @param afterTime: (optional) Return GridSets modified after this time 
                                    (Modified Julian Day)
        @param beforeTime: (optional) Return GridSets modified before this time 
                                     (Modified Julian Day)
        @param epsgCode: (optional) Return GridSets with this EPSG code
        """
        gsCount = self.scribe.countGridsets(
            userId=userId, shpgrdLyrid=shapegridId, metastring=metaString,
            afterTime=afterTime, beforeTime=beforeTime, epsg=epsgCode)
        # Format return
        # Set headers
        return {"count" : gsCount}

    # ................................
    def _getGridSet(self, pathGridSetId):
        """
        @summary: Attempt to get a GridSet
        """
        gs = self.scribe.getGridset(gridsetId=pathGridSetId, fillMatrices=True)
        if gs is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 
                'GridSet {} was not found'.format(pathGridSetId))
        if checkUserPermission(self.getUserId(), gs, HTTPMethod.GET):
            return gs
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN, 
                'User {} does not have permission to access GridSet {}'.format(
                    self.getUserId(), pathGridSetId))
    
    # ................................
    def _listGridSets(self, userId, afterTime=None, beforeTime=None, 
                      epsgCode=None, limit=100, metaString=None, offset=0, 
                      shapegridId=None):
        """
        @summary: Count GridSet objects matching the specified criteria
        @param userId: The user to count GridSets for.  Note that this may not 
                                be the same user logged into the system
        @param afterTime: (optional) Return GridSets modified after this time 
                                    (Modified Julian Day)
        @param beforeTime: (optional) Return GridSets modified before this time 
                                     (Modified Julian Day)
        @param epsgCode: (optional) Return GridSets with this EPSG code
        @param limit: (optional) Return this number of GridSets, at most
        @param offset: (optional) Offset the returned GridSets by this number
        """
        gsAtoms = self.scribe.listGridsets(
            offset, limit, userId=userId, shpgrdLyrid=shapegridId, 
            metastring=metaString, afterTime=afterTime, beforeTime=beforeTime, 
            epsg=epsgCode)

        return gsAtoms

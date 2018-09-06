"""
@summary Module that contains the RADExperiment class
@author Aimee Stewart
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
import os
from osgeo import ogr
import subprocess
from types import StringType

from LmBackend.command.common import (ChainCommand,
                                      ConcatenateMatricesCommand,
                                      SystemCommand)
from LmBackend.command.multi import (CalculateStatsCommand,
                                     EncodePhylogenyCommand,
                                     McpaCorrectPValuesCommand,
                                     CreateAncestralPamCommand,
                                     SyncPamAndTreeCommand, McpaRunCommand)
from LmBackend.command.server import (LmTouchCommand, SquidIncCommand, 
                                      StockpileCommand)
from LmBackend.common.lmobj import LMError

from LmCommon.common.lmconstants import (JobStatus, LMFormat, 
                                         MatrixType, ProcessType)

from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import (ID_PLACEHOLDER, LMFileType, 
                                         LMServiceType)
from LmServer.legion.lmmatrix import LMMatrix                                  
from LmServer.legion.tree import Tree                                  

# TODO: Move these to localconstants
NUM_RAND_GROUPS = 30
NUM_RAND_PER_GROUP = 10


# .............................................................................
class Gridset(ServiceObject): #LMMap
   """
   The Gridset class contains all of the information for one view (extent and 
   resolution) of a RAD experiment.  
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, name=None, metadata={}, 
                shapeGrid=None, shapeGridId=None, tree=None, treeId=None,
                siteIndicesFilename=None, 
                dlocation=None, epsgcode=None, matrices=None, 
                userId=None, gridsetId=None, metadataUrl=None, modTime=None):
      """
      @summary Constructor for the Gridset class
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @param gridsetId: dbId  for ServiceObject
      @param name: Short identifier for this gridset, unique for userid.
      @param shapeGrid: Vector layer with polygons representing geographic sites.
      @param siteIndices: A filename containing a dictionary with keys the 
             unique/record identifiers and values the x, y coordinates of the 
             sites in a Matrix (if shapeGrid is not provided)
      @param epsgcode: The EPSG code of the spatial reference system of data.
      @param matrices: list of matrices for this gridset
      @param tree: A Tree with taxa matching those in the PAM 
      """
      if shapeGrid is not None:
         if userId is None:
            userId = shapeGrid.getUserId()
         if shapeGridId is None:
            shapeGridId = shapeGrid.getId()
         if epsgcode is None:
            epsgcode = shapeGrid.epsgcode
         elif epsgcode != shapeGrid.epsgcode:
            raise LMError('Gridset EPSG {} does not match Shapegrid EPSG {}'
                          .format(self._epsg, shapeGrid.epsgcode))
         bbox = shapeGrid.bbox
         mapunits = shapeGrid.mapUnits
            
      ServiceObject.__init__(self, userId, gridsetId, LMServiceType.GRIDSETS, 
                             metadataUrl=metadataUrl, modTime=modTime)
      title = 'Matrix map for Gridset {}'.format(name)
      #LMMap.__init__(self, name, title, self._mapPrefix, 
      #               epsgcode, bbox, mapunits, mapType=LMFileType.OTHER_MAP)
      # TODO: Aimee, do you want to move this somewhere else?
      self._dlocation = None
      self._mapFilename = None
      self._setMapPrefix()
      self.name = name
      self.grdMetadata = {}
      self.loadGrdMetadata(metadata)
      self._shapeGrid = shapeGrid
      self._shapeGridId = shapeGridId
      self._dlocation = None
      if dlocation is not None:
         self.setDLocation(dlocation=dlocation)
      self._setEPSG(epsgcode)
      self._matrices = []
      self.setMatrices(matrices, doRead=False)
      self._tree = tree
      
# ...............................................
   @classmethod
   def initFromFiles(cls):
      pass

# .............................................................................
# Properties
# .............................................................................
   def _setEPSG(self, epsg=None):
      if epsg is None:
         if self._shapeGrid is not None:
            epsg = self._shapeGrid.epsgcode
      self._epsg = epsg

   def _getEPSG(self):
      if self._epsg is None:
         self._setEPSG()
      return self._epsg

   epsgcode = property(_getEPSG, _setEPSG)
      
# ...............................................
   @property
   def treeId(self):
      try:
         return self._tree.getId()
      except:
         return None
         
   @property
   def tree(self):
      return self._tree
         
# ...............................................
   def setLocalMapFilename(self, mapfname=None):
      """
      @note: Overrides existing _mapFilename
      @summary: Set absolute mapfilename containing all computed layers for this 
                Gridset. 
      """
      if mapfname is None:
         mapfname = self._earlJr.createFilename(LMFileType.RAD_MAP, 
                                             gridsetId=self.getId(),
                                             usr=self._userId)
      self._mapFilename = mapfname

# ...............................................
   def clearLocalMapfile(self):
      """
      @summary: Delete the mapfile containing this layer
      """
      if self.mapfilename is None:
         self.setLocalMapFilename()
      success, msg = self.deleteFile(self.mapfilename)

# ...............................................
   def _createMapPrefix(self):
      """
      @summary: Construct the endpoint of a Lifemapper WMS URL for 
                this object.
      @note: Uses the metatadataUrl for this object, plus 'ogc' format, 
             map=<mapname>, and key/value pairs.  
      @note: If the object has not yet been inserted into the database, a 
             placeholder is used until replacement after database insertion.
      """
      grdid = self.getId()
      if grdid is None:
         grdid = ID_PLACEHOLDER
      mapprefix = self._earlJr.constructMapPrefixNew(urlprefix=self.metadataUrl,
                              ftype=LMFileType.RAD_MAP, mapname=self.mapName, 
                              usr=self._userId)
      return mapprefix
   
   def _setMapPrefix(self):
      mapprefix = self._createMapPrefix()
      self._mapPrefix = mapprefix
          
   @property
   def mapPrefix(self): 
      return self._mapPrefix

# ...............................................
   @property
   def mapFilename(self):
      if self._mapFilename is None:
         self.setLocalMapFilename()
      return self._mapFilename
   
# ...............................................
   @property
   def mapName(self):
      mapname = None
      if self._mapFilename is not None:
         pth, mapfname = os.path.split(self._mapFilename)
         mapname, ext = os.path.splitext(mapfname)
      return mapname
   

# .............................................................................
# Private methods
# .............................................................................
# .............................................................................
# Methods
# .............................................................................
   # ............................................
   def computeMe(self, workDir=None, doCalc=False, doMCPA=False, pamDict=None,
                 numPermutations=500):
      """
      @summary: Perform analyses on a grid set
      @todo: Better names for corrected matrices
      """
      rules = []
      if workDir is None:
         workDir = ''
      # TODO: Use a function to get relative directory name instead of 'gs_{}'
      targetDir = os.path.join(workDir, 'gs_{}'.format(self.getId()))
      
      if doMCPA:
#          mcpaRule = self._getMCPARule(workDir, targetDir)
#          rules.append(mcpaRule)
         
         # Copy encoded biogeographic hypotheses to workspace
         bgs = self.getBiogeographicHypotheses()
         if len(bgs) > 0:
            bgs = bgs[0] # Just get the first for now
         
         wsBGFilename = os.path.join(targetDir, 
                                     'bg{}'.format(LMFormat.MATRIX.ext))
         
         if JobStatus.finished(bgs.status):
            # If the matrix is completed, copy it
            touchCmd = LmTouchCommand(os.path.join(targetDir, 'touchBG.out'))
            cpCmd = SystemCommand('cp', 
                                  '{} {}'.format(bgs.getDLocation(), 
                                                 wsBGFilename),
                                  inputs=[bgs.getDLocation()],
                                  outputs=[wsBGFilename])
            
            touchAndCopyCmd = ChainCommand([touchCmd, cpCmd])
            
            rules.append(touchAndCopyCmd.getMakeflowRule(local=True))
         else:
            #TODO: Handle matrix columns
            raise Exception, "Not currently handling non-completed BGs"
         
         # If valid tree, we can resolve polytomies
         if self.tree.isBinary() and (
               not self.tree.hasBranchLengths() or self.tree.isUltrametric()):
            
            # Copy tree to workspace, touch the directory to ensure creation,
            #   then copy tree
            wsTreeFilename = os.path.join(targetDir, 'wsTree.nex')

            treeTouchCmd = LmTouchCommand(os.path.join(targetDir, 
                                                       'touchTree.out'))
            cpTreeCmd = SystemCommand('cp', 
                                  '{} {}'.format(self.tree.getDLocation(),
                                                 wsTreeFilename),
                                  outputs=[wsTreeFilename])
            
            touchAndCopyTreeCmd = ChainCommand([treeTouchCmd, cpTreeCmd])
            
            rules.append(touchAndCopyTreeCmd.getMakeflowRule(local=True))

            # Add squids to workspace tree via SQUID_INC
            squidTreeFilename = os.path.join(targetDir, 'squidTree.nex')
            squidCmd = SquidIncCommand(wsTreeFilename, self.getUserId(), 
                                       squidTreeFilename)
            rules.append(squidCmd.getMakeflowRule(local=True))
            
      for pamId in pamDict.keys():
         # Copy PAM into workspace
         pam = pamDict[pamId][MatrixType.PAM]
         pamWorkDir = os.path.join(targetDir, 'pam_{}_work'.format(pam.getId()))
         wsPamFilename = os.path.join(pamWorkDir, 
                                       'pam_{}{}'.format(pam.getId(),
                                                         LMFormat.MATRIX.ext))
         
         pamTouchCmd = LmTouchCommand(os.path.join(pamWorkDir, 'touch.out'))
         cpPamCmd = SystemCommand('cp', 
                                  '{} {}'.format(pam.getDLocation(), 
                                                 wsPamFilename), 
                                  inputs=[pam.getDLocation()], 
                                  outputs=[wsPamFilename])
         touchAndCopyPamCmd = ChainCommand([pamTouchCmd, cpPamCmd])
         rules.append(touchAndCopyPamCmd.getMakeflowRule(local=True))
         
         # RAD calculations
         if doCalc:
            
            # Site stats files
            siteStatsMtx = pamDict[pamId][MatrixType.SITES_OBSERVED]
            siteStatsFilename = os.path.join(pamWorkDir, 
                                    'siteStats{}'.format(LMFormat.MATRIX.ext))
            sitesSuccessFilename = os.path.join(pamWorkDir, 'sites.success')
            
            # Species stats files
            spStatsMtx = pamDict[pamId][MatrixType.SPECIES_OBSERVED]
            spStatsFilename = os.path.join(pamWorkDir, 
                                       'spStats{}'.format(LMFormat.MATRIX.ext))
            spSuccessFilename = os.path.join(pamWorkDir, 'species.success')

            # Diversity stats files
            divStatsMtx = pamDict[pamId][MatrixType.DIVERSITY_OBSERVED]
            divStatsFilename = os.path.join(pamWorkDir, 
                                            'divStats{}'.format(
                                               LMFormat.MATRIX.ext))
            divSuccessFilename = os.path.join(pamWorkDir, 'diversity.success')
            
            # TODO: Site covariance, species covariance, schluter

            # TODO: Add tree, it may already be in workspace
            try:
               statsTreeFn = squidTreeFilename
               # TODO: Trees should probably have squids added if they exist,
               #          Reorganize when this settles down
               ancPamMtx = pamDict[pamId][MatrixType.ANC_PAM]
               ancPamSuccessFilename = os.path.join(pamWorkDir, 'ancPam.success')
               ancPamFilename = os.path.join(pamWorkDir, 
                                             'ancPam{}'.format(
                                                LMFormat.MATRIX.ext))
               
               
               ancestralCmd = CreateAncestralPamCommand(wsPamFilename, 
                                                        squidTreeFilename, 
                                                        ancPamFilename)
               ancPamCatalogCmd = StockpileCommand(ProcessType.RAD_CALCULATE,
                                                   ancPamMtx.getId(),
                                                   ancPamSuccessFilename,
                                                   ancPamFilename)
               rules.append(ancestralCmd.getMakeflowRule())
               rules.append(ancPamCatalogCmd.getMakeflowRule(local=True))
            except:
               statsTreeFn = None
            statsCmd = CalculateStatsCommand(wsPamFilename, siteStatsFilename,
                                             spStatsFilename, divStatsFilename,
                                             treeFilename=statsTreeFn)
            
            spSiteStatsCmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
                                              siteStatsMtx.getId(), 
                                              sitesSuccessFilename,
                                              siteStatsFilename)
            spSpeciesStatsCmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
                                              spStatsMtx.getId(), 
                                              spSuccessFilename,
                                              spStatsFilename)
            spDiversityStatsCmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
                                              divStatsMtx.getId(), 
                                              divSuccessFilename,
                                              divStatsFilename)
            
            rules.extend([statsCmd.getMakeflowRule(),
                          spSiteStatsCmd.getMakeflowRule(local=True),
                          spSpeciesStatsCmd.getMakeflowRule(local=True),
                          spDiversityStatsCmd.getMakeflowRule(local=True)])
            
         # MCPA
         if doMCPA:
            # Sync PAM and tree
            prunedPamFilename = os.path.join(pamWorkDir, 
                                             'prunedPAM{}'.format(
                                                LMFormat.MATRIX.ext))
            prunedTreeFilename = os.path.join(pamWorkDir, 
                                              'prunedTree{}'.format(
                                                 LMFormat.NEXUS.ext))
            pruneMetadataFilename = os.path.join(pamWorkDir, 
                                                 'pruneMetadata{}'.format(
                                                    LMFormat.JSON.ext))
            
            syncCmd = SyncPamAndTreeCommand(wsPamFilename, prunedPamFilename,
                                         squidTreeFilename, prunedTreeFilename,
                                         pruneMetadataFilename)
            rules.append(syncCmd.getMakeflowRule())
            
            # Encode tree
            encTreeFilename = os.path.join(pamWorkDir, 'tree{}'.format(
                                                         LMFormat.MATRIX.ext))
            
            encTreeCmd = EncodePhylogenyCommand(prunedTreeFilename, 
                                                prunedPamFilename, 
                                                encTreeFilename)
            rules.append(encTreeCmd.getMakeflowRule())
            
            grim = pamDict[pamId][MatrixType.GRIM]
               
            # TODO: Add check for GRIM status and create if necessary
            # Assume GRIM exists and copy to workspace
            wsGrimFilename = os.path.join(pamWorkDir, 'grim{}'.format(
                                                         LMFormat.MATRIX.ext))
            
            cpGrimCmd = SystemCommand('cp', 
                                      '{} {}'.format(grim.getDLocation(), 
                                                     wsGrimFilename), 
                                      inputs=[grim.getDLocation()],
                                      outputs=[wsGrimFilename])
            # Need to make sure the directory is created, so add dependencies
            cpGrimCmd.inputs.extend(pamTouchCmd.outputs)
            rules.append(cpGrimCmd.getMakeflowRule(local=True))
            
            
            # Get MCPA matrices
            mcpaOutMtx = pamDict[pamId][MatrixType.MCPA_OUTPUTS]
            
            # Get workspace filenames
            ws_obs_filename = os.path.join(
                pamWorkDir, 'obs_cor{}'.format(LMFormat.MATRIX.ext))
            ws_obs_f_filename = os.path.join(
                pamWorkDir, 'obs_f'.format(LMFormat.MATRIX.ext))
            
            
            
            
            wsMcpaOutFilename = os.path.join(pamWorkDir, 
                                       'mcpaOut{}'.format(LMFormat.MATRIX.ext))
            
            # MCPA observed command
            mcpa_obs_cmd = McpaRunCommand(wsPamFilename, encTreeFilename,
                                          wsGrimFilename, wsBGFilename,
                                          obs_filename=ws_obs_filename,
                                          f_mtx_filename=ws_obs_f_filename)
            rules.append(mcpa_obs_cmd.getMakeflowRule())
            
            # MCPA randomized runs
            
            i = 0
            rand_f_mtxs = []
            while i < numPermutations:
               j = NUM_RAND_PER_GROUP
               if i + j >= numPermutations:
                  j = numPermutations - i
            
               rand_f_mtx_filename = os.path.join(
                   pamWorkDir, 'f_mtx_rand{}{}'.format(i, LMFormat.MATRIX.ext))
               rand_f_mtxs.append(rand_f_mtx_filename)
               rand_cmd = McpaRunCommand(wsPamFilename, encTreeFilename,
                                         wsGrimFilename, wsBGFilename,
                                         f_mtx_filename=rand_f_mtx_filename,
                                         randomize=True, 
                                         num_permutations=NUM_RAND_PER_GROUP)
               rules.append(rand_cmd.getMakeflowRule())
               i += NUM_RAND_PER_GROUP
            
            i = 0
            # TODO: Consider a different constant for this
            group_size = NUM_RAND_PER_GROUP  
            while len(rand_f_mtxs) > group_size:
               i += 1
               agg_filename = os.path.join(
                    pamWorkDir, 'f_rand_agg{}{}'.format(i, 
                                                        LMFormat.MATRIX.ext))
               # Create a concatenate command for this group
               concat_cmd = ConcatenateMatricesCommand(
                    rand_f_mtxs[:group_size], 2, agg_filename)
               rules.append(concat_cmd.getMakeflowRule())

               # Remove these from list and append new file
               rand_f_mtxs = rand_f_mtxs[group_size:]
               rand_f_mtxs.append(agg_filename)
            
            """
            # If we have multiple files left, aggregate them
            if len(rand_f_mtxs) > 1:
               i += 1
               f_rand_agg_filename = os.path.join(
                    pamWorkDir, 'f_rand_agg{}{}'.format(i, 
                                                        LMFormat.MATRIX.ext))
               # Create a concatenate command for this group
               concat_cmd = ConcatenateMatricesCommand(
                    rand_f_mtxs[:group_size], axis=2, agg_filename)
               rules.append(concat_cmd.getMakeflowRule())
            else:
               f_rand_agg_filename = rand_f_mtxs[0]
            """
            
            # TODO: Correct P-Values
            out_p_values_filename = os.path.join(
                pamWorkDir, 'p_values{}'.format(LMFormat.MATRIX.ext))
            out_bh_values_filename = os.path.join(
                pamWorkDir, 'bh_values{}'.format(LMFormat.MATRIX.ext))
            
            # TODO: Use ws_obs_filename?
            corr_p_cmd = McpaCorrectPValuesCommand(ws_obs_f_filename,
                                                   out_p_values_filename,
                                                   out_bh_values_filename,
                                                   rand_f_mtxs)
            rules.append(corr_p_cmd.getMakeflowRule())
            
            # Assemble final MCPA matrix
            mcpa_concat_cmd = ConcatenateMatricesCommand(
                [ws_obs_filename, 
                 out_p_values_filename, 
                 out_bh_values_filename], 2, wsMcpaOutFilename)
            rules.append(mcpa_concat_cmd.getMakeflowRule())
            
            # Stockpile matrix
            mcpaOutSuccessFilename = os.path.join(pamWorkDir, 'mcpaOut.success')
            
            mcpaOutStockpileCmd = StockpileCommand(ProcessType.MCPA_ASSEMBLE,
                                    mcpaOutMtx.getId(), mcpaOutSuccessFilename, 
                                    wsMcpaOutFilename, 
                                    metadataFilename=pruneMetadataFilename)
            rules.append(mcpaOutStockpileCmd.getMakeflowRule(local=True))

      return rules
   
# ...............................................
   def getShapegrid(self):
      return self._shapeGrid

# ...............................................
   def setId(self, expid):
      """
      Overrides ServiceObject.setId.  
      @note: ExperimentId should always be set before this is called.
      """
      ServiceObject.setId(self, expid)
      self.setPath()

# ...............................................
   def setPath(self):
      if self._path is None:
         if (self._userId is not None and 
             self.getId() and 
             self._getEPSG() is not None):
            self._path = self._earlJr.createDataPath(self._userId, 
                               LMFileType.UNSPECIFIED_RAD,
                               epsg=self._epsg, gridsetId=self.getId())
         else:
            raise LMError
         
   @property
   def path(self):
      if self._path is None:
         self.setPath()
      return self._path

# ...............................................
   def createLocalDLocation(self):
      """
      @summary: Create an absolute filepath from object attributes
      @note: If the object does not have an ID, this returns None
      """
      dloc = self._earlJr.createFilename(LMFileType.GRIDSET_PACKAGE, 
                                         objCode=self.getId(), 
                                         gridsetId=self.getId(), 
                                         usr=self.getUserId())
      return dloc

   def getDLocation(self):
      """
      @summary: Return the _dlocation attribute; create and set it if empty
      """
      self.setDLocation()
      return self._dlocation
   
   def setDLocation(self, dlocation=None):
      """
      @summary: Set the _dlocation attribute if it is None.  Use dlocation
                if provided, otherwise calculate it.
      @note: Does NOT override existing dlocation, use clearDLocation for that
      """
      if self._dlocation is None:
         if dlocation is None: 
            dlocation = self.createLocalDLocation()
         self._dlocation = dlocation

   def clearDLocation(self): 
      self._dlocation = None

   # .............................
   def getPackageLocation(self):
      """
      @summary: Get the file path for storing or retrieving a gridset package
      @todo: Aimee, please change this as you see fit.  If you change the 
                function name, modify the package formatter.
      """
      return os.path.join(os.path.dirname(self.getDLocation()), 
                     'gs_{}_package{}'.format(self.getId(), LMFormat.ZIP.ext))

# ...............................................
   def setMatrices(self, matrices, doRead=False):
      """
      @summary Fill a Matrix object from Matrix or existing file
      """
      if matrices is not None:
         for mtx in matrices:
            try:
               self.addMatrix(mtx)
            except Exception, e:
               raise LMError('Failed to add matrix {}'.format(mtx))

# ...............................................
   def addTree(self, tree, doRead=False):
      """
      @summary Fill the Tree object, updating the tree dlocation
      """
      if isinstance(tree, Tree):
         tree.setUserId(self.getUserId())
         # Make sure to set the parent Id and URL
         if self.getId() is not None:
            tree.parentId = self.getId()
            tree.setParentMetadataUrl(self.metadataUrl)
         self._tree = tree

# ...............................................
   def addMatrix(self, mtxFileOrObj, doRead=False):
      """
      @summary Fill a Matrix object from Matrix or existing file
      """
      mtx = None
      if mtxFileOrObj is not None:
         usr = self.getUserId()
         if isinstance(mtxFileOrObj, StringType) and os.path.exists(mtxFileOrObj):
            mtx = LMMatrix(dlocation=mtxFileOrObj, userId=usr)
            if doRead:
               mtx.readData()            
         elif isinstance(mtxFileOrObj, LMMatrix):
            mtx = mtxFileOrObj
            mtx.setUserId(usr)
         if mtx is not None:
            # Make sure to set the parent Id and URL
            if self.getId() is not None:
               mtx.parentId = self.getId()
               mtx.setParentMetadataUrl(self.metadataUrl)
            if mtx.getId() is None:
               self._matrices.append(mtx)
            else:
               existingIds = [m.getId() for m in self._matrices]
               if mtx.getId() not in existingIds:
                  self._matrices.append(mtx)
                                       
   def getMatrices(self):
      return self._matrices

   def _getMatrixTypes(self, mtypes):
      if type(mtypes) is int:
         mtypes = [mtypes]
      mtxs = []
      for mtx in self._matrices:
         if mtx.matrixType in mtypes:
            mtxs.append(mtx)
      return mtxs

   def getPAMs(self):
      return  self._getMatrixTypes([MatrixType.PAM, MatrixType.ROLLING_PAM])

   def getGRIMs(self):
      return self._getMatrixTypes(MatrixType.GRIM)

   def getBiogeographicHypotheses(self):
      return self._getMatrixTypes(MatrixType.BIOGEO_HYPOTHESES)

   def getPAMForCodes(self, gcmCode, altpredCode, dateCode):
      for pam in self.getPAMs():
         if (pam.gcmCode == gcmCode and 
             pam.altpredCode == altpredCode and 
             pam.dateCode == dateCode):
            return pam
      return None

   def setMatrixProcessType(self, processType, matrixTypes=[], matrixId=None):
      if type(matrixTypes) is int:
         matrixTypes = [matrixTypes]
      matching = []
      for mtx in self._matrices:
         if matrixTypes:
            if mtx.matrixType in matrixTypes:
               matching.append(mtx)
         elif matrixId is not None:
            matching.append(mtx)
            break
      for mtx in matching:
         mtx.processType = processType

# ................................................
   def createLayerShapefileFromMatrix(self, shpfilename, isPresenceAbsence=True):
      """
      Only partially tested, field creation is not holding
      """
      if isPresenceAbsence:
         matrix = self.getFullPAM()
      else:
         matrix = self.getFullGRIM()
      if matrix is None or self._shapeGrid is None:
         return False
      else:
         self._shapeGrid.copyData(self._shapeGrid.getDLocation(), 
                                 targetDataLocation=shpfilename,
                                 format=self._shapeGrid.dataFormat)
         ogr.RegisterAll()
         drv = ogr.GetDriverByName(self._shapeGrid.dataFormat)
         try:
            shpDs = drv.Open(shpfilename, True)
         except Exception, e:
            raise LMError(['Invalid datasource %s' % shpfilename, str(e)])
         shpLyr = shpDs.GetLayer(0)

         mlyrCount = matrix.columnCount
         fldtype = matrix.ogrDataType
         # For each layer present, add a field/column to the shapefile
         for lyridx in range(mlyrCount):
            if (not self._layersPresent 
                or (self._layersPresent and self._layersPresent[lyridx])):
               # 8 character limit, must save fieldname
               fldname = 'lyr%s' % str(lyridx)
               fldDefn = ogr.FieldDefn(fldname, fldtype)
               if shpLyr.CreateField(fldDefn) != 0:
                  raise LMError('CreateField failed for %s in %s' 
                                % (fldname, shpfilename))             

         # For each site/feature, fill with value from matrix
         currFeat = shpLyr.GetNextFeature()
         sitesKeys = sorted(self.getSitesPresent().keys())
         print "starting feature loop"         
         while currFeat is not None:
            #for lyridx in range(mlyrCount):
            for lyridx,exists in self._layersPresent.iteritems():
               if exists:
                  # add field to the layer
                  fldname = 'lyr%s' % str(lyridx)
                  siteidx = currFeat.GetFieldAsInteger(self._shapeGrid.siteId)
                  #sitesKeys = sorted(self.getSitesPresent().keys())
                  realsiteidx = sitesKeys.index(siteidx)
                  currval = matrix.getValue(realsiteidx,lyridx)
                  # debug
                  currFeat.SetField(fldname, currval)
            # add feature to the layer
            shpLyr.SetFeature(currFeat)
            currFeat.Destroy()
            currFeat = shpLyr.GetNextFeature()
         #print 'Last siteidx %d' % siteidx
   
         # Closes and flushes to disk
         shpDs.Destroy()
         print('Closed/wrote dataset %s' % shpfilename)
         success = True
         try:
            retcode = subprocess.call(["shptree", "%s" % shpfilename])
            if retcode != 0: 
               print 'Unable to create shapetree index on %s' % shpfilename
         except Exception, e:
            print 'Unable to create shapetree index on %s: %s' % (shpfilename, 
                                                                  str(e))
      return success
      
   # ...............................................
   def writeMap(self, mapfilename):
      pass
      #LMMap.writeMap(self, mapfilename, shpGrid=self._shapeGrid, 
      #               matrices=self._matrices)

#    # ...............................................
#    def updateModtime(self, modTime=mx.DateTime.gmt().mjd):
#       """
#       @copydoc LmServer.base.serviceobject2.ProcessObject::updateModtime()
#       """
#       ServiceObject.updateModtime(self, modTime)

# .............................................................................
# Public methods
# .............................................................................
# ...............................................
   def dumpGrdMetadata(self):
      return super(Gridset, self)._dumpMetadata(self.grdMetadata)
 
# ...............................................
   def loadGrdMetadata(self, newMetadata):
      self.grdMetadata = super(Gridset, self)._loadMetadata(newMetadata)

# ...............................................
   def addGrdMetadata(self, newMetadataDict):
      self.grdMetadata = super(Gridset, self)._addMetadata(newMetadataDict, 
                                  existingMetadataDict=self.grdMetadata)
            
# .............................................................................
# Read-0nly Properties
# .............................................................................
# ...............................................
# ...............................................
   @property
   def epsgcode(self):
      return self._epsg

   @property
   def shapeGridId(self):
      return self._shapeGridId

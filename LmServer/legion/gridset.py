"""
@summary Module that contains the RADExperiment class
@author Aimee Stewart
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
import mx.DateTime
import os
from osgeo import ogr
import subprocess
from types import StringType

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import MatrixType, JobStatus, ProcessType
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import LMFileType, LMServiceType, ProcessTool
from LmServer.legion.lmmatrix import LMMatrix                                  
from LmServer.legion.cmd import MfRule

# TODO: Move these to localconstants
NUM_RAND_GROUPS = 20
NUM_RAND_PER_GROUP = 20


# .............................................................................
class Gridset(ServiceObject):
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
      @param name: Short identifier for this gridset, not required to be unique.
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

      ServiceObject.__init__(self, userId, gridsetId, LMServiceType.GRIDSETS, 
                             metadataUrl=metadataUrl, modTime=modTime)
      # TODO: Aimee, do you want to move this somewhere else?
      self._dlocation = None
      self.name = name
      self.grdMetadata = {}
      self.loadGrdMetadata(metadata)
      self._shapeGrid = shapeGrid
      self._shapeGridId = shapeGridId
      self._dlocation = None
      self.setDLocation(dlocation=dlocation)
      self._setEPSG(epsgcode)
      self._matrices = []
      self.setMatrices(matrices, doRead=False)
      self.tree = tree
      
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
      
   @property
   def treeId(self):
      try:
         return self.tree.getId()
      except:
         return None
         
# .............................................................................
# Private methods
# .............................................................................
   # ............................................
   def _getMCPARule(self, workDir, targetDir):
      # Copy encoded biogeographic hypotheses to workspace
      bgs = self.getBiogeographicHypotheses()
      if len(bgs) > 0:
         bgs = bgs[0] # Just get the first for now
      
      if JobStatus.finished(bgs.status):
         wsBGFilename = os.path.join(targetDir, 'bg.json')
         cmdArgs = ['LOCAL', '$PYTHON',
                    ProcessTool.get(ProcessType.TOUCH), 
                    os.path.join(targetDir, 'touchBG.out'),
                    ';',
                    'cp',
                    bgs.getDLocation(),
                    wsBGFilename]
         # If the matrix is completed, copy it
         touchWsBGCmd = '$PYTHON {} {}'.format(ProcessTool.get(ProcessType.TOUCH),
                                       os.path.join(targetDir, 'touchBG.out'))
         cpTreeCmd = 'LOCAL {} ; cp {} {}'.format(touchWsBGCmd,
                                                  bgs.getDLocation(), 
                                                  wsBGFilename)
         rule = MfRule(cpTreeCmd, [wsBGFilename])
      else:
         #TODO: Handle matrix columns
         raise Exception, "Not currently handling non-completed BGs"
      return rule

# .............................................................................
# Methods
# .............................................................................
   # ............................................
   def computeMe(self, workDir=None, doCalc=False, doMCPA=False, pamDict=None):
      """
      @summary: Perform analyses on a grid set
      @todo: Better names for corrected matrices
      """
      rules = []
      if workDir is None:
         workDir = ''
      # TODO: Use a function to get relative directory name instead of 'gs_{}'
      targetDir = os.path.join(workDir, 'gs_{}'.format(self.getId()))
      
      # Script names
      obsMcpaScript = ProcessTool.get(ProcessType.MCPA_OBSERVED)
      randMcpaScript = ProcessTool.get(ProcessType.MCPA_RANDOM)
      stockpileScript = ProcessTool.get(ProcessType.UPDATE_OBJECT)
      touchScript = ProcessTool.get(ProcessType.TOUCH)
      correctPvaluesScript = ProcessTool.get(ProcessType.MCPA_CORRECT_PVALUES)
      mcpaAssembleScript = ProcessTool.get(ProcessType.MCPA_ASSEMBLE)
      
      if doMCPA:
#          mcpaRule = self._getMCPARule(workDir, targetDir)
#          rules.append(mcpaRule)
         
         # Copy encoded biogeographic hypotheses to workspace
         bgs = self.getBiogeographicHypotheses()
         if len(bgs) > 0:
            bgs = bgs[0] # Just get the first for now
         
         wsBGFilename = os.path.join(targetDir, 'bg.json')
         
         if JobStatus.finished(bgs.status):
            # If the matrix is completed, copy it
            touchWsBGCmd = '$PYTHON {} {}'.format(touchScript,
                                          os.path.join(targetDir, 'touchBG.out'))
            cpTreeCmd = 'LOCAL {} ; cp {} {}'.format(touchWsBGCmd,
                                                     bgs.getDLocation(), 
                                                     wsBGFilename)
            rules.append(MfRule(cpTreeCmd, [wsBGFilename]))
         else:
            #TODO: Handle matrix columns
            raise Exception, "Not currently handling non-completed BGs"
         
         # If valid tree, we can resolve polytomies
         if self.tree.isBinary() and (
               not self.tree.hasBranchLengths() or self.tree.isUltrametric()):
            
            # Copy tree to workspace, touch the directory to ensure creation,
            #   then copy tree
            wsTreeFilename = os.path.join(targetDir, 'wsTree.json')
            touchWsTreeCmd = '$PYTHON {} {}'.format(touchScript,
                                          os.path.join(targetDir, 'touch.out'))
            cpTreeCmd = 'LOCAL {} ; cp {} {}'.format(touchWsTreeCmd, 
                                                     self.tree.getDLocation(), 
                                                     wsTreeFilename)
            rules.append(MfRule(cpTreeCmd, [wsTreeFilename]))
            
            # Add squids to workspace tree via SQUID_INC
            squidTreeFilename = os.path.join(targetDir, 'squidTree.json')
            squidArgs = ['LOCAL',
                         '$PYTHON', 
                         ProcessTool.get(ProcessType.SQUID_INC), 
                         wsTreeFilename, 
                         self.getUserId(), 
                         squidTreeFilename]
            squidCmd = ' '.join(squidArgs)
            squidRule = MfRule(squidCmd, [squidTreeFilename], 
                               dependencies=[wsTreeFilename])
            rules.append(squidRule)
                  
      for pamId in pamDict.keys():
         # Copy PAM into workspace
         pam = pamDict[pamId][MatrixType.PAM]
         pamWorkDir = os.path.join(targetDir, 'pam_{}_work'.format(pam.getId()))
         wsPamFilename = os.path.join(pamWorkDir, 
                                            'pam_{}.json'.format(pam.getId()))
         pamDirTouchFilename = os.path.join(pamWorkDir, 'touch.out')
         touchCopyPamArgs = [
            'LOCAL', '$PYTHON',
            touchScript, 
            pamDirTouchFilename,
            ';', 
            'cp',
            pam.getDLocation(),
            wsPamFilename
         ]
         touchCopyPamCmd = ' '.join(touchCopyPamArgs)
         rules.append(MfRule(touchCopyPamCmd, [wsPamFilename, pamDirTouchFilename]))
         
         # RAD calculations
         if doCalc:
            calcOptions = []
            calcTargets = []
            
            # Site stats
            siteStatsMtx = pamDict[pamId][MatrixType.SITES_OBSERVED]
            siteStatsFilename = os.path.join(pamWorkDir, 'siteStats.json')
            calcTargets.append(siteStatsFilename)
            # Stockpile
            sitesSuccessFilename = os.path.join(pamWorkDir, 'sites.success')
            siteStatsStockPileArgs = [
               'LOCAL',
               '$PYTHON',
               stockpileScript,
               str(ProcessType.RAD_CALCULATE),
               str(siteStatsMtx.getId()),
               sitesSuccessFilename,
               siteStatsFilename
            ]
            rules.append(
               MfRule(' '.join(siteStatsStockPileArgs), [sitesSuccessFilename], 
                      dependencies=[siteStatsFilename]))

            # Species stats
            spStatsMtx = pamDict[pamId][MatrixType.SPECIES_OBSERVED]
            spStatsFilename = os.path.join(pamWorkDir, 'spStats.json')
            calcTargets.append(spStatsFilename)
            # Stockpile
            spSuccessFilename = os.path.join(pamWorkDir, 'species.success')
            spStatsStockPileArgs = [
               'LOCAL',
               '$PYTHON',
               stockpileScript,
               str(ProcessType.RAD_CALCULATE),
               str(spStatsMtx.getId()),
               spSuccessFilename,
               spStatsFilename
            ]
            rules.append(
               MfRule(' '.join(spStatsStockPileArgs), [spSuccessFilename], 
                      dependencies=[spStatsFilename]))
            
            # Diversity stats
            divStatsMtx = pamDict[pamId][MatrixType.DIVERSITY_OBSERVED]
            divStatsFilename = os.path.join(pamWorkDir, 'divStats.json')
            calcTargets.append(divStatsFilename)
            # Stockpile
            divSuccessFilename = os.path.join(pamWorkDir, 'diversity.success')
            divStatsStockPileArgs = [
               'LOCAL',
               '$PYTHON',
               stockpileScript,
               str(ProcessType.RAD_CALCULATE),
               str(divStatsMtx.getId()),
               divSuccessFilename,
               divStatsFilename
            ]
            rules.append(
               MfRule(' '.join(divStatsStockPileArgs), [divSuccessFilename], 
                      dependencies=[divStatsFilename]))
            
            # TODO: Site covariance, species covariance, schluter
            
            statsArgs = [
               '$PYTHON',
               ProcessTool.get(ProcessType.RAD_CALCULATE),
               ' '.join(calcOptions),
               wsPamFilename,
               siteStatsFilename,
               spStatsFilename,
               divStatsFilename
            ]
            statsCmd = ' '.join(statsArgs)
            rules.append(MfRule(statsCmd, calcTargets, 
                                dependencies=[wsPamFilename]))
            
         # MCPA
         if doMCPA:
            # Encode tree
            encTreeFilename = os.path.join(pamWorkDir, 'tree.json')
            encTreeArgs = ['$PYTHON',
                           ProcessTool.get(ProcessType.ENCODE_PHYLOGENY),
                           squidTreeFilename,
                           wsPamFilename,
                           encTreeFilename]
            encTreeCmd = ' '.join(encTreeArgs)
            rules.append(MfRule(encTreeCmd, [encTreeFilename], 
                           dependencies=[squidTreeFilename, wsPamFilename])) 
            
            grim = pamDict[pamId][MatrixType.GRIM]
               
            # TODO: Add check for GRIM status and create if necessary
            # Assume GRIM exists and copy to workspace
            wsGrimFilename = os.path.join(pamWorkDir, 'grim.json')
            cpGrimArgs = ['LOCAL', 'cp', grim.getDLocation(), wsGrimFilename]
            cpGrimCmd = ' '.join(cpGrimArgs)
            rules.append(MfRule(cpGrimCmd, [wsGrimFilename], 
                                dependencies=[pamDirTouchFilename]))
            
            # Get MCPA matrices
            mcpaOutMtx = pamDict[pamId][MatrixType.MCPA_OUTPUTS]
            
            # Get workspace filenames
            wsEnvAdjRsqFilename = os.path.join(pamWorkDir, 'envAdjRsq.json')
            wsEnvPartCorFilename = os.path.join(pamWorkDir, 'envPartCor.json')
            wsEnvFglobalFilename = os.path.join(pamWorkDir, 'envFglobal.json')
            wsEnvFpartialFilename = os.path.join(pamWorkDir, 'envFpartial.json')
            
            wsBGAdjRsqFilename = os.path.join(pamWorkDir, 'bgAdjRsq.json')
            wsBGPartCorFilename = os.path.join(pamWorkDir, 'bgPartCor.json')
            wsBGFglobalFilename = os.path.join(pamWorkDir, 'bgFglobal.json')
            wsBGFpartialFilename = os.path.join(pamWorkDir, 'bgFpartial.json')
            
            wsMcpaOutFilename = os.path.join(pamWorkDir, 'mcpaOut.json')
            
            # MCPA env observed command
            mcpaEnvObsArgs = ['$PYTHON', 
                              obsMcpaScript, 
                              wsPamFilename,
                              encTreeFilename,
                              wsGrimFilename,
                              wsEnvAdjRsqFilename,
                              wsEnvPartCorFilename,
                              wsEnvFglobalFilename,
                              wsEnvFpartialFilename]
            mcpaEnvObsCmd = ' '.join(mcpaEnvObsArgs)
            rules.append(MfRule(mcpaEnvObsCmd, 
                                [wsEnvAdjRsqFilename, wsEnvFglobalFilename, 
                                 wsEnvFpartialFilename, wsEnvPartCorFilename],
                                dependencies=[wsPamFilename, encTreeFilename, 
                                              wsGrimFilename]))
               
            # Env Randomizations
            envFglobRands = []
            envFpartRands = []
            for i in range(NUM_RAND_GROUPS):
               envFglobRandFilename = os.path.join(pamWorkDir, 
                                               'envFglobRand{}.json'.format(i))
               envFpartRandFilename = os.path.join(pamWorkDir, 
                                               'envFpartRand{}.json'.format(i))
               envFglobRands.append(envFglobRandFilename)
               envFpartRands.append(envFpartRandFilename)
               randCmd = ' '.join([
                  '$PYTHON',
                  randMcpaScript,
                  '-n {}'.format(NUM_RAND_PER_GROUP),
                  wsPamFilename,
                  encTreeFilename,
                  wsGrimFilename,
                  envFglobRandFilename,
                  envFpartRandFilename
               ])
               rules.append(MfRule(randCmd, 
                                   [envFglobRandFilename, envFpartRandFilename],
                                   dependencies=[wsPamFilename, 
                                                 encTreeFilename, 
                                                 wsGrimFilename]))
            
            # TODO: Consider saving randomized matrices
            
            # Env F-global
            envFglobFilename = os.path.join(pamWorkDir, 'envFglobP.json')
            envFglobBHfilename = os.path.join(pamWorkDir, 'envFglobBH.json')
            envFglobCmd = ' '.join([
               '$PYTHON',
               correctPvaluesScript,
               wsEnvFglobalFilename,
               envFglobFilename,
               envFglobBHfilename,
               ' '.join(envFglobRands)
            ])
            rules.append(MfRule(envFglobCmd, [envFglobFilename], 
                                dependencies=envFglobRands + [wsEnvFglobalFilename]))
            
            # Env F-semipartial
            envFpartFilename = os.path.join(pamWorkDir, 'envFpartP.json')   
            envFpartBHfilename = os.path.join(pamWorkDir, 'envFpartBH.json')   
            envFpartCmd = ' '.join([
               '$PYTHON',
               correctPvaluesScript,
               wsEnvFpartialFilename,
               envFpartFilename,
               envFpartBHfilename,
               ' '.join(envFpartRands)
            ])
            rules.append(MfRule(envFpartCmd, [envFpartFilename], 
                                dependencies=envFpartRands + [wsEnvFpartialFilename]))
            
            # Bio geo
            # MCPA bg observed command
            mcpaBGObsArgs = ['$PYTHON', 
                             obsMcpaScript, 
                             '-b {}'.format(wsBGFilename),
                              wsPamFilename,
                              encTreeFilename,
                              wsGrimFilename,
                              wsBGAdjRsqFilename,
                              wsBGPartCorFilename,
                              wsBGFglobalFilename,
                              wsBGFpartialFilename]
            mcpaBGObsCmd = ' '.join(mcpaBGObsArgs)
            rules.append(MfRule(mcpaBGObsCmd, 
                                [wsBGAdjRsqFilename, wsBGFglobalFilename, 
                                 wsBGFpartialFilename, wsBGPartCorFilename],
                                dependencies=[wsPamFilename, encTreeFilename, 
                                              wsGrimFilename, wsBGFilename]))
               
            # BG Randomizations
            bgFglobRands = []
            bgFpartRands = []
            # TODO: This should be configurable 
            for i in range(NUM_RAND_GROUPS):
               bgFglobRandFilename = os.path.join(pamWorkDir, 
                                               'bgFglobRand{}.json'.format(i))
               bgFpartRandFilename = os.path.join(pamWorkDir, 
                                               'bgFpartRand{}.json'.format(i))
               bgFglobRands.append(bgFglobRandFilename)
               bgFpartRands.append(bgFpartRandFilename)
               randCmd = ' '.join(['$PYTHON',
                                   randMcpaScript,
                                   '-b {}'.format(wsBGFilename),
                                   '-n {}'.format(NUM_RAND_PER_GROUP),
                                   wsPamFilename,
                                   encTreeFilename,
                                   wsGrimFilename,
                                   bgFglobRandFilename,
                                   bgFpartRandFilename])
               rules.append(MfRule(randCmd, 
                                   [bgFglobRandFilename, bgFpartRandFilename],
                                   dependencies=[wsPamFilename, 
                                                 encTreeFilename, 
                                                 wsGrimFilename,
                                                 wsBGFilename]))
            
            # TODO: Consider saving randomized matrices
            
            # BG F-global
            bgFglobFilename = os.path.join(pamWorkDir, 'bgFglobP.json')
            bgFglobBHfilename = os.path.join(pamWorkDir, 'bgFglobBH.json')
            bgFglobCmd = ' '.join(['$PYTHON',
                                   correctPvaluesScript,
                                   wsBGFglobalFilename,
                                   bgFglobFilename,
                                   bgFglobBHfilename,
                                   ' '.join(bgFglobRands)])
            rules.append(MfRule(bgFglobCmd, [bgFglobFilename], 
                                dependencies=bgFglobRands + [wsBGFglobalFilename]))
            
            # BG F-semipartial
            bgFpartFilename = os.path.join(pamWorkDir, 'bgFpartP.json')   
            bgFpartBHfilename = os.path.join(pamWorkDir, 'bgFpartBH.json')
            bgFpartCmd = ' '.join(['$PYTHON',
                                   correctPvaluesScript,
                                   wsBGFpartialFilename,
                                   bgFpartFilename,
                                   bgFpartBHfilename,
                                   ' '.join(bgFpartRands)])
            rules.append(MfRule(bgFpartCmd, [bgFpartFilename], 
                                dependencies=bgFpartRands + [wsBGFpartialFilename]))

            # Assemble outputs
            assembleCmd = ' '.join(['$PYTHON',
                                    mcpaAssembleScript,
                                    wsEnvPartCorFilename,
                                    wsEnvAdjRsqFilename,
                                    envFglobFilename,
                                    envFpartFilename,
                                    envFglobBHfilename,
                                    envFpartBHfilename,
                                    wsBGPartCorFilename,
                                    wsBGAdjRsqFilename,
                                    bgFglobFilename,
                                    bgFpartFilename,
                                    bgFglobBHfilename,
                                    bgFpartBHfilename,
                                    wsMcpaOutFilename])
            rules.append(MfRule(assembleCmd, [wsMcpaOutFilename],
                                dependencies=[wsEnvPartCorFilename,
                                              wsEnvAdjRsqFilename,
                                              envFglobFilename,
                                              envFpartFilename,
                                              envFglobBHfilename,
                                              envFpartBHfilename,
                                              wsBGPartCorFilename,
                                              wsBGAdjRsqFilename,
                                              bgFglobFilename,
                                              bgFpartFilename,
                                              bgFglobBHfilename,
                                              bgFpartBHfilename
                                              ]))
            # Stockpile matrix
            mcpaOutSuccessFilename = os.path.join(pamWorkDir, 'mcpaOut.success')
            mcpaOutStockpileCmd = ' '.join([
               'LOCAL',
               '$PYTHON',
               stockpileScript,
               str(ProcessType.MCPA_ASSEMBLE),
               str(mcpaOutMtx.getId()),
               mcpaOutSuccessFilename,
               wsMcpaOutFilename
            ])
            
            rules.append(MfRule(mcpaOutStockpileCmd, [mcpaOutSuccessFilename], 
                                dependencies=[wsMcpaOutFilename]))

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
      dloc = self._earlJr.createFilename(LMFileType.BOOM_CONFIG, 
                                         objCode=self.getId(), 
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
            if mtx.getId() is None:
               self._matrices.append(mtx)
            else:
               existingIds = [m.getId() for m in self._matrices]
               if mtx.getId() not in existingIds:
                  self._matrices.append(mtx)
                                       
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
         
#          # Debug only
#          featdef = shpLyr.GetLayerDefn()
#          featcount = shpLyr.GetFeatureCount()
#          for i in range(featdef.GetFieldCount()):
#             fld = featdef.GetFieldDefn(i)
#             print '%s  %d  %d' % (fld.name, fld.type, fld.precision)  
#          print  "done with diagnostic loop"
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
   def updateModtime(self, modTime=mx.DateTime.gmt().mjd):
      """
      @copydoc LmServer.base.serviceobject2.ProcessObject::updateModtime()
      """
      ServiceObject.updateModtime(self, modTime)

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

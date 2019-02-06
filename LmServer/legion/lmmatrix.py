"""
@summary Module that contains the Matrix class
@author Aimee Stewart
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
import mx.DateTime

#from LmBackend.command.common import (ConcatenateMatricesCommand, SystemCommand, 
#                                      ChainCommand)
from LmBackend.common.lmobj import LMError
#from LmBackend.command.server import StockpileCommand, LmTouchCommand

from LmCommon.common.lmconstants import CSV_INTERFACE, MatrixType
#from LmCommon.common.lmconstants import (MatrixType, ProcessType, CSV_INTERFACE, 
#                                         LMFormat, JobStatus)
from LmCommon.common.matrix import Matrix

from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMServiceType, LMFileType)

# .............................................................................
class LMMatrix(Matrix, ServiceObject, ProcessObject):
    """
    The Matrix class contains a 2-dimensional numeric matrix.
    """
# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, matrix, headers=None,
                     matrixType=MatrixType.PAM, 
                     processType=None,
                     gcmCode=None, altpredCode=None, dateCode=None, algCode=None,
                     metadata={},
                     dlocation=None, 
                     metadataUrl=None,
                     userId=None,
                     gridset=None, 
                     matrixId=None,
                     status=None, statusModTime=None):
        """
        @copydoc LmCommon.common.matrix.Matrix::__init__()
        @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
        @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
        @param matrix: data (numpy) array for Matrix base object
        @param matrixType: Constant from LmCommon.common.lmconstants.MatrixType
        @param gcmCode: Code for the Global Climate Model used to create these data
        @param altpredCode: Code for the alternate prediction (i.e. IPCC scenario 
                 or Representative Concentration Pathways/RCPs) used to create 
                 these data
        @param dateCode: Code for the time period for which these data are predicted.
        @param metadata: dictionary of metadata using Keys defined in superclasses
        @param dlocation: file location of the array
        @param gridset: parent gridset of this MatrixupdateModtime
        @param matrixId: dbId  for ServiceObject
        """
        self.matrixType = matrixType
        self._dlocation = dlocation
        self.gcmCode = gcmCode
        self.altpredCode = altpredCode
        self.dateCode = dateCode
        self.algorithmCode = algCode
        self.mtxMetadata = {}
        self.loadMtxMetadata(metadata)
        self._gridset = gridset
        # parent values
        gridsetUrl = gridsetId = None
        if gridset is not None:
            gridsetUrl = gridset.metadataUrl
            gridsetId = gridset.getId()
        Matrix.__init__(self, matrix, headers=headers)
        ServiceObject.__init__(self,  userId, matrixId, LMServiceType.MATRICES, 
                                      metadataUrl=metadataUrl, 
                                      parentMetadataUrl=gridsetUrl,
                                      parentId=gridsetId, 
                                      modTime=statusModTime)
        ProcessObject.__init__(self, objId=matrixId, processType=processType,
                                      status=status, statusModTime=statusModTime)         

# ...............................................
    @classmethod
    def initFromParts(cls, baseMtx, gridset=None, 
                            processType=None, metadataUrl=None, userId=None,
                            status=None, statusModTime=None):
        mtxobj = LMMatrix(None, matrixType=baseMtx.matrixType, 
                                processType=processType, metadata=baseMtx.mtxMetadata,
                                dlocation=baseMtx.getDLocation(), 
                                columnIndices=baseMtx.getColumnIndices(),
                                columnIndicesFilename=baseMtx.getColumnIndicesFilename(),
                                metadataUrl=metadataUrl, userId=userId, gridset=gridset, 
                                matrixId=baseMtx.getMatrixId(), status=baseMtx.status, 
                                statusModTime=baseMtx.statusModTime)
        return mtxobj

    # ...............................................
    def updateStatus(self, status, metadata=None, modTime=mx.DateTime.gmt().mjd):
        """
        @summary: Updates matrixIndex, paramMetadata, and modTime.
        @param metadata: Dictionary of Matrix metadata keys/values; key constants  
                              are ServiceObject class attributes.
        @copydoc LmServer.base.serviceobject2.ProcessObject::updateStatus()
        @copydoc LmServer.base.serviceobject2.ServiceObject::updateModtime()
        @note: Missing keyword parameters are ignored.
        """
        if metadata is not None:
            self.loadMtxMetadata(metadata)
        ProcessObject.updateStatus(self, status, modTime)
        ServiceObject.updateModtime(self, modTime)

# ...............................................
    @property
    def gridsetName(self):
        name = None
        if self._gridset is not None:
            name = self._gridset.name
        return name
    
# ...............................................
    @property
    def gridsetId(self):
        gid = None
        if self._gridset is not None:
            gid = self._gridset.getId()
        return gid

# ...............................................
    @property
    def gridsetUrl(self):
        url = None
        if self._gridset is not None:
            url = self._gridset.metadataUrl
        return url

# ...............................................
    def getDataUrl(self, interface=CSV_INTERFACE):
        durl = self._earlJr.constructLMDataUrl(self.serviceType, self.getId(), 
                                                            interface)
        return durl

# ...............................................
    def getRelativeDLocation(self):
        """
        @summary: Return the relative filepath from object attributes
        @note: If the object does not have an ID, this returns None
        @note: This is to be pre-pended with a relative directory name for data  
                 used by a single workflow/Makeflow 
        """
        basename = None
        self.setDLocation()
        if self._dlocation is not None:
            pth, basename = os.path.split(self._dlocation)
        return basename

    def createLocalDLocation(self):
        """
        @summary: Create an absolute filepath from object attributes
        @note: If the object does not have an ID, this returns None
        """
        ftype = LMFileType.getMatrixFiletype(self.matrixType)
        if self.parentId is None:
            raise LMError('Must have parent gridset ID for filepath')
        dloc = self._earlJr.createFilename(ftype, gridsetId=self.parentId, 
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
    def getGridset(self):
        return self._gridset

# ...............................................
    def getShapegrid(self):
        return self._gridset.getShapegrid()    
    
# ...............................................
    def dumpMtxMetadata(self):
        return super(LMMatrix, self)._dumpMetadata(self.mtxMetadata)

# ...............................................
    def addMtxMetadata(self, newMetadataDict):
        self.mtxMetadata = super(LMMatrix, self)._addMetadata(newMetadataDict, 
                                             existingMetadataDict=self.mtxMetadata)

# ...............................................
    def loadMtxMetadata(self, newMetadata):
        self.mtxMetadata = super(LMMatrix, self)._loadMetadata(newMetadata)

# # ...............................................
#     def computeMe(self, triageInFname, triageOutFname, workDir=None):
#         """
#         @summary: Creates a command to triage possible MatrixColumn inputs,
#                      assemble into a LMMatrix, then test and catalog results.
#         """
#         rules = []
#         # Make sure work dir is not None
#         if workDir is None:
#             workDir = ''
# 
#         #TODO: Update
#         matrixOutputFname = os.path.join(workDir, os.path.basename(self.getDLocation()))
#         # Triage "Mash the potato" rule 
#         tCmd = TriageCommand(triageInFname, triageOutFname)
#         rules.append(tCmd.getMakeflowRule(local=True))
#         
#         # Assemble Matrix rule
#         concatCmd = ConcatenateMatricesCommand([], '1', matrixOutputFname,
#                                                mashedPotatoFilename=triageOutFname)
#         rules.append(concatCmd.getMakeflowRule())
#         # Store Matrix Rule
#         status = None
#         successFilename = '{}.success'.format(
#             os.path.splitext(matrixOutputFname)[0])
#         spCmd = StockpileCommand(self.processType, self.getId(), successFilename, 
#                                          [matrixOutputFname], status=status)
#         rules.append(spCmd.getMakeflowRule(local=True))
#         
#         return rules

#     # .............................
#     def getConcatAndStockpileRules(self, mtxcolFnames, workDir=''):
#         # Make sure work dir is not None
#         if workDir is None:
#             workDir = ''
#         rules = []
#         # Add concatenate command
#         mtxOutputFname = os.path.join(workDir, 
#                             'mtx_{}{}'.format(self.getId(), LMFormat.MATRIX.ext))
#         
#         concatCmd = ConcatenateMatricesCommand(mtxcolFnames, '1', mtxOutputFname)
#         rules.append(concatCmd.getMakeflowRule())
# 
#         # Stockpile Matrix
#         mtxSuccessFilename = os.path.join(workDir, 'mtx_{}.success'
#                                                      .format(self.getId()))
#         spCmd = StockpileCommand(ProcessType.CONCATENATE_MATRICES, self.getId(),
#                                          mtxSuccessFilename, mtxOutputFname, 
#                                          status=JobStatus.COMPLETE)
#         rules.append(spCmd.getMakeflowRule(local=True))
#         return rules
    
#     # ............................................
#     def getCalcRules(self, workDir=''):
#         """
#         """
#         # Site stats files
#         siteStatsMtx = pamDict[pamId][MatrixType.SITES_OBSERVED]
#         siteStatsFilename = os.path.join(workDir, 
#                                         'siteStats{}'.format(LMFormat.MATRIX.ext))
#         sitesSuccessFilename = os.path.join(workDir, 'sites.success')
#         
#         # Species stats files
#         spStatsMtx = pamDict[pamId][MatrixType.SPECIES_OBSERVED]
#         spStatsFilename = os.path.join(workDir, 
#                                             'spStats{}'.format(LMFormat.MATRIX.ext))
#         spSuccessFilename = os.path.join(workDir, 'species.success')
# 
#         # Diversity stats files
#         divStatsMtx = pamDict[pamId][MatrixType.DIVERSITY_OBSERVED]
#         if workDir is None:
#             workDir = ''
#         if not self.matrixType == MatrixType.PAM:
#             raise LMError('Cannot Calculate non-PAM matrix')
#         rules = []
#         # Copy PAM into workspace
#         wsPamFilename = os.path.join(workDir, 'pam_{}{}'
#                                      .format(self.getId(), LMFormat.MATRIX.ext))
#         
#         pamTouchCmd = LmTouchCommand(os.path.join(workDir, 'touch.out'))
#         cpPamCmd = SystemCommand('cp', 
#                                  '{} {}'.format(self.getDLocation(), wsPamFilename), 
#                                  inputs=[self.getDLocation()], 
#                                  outputs=[wsPamFilename])
#         touchAndCopyPamCmd = ChainCommand([pamTouchCmd, cpPamCmd])
#         rules.append(touchAndCopyPamCmd.getMakeflowRule(local=True))
#         
#         # RAD calculations
#         # Site stats files
#         siteStatsMtx = pamDict[pamId][MatrixType.SITES_OBSERVED]
#         siteStatsFilename = os.path.join(workDir, 
#                                         'siteStats{}'.format(LMFormat.MATRIX.ext))
#         sitesSuccessFilename = os.path.join(workDir, 'sites.success')
#         
#         # Species stats files
#         spStatsMtx = pamDict[pamId][MatrixType.SPECIES_OBSERVED]
#         spStatsFilename = os.path.join(workDir, 
#                                             'spStats{}'.format(LMFormat.MATRIX.ext))
#         spSuccessFilename = os.path.join(workDir, 'species.success')
# 
#         # Diversity stats files
#         divStatsMtx = pamDict[pamId][MatrixType.DIVERSITY_OBSERVED]
#         divStatsFilename = os.path.join(workDir, 
#                                                   'divStats{}'.format(
#                                                       LMFormat.MATRIX.ext))
#         divSuccessFilename = os.path.join(workDir, 'diversity.success')
#             
#         # TODO: Site covariance, species covariance, schluter
# 
#         # TODO: Add tree, it may already be in workspace
#         try:
#             statsTreeFn = squidTreeFilename
#             # TODO: Trees should probably have squids added if they exist,
#             #             Reorganize when this settles down
#             ancPamMtx = pamDict[pamId][MatrixType.ANC_PAM]
#             ancPamSuccessFilename = os.path.join(workDir, 'ancPam.success')
#             ancPamFilename = os.path.join(workDir, 
#                                                     'ancPam{}'.format(
#                                                         LMFormat.MATRIX.ext))
#             
#             
#             ancestralCmd = CreateAncestralPamCommand(wsPamFilename, 
#                                                                   squidTreeFilename, 
#                                                                   ancPamFilename)
#             ancPamCatalogCmd = StockpileCommand(ProcessType.RAD_CALCULATE,
#                                                             ancPamMtx.getId(),
#                                                             ancPamSuccessFilename,
#                                                             ancPamFilename)
#             rules.append(ancestralCmd.getMakeflowRule())
#             rules.append(ancPamCatalogCmd.getMakeflowRule(local=True))
#         except:
#             statsTreeFn = None
#         statsCmd = CalculateStatsCommand(wsPamFilename, siteStatsFilename,
#                                                     spStatsFilename, divStatsFilename,
#                                                     treeFilename=statsTreeFn)
#         
#         spSiteStatsCmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
#                                                      siteStatsMtx.getId(), 
#                                                      sitesSuccessFilename,
#                                                      siteStatsFilename)
#         spSpeciesStatsCmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
#                                                      spStatsMtx.getId(), 
#                                                      spSuccessFilename,
#                                                      spStatsFilename)
#         spDiversityStatsCmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
#                                                      divStatsMtx.getId(), 
#                                                      divSuccessFilename,
#                                                      divStatsFilename)
#         
#         rules.extend([statsCmd.getMakeflowRule(),
#                           spSiteStatsCmd.getMakeflowRule(local=True),
#                           spSpeciesStatsCmd.getMakeflowRule(local=True),
#                           spDiversityStatsCmd.getMakeflowRule(local=True)])
#         
#     # ............................................
#     def getMCPARules(self, workDir=None, doCalc=False, doMCPA=False, pamDict=None,
#                       numPermutations=500):
#         if doMCPA:
# #             mcpaRule = self._getMCPARule(workDir, targetDir)
# #             rules.append(mcpaRule)
#             
#             # Copy encoded biogeographic hypotheses to workspace
#             bgs = self.getBiogeographicHypotheses()
#             if len(bgs) > 0:
#                 bgs = bgs[0] # Just get the first for now
#             
#             wsBGFilename = os.path.join(targetDir, 
#                                                  'bg{}'.format(LMFormat.MATRIX.ext))
#             
#             if JobStatus.finished(bgs.status):
#                 # If the matrix is completed, copy it
#                 touchCmd = LmTouchCommand(os.path.join(targetDir, 'touchBG.out'))
#                 cpCmd = SystemCommand('cp', 
#                                              '{} {}'.format(bgs.getDLocation(), 
#                                                                  wsBGFilename),
#                                              inputs=[bgs.getDLocation()],
#                                              outputs=[wsBGFilename])
#                 
#                 touchAndCopyCmd = ChainCommand([touchCmd, cpCmd])
#                 
#                 rules.append(touchAndCopyCmd.getMakeflowRule(local=True))
#             else:
#                 #TODO: Handle matrix columns
#                 raise Exception, "Not currently handling non-completed BGs"
#             
#             # If valid tree, we can resolve polytomies
#             if self.tree.isBinary() and (
#                     not self.tree.hasBranchLengths() or self.tree.isUltrametric()):
#                 
#                 # Copy tree to workspace, touch the directory to ensure creation,
#                 #    then copy tree
#                 wsTreeFilename = os.path.join(targetDir, 'wsTree.nex')
# 
#                 treeTouchCmd = LmTouchCommand(os.path.join(targetDir, 
#                                                                          'touchTree.out'))
#                 cpTreeCmd = SystemCommand('cp', 
#                                              '{} {}'.format(self.tree.getDLocation(),
#                                                                  wsTreeFilename),
#                                              outputs=[wsTreeFilename])
#                 
#                 touchAndCopyTreeCmd = ChainCommand([treeTouchCmd, cpTreeCmd])
#                 
#                 rules.append(touchAndCopyTreeCmd.getMakeflowRule(local=True))
# 
#                 # Add squids to workspace tree via SQUID_INC
#                 squidTreeFilename = os.path.join(targetDir, 'squidTree.nex')
#                 squidCmd = SquidIncCommand(wsTreeFilename, self.getUserId(), 
#                                                     squidTreeFilename)
#                 rules.append(squidCmd.getMakeflowRule(local=True))
#                 
#         for pamId in pamDict.keys():
#                 
#             # MCPA
#             if doMCPA:
#                 # Sync PAM and tree
#                 prunedPamFilename = os.path.join(workDir, 
#                                                             'prunedPAM{}'.format(
#                                                                 LMFormat.MATRIX.ext))
#                 prunedTreeFilename = os.path.join(workDir, 
#                                                              'prunedTree{}'.format(
#                                                                  LMFormat.NEXUS.ext))
#                 pruneMetadataFilename = os.path.join(workDir, 
#                                                                  'pruneMetadata{}'.format(
#                                                                      LMFormat.JSON.ext))
#                 
#                 syncCmd = SyncPamAndTreeCommand(wsPamFilename, prunedPamFilename,
#                                                       squidTreeFilename, prunedTreeFilename,
#                                                       pruneMetadataFilename)
#                 rules.append(syncCmd.getMakeflowRule())
#                 
#                 # Encode tree
#                 encTreeFilename = os.path.join(workDir, 'tree{}'.format(
#                                                                             LMFormat.MATRIX.ext))
#                 
#                 encTreeCmd = EncodePhylogenyCommand(prunedTreeFilename, 
#                                                                 prunedPamFilename, 
#                                                                 encTreeFilename)
#                 rules.append(encTreeCmd.getMakeflowRule())
#                 
#                 grim = pamDict[pamId][MatrixType.GRIM]
#                     
#                 # TODO: Add check for GRIM status and create if necessary
#                 # Assume GRIM exists and copy to workspace
#                 wsGrimFilename = os.path.join(workDir, 'grim{}'.format(
#                                                                             LMFormat.MATRIX.ext))
#                 
#                 cpGrimCmd = SystemCommand('cp', 
#                                                   '{} {}'.format(grim.getDLocation(), 
#                                                                       wsGrimFilename), 
#                                                   inputs=[grim.getDLocation()],
#                                                   outputs=[wsGrimFilename])
#                 # Need to make sure the directory is created, so add dependencies
#                 cpGrimCmd.inputs.extend(pamTouchCmd.outputs)
#                 rules.append(cpGrimCmd.getMakeflowRule(local=True))
#                 
#                 
#                 # Get MCPA matrices
#                 mcpaOutMtx = pamDict[pamId][MatrixType.MCPA_OUTPUTS]
#                 
#                 # Get workspace filenames
#                 ws_obs_filename = os.path.join(
#                      workDir, 'obs_cor{}'.format(LMFormat.MATRIX.ext))
#                 ws_obs_f_filename = os.path.join(
#                      workDir, 'obs_f{}'.format(LMFormat.MATRIX.ext))
#                 
#                 
#                 
#                 
#                 wsMcpaOutFilename = os.path.join(workDir, 
#                                                     'mcpaOut{}'.format(LMFormat.MATRIX.ext))
#                 
#                 # MCPA observed command
#                 mcpa_obs_cmd = McpaRunCommand(wsPamFilename, encTreeFilename,
#                                                         wsGrimFilename, wsBGFilename,
#                                                         obs_filename=ws_obs_filename,
#                                                         f_mtx_filename=ws_obs_f_filename)
#                 rules.append(mcpa_obs_cmd.getMakeflowRule())
#                 
#                 # MCPA randomized runs
#                 
#                 i = 0
#                 rand_f_mtxs = []
#                 while i < numPermutations:
#                     j = NUM_RAND_PER_GROUP
#                     if i + j >= numPermutations:
#                         j = numPermutations - i
#                 
#                     rand_f_mtx_filename = os.path.join(
#                          workDir, 'f_mtx_rand{}{}'.format(i, LMFormat.MATRIX.ext))
#                     rand_f_mtxs.append(rand_f_mtx_filename)
#                     rand_cmd = McpaRunCommand(wsPamFilename, encTreeFilename,
#                                                       wsGrimFilename, wsBGFilename,
#                                                       f_mtx_filename=rand_f_mtx_filename,
#                                                       randomize=True, 
#                                                       num_permutations=NUM_RAND_PER_GROUP)
#                     rules.append(rand_cmd.getMakeflowRule())
#                     i += NUM_RAND_PER_GROUP
#                 
#                 i = 0
#                 # TODO: Consider a different constant for this
#                 group_size = NUM_RAND_PER_GROUP  
#                 while len(rand_f_mtxs) > group_size:
#                     i += 1
#                     agg_filename = os.path.join(
#                           workDir, 'f_rand_agg{}{}'.format(i, 
#                                                                           LMFormat.MATRIX.ext))
#                     # Create a concatenate command for this group
#                     concat_cmd = ConcatenateMatricesCommand(
#                           rand_f_mtxs[:group_size], 2, agg_filename)
#                     rules.append(concat_cmd.getMakeflowRule())
# 
#                     # Remove these from list and append new file
#                     rand_f_mtxs = rand_f_mtxs[group_size:]
#                     rand_f_mtxs.append(agg_filename)
#                 
#                 """
#                 # If we have multiple files left, aggregate them
#                 if len(rand_f_mtxs) > 1:
#                     i += 1
#                     f_rand_agg_filename = os.path.join(
#                           workDir, 'f_rand_agg{}{}'.format(i, 
#                                                                           LMFormat.MATRIX.ext))
#                     # Create a concatenate command for this group
#                     concat_cmd = ConcatenateMatricesCommand(
#                           rand_f_mtxs[:group_size], axis=2, agg_filename)
#                     rules.append(concat_cmd.getMakeflowRule())
#                 else:
#                     f_rand_agg_filename = rand_f_mtxs[0]
#                 """
#                 
#                 # TODO: Correct P-Values
#                 out_p_values_filename = os.path.join(
#                      workDir, 'p_values{}'.format(LMFormat.MATRIX.ext))
#                 out_bh_values_filename = os.path.join(
#                      workDir, 'bh_values{}'.format(LMFormat.MATRIX.ext))
#                 
#                 # TODO: Use ws_obs_filename?
#                 corr_p_cmd = McpaCorrectPValuesCommand(ws_obs_f_filename,
#                                                                     out_p_values_filename,
#                                                                     out_bh_values_filename,
#                                                                     rand_f_mtxs)
#                 rules.append(corr_p_cmd.getMakeflowRule())
#                 
#                 # Assemble final MCPA matrix
#                 mcpa_concat_cmd = ConcatenateMatricesCommand(
#                      [ws_obs_filename, 
#                       out_p_values_filename, 
#                       out_bh_values_filename], 2, wsMcpaOutFilename)
#                 rules.append(mcpa_concat_cmd.getMakeflowRule())
#                 
#                 # Stockpile matrix
#                 mcpaOutSuccessFilename = os.path.join(workDir, 'mcpaOut.success')
#                 
#                 mcpaOutStockpileCmd = StockpileCommand(ProcessType.MCPA_ASSEMBLE,
#                                                 mcpaOutMtx.getId(), mcpaOutSuccessFilename, 
#                                                 wsMcpaOutFilename, 
#                                                 metadataFilename=pruneMetadataFilename)
#                 rules.append(mcpaOutStockpileCmd.getMakeflowRule(local=True))
# 
#         return rules
    # .............................
    def write(self, dlocation=None, overwrite=False):
        """
        @summary: Writes this matrix to the file system
        """
        if dlocation is None:
            dlocation = self.getDLocation()
        self.readyFilename(dlocation, overwrite=overwrite)

        with open(dlocation, 'w') as outF:
            self.save(outF)

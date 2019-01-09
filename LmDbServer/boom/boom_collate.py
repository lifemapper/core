"""
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
import argparse
import logging
import mx.DateTime as dt
import os, sys, time
import signal

from LmBackend.command.common import ChainCommand, SystemCommand
from LmBackend.command.server import LmTouchCommand
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, LM_USER, MatrixType, 
          LMFormat, ProcessType, SERVER_BOOM_HEADING, SERVER_PIPELINE_HEADING, 
          SERVER_DEFAULT_HEADING_POSTFIX)

from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LMFileType, PUBLIC_ARCHIVE_NAME, 
                                         Priority) 
from LmServer.common.localconstants import (PUBLIC_FQDN, PUBLIC_USER, 
                                            SCRATCH_PATH)
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain
from LmServer.legion.lmmatrix import LMMatrix


SPUD_LIMIT = 200

# .............................................................................
class BoomCollate(LMObject):
    """
    Class to iterate with a ChristopherWalken through a sequence of species data
    creating individual species (Spud) MFChains, multi-species (Bushel) MFChains
    with SPUD_LIMIT number of species and aggregated by projection scenario, 
    and a master (MasterPotatoHead) MFChain
    until it is complete.  If the daemon is interrupted, it will write out the 
    current MFChains, and pick up where it left off to create new MFChains for 
    unprocessed species data.
    @todo: Next instance of boom.Walker will create new MFChains, but add data
    to the existing Global PAM matrices.  Make sure LMMatrix.computeMe handles 
    appending new PAVs or re-assembling. 
    """
    # .............................
    def __init__(self, configFname, successFname, log=None):      

        self.configFname = configFname
        baseAbsFilename, _ = os.path.splitext(configFname)
        basename = os.path.basename(baseAbsFilename)

        self.name = '{}_{}'.format(self.__class__.__name__.lower(), basename)       
        # Config
        if configFname is not None and os.path.exists(configFname):
            self.cfg = Config(siteFn=configFname)
        else:
            raise LMError(currargs='Missing config file {}'.format(configFname))

        # Logfile
        if log is None:
            secs = time.time()
            timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}'.format(self.name, timestamp)
            log = ScriptLogger(logname, level=logging.INFO)
        self.log = log
        
        self.configFname = configFname
        self._successFname = successFname
        self.userId = None
        self.archiveName, 
        self.priority = None 
        self.boomGridset = None
        self.globalPams = None 
        # Send Database connection
        self._scribe = BorgScribe(self.log)
        # Stop indicator
        self.keepWalken = False
        
        signal.signal(signal.SIGTERM, self._receiveSignal) # Stop signal

    # .............................
    def _receiveSignal(self, sigNum, stack):
        """
        @summary: Handler used to receive signals
        @param sigNum: The signal received
        @param stack: The stack at the time of signal
        """
        if sigNum in (signal.SIGTERM, signal.SIGKILL):
            self.close()
        else:
            message = "Unknown signal: %s" % sigNum
            self.log.error(message)

    # ...............................................
    def _getBoomOrDefault(self, varname, defaultValue=None, isList=False, isBool=False):
        var = None
        # Get value from BOOM or default config file
        if isBool:
            try:
                var = self.cfg.getboolean(SERVER_BOOM_HEADING, varname)
            except:
                try:
                    var = self.cfg.getboolean(SERVER_PIPELINE_HEADING, varname)
                except:
                    pass
        else:
            try:
                var = self.cfg.get(SERVER_BOOM_HEADING, varname)
            except:
                try:
                    var = self.cfg.get(SERVER_PIPELINE_HEADING, varname)
                except:
                    pass
        # Take default if present
        if var is None:
            if defaultValue is not None:
                var = defaultValue
        # or interpret value
        elif not isBool:
            if not isList:
                var = self._getVarValue(var)
            else:
                try:
                    tmplist = [v.strip() for v in var.split(',')]
                    var = []
                except:
                    raise LMError('Failed to split variables on \',\'')
                for v in tmplist:
                    v = self._getVarValue(v)
                    var.append(v)
        return var

    # .............................................................................
    def _getConfiguredObjects(self):
        """
        @summary: Get configured string values and any corresponding db objects 
        @TODO: Make all archive/default config keys consistent
        """
        userId = self._getBoomOrDefault('ARCHIVE_USER', defaultValue=PUBLIC_USER)
        archiveName = self._getBoomOrDefault('ARCHIVE_NAME')
        if archiveName is None:
            raise LMError(currargs='Missing ARCHIVE_NAME in {}'
                          .format(self.cfg.configFiles))
        archivePriority = self._getBoomOrDefault('ARCHIVE_PRIORITY', 
                                                 defaultValue=Priority.NORMAL)
        # Get user-archive configuration file
        boomGridset = self._scribe.getGridset(name=archiveName, userId=userId)
        globalPams = self._scribe.getMatricesForGridset(boomGridset.getId(), 
                                                        mtxType=MatrixType.PAM)

        assemblePams = self._getBoomOrDefault('ASSEMBLE_PAMS', isBool=True)
        
        return (userId, archiveName, archivePriority, boomGridset, globalPams, 
                assemblePams)  

    # .............................
    def initializeMe(self):
        """
        @summary: Creates objects (ChristopherWalken for walking the species
                  and MFChain objects for workflow computation requests.
        """
        # Send Database connection
        try:
            success = self._scribe.openConnections()
        except Exception, e:
            raise LMError(currargs='Exception opening database', prevargs=e.args)
        else:
            if not success:
                raise LMError(currargs='Failed to open database')
            else:
                self.log.info('{} opened databases'.format(self.name))
        # Get Boom Gridset values from config file and database
        (self.userId, 
         self.archiveName, 
         self.priority, 
         self.boomGridset, 
         self.globalPams, 
         assemblePams) = self._getConfiguredObjects()
        self.gridset_id = self.boomGridset.getId()
        if not assemblePams:
            raise LMError('NO Collation requested for gridset {}, user {}, id {}'
                          .format(self.gridset_id, self.userId, self.archiveName))
            
    # .............................
    def close(self):
        pass
#         self.keepWalken = False
#         self.log.info('Closing boomer ...')
#         # Stop walken the archive and saveNextStart
#         self.christopher.stopWalken()
#         self.rotatePotatoes()

    # ...............................................
    def _createMasterMakeflow(self):
        meta = {MFChain.META_CREATED_BY: self.name,
                MFChain.META_DESCRIPTION: 'Boom Collation for User {}, Archive {}'
                    .format(self.userId, self.archiveName),
                'GridsetId': self.gridset_id 
        }
        newMFC = MFChain(self.userId, priority=self.priority, 
                         metadata=meta, status=JobStatus.GENERAL, 
                         statusModTime=dt.gmt().mjd)
        mfChain = self._scribe.insertMFChain(newMFC, self.boomGridset.getId())
        return mfChain

    # .............................
    def _addRuleToMasterPotatoHead(self, mfchain, dependencies=None, prefix='spud'):
        """
        @summary: Create a Spud or Potato rule for the MasterPotatoHead MF 
        """
        if dependencies is None:
            dependencies = []
           
        targetFname = mfchain.getArfFilename(
                               arfDir=self.potatoBushel.getRelativeDirectory(),
                               prefix=prefix)
        
        origMfName = mfchain.getDLocation()
        wsMfName = os.path.join(self.potatoBushel.getRelativeDirectory(), 
                                os.path.basename(origMfName))
        
        # Copy makeflow to workspace
        cpCmd = SystemCommand('cp', 
                              '{} {}'.format(origMfName, wsMfName), 
                              inputs=[targetFname], 
                              outputs=wsMfName)
        
        
        mfCmd = SystemCommand('makeflow', 
                              ' '.join(['-T wq', 
                                        '-N lifemapper-{}b'.format(mfchain.getId()),
                                        '-C {}:9097'.format(PUBLIC_FQDN),
                                        '-X {}/worker/'.format(SCRATCH_PATH),
                                        '-a {}'.format(wsMfName)]),
                              inputs=[wsMfName])
        arfCmd = LmTouchCommand(targetFname)
        arfCmd.inputs.extend(dependencies)
        
        delCmd = SystemCommand('rm', '-rf {}'.format(mfchain.getRelativeDirectory()))
        
        mpCmd = ChainCommand([mfCmd, delCmd])
        self.potatoBushel.addCommands([arfCmd.getMakeflowRule(local=True),
                                       cpCmd.getMakeflowRule(local=True),
                                       mpCmd.getMakeflowRule(local=True)])
      

    # ...............................................
    def processMultiSpecies(self):
        # Master makeflow for 
        masterChain = self._createMasterMakeflow()
        for pam in self.globalPams:
            filterDesc = 'Filters: GCM {}, Altpred {}, Date {}'.format(
                                pam.gcmCode, pam.altpredCode, pam.dateCode)
            # Add intersect and concatenate rules for this PAM
            masterChain = self._addPamAssembleRules(masterChain, pam, filterDesc)
            masterChain = self._addPamCalcRules()

        masterChain = self._write_update_MF(masterChain)            
     
    # .............................
    def _addPamAssembleRules(self, masterChain, pam, filterDesc):
        # Create MFChain for this GPAM
        self._scribe.log.info('Adding rules for PAM assembly of matrix {}, {}'
                          .format(pam.getId(), filterDesc))
        targetDir = masterChain.getRelativeDirectory()
        colFilenames = []

        colPrjPairs = self._scribe.getSDMColumnsForMatrix(pam.getId(), 
                                    returnColumns=True, returnProjections=True)
        for mtxcol, prj in colPrjPairs:
            if prj.status == JobStatus.COMPLETE:
                mtxcol.postToSolr = False
                mtxcol.processType = ProcessType.INTERSECT_RASTER
                mtxcol.shapegrid = self.boomGridset.getShapegrid()
                # TODO: ? Assemble commands to pull PAV from Solr   
                # TODO: ? or Remove intersect from sweepconfig
                lyrRules = mtxcol.computeMe(workDir=targetDir)
                masterChain.addCommands(lyrRules)
                
                # Save new, temp intersection filenames for matrix concatenation
                relDir, _ = os.path.splitext(mtxcol.layer.getRelativeDLocation())
                outFname = os.path.join(targetDir, relDir, mtxcol.getTargetFilename())
                colFilenames.append(outFname)            

        # Add concatenate command
        pamRules = pam.getConcatAndStockpileRules(colFilenames, workDir=targetDir)
        self._scribe.log.info('  Add rules to concat and save {} pam columns into matrix {}'
                      .format(len(colFilenames), pam.getId()))
        masterChain.addCommands(pamRules)
        return masterChain
    
    # ...............................................
    def _findOrAddPAMStatMatrices(self, pam, filterDesc, workdir):
        calcMtxs = {}
        for keyword, mtx_type in (('SITES_OBSERVED', MatrixType.SITES_OBSERVED),
                               ('SPECIES_OBSERVED', MatrixType.SPECIES_OBSERVED),
                               ('DIVERSITY_OBSERVED', MatrixType.DIVERSITY_OBSERVED)):
            desc = 'Computed matrix {} for PAM {}, {}'.format(keyword, pam.getId(),
                                                              filterDesc)
            meta = {ServiceObject.META_DESCRIPTION: desc,
                       ServiceObject.META_KEYWORDS: [keyword]}
            mtx = LMMatrix(None, matrixType=mtx_type, 
                           gcmCode=pam.gcmCode, altpredCode=pam.altpredCode, 
                           dateCode=pam.dateCode, metadata=meta, userId=self.userId, 
                           gridset=self.boomGridset, 
                           status=JobStatus.GENERAL, 
                           statusModTime=dt.gmt().mjd)
            mtx = self.scribe.findOrInsertMatrix(mtx)
            out_filename = os.path.join(workDir, '{}_{}'
                            .format(keyword, pam.getId(), LMFormat.MATRIX.ext))

            calcMtxs[mtx_type] = (mtx, out_filename)
        return calcMtxs
    
    # ...............................................
    def _addPamCalcRules(self, masterChain, pam, filterDesc):
        calcMtxs = self._findOrAddPAMStatMatrices(pam, filterDesc, 
                                                  masterChain.getRelativeDirectory())
        for mtx_type, (mtx, out_filename) in calcMtxs.iteritems():
            

#     # ............................................
#     def computeMe(self, workDir=None, doCalc=False, doMCPA=False, pamDict=None,
#                       numPermutations=500):
#     # ............................................
#     def computeMe(self, workDir=None, doCalc=False, doMCPA=False, pamDict=None,
#                       numPermutations=500):
    # ............................................
    def getCalcRules(self, workDir=''):
        """
        """
        # Site stats files
        siteStatsMtx = pamDict[pamId][MatrixType.SITES_OBSERVED]
        siteStatsFilename = os.path.join(workDir, 
                                        'siteStats{}'.format(LMFormat.MATRIX.ext))
        sitesSuccessFilename = os.path.join(workDir, 'sites.success')
        
        # Species stats files
        spStatsMtx = pamDict[pamId][MatrixType.SPECIES_OBSERVED]
        spStatsFilename = os.path.join(workDir, 
                                            'spStats{}'.format(LMFormat.MATRIX.ext))
        spSuccessFilename = os.path.join(workDir, 'species.success')

        # Diversity stats files
        divStatsMtx = pamDict[pamId][MatrixType.DIVERSITY_OBSERVED]
        if workDir is None:
            workDir = ''
        if not self.matrixType == MatrixType.PAM:
            raise LMError('Cannot Calculate non-PAM matrix')
        rules = []
        # Copy PAM into workspace
        wsPamFilename = os.path.join(workDir, 'pam_{}{}'
                                     .format(self.getId(), LMFormat.MATRIX.ext))
        
        pamTouchCmd = LmTouchCommand(os.path.join(workDir, 'touch.out'))
        cpPamCmd = SystemCommand('cp', 
                                 '{} {}'.format(self.getDLocation(), wsPamFilename), 
                                 inputs=[self.getDLocation()], 
                                 outputs=[wsPamFilename])
        touchAndCopyPamCmd = ChainCommand([pamTouchCmd, cpPamCmd])
        rules.append(touchAndCopyPamCmd.getMakeflowRule(local=True))
        
        # RAD calculations
        # Site stats files
        siteStatsMtx = pamDict[pamId][MatrixType.SITES_OBSERVED]
        siteStatsFilename = os.path.join(workDir, 
                                        'siteStats{}'.format(LMFormat.MATRIX.ext))
        sitesSuccessFilename = os.path.join(workDir, 'sites.success')
        
        # Species stats files
        spStatsMtx = pamDict[pamId][MatrixType.SPECIES_OBSERVED]
        spStatsFilename = os.path.join(workDir, 
                                            'spStats{}'.format(LMFormat.MATRIX.ext))
        spSuccessFilename = os.path.join(workDir, 'species.success')

        # Diversity stats files
        divStatsMtx = pamDict[pamId][MatrixType.DIVERSITY_OBSERVED]
        divStatsFilename = os.path.join(workDir, 
                                                  'divStats{}'.format(
                                                      LMFormat.MATRIX.ext))
        divSuccessFilename = os.path.join(workDir, 'diversity.success')
            
        # TODO: Site covariance, species covariance, schluter

        # TODO: Add tree, it may already be in workspace
        try:
            statsTreeFn = squidTreeFilename
            # TODO: Trees should probably have squids added if they exist,
            #             Reorganize when this settles down
            ancPamMtx = pamDict[pamId][MatrixType.ANC_PAM]
            ancPamSuccessFilename = os.path.join(workDir, 'ancPam.success')
            ancPamFilename = os.path.join(workDir, 
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
        
    # ............................................
    def getMCPARules(self, workDir=None, doCalc=False, doMCPA=False, pamDict=None,
                      numPermutations=500):
        if doMCPA:
#             mcpaRule = self._getMCPARule(workDir, targetDir)
#             rules.append(mcpaRule)
            
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
                #    then copy tree
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
                
            # MCPA
            if doMCPA:
                # Sync PAM and tree
                prunedPamFilename = os.path.join(workDir, 
                                                            'prunedPAM{}'.format(
                                                                LMFormat.MATRIX.ext))
                prunedTreeFilename = os.path.join(workDir, 
                                                             'prunedTree{}'.format(
                                                                 LMFormat.NEXUS.ext))
                pruneMetadataFilename = os.path.join(workDir, 
                                                                 'pruneMetadata{}'.format(
                                                                     LMFormat.JSON.ext))
                
                syncCmd = SyncPamAndTreeCommand(wsPamFilename, prunedPamFilename,
                                                      squidTreeFilename, prunedTreeFilename,
                                                      pruneMetadataFilename)
                rules.append(syncCmd.getMakeflowRule())
                
                # Encode tree
                encTreeFilename = os.path.join(workDir, 'tree{}'.format(
                                                                            LMFormat.MATRIX.ext))
                
                encTreeCmd = EncodePhylogenyCommand(prunedTreeFilename, 
                                                                prunedPamFilename, 
                                                                encTreeFilename)
                rules.append(encTreeCmd.getMakeflowRule())
                
                grim = pamDict[pamId][MatrixType.GRIM]
                    
                # TODO: Add check for GRIM status and create if necessary
                # Assume GRIM exists and copy to workspace
                wsGrimFilename = os.path.join(workDir, 'grim{}'.format(
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
                     workDir, 'obs_cor{}'.format(LMFormat.MATRIX.ext))
                ws_obs_f_filename = os.path.join(
                     workDir, 'obs_f{}'.format(LMFormat.MATRIX.ext))
                
                
                
                
                wsMcpaOutFilename = os.path.join(workDir, 
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
                         workDir, 'f_mtx_rand{}{}'.format(i, LMFormat.MATRIX.ext))
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
                          workDir, 'f_rand_agg{}{}'.format(i, 
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
                          workDir, 'f_rand_agg{}{}'.format(i, 
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
                     workDir, 'p_values{}'.format(LMFormat.MATRIX.ext))
                out_bh_values_filename = os.path.join(
                     workDir, 'bh_values{}'.format(LMFormat.MATRIX.ext))
                
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
                mcpaOutSuccessFilename = os.path.join(workDir, 'mcpaOut.success')
                
                mcpaOutStockpileCmd = StockpileCommand(ProcessType.MCPA_ASSEMBLE,
                                                mcpaOutMtx.getId(), mcpaOutSuccessFilename, 
                                                wsMcpaOutFilename, 
                                                metadataFilename=pruneMetadataFilename)
                rules.append(mcpaOutStockpileCmd.getMakeflowRule(local=True))

        return rules


    # ...............................................
    def writeSuccessFile(self, message):
        self.readyFilename(self._successFname, overwrite=True)
        try:
            f = open(self._successFname, 'w')
            f.write(message)
        except:
            raise
        finally:
            f.close()


# .............................................................................
if __name__ == "__main__":
    if not isLMUser():
        print("Run this script as `{}`".format(LM_USER))
        sys.exit(2)
    earl = EarlJr()
    defaultConfigFile = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                            objCode=PUBLIC_ARCHIVE_NAME, 
                                            usr=PUBLIC_USER)
    parser = argparse.ArgumentParser(
             description=('Populate a Lifemapper archive with metadata ' +
                          'for single- or multi-species computations ' + 
                          'specific to the configured input data or the ' +
                          'data package named.'))
    parser.add_argument('--config_file', default=defaultConfigFile,
             help=('Configuration file for the archive, gridset, and grid ' +
                   'to be created from these data.'))
    parser.add_argument('--success_file', default=None,
             help=('Filename to be written on successful completion of script.'))
    
    args = parser.parse_args()
    configFname = args.config_file
    successFname = args.success_file
    if not os.path.exists(configFname):
        raise Exception('Configuration file {} does not exist'.format(configFname))
    if successFname is None:
        boombasename, _ = os.path.splitext(configFname)
        successFname = boombasename + '.success'
    
    secs = time.time()
    timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
    
    scriptname = os.path.splitext(os.path.basename(__file__))[0]
    logname = '{}.{}'.format(scriptname, timestamp)
    logger = ScriptLogger(logname, level=logging.INFO)
    boomer = BoomCollate(configFname, successFname, log=logger)
    boomer.initializeMe()
    boomer.processMultiSpecies()
   
"""
$PYTHON LmDbServer/boom/boom.py --help

import mx.DateTime as dt
import logging
import os, sys, time

from LmBackend.common.lmconstants import RegistryKey, MaskMethod
from LmBackend.common.parameter_sweep_config import ParameterSweepConfiguration
from LmBackend.command.server import IndexPAVCommand, MultiStockpileCommand
from LmBackend.command.single import SpeciesParameterSweepCommand
from LmServer.common.localconstants import (PUBLIC_USER, DEFAULT_EPSG, 
                                            POINT_COUNT_MAX)

from LmDbServer.boom.boomer import *
from LmCommon.common.apiquery import BisonAPI, GbifAPI
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
          SERVER_BOOM_HEADING, SERVER_PIPELINE_HEADING, 
          SERVER_SDM_ALGORITHM_HEADING_PREFIX, SERVER_SDM_MASK_HEADING_PREFIX,
          SERVER_DEFAULT_HEADING_POSTFIX, MatrixType, IDIG_DUMP) 
from LmCommon.common.readyfile import readyFilename
from LmBackend.common.lmobj import LMError, LMObject
from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import (PUBLIC_FQDN, PUBLIC_USER, 
                                            SCRATCH_PATH)
from LmServer.common.lmconstants import (LMFileType, SPECIES_DATA_PATH,
                                         Priority, BUFFER_KEY, CODE_KEY,
                                         ECOREGION_MASK_METHOD, MASK_KEY, 
                                         MASK_LAYER_KEY, PRE_PROCESS_KEY,
                                         PROCESSING_KEY, MASK_LAYER_NAME_KEY,
    SCALE_PROJECTION_MINIMUM, SCALE_PROJECTION_MAXIMUM, LMFileType, PUBLIC_ARCHIVE_NAME, 
                                         )
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmBackend.common.cmd import MfRule
from LmServer.legion.processchain import MFChain
from LmServer.tools.cwalken import ChristopherWalken
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
          SERVER_BOOM_HEADING, MatrixType) 
from LmCommon.common.occparse import OccDataParser
from LmServer.legion.occlayer import OccurrenceLayer

from LmDbServer.boom.boomer import *

PROCESSING_KEY = 'processing'

scriptname = 'boomerTesting'
logger = ScriptLogger(scriptname, level=logging.DEBUG)
currtime = dt.gmt().mjd

config_file='/share/lm/data/archive/taffy/heuchera_global_10min_ppf.ini'
success_file='/share/lm/data/archive/taffy/heuchera_global_10min.success'

config_file='/share/lm/data/archive/anon/Hechera2.ini'
success_file='/share/lm/data/archive/anon/Hechera2.success'

config_file='/share/lm/data/archive/anon/idigtest4.ini'
success_file='/share/lm/data/archive/anon/idigtest4.success'

boomer = Boomer(config_file, success_file, log=logger)
###############################################

# ##########################################################################

# ##########################################################################

       
"""

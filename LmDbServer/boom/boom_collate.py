"""
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
import argparse
import logging
import mx.DateTime as dt
import os, sys, time
import signal

from LmBackend.command.common import (ChainCommand, SystemCommand, 
                                      ConcatenateMatricesCommand)
from LmBackend.command.server import (LmTouchCommand, StockpileCommand)
from LmBackend.command.multi import (CalculateStatsCommand, 
        CreateAncestralPamCommand, SyncPamAndTreeCommand, EncodePhylogenyCommand,
        McpaRunCommand, McpaCorrectPValuesCommand)
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, LM_USER, MatrixType, 
          LMFormat, ProcessType, SERVER_BOOM_HEADING, SERVER_PIPELINE_HEADING)

from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LMFileType, PUBLIC_ARCHIVE_NAME, 
                                         Priority) 
from LmServer.common.localconstants import PUBLIC_USER 
from LmServer.common.log import ScriptLogger
from LmServer.common.solr import queryArchiveIndex
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.tree import Tree


# TODO: Move these to localconstants
NUM_RAND_GROUPS = 30
NUM_RAND_PER_GROUP = 2

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
    @todo: Next instance of BoomCollate will create new MFChains, but add data
           to existing Global PAM matrices.  Make sure LMMatrix handles 
           appending new PAVs or re-assembling. 
    @todo: Replace with consistent file construction from 
           LmServer.common.datalocator.EarlJr.createBasename!
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
        self.archiveName = None 
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
    def _getVarValue(self, var):
        # Remove spaces and empty strings
        if var is not None and not isinstance(var, bool):
            var = var.strip()
            if var == '':
                var = None
        # Convert to number if needed
        try:
            var = int(var)
        except:
            try:
                var = float(var)
            except:
                pass
        return var

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
        treename = self._getBoomOrDefault('TREE')
        assemblePams = self._getBoomOrDefault('ASSEMBLE_PAMS', isBool=True)

        boomGridset = self._scribe.getGridset(name=archiveName, userId=userId,
                                              fillMatrices=True)
        
        baretree = Tree(treename, userId=userId)
        tree = self._scribe.getTree(baretree)
        
        return (userId, archiveName, archivePriority, assemblePams, 
                boomGridset, tree)  

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
         assemblePams,
         self.boomGridset, 
         self.tree) = self._getConfiguredObjects()
         
        self.gridset_id = self.boomGridset.getId()
        if not assemblePams:
            raise LMError('NO Collation requested for gridset {}, user {}, id {}'
                          .format(self.gridset_id, self.userId, self.archiveName))
            
    # .............................
    def close(self):
        pass

    # ...............................................
    def _createMasterMakeflow(self):
        meta = {MFChain.META_CREATED_BY: self.name,
                MFChain.META_DESCRIPTION: 'Boom Collation for User {}, Archive {}'
                    .format(self.userId, self.archiveName),
                MFChain.META_GRIDSET: self.gridset_id }
        newMFC = MFChain(self.userId, priority=self.priority, 
                         metadata=meta, status=JobStatus.GENERAL, 
                         statusModTime=dt.gmt().mjd)
        mfChain = self._scribe.insertMFChain(newMFC, self.boomGridset.getId())
        return mfChain

    # ...............................................
    def processMultiSpecies(self, numPermutations=500):
        # Master makeflow for 
        master_chain = self._createMasterMakeflow()
        workdir = master_chain.getRelativeDirectory()
        globalPams = self.boomGridset.getAllPAMs()
        # TODO: need mdl/prj scenario to query Solr
        for pam in globalPams:
            mtxcol_count = self._scribe.countMatrixColumns(matrixId=pam.getId())
            if mtxcol_count == 0:
                self.log.error('Pam {} has no matrixColumns'.format(pam.getId()))
                break
            filter_desc = ('Filters: GCM {}, Altpred {}, Date {}'
                           .format(pam.gcmCode, pam.altpredCode, pam.dateCode))
            # Add intersect and concatenate rules for this PAM
            ass_rules, ass_success_fname = self._getPamAssembleRules(workdir, 
                                                            pam, filter_desc)
            master_chain.addCommands(ass_rules)
            
            # PamCalc depends on success of PamAssemble (ass_success_fname)
            calc_rules = self._getPamCalcRules(workdir, pam, ass_success_fname, 
                                               filter_desc)
            master_chain.addCommands(calc_rules)
            
            if self.tree is not None:
                anc_rules = self._getAncestralRules(workdir, pam)
                master_chain.addCommands(anc_rules)
            
                biogeo_hyp = self.boomGridset.getBiogeographicHypotheses()
                # TODO: handle > 1 Biogeographic Hypotheses
                if len(biogeo_hyp) > 0: 
                    biogeo_hyp = biogeo_hyp[0]
                    mcpa_rules = self._getMCPARules(workdir, pam, biogeo_hyp, 
                                                    filter_desc, numPermutations)
                    master_chain.addCommands(mcpa_rules)

        master_chain = self._write_update_MF(master_chain)
        self.writeSuccessFile('Boom_collate finished writing makefiles')
        
    # ...............................................
    def _getPAVs(self, model_scen_code, prj_scen_code):
        solr_data = queryArchiveIndex(gridSetId=self.boomGridset.getId(), 
                                      modelScenarioCode=model_scen_code, 
                                      projectionScenarioCode=prj_scen_code, 
                                      userId=self.userId)
        for doc in solr_data:
            pass
        return solr_data

    # ...............................................
    def processMultiSpeciesOld(self, numPermutations=500):
        # Master makeflow for 
        master_chain = self._createMasterMakeflow()
        workdir = master_chain.getRelativeDirectory()
        globalPams = self.boomGridset.getAllPAMs()
        for pam in globalPams:
            filter_desc = 'Filters: GCM {}, Altpred {}, Date {}'.format(
                                pam.gcmCode, pam.altpredCode, pam.dateCode)
            # Add intersect and concatenate rules for this PAM
            ass_rules, ass_success_fname = self._getPamAssembleRules(workdir, 
                                                                pam, filter_desc)
            master_chain.addCommands(ass_rules)
            
            # PamCalc depends on success of PamAssemble (ass_success_fname)
            calc_rules = self._getPamCalcRules(workdir, pam, ass_success_fname, 
                                               filter_desc)
            master_chain.addCommands(calc_rules)
            
            if self.tree is not None:
                anc_rules = self._getAncestralRules(workdir, pam)
                master_chain.addCommands(anc_rules)
            
                biogeo_hyp = self.boomGridset.getBiogeographicHypotheses()
                # TODO: handle > 1 Biogeographic Hypotheses
                if len(biogeo_hyp) > 0: 
                    biogeo_hyp = biogeo_hyp[0]
                    mcpa_rules = self._getMCPARules(workdir, pam, biogeo_hyp, 
                                                    filter_desc, numPermutations)
                    master_chain.addCommands(mcpa_rules)

        master_chain = self._write_update_MF(master_chain)
        self.writeSuccessFile('Boom_collate finished writing makefiles')

    # .............................
    def _process_pav(self, mtxcol, lyr, workdir, mdl_scen_code=None, 
                            prj_scen_code=None):
        """
        @todo: allow solr query using scenario codes and/or lyr.squid
        """
        rules = []
        mtxcol.postToSolr = False
        mtxcol.shapegrid = self.shapegrid
        
        intersect_rules = mtxcol.computeMe(workDir=workdir)
        rules.append(intersect_rules)
                
        # Save new, temp intersection filenames for matrix concatenation
        # mtxcol.computeMe uses mtxcol.getTargetFilename()
        # TODO: Use consistent file construction method, 
        #       i.e. LmServer.common.datalocator.EarlJr.createBasename or
        #            self._getTempFinalFilenames()
        pavFname = os.path.join(workdir, self.getTargetFilename())
        return pavFname 


    # .............................
    def _getPamAssembleRules(self, workdir, pam, filter_desc):
        rules = []
        colFilenames = []
        
        # IFF this is a matrix of SDM projections
        mdl_scen_code = prj_scen_code = None
        colPrjPairs = self._scribe.getSDMColumnsForMatrix(pam.getId(), 
                                returnColumns=True, returnProjections=True)
        if len(colPrjPairs) > 0:
            self._scribe.log.info('Adding rules for PAM assembly of SDM matrix {}, {}'
                              .format(pam.getId(), filter_desc))
            _, prj = colPrjPairs[0]
            mdl_scen_code = prj.modelScenarioCode
            prj_scen_code = prj.projScenarioCode
            for mtxcol, prj in colPrjPairs:
                if JobStatus.failed(prj.status):
                    mtxcol.updateStatus(JobStatus.DEPENDENCY_ERROR)
                    self._scribe.updateObject(mtxcol)
                elif JobStatus.incomplete(prj.status):
                    raise LMError('sdmproject {}, layerid {}, dependency unfinished'
                                  .format(prj.objId, prj.getId()))
                elif prj.minVal == prj.maxVal and prj.minVal == 0.0:
                    print('No prediction for sdmproject {}, layerid {}, skipping'
                          .format(prj.objId, prj.getId()))
                else:
                    mtxcol_fname = self._process_pav(mtxcol, prj, workdir,
                                                     mdl_scen_code=mdl_scen_code, 
                                                     prj_scen_code=prj_scen_code)
                    if mtxcol_fname:
                        colFilenames.append(mtxcol_fname)
        else:
            mtxcols = self._scribe.getColumnsForMatrix(pam.getId())
            self._scribe.log.info('Adding rules for PAM assembly of matrix {}, {}'
                              .format(pam.getId(), filter_desc))
            for mtxcol in mtxcols:
                mtxcol_fname = self._process_pav(mtxcol, mtxcol.layer, workdir)
                if mtxcol_fname:
                    colFilenames.append(mtxcol_fname)

        # Concatenate PAM
#         pamRules = pam.getConcatAndStockpileRules(colFilenames, workDir=workdir)
        ws_pam_fname, _ = self._getTempFinalFilenames(workdir, pam)
        concat_cmd = ConcatenateMatricesCommand(colFilenames, '1', ws_pam_fname)
        rules.append(concat_cmd.getMakeflowRule())

        # Save PAM
        ass_success_fname = ws_pam_fname + '.success'
        pam_save_cmd = StockpileCommand(ProcessType.CONCATENATE_MATRICES, 
                                        pam.getId(), ass_success_fname, 
                                        ws_pam_fname, status=JobStatus.COMPLETE)
        rules.append(pam_save_cmd.getMakeflowRule(local=True))

        self._scribe.log.info('  Added rules to save {} pam columns into matrix {}'
                      .format(len(colFilenames), pam.getId()))
        return rules, ass_success_fname
     
    # .............................
    def _getPamAssembleRulesOld(self, workdir, pam, filter_desc):
        rules = []
        # Create MFChain for this GPAM
        self._scribe.log.info('Adding rules for PAM assembly of matrix {}, {}'
                          .format(pam.getId(), filter_desc))
        colFilenames = []

        colPrjPairs = self._scribe.getSDMColumnsForMatrix(pam.getId(), 
                                   returnColumns=True, returnProjections=True)
        for mtxcol, prj in colPrjPairs:
            if JobStatus.failed(prj.status):
                mtxcol.updateStatus(JobStatus.DEPENDENCY_ERROR)
                self._scribe.updateObject(mtxcol)
            elif JobStatus.incomplete(prj.status):
                raise LMError('sdmproject {}, layerid {}, dependency unfinished'
                              .format(prj.objId, prj.getId()))
            elif prj.minVal == prj.maxVal and prj.minVal == 0.0:
                print('No prediction for sdmproject {}, layerid {}, skipping'
                      .format(prj.objId, prj.getId()))
            else:
                mtxcol.postToSolr = False
                mtxcol.processType = ProcessType.INTERSECT_RASTER
                mtxcol.shapegrid = self.boomGridset.getShapegrid()
                mtxcol.updateStatus(JobStatus.INITIALIZE)
                self._scribe.updateObject(mtxcol)
                # TODO: ? Assemble commands to pull PAV from Solr   
                # TODO: ? or Remove intersect from sweepconfig
                lyrRules = mtxcol.computeMe(workDir=workdir)
                rules.extend(lyrRules)
                
                # TODO: Why create a subdir in workdir for projection? Just 
                # touching for dependency = ready status?
                lyrbasename = os.path.splitext(prj.getRelativeDLocation())[0]
                prj_target_dir = os.path.join(workdir, lyrbasename)
                prj_touch_fname = os.path.join(prj_target_dir, 'touch.out')
                prj_stat_filename = os.path.join(prj_target_dir, lyrbasename+'.status')
                           
                # TODO: is touch necessary prior to echo to file?     
                touch_cmd = LmTouchCommand(prj_touch_fname)                
                rules.append(touch_cmd.getMakeflowRule(local=True))
                
                touch_stat_cmd = SystemCommand('echo', '{} > {}'
                                               .format(JobStatus.COMPLETE, 
                                                       prj_stat_filename),
                                               inputs=[prj_touch_fname],
                                               outputs=[prj_stat_filename])
                rules.append(touch_stat_cmd.getMakeflowRule(local=True))
                
                # Save new, temp intersection filenames for matrix concatenation
                # mtxcol.computeMe uses mtxcol.getTargetFilename()
                # TODO: Replace with consistent file construction from 
                #       LmServer.common.datalocator.EarlJr.createBasename!
                mtxcol_fname = os.path.join(prj_target_dir, mtxcol.getTargetFilename())
                colFilenames.append(mtxcol_fname)            

        # Concatenate PAM
        ws_pam_fname, _ = self._getTempFinalFilenames(workdir, pam)
        concat_cmd = ConcatenateMatricesCommand(colFilenames, '1', ws_pam_fname)
        rules.append(concat_cmd.getMakeflowRule())

        # Save PAM
        ass_success_fname = ws_pam_fname + '.success'
        pam_save_cmd = StockpileCommand(ProcessType.CONCATENATE_MATRICES, 
                                        pam.getId(), ass_success_fname, 
                                        ws_pam_fname, status=JobStatus.COMPLETE)
        rules.append(pam_save_cmd.getMakeflowRule(local=True))

        self._scribe.log.info('  Added rules to save {} pam columns into matrix {}'
                      .format(len(colFilenames), pam.getId()))
        return rules, ass_success_fname
    
    # ...............................................
    def _findOrAddPAMStatMatrix(self, pam, mtx_type, mtx_key, filter_desc, workdir):
        desc = 'Computed matrix {} for PAM {}, {}'.format(mtx_key, pam.getId(),
                                                          filter_desc)
        meta = {ServiceObject.META_DESCRIPTION: desc,
                ServiceObject.META_KEYWORDS: [mtx_key]}
        mtx = LMMatrix(None, matrixType=mtx_type, 
                       gcmCode=pam.gcmCode, altpredCode=pam.altpredCode, 
                       dateCode=pam.dateCode, metadata=meta, userId=self.userId, 
                       gridset=self.boomGridset, 
                       status=JobStatus.GENERAL, 
                       statusModTime=dt.gmt().mjd)
        mtx = self._scribe.findOrInsertMatrix(mtx)
        ws_mtx_fname, _ = self._getTempFinalFilenames(workdir, pam, prefix=mtx_key)
        success_fname = ws_mtx_fname + '.success'

        return mtx, ws_mtx_fname, success_fname
        
    # ...............................................
    def _getTempFinalFilenames(self, workdir, obj, prefix=None):
        try:
            fname = obj.getDLocation()
            pth, basename = os.path.split(fname)
        except:
            raise LMError('{} must implement getDLocation'.format(type(obj)))
        if prefix is None:
            wsfname = os.path.join(workdir, basename)
        else:
            fname = os.path.join(pth, '{}_{}'.format(prefix, basename))
            wsfname = os.path.join(workdir, '{}_{}'.format(prefix, basename))
        return wsfname, fname

    # ...............................................
    def _getCopyDataToWorkdirRule(self, workdir, obj):
        # Copy object into workspace if it does not exist
        wsfname, fname = self._getTempFinalFilenames(workdir, obj)
        touch_cmd = LmTouchCommand(os.path.join(workdir, 'touch.out'))
        copy_cmd= SystemCommand('cp', 
                                '{} {}'.format(fname, wsfname), 
                                inputs=[fname], 
                                outputs=[wsfname])
        tchcp_pam_cmd = ChainCommand([touch_cmd, copy_cmd])
        rule = tchcp_pam_cmd.getMakeflowRule(local=True)
        return rule, wsfname

    # ...............................................
    def _getPamCalcRules(self, workdir, pam, ass_success_fname, filter_desc):
        # TODO: Site covariance, species covariance, schluter
        rules = []

#         # Copy PAM into workspace if necessary 
#         tchcp_pam_rule, ws_pam_fname = self._getCopyDataToWorkdirRule(workdir, pam)
#         rules.append(tchcp_pam_rule)
        ws_pam_fname, _ = self._getTempFinalFilenames(workdir, pam)
        
        # Copy encoded tree into workspace
        wstree_fname = None
        if self.tree:
            tchcp_tree_rule, wstree_fname = self._getCopyDataToWorkdirRule(workdir, self.tree)
            rules.append(tchcp_tree_rule)    

        # Sites stats outputs
        sites_mtx, sites_fname, sites_success_fname  = \
            self._findOrAddPAMStatMatrix(pam, MatrixType.SITES_OBSERVED, 
                                         'SITES_OBSERVED', filter_desc, workdir)
        # Species stats outputs
        species_mtx, species_fname, species_success_fname = \
            self._findOrAddPAMStatMatrix(pam, MatrixType.SPECIES_OBSERVED, 
                                         'SPECIES_OBSERVED', filter_desc, workdir)        
        # Diversity stats outputs
        div_mtx, div_fname, div_success_fname = \
            self._findOrAddPAMStatMatrix(pam, MatrixType.DIVERSITY_OBSERVED, 
                                         'DIVERSITY_OBSERVED', filter_desc, workdir)
        
        # Calulate multi-species statistics
        stats_cmd = CalculateStatsCommand(ws_pam_fname, sites_fname, 
                    species_fname, div_fname, treeFilename=wstree_fname)
        # add dependency of PAM assembly before calculate possible
        stats_cmd.inputs.append(ass_success_fname)
        rules.append(stats_cmd.getMakeflowRule())
        
        # Save multi-species sites outputs
        sites_save_cmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
                    sites_mtx.getId(), sites_success_fname, sites_fname)
        rules.append(sites_save_cmd.getMakeflowRule(local=True))
        
        # Save multi-species species outputs
        species_save_cmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
                    species_mtx.getId(), species_success_fname, species_fname)
        rules.append(species_save_cmd.getMakeflowRule(local=True))
        
        # Save multi-species diversity outputs
        div_save_cmd = StockpileCommand(ProcessType.RAD_CALCULATE, 
                    div_mtx.getId(), div_success_fname, div_fname)
        rules.append(div_save_cmd.getMakeflowRule(local=True))
        
        return rules
#         return rules, [sites_success_fname, species_success_fname, 
#                        div_success_fname]

    # ............................................
    def _getAncestralRules(self, workdir, pam):
        if not self.tree:
            return []
        # else
        rules = []
        # Copy encoded tree into workspace
        tchcp_tree_rule, wstree_fname = self._getCopyDataToWorkdirRule(workdir, 
                                                                       self.tree)
        rules.append(tchcp_tree_rule)

#         # Copy PAM into workspace if necessary 
#         tchcp_pam_rule, ws_pam_fname = self._getCopyDataToWorkdirRule(workdir, pam)
#         rules.append(tchcp_pam_rule)
        ws_pam_fname, _ = self._getTempFinalFilenames(workdir, pam)
        
            
        ancpam_mtx, ws_ancpam_fname, ancpam_success_fname = \
                self._findOrAddPAMStatMatrix(pam, MatrixType.ANC_PAM, 'ANC_PAM', 
                                             'Ancestral', workdir)
        # Create ancestral pam
        ancpam_cmd = CreateAncestralPamCommand(ws_pam_fname, wstree_fname, 
                                               ws_ancpam_fname)
        rules.append(ancpam_cmd.getMakeflowRule())
        # Save ancestral pam
        ancpam_save_cmd = StockpileCommand(ProcessType.RAD_CALCULATE,
                                            ancpam_mtx.getId(),
                                            ancpam_success_fname,
                                            ws_ancpam_fname)
        rules.append(ancpam_save_cmd.getMakeflowRule(local=True))
        return rules
#         return rules, ancpam_success_fname
    
    # ............................................
    def _getMCPARules(self, workdir, pam, biogeo_hyp, filter_desc, numPermutations):
        rules = []
        
        # Copy BiogeographicHypotheses into workspace if necessary 
        tchcp_bgh_rule, ws_bgh_fname = self._getCopyDataToWorkdirRule(workdir, biogeo_hyp)
        rules.append(tchcp_bgh_rule)    

        # Copy PAM into workspace if necessary 
#         tchcp_pam_rule, ws_pam_fname = self._getCopyDataToWorkdirRule(workdir, pam)
#         rules.append(tchcp_pam_rule)
        ws_pam_fname, _ = self._getTempFinalFilenames(workdir, pam)
            
        # TODO: Do we need to resolvePolytomies??
#         if self.tree.isBinary() and (not self.tree.hasBranchLengths() 
#                                      or self.tree.isUltrametric()):
        # Copy Tree into workspace if necessary 
        tchcp_tree_rule, ws_tree_fname = self._getCopyDataToWorkdirRule(workdir, self.tree)
        rules.append(tchcp_tree_rule)
                
        # Sync PAM and tree
        prune_pam_fname = os.path.join(workdir, 'prunedPAM'+LMFormat.MATRIX.ext)
        prune_tree_fname = os.path.join(workdir, 'prunedTree'+LMFormat.NEXUS.ext)
        prune_meta_fname = os.path.join(workdir, 'prunedMeta'+LMFormat.JSON.ext)
                
        # Need Pam and Tree
        syncCmd = SyncPamAndTreeCommand(ws_pam_fname, prune_pam_fname,
                    ws_tree_fname, prune_tree_fname, prune_meta_fname)
        rules.append(syncCmd.getMakeflowRule())
                
        # Encode tree
        enc_tree_fname = os.path.join(workdir, 'tree'+LMFormat.MATRIX.ext)
        enc_tree_cmd = EncodePhylogenyCommand(prune_tree_fname, prune_pam_fname, 
                                            enc_tree_fname)
        rules.append(enc_tree_cmd.getMakeflowRule())
        
        # Get correct GRIM matching PAM
        grim = self.boomGridset.getGRIMForCodes(pam.gcmCode, pam.altpredCode, 
                                                pam.dateCode)
        # Copy GRIM into workspace if necessary 
        tchcp_grim_rule, ws_grim_fname = self._getCopyDataToWorkdirRule(workdir, grim)
        rules.append(tchcp_grim_rule)    
                            
        # Get MCPA matrices
        mcpa_out_mtx, ws_mcpa_out_fname, mcpa_out_success_fname = \
                self._findOrAddPAMStatMatrix(pam, MatrixType.MCPA_OUTPUTS, 'MCPA_OUTPUTS', 
                                             filter_desc, workdir)
        
        # Get workspace filenames
        ws_obs_filename = os.path.join(workdir, 'obs_cor'+LMFormat.MATRIX.ext)
        ws_obs_f_filename = os.path.join(workdir, 'obs_f'.format(LMFormat.MATRIX.ext))
                
        # MCPA observed command
        mcpa_obs_cmd = McpaRunCommand(ws_pam_fname, enc_tree_fname,
                                      ws_grim_fname, ws_bgh_fname,
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
        
            rand_f_mtx_filename = os.path.join(workdir, 'f_mtx_rand{}{}'
                                               .format(i, LMFormat.MATRIX.ext))
            rand_f_mtxs.append(rand_f_mtx_filename)
            rand_cmd = McpaRunCommand(ws_pam_fname, enc_tree_fname,
                                              ws_grim_fname, ws_bgh_fname,
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
            agg_filename = os.path.join(workdir, 'f_rand_agg{}{}'
                                        .format(i, LMFormat.MATRIX.ext))
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
                  workdir, 'f_rand_agg{}{}'.format(i, 
                                                                  LMFormat.MATRIX.ext))
            # Create a concatenate command for this group
            concat_cmd = ConcatenateMatricesCommand(
                  rand_f_mtxs[:group_size], axis=2, agg_filename)
            rules.append(concat_cmd.getMakeflowRule())
        else:
            f_rand_agg_filename = rand_f_mtxs[0]
        """
        
        # TODO: Correct P-Values
        out_p_values_filename = os.path.join(workdir, 'p_values'+LMFormat.MATRIX.ext)
        out_bh_values_filename = os.path.join(workdir, 'bh_values'+LMFormat.MATRIX.ext)
        
        # TODO: Use ws_obs_filename?
        corr_p_cmd = McpaCorrectPValuesCommand(ws_obs_f_filename,
                                               out_p_values_filename,
                                               out_bh_values_filename,
                                               rand_f_mtxs)
        rules.append(corr_p_cmd.getMakeflowRule())
        
        # Assemble final MCPA matrix
        mcpa_concat_cmd = ConcatenateMatricesCommand([ws_obs_filename, 
                                                      out_p_values_filename, 
                                                      out_bh_values_filename], 
                                                      2, ws_mcpa_out_fname)
        rules.append(mcpa_concat_cmd.getMakeflowRule())
        
        # Save matrix
        mcpaOutStockpileCmd = StockpileCommand(ProcessType.MCPA_ASSEMBLE,
                                               mcpa_out_mtx.getId(), 
                                               mcpa_out_success_fname, 
                                               ws_mcpa_out_fname, 
                                               metadataFilename=prune_meta_fname)
        rules.append(mcpaOutStockpileCmd.getMakeflowRule(local=True))

        return rules

    # ...............................................
    def _write_update_MF(self, mfchain):
        mfchain.write()
        # Give lmwriter rw access (this script may be run as root)
        self._fixPermissions(mfchain.getDLocation())
        # Set as ready to go
        mfchain.updateStatus(JobStatus.INITIALIZE)
        self._scribe.updateObject(mfchain)
        try:
            self._scribe.log.info('  Wrote Makeflow {} for {} for gridset {}'
                .format(mfchain.objId, 
                        mfchain.mfMetadata[MFChain.META_DESCRIPTION], 
                        mfchain.mfMetadata[MFChain.META_GRIDSET]))
        except:
            self._scribe.log.info('  Wrote Makeflow {}'.format(mfchain.objId))
        return mfchain

    # ...............................................
    def _fixPermissions(self, fname):
        if isLMUser:
            print('Permissions created correctly by LMUser')
        else:
            dirname = os.path.dirname(self.configFname)
            stats = os.stat(dirname)
            # item 5 is group id; get for lmwriter
            gid = stats[5]
            try:
                os.chown(fname, -1, gid)
                os.chmod(fname, 0664)
            except Exception, e:
                print('Failed to fix permissions on {}'.format(fname))

    # ...............................................
    def writeSuccessFile(self, message='Success'):
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
# ##########################################################################
# $PYTHON LmDbServer/boom/boom_collate.py  --config_file=/share/lm/data/archive/taffy/heuchera_global_10min.ini --success_file=/share/lm/data/archive/taffy/heuchera_global_10min.collate.success
from LmDbServer.boom.boom_collate import *

import logging
import mx.DateTime as dt
import os, sys, time
import signal

from LmBackend.command.common import (ChainCommand, SystemCommand, 
                                      ConcatenateMatricesCommand)
from LmBackend.command.server import (LmTouchCommand, StockpileCommand)
from LmBackend.command.multi import (CalculateStatsCommand, 
        CreateAncestralPamCommand, SyncPamAndTreeCommand, EncodePhylogenyCommand,
        McpaRunCommand, McpaCorrectPValuesCommand)
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, LM_USER, MatrixType, 
          LMFormat, ProcessType, SERVER_BOOM_HEADING, SERVER_PIPELINE_HEADING)

from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LMFileType, PUBLIC_ARCHIVE_NAME, 
                                         Priority) 
from LmServer.common.localconstants import PUBLIC_USER 
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.tree import Tree


# TODO: Move these to localconstants
NUM_RAND_GROUPS = 30
NUM_RAND_PER_GROUP = 2

configFname = '/share/lm/data/archive/taffy/heuchera_global_10min.ini'
successFname = '/share/lm/data/archive/taffy/heuchera_global_10min.ini.collate.success'


logname = 'boom_collate_test'
logger = ScriptLogger(logname, level=logging.INFO)
boomer = BoomCollate(configFname, successFname, log=logger)
boomer.initializeMe()

# ##########################################################################
self = boomer
numPermutations=500

master_chain = self._createMasterMakeflow()
workdir = master_chain.getRelativeDirectory()
globalPams = self.boomGridset.getAllPAMs()

# for pam in globalPams:
pam = globalPams[0]
filter_desc = 'Filters: GCM {}, Altpred {}, Date {}'.format(
                    pam.gcmCode, pam.altpredCode, pam.dateCode)

ass_rules, ass_success_fname = self._getPamAssembleRules(workdir, 
                                                    pam, filter_desc)
master_chain.addCommands(ass_rules)

# PamCalc depends on success of PamAssemble (ass_success_fname)
calc_rules = self._getPamCalcRules(workdir, pam, ass_success_fname, 
                                   filter_desc)
master_chain.addCommands(calc_rules)

# if self.tree is not None:
anc_rules = self._getAncestralRules(workdir, pam)
master_chain.addCommands(anc_rules)

biogeo_hyp = self.boomGridset.getBiogeographicHypotheses()
# if len(biogeo_hyp) > 1: 
biogeo_hyp = biogeo_hyp[0]

mcpa_rules = self._getMCPARules(workdir, pam, biogeo_hyp, 
                                filter_desc, numPermutations)
master_chain.addCommands(mcpa_rules)

master_chain = self._write_update_MF(master_chain)
self.writeSuccessFile('Boom_collate finished writing makefiles')


    # ...............................................
mfchain = master_chain

mfchain.updateStatus(JobStatus.INITIALIZE)
self._scribe.updateObject(mfchain)
self._scribe.log.info('  Wrote Makeflow {} for {} for gridset {}'
    .format(mfchain.objId, 
            mfchain.mfMetadata[MFChain.META_DESCRIPTION], 
            mfchain.mfMetadata[MFChain.META_GRIDSET]))

dirname = os.path.dirname(self.configFname)
stats = os.stat(dirname)
# item 5 is group id; get for lmwriter
gid = stats[5]
os.chown(fname, -1, gid)
os.chmod(fname, 0664)

# ##########################################################################

# boomer.processMultiSpecies()
/share/lm/data/archive/taffy2/pam_1410.lmm

# ##########################################################################

    # ...............................................
    def processMultiSpecies(self, numPermutations=500):
        # Master makeflow for 
        master_chain = self._createMasterMakeflow()
        workdir = master_chain.getRelativeDirectory()
        globalPams = self.boomGridset.getAllPAMs()
        # TODO: need mdl/prj scenario to query Solr
        for pam in globalPams:
            mtxcol_count = self._scribe.countMatrixColumns(matrixId=pam.getId())
            if mtxcol_count == 0:
                self.log.error('Pam {} has no matrixColumns'.format(pam.getId()))
                break
            filter_desc = ('Filters: GCM {}, Altpred {}, Date {}'
                           .format(pam.gcmCode, pam.altpredCode, pam.dateCode))
            # Add intersect and concatenate rules for this PAM
            ass_rules, ass_success_fname = self._getPamAssembleRules(workdir, 
                                                            pam, filter_desc)
            master_chain.addCommands(ass_rules)
            
            # PamCalc depends on success of PamAssemble (ass_success_fname)
            calc_rules = self._getPamCalcRules(workdir, pam, ass_success_fname, 
                                               filter_desc)
            master_chain.addCommands(calc_rules)
            
            if self.tree is not None:
                anc_rules = self._getAncestralRules(workdir, pam)
                master_chain.addCommands(anc_rules)
            
                biogeo_hyp = self.boomGridset.getBiogeographicHypotheses()
                # TODO: handle > 1 Biogeographic Hypotheses
                if len(biogeo_hyp) > 0: 
                    biogeo_hyp = biogeo_hyp[0]
                    mcpa_rules = self._getMCPARules(workdir, pam, biogeo_hyp, 
                                                    filter_desc, numPermutations)
                    master_chain.addCommands(mcpa_rules)

        master_chain = self._write_update_MF(master_chain)
        self.writeSuccessFile('Boom_collate finished writing makefiles')
        
    # ...............................................
    def _getPAVs(self, model_scen_code, prj_scen_code):
        solr_data = queryArchiveIndex(gridSetId=self.boomGridset.getId(), 
                                      modelScenarioCode=model_scen_code, 
                                      projectionScenarioCode=prj_scen_code, 
                                      userId=self.userId)
        for doc in solr_data:
            pass
        return solr_data
    
    # .............................
    def _process_sdm_column(self, mtxcol, prj, workdir):
        rules = []
        if JobStatus.failed(prj.status):
            mtxcol.updateStatus(JobStatus.DEPENDENCY_ERROR)
            self._scribe.updateObject(mtxcol)
        elif JobStatus.incomplete(prj.status):
            raise LMError('sdmproject {}, layerid {}, dependency unfinished'
                          .format(prj.objId, prj.getId()))
        elif prj.minVal == prj.maxVal and prj.minVal == 0.0:
            print('No prediction for sdmproject {}, layerid {}, skipping'
                  .format(prj.objId, prj.getId()))
        else:
            mtxcol.postToSolr = False
            mtxcol.processType = ProcessType.INTERSECT_RASTER
            mtxcol.shapegrid = self.boomGridset.getShapegrid()
#             mtxcol.updateStatus(JobStatus.INITIALIZE)
#             self._scribe.updateObject(mtxcol)
            # TODO: ? Assemble commands to pull PAV from Solr   
            # TODO: ? or Remove intersect from sweepconfig
            lyrRules = mtxcol.computeMe(workDir=workdir)
            rules.extend(lyrRules)
            
            # TODO: Why create a subdir in workdir for projection? Just 
            # touching for dependency = ready status?
            lyrbasename = os.path.splitext(prj.getRelativeDLocation())[0]
            prj_target_dir = os.path.join(workdir, lyrbasename)
            prj_touch_fname = os.path.join(prj_target_dir, 'touch.out')
            prj_stat_filename = os.path.join(prj_target_dir, lyrbasename+'.status')
                       
            # TODO: is touch necessary prior to echo to file?     
            touch_cmd = LmTouchCommand(prj_touch_fname)                
            rules.append(touch_cmd.getMakeflowRule(local=True))
            
            touch_stat_cmd = SystemCommand('echo', '{} > {}'
                                           .format(JobStatus.COMPLETE, 
                                                   prj_stat_filename),
                                           inputs=[prj_touch_fname],
                                           outputs=[prj_stat_filename])
            rules.append(touch_stat_cmd.getMakeflowRule(local=True))
            
            # Save new, temp intersection filenames for matrix concatenation
            # mtxcol.computeMe uses mtxcol.getTargetFilename()
            # TODO: Replace with consistent file construction from 
            #       LmServer.common.datalocator.EarlJr.createBasename!
            mtxcol_fname = os.path.join(prj_target_dir, mtxcol.getTargetFilename())
            return mtxcol_fname
     
    

"""

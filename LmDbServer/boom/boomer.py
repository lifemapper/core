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

from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.lmconstants import JobStatus, LM_USER

from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LMFileType, PUBLIC_ARCHIVE_NAME) 
from LmServer.common.localconstants import PUBLIC_USER 
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain
from LmServer.tools.cwalken import ChristopherWalken


SPUD_LIMIT = 200

# .............................................................................
class Boomer(LMObject):
    """
    Class to iterate with a ChristopherWalken through a sequence of species data
    creating individual species (Spud) MFChains, multi-species (Bushel) MFChains
    with SPUD_LIMIT number of species and aggregated by projection scenario, 
    and a master (MasterPotatoHead) MFChain to identify completion of all bushels.
    until it is complete.  If the daemon is interrupted, it will write out the 
    current MFChains, and pick up where it left off to create new MFChains for 
    unprocessed species data.
    @todo: Next instance of boom.Walker will create new MFChains, but add data
    to the existing Global PAM matrices.  
    """
    # .............................
    def __init__(self, configFname, successFname, log=None):      
        self.name = self.__class__.__name__.lower()
        # Logfile
        if log is None:
            secs = time.time()
            timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}'.format(self.name, timestamp)
            log = ScriptLogger(logname, level=logging.INFO)
        self.log = log
        
        self.configFname = configFname
        self._successFname = successFname
        self.assemblePams = None
        # Send Database connection
        self._scribe = BorgScribe(self.log)
        # iterator tool for species
        self.christopher = None

#         # Dictionary of {scenCode: (potatoChain, triagePotatoFile)}
#         self.potatoes = None

        # MFChain for lots of spuds 
        self.potatoBushel = None
        self.squidNames = None
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
              
        try:
            self.christopher = ChristopherWalken(self.configFname,
                                                 scribe=self._scribe)
            self.christopher.initializeMe()
        except Exception, e:
            raise LMError(currargs='Failed to initialize Chris with config {} ({})'
                         .format(self.configFname, e))
        try:
            self.gridsetId = self.christopher.boomGridset.getId()
        except:
            self.log.warning('Exception getting christopher.boomGridset id!!')
        if self.gridsetId is None:
            self.log.warning('Missing christopher.boomGridset id!!')
        
        self.assemblePams = self.christopher.assemblePams
        self.priority = self.christopher.priority
        # Start where we left off 
        self.christopher.moveToStart()
        self.log.debug('Starting Chris at location {} ... '
                       .format(self.christopher.currRecnum))
        self.keepWalken = True
        
        self.squidNames = []
        # master MF chain
        self.masterPotatoHead = None
        self.potatoBushel = None
        self.rotatePotatoes()
         
    # .............................
    def processOneSpecies(self):
        try:
            self.log.info('Next species ...')
            # Get Spud rules (single-species SDM) and dict of {scencode: pavFilename}
            workdir = self.potatoBushel.getRelativeDirectory()
            squid, spudRules = self.christopher.startWalken(workdir)
            
            # TODO: Track squids
            if squid is not None:
                self.squidNames.append(squid)
            
            self.keepWalken = not self.christopher.complete
            # TODO: Master process for occurrence only? SDM only? 
            if self.assemblePams and spudRules:
                self.log.debug('Processing spud for potatoes')
               
                self.potatoBushel.addCommands(spudRules)
                # TODO: Don't write triage file, but don't delete code
                #if potatoInputs:
                #   for scencode, (pc, triagePotatoFile) in self.potatoes.iteritems():
                #      pavFname = potatoInputs[scencode]
                #      triagePotatoFile.write('{}: {}\n'.format(squid, pavFname))
                #   self.log.info('Wrote spud squid to {} triage files'
                #                 .format(len(potatoInputs)))
                #if len(self.spudArfFnames) >= SPUD_LIMIT:
                if len(self.squidNames) >= SPUD_LIMIT:
                    self.rotatePotatoes()
            self.log.info('-----------------')
        except Exception, e:
            self.log.debug('Exception {} on spud, closing ...'.format(str(e)))
            self.close()
            raise e

    # .............................
    def rotatePotatoes(self):
        # Finish up existing potatoes
        #   Write spud to Bushel
        if self.potatoBushel:
            self.log.info('Rotate potatoes ...')
            # Write the potatoBushel MFChain
            if self.potatoBushel.jobs:
                self.potatoBushel.write()
                self.potatoBushel.updateStatus(JobStatus.INITIALIZE)
                self._scribe.updateObject(self.potatoBushel)
                self.log.info('   Wrote potatoBushel {} ({} spuds)'
                              .format(self.potatoBushel.objId, len(self.squidNames)))
        
        # Create new bushel
        if not self.christopher.complete:
#             self.potatoBushel = self._createMasterMakeflow()
            self.potatoBushel = self._createBushelMakeflow()
            if self.christopher.assemblePams:
                self.log.info('Create new potatoes')
                self.squidNames = []
            
    # .............................
    def close(self):
        self.keepWalken = False
        self.log.info('Closing boomer ...')
        # Stop walken the archive and saveNextStart
        self.christopher.stopWalken()
        self.rotatePotatoes()

    # .............................
    def restartWalken(self):
        if self.christopher.complete() and self.christopher.moreDataToProcess():
            # Rename old file
            oldfname = self.christopher.weaponOfChoice.occParser.dataFname
            ts = dt.localtime().tuple()
            timestamp = '{}{:02d}{:02d}-{:02d}{:02d}'.format(ts[0], ts[1], ts[2], ts[3], ts[4])
            newfname = oldfname + '.' + timestamp
            try:
                os.rename(oldfname, newfname)
            except Exception, e:
                self.log.error('Failed to rename {} to {}'.format(oldfname, newfname))
            # Restart with next file
            self.initializeMe()

    # ...............................................
    def _createBushelMakeflow(self):
        meta = {MFChain.META_CREATED_BY: self.name,
                MFChain.META_DESCRIPTION: 'Bushel for User {}, Archive {}'
                    .format(self.christopher.userId, self.christopher.archiveName),
                MFChain.META_GRIDSET: self.gridsetId 
        }
        newMFC = MFChain(self.christopher.userId, priority=self.priority, 
                         metadata=meta, status=JobStatus.GENERAL, 
                         statusModTime=dt.gmt().mjd)
        mfChain = self._scribe.insertMFChain(newMFC, self.gridsetId)
        return mfChain

    # ...............................................
    def _createMasterPotatoHeadMakeflow(self):
        meta = {MFChain.META_CREATED_BY: self.name,
                MFChain.META_DESCRIPTION: 'MasterPotatoHead for User {}, Archive {}'
                    .format(self.christopher.userId, self.christopher.archiveName),
                MFChain.META_GRIDSET: self.gridsetId 
        }
        newMFC = MFChain(self.christopher.userId, priority=self.priority, 
                         metadata=meta, status=JobStatus.GENERAL, 
                         statusModTime=dt.gmt().mjd)
        mfChain = self._scribe.insertMFChain(newMFC, self.gridsetId)
        return mfChain

#     # .............................
#     def _addRuleToMasterPotatoHead(self, mfchain, dependencies=None, prefix='spud'):
#         """
#         @summary: Create a Spud or Potato rule for the MasterPotatoHead MF 
#         """
#         if dependencies is None:
#             dependencies = []
#            
#         targetFname = mfchain.getArfFilename(
#                                arfDir=self.potatoBushel.getRelativeDirectory(),
#                                prefix=prefix)
#         
#         origMfName = mfchain.getDLocation()
#         wsMfName = os.path.join(self.potatoBushel.getRelativeDirectory(), 
#                                 os.path.basename(origMfName))
#         
#         # Copy makeflow to workspace
#         cpCmd = SystemCommand('cp', 
#                               '{} {}'.format(origMfName, wsMfName), 
#                               inputs=[targetFname], 
#                               outputs=wsMfName)
#         
#         
#         mfCmd = SystemCommand('makeflow', 
#                               ' '.join(['-T wq', 
#                                         '-N lifemapper-{}b'.format(mfchain.getId()),
#                                         '-C {}:9097'.format(PUBLIC_FQDN),
#                                         '-X {}/worker/'.format(SCRATCH_PATH),
#                                         '-a {}'.format(wsMfName)]),
#                               inputs=[wsMfName])
#         arfCmd = LmTouchCommand(targetFname)
#         arfCmd.inputs.extend(dependencies)
#         
#         delCmd = SystemCommand('rm', '-rf {}'.format(mfchain.getRelativeDirectory()))
#         
#         mpCmd = ChainCommand([mfCmd, delCmd])
#         self.potatoBushel.addCommands([arfCmd.getMakeflowRule(local=True),
#                                        cpCmd.getMakeflowRule(local=True),
#                                        mpCmd.getMakeflowRule(local=True)])
#       
#     # .............................
#     def _addDelayRuleToMasterPotatoHead(self, mfchain):
#         """
#         @summary: Create an intermediate rule for the MasterPotatoHead MF to check
#                   for the existence of all single-species dependencies (ARF files)  
#                   of the multi-species makeflows.
#         @TODO: Replace adding all dependencies to the Potato makeflow command
#                with this Delay rule
#         @todo: When implementing this, use a ChainCommand object with a touch
#                   command and something else.  Don't use MfRule directly
#         """
#         pass

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

    # .............................
    def processAllSpecies(self):
        print('processAll with configFname = {}'.format(self.configFname))
        count = 0
        while self.keepWalken:
            self.processOneSpecies()
            count += 1
        if not self.keepWalken:
            self.close()
        self.writeSuccessFile('Boomer finished walken {} species'.format(count))



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
    boomer = Boomer(configFname, successFname, log=logger)
    boomer.initializeMe()
    boomer.processAllSpecies()
   
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
          SERVER_SDM_MASK_HEADING_PREFIX,
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
from LmServer.tools.occwoc import (GBIFWoC, UserWoC, ExistingWoC, TinyBubblesWoC)

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

config_file='/share/lm/data/archive/anon/CJ2.ini'
success_file='mf_3149/CJ2.ini.success'

boomer = Boomer(config_file, success_file, log=logger)
###############################################
self = boomer
success = self._scribe.openConnections()
self.christopher = ChristopherWalken(self.configFname,
                                                 scribe=self._scribe)
                                                 
###############################################
self = boomer.christopher
self.moreDataToProcess = False

###############################################
userId = self._getBoomOrDefault('ARCHIVE_USER', defaultValue=PUBLIC_USER)
archiveName = self._getBoomOrDefault('ARCHIVE_NAME')
archivePriority = self._getBoomOrDefault('ARCHIVE_PRIORITY')
earl = EarlJr()
boompath = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
epsg = self._getBoomOrDefault('SCENARIO_PACKAGE_EPSG', 
                              defaultValue=DEFAULT_EPSG)

useGBIFTaxonIds = False
# Get datasource and optional taxonomy source
datasource = self._getBoomOrDefault('DATASOURCE')
try:
    taxonSourceName = TAXONOMIC_SOURCE[datasource]['name']
except:
    taxonSourceName = None
   
# Expiration date for retrieved species data 
expDate = dt.DateTime(self._getBoomOrDefault('SPECIES_EXP_YEAR'), 
                      self._getBoomOrDefault('SPECIES_EXP_MONTH'), 
                      self._getBoomOrDefault('SPECIES_EXP_DAY')).mjd
occCSV, occMeta, occDelimiter, self.moreDataToProcess = \
               self._findData(datasource, boompath)

weaponOfChoice = UserWoC(self._scribe, userId, archiveName, 
                         epsg, expDate, occCSV, occMeta, 
                         occDelimiter, logger=self.log, 
                         processType=ProcessType.USER_TAXA_OCCURRENCE,
                         useGBIFTaxonomy=useGBIFTaxonIds,
                         taxonSourceName=taxonSourceName)

weaponOfChoice, expDate = self._getOccWeaponOfChoice(userId, archiveName, 
                                              epsg, boompath)
                                              
# SDM inputs
minPoints = self._getBoomOrDefault('POINT_COUNT_MIN')
algorithms = self._getAlgorithms(sectionPrefix=BoomKeys.ALG_CODE)

(mdlScen, prjScens, model_mask_base) = self._getProjParams(userId, epsg)
# Global PAM inputs
(boomGridset, intersectParams) = self._getGlobalPamObjects(userId, 
                                                      archiveName, epsg)
assemblePams = self._getBoomOrDefault('ASSEMBLE_PAMS', isBool=True)
###############################################
        

(self.userId, 
 self.archiveName, 
 self.priority, 
 self.boompath, 
 self.weaponOfChoice,
 self._obsoleteTime, 
 self.epsg, 
 self.minPoints, 
 self.algs, 
 self.mdlScen, 
 self.prjScens, 
 self.model_mask_base,
 self.boomGridset, 
 self.intersectParams, 
 self.assemblePams) = self._getConfiguredObjects()
###############################################

self.christopher.initializeMe()
self.gridsetId = self.christopher.boomGridset.getId()
self.priority = self.christopher.priority

self.christopher.moveToStart()
self.log.debug('Starting Chris at location {} ... '
               .format(self.christopher.currRecnum))
self.keepWalken = True

self.squidNames = []
# master MF chain
self.potatoBushel = None
self.rotatePotatoes()
###############################################






boomer.initializeMe()                      
chris = boomer.christopher
woc = chris.weaponOfChoice
scribe = boomer._scribe
borg = scribe._borg

workdir = boomer.potatoBushel.getRelativeDirectory()


squid, spudRules = boomer.christopher.startWalken(workdir)
boomer.squidNames.append(squid)
boomer.potatoBushel.addCommands(spudRules)

# ############# STOP and write and rotate
boomer.rotatePotatoes()

# ############# STOP completely and write
boomer.christopher.stopWalken()
boomer.rotatePotatoes()



alg = boomer.algs[0]
prj_scen = boomer.prjScens[0]


# ##########################################################################

# ##########################################################################

       
"""

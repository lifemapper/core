"""
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
import argparse
import logging
import mx.DateTime as dt
import os, sys, time

from LmBackend.common.daemon import Daemon
from LmCommon.common.lmconstants import JobStatus, OutputFormat, ProcessType
from LmCommon.common.log import DaemonLogger
from LmCommon.common.readyfile import readyFilename
from LmDbServer.common.lmconstants import BOOM_PID_FILE
from LmServer.base.lmobj import LMError
from LmServer.base.utilities import isCorrectUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import (PUBLIC_FQDN, PUBLIC_USER, 
                                            SCRATCH_PATH, APP_PATH)
from LmServer.common.lmconstants import LMFileType, PUBLIC_ARCHIVE_NAME
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.cmd import MfRule
from LmServer.legion.processchain import MFChain
from LmServer.tools.cwalken import ChristopherWalken

SPUD_LIMIT = 100

# .............................................................................
class Boomer(Daemon):
   """
   Class to iterate with a ChristopherWalken through a sequence of species data
   creating individual species (Spud) MFChains, multi-species (Potato) MFChains, 
   aggregated by projection scenario, and a master (MasterPotatoHead) MFChain
   until it is complete.  If the daemon is interrupted, it will write out the 
   current MFChains, and pick up where it left off to create new MFChains for 
   unprocessed species data.
   @todo: Next instance of boom.Walker will create new MFChains, but add data
   to the existing Global PAM matrices.  Make sure LMMatrix.computeMe handles 
   appending new PAVs or re-assembling. 
   """
   # .............................
   def __init__(self, pidfile, configFname, 
                assemblePams=True, priority=None, log=None):      
      Daemon.__init__(self, pidfile, log=log)
      self.name = self.__class__.__name__.lower()
      self.configFname = configFname
      self.assemblePams = assemblePams
      self.priority = priority
      # Send Database connection
      self._scribe = BorgScribe(self.log)
      # iterator tool for species
      self.christopher = None
      # Dictionary of {scenCode: (potatoChain, rawPotatoFile)}
      self.potatoes = None
      # MFChain for masterPotatoHead MF
      self.masterPotato = None
      # open file for writing Spud Arf filenames for Potato triage
      self.spudArfFnames = None
      # Stop indicator
      self.keepWalken = False

   # .............................
   def initialize(self):
      """
      @summary: Creates objects (OccurrenceSets, SMDModels, SDMProjections, 
                and MatrixColumns (intersections with the default grid)) 
                and job requests for their calculation.
      @note: The argument to this script/daemon contains variables to override 
             installed defaults
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
      except Exception, e:
         raise LMError(currargs='Failed to initialize Chris with config {} ({})'
                       .format(self.configFname, e))

      self.spudArfFnames = []
      # potatoes = {scencode: (potatoChain, rawPotatoFile)
      self.potatoes = {}
      # master MF chain
      self.masterPotato = None
      if self.christopher.assemblePams:
         self._rotatePotatoes()
         
   # .............................
   def run(self):
      try:
         self.christopher.moveToStart()
         self.log.debug('Starting Chris at location {} ... '
                        .format(self.christopher.currRecnum))
         self.keepWalken = True
         while self.keepWalken:
            try:
               self.log.info('Next species ...')
               # Get a Spud MFChain (single-species MF) and dict of 
               # {scencode: pavFilename}
               spud, potatoInputs = self.christopher.startWalken()
               self.keepWalken = not self.christopher.complete
               # TODO: Master process for occurrence only? SDM only? 
               if self.assemblePams and spud:
                  self.log.debug('Processing spud for potatoes')
                  # Add MF rule for Spud execution to Master MF
                  self._addRuleToMasterPotatoHead(spud, prefix='spud')
                  # Gather species ARF dependency to delay start of multi-species MF
                  spudArf = spud.getArfFilename(prefix='spud')
                  self.spudArfFnames.append(spudArf)
                  # Add PAV outputs to raw potato files for triage input
                  squid = spud.mfMetadata[MFChain.META_SQUID]
                  if potatoInputs:
                     for scencode, (pc, rawPotatoFile) in self.potatoes.iteritems():
                        pavFname = potatoInputs[scencode]
                        rawPotatoFile.write('{}: {}\n'.format(squid, pavFname))
                        self.log.info('Wrote spud squid to arf files')
                  if len(self.spudArfFnames) >= SPUD_LIMIT:
                     self._rotatePotatoes()
            except Exception, e:
               self.log.debug('Exception {}; If not already, stop walken'
                              .format(str(e)))
               self.christopher.stopWalken()
               raise e
            time.sleep(10)
      finally:
         self.log.debug('Christopher is finally done walken')
         self._rotatePotatoes()
         self.onShutdown()
    
   # .............................
   def _rotatePotatoes(self):
      # Finish up existing potatoes
      #   Write triage rule to each. then write potato to Master Potato      
      if self.potatoes:
         self.log.info('Rotate potatoes ...')
         # Write each potato MFChain, then add the MFRule to execute it to the Master
         for scencode, (potatoChain, rawPotatoFile) in self.potatoes.iteritems():
            # Close this potato input file
            rawPotatoFile.close()
            # Create triage command for potato inputs, add to MF chain
            mtx = self.christopher.globalPAMs[scencode]
            
            targetDir = potatoChain.getRelativeDirectory()
            triageIn = os.path.join(targetDir, rawPotatoFile.name)
            triageOut = os.path.join(targetDir, 
                         potatoChain.getTriageFilename(prefix='mashedPotato'))
            rules = mtx.computeMe(triageIn, triageOut, workDir=targetDir)
            potatoChain.addCommands(rules)
            potatoChain.write()
   #          potatoChain.updateStatus(JobStatus.INITIALIZE)
            self._scribe.updateObject(potatoChain)
            # Add this potato to MasterPotato
            self._addRuleToMasterPotatoHead(potatoChain, 
                                            dependencies=self.spudArfFnames, 
                                            prefix='potato')
            self.log.info('  Wrote and added {} potato to Master'.format(scencode))
         # Write the masterPotatoHead MFChain
         self.masterPotato.write()
         self.masterPotato.updateStatus(JobStatus.INITIALIZE)
         self._scribe.updateObject(self.masterPotato)
         self.log.info('   Completed MasterPotato ({} potatoes and {} spuds)'
                       .format(len(self.potatoes), len(self.spudArfFnames)))
      
      # Create new potatoes
      if not self.christopher.complete:
         self.log.info('Create new potatoes')
         # Initialize new potatoes, MasterPotato
         # potatoes = {scencode: (potatoChain, rawPotatoFile)
         self.spudArfFnames = []
         self.potatoes = self._createPotatoMakeflows()
         self.masterPotato = self._createMasterMakeflow()
            
   # .............................
   def onUpdate(self):
      self.log.debug("Update signal caught!")
       
   # .............................
   def onShutdown(self):
      self.keepWalken = False
      self.log.debug('Shutdown! If not already, stop walken')
      # Stop walken the archive and saveNextStart
      self.christopher.stopWalken()
      Daemon.onShutdown(self)

# ...............................................
   def _createMasterMakeflow(self):
      meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
              MFChain.META_DESC: 'MasterPotatoHead for User {}, Archive {}'
      .format(self.christopher.userId, self.christopher.archiveName)}
      newMFC = MFChain(self.christopher.userId, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=dt.gmt().mjd)
      mfChain = self._scribe.insertMFChain(newMFC)
      return mfChain

# ...............................................
   def _createPotatoMakeflows(self):
      """
      @summary: Create and return dict of {code: potatoChain, rawPotatoFile} 
         {scenariocode: (makeflow chain, (open)file for squid/PAVfile inputs)}
      @return 
      """
      potatoes = {}
      for scencode in self.christopher.globalPAMs.keys():
         # Create MFChain for this GPAM
         meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
                 MFChain.META_DESC: 'Potato for User {}, Archive {}, Scencode {}'
         .format(self.christopher.userId, self.christopher.archiveName, scencode)}
         newMFC = MFChain(self.christopher.userId, priority=self.priority, 
                          metadata=meta, status=JobStatus.GENERAL, 
                          statusModTime=dt.gmt().mjd)
         potatoChain = self._scribe.insertMFChain(newMFC)
         # Get rawPotato input file from MFChain
         rawPotatoFname = potatoChain.getTriageFilename(prefix='rawPotato')
         if not readyFilename(rawPotatoFname, overwrite=True):
            raise LMError(currargs='{} is not ready for write (overwrite=True)'
                              .format(rawPotatoFname))
         try:
            rawPotatoFile = open(rawPotatoFname, 'w')
         except Exception, e:
            raise LMError(currargs='Failed to open {} for writing ({})'
                          .format(rawPotatoFname, str(e)))
         potatoes[scencode] = (potatoChain, rawPotatoFile) 
      return potatoes

   # .............................
   def _addRuleToMasterPotatoHead(self, mfchain, dependencies=[], prefix='spud'):
      """
      @summary: Create a Spud or Potato rule for the MasterPotatoHead MF 
      """
      targetFname = mfchain.getArfFilename(prefix=prefix)
      outputFname = mfchain.getDLocation()
      # Add MF doc (existence) as dependency to run MF doc
      
      #TODO: Add this back with a relative path for makeflow files if needed
      #dependencies.append(outputFname)
      cmdArgs = ['LOCAL makeflow',
                 '-T wq', 
                 '-N lifemapper-{}b'.format(mfchain.getId()),
                 '-C {}:9097'.format(PUBLIC_FQDN),
                 '-X {}/worker/'.format(SCRATCH_PATH),
                 '-a {}'.format(outputFname)]
      mfCmd = ' '.join(cmdArgs)
      
      touchScriptFname = os.path.join(APP_PATH, 
                                      ProcessType.getTool(ProcessType.TOUCH))
      arfCmdArgs = [
         os.getenv('PYTHON'),
         touchScriptFname,
         targetFname
      ]
      arfCmd = ' '.join(arfCmdArgs)
      
      #arfCmd = 'touch {}'.format(targetFname)
      cmd = '{} ; {}'.format(mfCmd, arfCmd)
      # Create a rule from the MF and Arf file creation
      # TODO: Replace these dependencies with Delay rule
      rule = MfRule(cmd, [targetFname], dependencies=dependencies)
      self.masterPotato.addCommands([rule])

   # .............................
   def _addDelayRuleToMasterPotatoHead(self, mfchain):
      """
      @summary: Create an intermediate rule for the MasterPotatoHead MF to check
                for the existence of all single-species dependencies (ARF files)  
                of the multi-species makeflows.
      @TODO: Replace adding all dependencies to the Potato makeflow command
             with this Delay rule
      """
      targetFname = self.masterPotato.getArfFilename(prefix='goPotato')
      cmdArgs = ['LOCAL checkArfFiles'].extend(self.spudArfFnames)
      mfCmd = ' '.join(cmdArgs)
      arfCmd = 'touch {}'.format(targetFname)
      cmd = '{} ; {}'.format(mfCmd, arfCmd)
      # Create a rule from the MF and Arf file creation
      rule = MfRule(cmd, [targetFname], dependencies=self.spudArfFnames)
      self.masterPotato.addCommands([rule])


# .............................................................................
if __name__ == "__main__":
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)
   earl = EarlJr()
   pth = earl.createDataPath(PUBLIC_USER, LMFileType.BOOM_CONFIG)
   defaultConfigFile = os.path.join(pth, '{}{}'.format(PUBLIC_ARCHIVE_NAME, 
                                                       OutputFormat.CONFIG))
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-', '--config_file', default=defaultConfigFile,
            help=('Configuration file for the archive, gridset, and grid ' +
                  'to be created from these data.'))
   parser.add_argument('cmd', choices=['start', 'stop', 'restart'],
              help="The action that should be performed by the Boom daemon")

   args = parser.parse_args()
   configFname = args.config_file
   cmd = args.cmd.lower()
      
   if os.path.exists(BOOM_PID_FILE):
      pid = open(BOOM_PID_FILE).read().strip()
   else:
      pid = os.getpid()
   
   secs = time.time()
   timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
   
   scriptname = os.path.splitext(os.path.basename(__file__))[0]
   logname = '{}.{}'.format(scriptname, timestamp)
   logger = ScriptLogger(logname, level=logging.INFO)
   boomer = Boomer(BOOM_PID_FILE, configFname, log=logger)
   
   if cmd == 'start':
      boomer.start()
   elif cmd == 'stop':
      boomer.stop()
   elif cmd == 'restart':
      boomer.restart()
   elif cmd == 'status':
      boomer.status()
   else:
      print("Unknown command: {}".format(cmd))
      sys.exit(2)

"""
$PYTHON LmDbServer/boom/boom.py --help
$PYTHON LmDbServer/boom/boom.py  --config_file /share/lm/data/archive/kubi/BOOM_Archive.ini start

select * from lm_v3.lm_updateMFChain(20505,'/share/lm/data/archive/kubi/makeflow/mf_20505.mf',0,57896.90625);

from LmDbServer.boom.boom import *

import argparse
import mx.DateTime as dt
import os, sys, time

from LmBackend.common.daemon import Daemon
from LmCommon.common.lmconstants import JobStatus, OutputFormat
from LmDbServer.common.lmconstants import BOOM_PID_FILE
from LmServer.base.lmobj import LMError
from LmServer.base.utilities import isCorrectUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import PUBLIC_FQDN, PUBLIC_USER
from LmServer.common.lmconstants import LMFileType, PUBLIC_ARCHIVE_NAME
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.cmd import MfRule
from LmServer.legion.processchain import MFChain
from LmServer.tools.cwalken import ChristopherWalken

from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.sdmproj import SDMProjection
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
         OutputFormat, SERVER_BOOM_HEADING, MatrixType) 

secs = time.time()
tuple = time.localtime(secs)
timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", tuple))
scriptname = 'boomerTesting'
logname = '{}.{}'.format(scriptname, timestamp)
logger = ScriptLogger(logname, level=logging.DEBUG)
currtime = dt.gmt().mjd

earl = EarlJr()
pth = earl.createDataPath(PUBLIC_USER, LMFileType.BOOM_CONFIG)
defaultConfigFile = os.path.join(pth, '{}{}'.format(PUBLIC_ARCHIVE_NAME, 
                                                 OutputFormat.CONFIG))
boomer = Boomer(BOOM_PID_FILE, defaultConfigFile, log=logger)

boomer.initialize()
chris = boomer.christopher
woc = chris.weaponOfChoice
alg = chris.algs[0]
prjscen = chris.prjScens[0]
mtx = chris.globalPAMs[prjscen.code]


# occ, setOrReset = woc.getOne()
# prj, pReset = chris._createOrResetSDMProject(occ, alg, prjscen, currtime)
# mtxcol, mReset = chris._createOrResetIntersect(prj, mtx, currtime)


spud, potatoInputs = boomer.christopher.startWalken()
# for i in range(61):
while not spud:
   spud, potatoInputs = boomer.christopher.startWalken()
   if spud:
      boomer._addRuleToMasterPotatoHead(spud, prefix='spud')
      spudArf = spud.getArfFilename(prefix='spud')
      boomer.spudArfFnames.append(spudArf)
      for scencode, f in boomer.rawPotatoFiles.keys():
         squid = spud.mfMetadata[MFChain.META_SQUID]
         fname = potatoInputs[scencode]
         f.write('{}: {}\n'.format(squid, fname))


boomer.christopher.stopWalken()

pcodes = ['AR5-CCSM4-RCP8.5-2050-10min', 'CMIP5-CCSM4-lgm-10min', 
          'CMIP5-CCSM4-mid-10min', 'observed-10min', 
          'AR5-CCSM4-RCP4.5-2050-10min', 'AR5-CCSM4-RCP4.5-2070-10min', 
          'AR5-CCSM4-RCP8.5-2070-10min']
for scencode, potato in boomer.potatoes.iteritems():
   if scencode != 'AR5-CCSM4-RCP8.5-2050-10min':
      print scencode
      mtx = boomer.christopher.globalPAMs[scencode]
      rules = mtx.computeMe()
      potato.addCommands(rules)
      potato.write() 
      boomer._scribe.updateObject(potato)
      boomer._addRuleToMasterPotatoHead(potato, prefix='potato')
   
boomer.masterPotato.write()
boomer.masterPotato.updateStatus(JobStatus.INITIALIZE)
boomer._scribe.updateObject((boomer.masterPotato)
boomer.spudArfFile.close()
Daemon.onShutdown(boomer)

"""

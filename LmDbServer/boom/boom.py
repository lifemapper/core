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
import mx.DateTime as dt
import os, sys, time

from LmBackend.common.daemon import Daemon
from LmCommon.common.lmconstants import JobStatus, OutputFormat
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
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
      # Dictionary of MFChains for each projScenarioCode
      self.potatoes = None
      # Dictionary of PAV input filenames for each projScenarioCode 
      self.rawPotatoFiles = None
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
         raise LMError(currargs='Failed to initialize Walker ({})'.format(e))
      
      self.spudArfFnames = []
      self.potatoes = None
      self.rawPotatoFiles = None
      self.masterPotato = None      
      if self.christopher.assemblePams:
         (self.potatoes, 
          self.rawPotatoFiles) = self._createPotatoMakeflows()
         self.masterPotato = self._createMasterMakeflow()         
         
   # .............................
   def run(self):
      try:
         self.christopher.moveToStart()
         self.log.debug('Starting Walker at location {} ... '
                        .format(self.christopher.currRecnum))
         self.keepWalken = True
         while self.keepWalken:
            try:
               self.log.info('Next species ...')
               # Get a Spud MFChain (single-species MF)
               spud, potatoInputs = self.christopher.startWalken()
               self.keepWalken = not self.christopher.complete
               if self.assemblePams and spud:
                  # Add MF rule for Spud execution to Master MF
                  self._addRuleToMasterPotatoHead(spud, prefix='spud')
                  # Gather species ARF dependency to delay start of multi-species MF
                  spudArf = spud.getArfFilename(prefix='spud')
                  self.spudArfFnames.append(spudArf)
                  # Add PAV outputs to raw potato files for triabe input
                  for scencode, f in self.rawPotatoFiles.iteritems():
                     squid = spud.mfMetadata[MFChain.META_SQUID]
                     fname = potatoInputs[scencode]
                     f.write('{}: {}\n'.format(squid, fname))
            except:
               self.log.info('Saving next start {} ...'
                             .format(self.christopher.nextStart))
               self.christopher.saveNextStart()
               raise
            else:
               time.sleep(10)
      finally:
         self.log.debug('Done walken')
         self.onShutdown()
    
   # .............................
   def onUpdate(self):
      self.log.debug("Update signal caught!")
       
   # .............................
   def onShutdown(self):
      self.keepWalken = False
      self.log.debug('Shutdown!')
      # Stop Walken the archive
      self.christopher.stopWalken()
      # Write each potato MFChain, then add the MFRule to execute it to the Master
      for scencode, potato in self.potatoes.iteritems():
         mtx = self.christopher.globalPAMs[scencode]
         potatoMF = self.potatoes[scencode]
         triageIn = self.rawPotatoFiles[scencode].name
         triageOut = potatoMF.getTriageFilename(prefix='mashedPotato')
         # Create Potato rules and write
         rules = mtx.computeMe(triageIn, triageOut)
         potato.addCommands(rules)
         potato.write()
         # Close this rawPotato file (containing GPAM PAVs)
         self.rawPotatoFiles[scencode].close()
#          potato.updateStatus(JobStatus.INITIALIZE)
         self._scribe.updateObject(potato)
         self._addRuleToMasterPotatoHead(potato, dependencies=self.spudArfFnames, 
                                         prefix='potato')
      # Write the masterPotatoHead MFChain
      self.masterPotato.write()
      self.masterPotato.updateStatus(JobStatus.INITIALIZE)
      self._scribe.updateObject(self.masterPotato)
      
      self.log.debug("Shutdown signal caught!")
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
      chains = {}
      rawPotatoFiles = {}
      for scencode in self.christopher.globalPAMs.keys():
         # Create MFChain for this GPAM
         meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
                 MFChain.META_DESC: 'Potato for User {}, Archive {}, Scencode {}'
         .format(self.christopher.userId, self.christopher.archiveName, scencode)}
         newMFC = MFChain(self.christopher.userId, priority=self.priority, 
                          metadata=meta, status=JobStatus.GENERAL, 
                          statusModTime=dt.gmt().mjd)
         mfChain = self._scribe.insertMFChain(newMFC)
         chains[scencode] = mfChain
         # Get rawPotato input file from MFChain
         rawPotatoFname = mfChain.getTriageFilename(prefix='rawPotato')
         if not readyFilename(rawPotatoFname, overwrite=True):
            raise LmException('{} is not ready for write (overwrite=True)'
                              .format(rawPotatoFname))
         try:
            f = open(rawPotatoFname, 'w')
         except Exception, e:
            raise LMError(currargs='Failed to open {} for writing ({})'
                          .format(rawPotatoFname, str(e)))
         rawPotatoFiles[scencode] = f
      return chains, rawPotatoFiles

   # .............................
   def _addRuleToMasterPotatoHead(self, mfchain, dependencies=[], prefix='spud'):
      """
      @summary: Create a Spud or Potato rule for the MasterPotatoHead MF 
      """
      targetFname = mfchain.getArfFilename(prefix=prefix)
      outputFname = mfchain.getDLocation()
      # Add MF doc (existence) as dependency to run MF doc
      dependencies.append(outputFname)
      cmdArgs = ['LOCAL makeflow',
                 '-T wq', 
                 '-N lifemapper-{}b'.format(mfchain.getId()),
                 '-C {}:9097'.format(PUBLIC_FQDN),
                 '-a {}'.format(outputFname)]
      mfCmd = ' '.join(cmdArgs)
      arfCmd = 'touch {}'.format(targetFname)
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
   logger = ScriptLogger('archivist.{}'.format(timestamp))
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

configFile = '/share/lm/data/archive/kubi/BOOM_Archive.ini'

secs = time.time()
tuple = time.localtime(secs)
timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", tuple))
logger = ScriptLogger('archivist.{}'.format(timestamp))
currtime = dt.gmt().mjd

boomer = Boomer(BOOM_PID_FILE, configFile, log=logger)
boomer.initialize()
chris = boomer.christopher
woc = chris.weaponOfChoice
alg = chris.algs[0]
prjscen = chris.prjScens[0]

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

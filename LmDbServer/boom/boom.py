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
from LmCommon.common.lmconstants import JobStatus
from LmDbServer.common.lmconstants import BOOM_PID_FILE
from LmServer.base.lmobj import LMError
from LmServer.base.utilities import isCorrectUser
from LmServer.common.lmconstants import PUBLIC_ARCHIVE_NAME
from LmServer.common.localconstants import PUBLIC_USER, PUBLIC_FQDN
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain
from LmServer.makeflow.cmd import MfRule
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
   def __init__(self, pidfile, userId, archiveName, priority=None, log=None):      
      Daemon.__init__(self, pidfile, log=log)
      self.name = self.__class__.__name__.lower()
      self.userId = userId
      self.archiveName = archiveName
      self.priority = priority
      # Send Database connection
      self._scribe = BorgScribe(self.log)
      # iterator tool for species
      self.christopher = None
      # dictionary of MFChains for each potato MF, key = projScenarioCode
      self.potatoes = None
      # MFChain for masterPotatoHead MF
      self.masterPotato = None
      # open file for writing Spud Arf filenames for Potato triage
      self.spudArfFile = None
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
         self.christopher = ChristopherWalken(self.userId, self.archiveName, 
                                              jsonFname=None, priority=None, 
                                              scribe=self._scribe)
      except Exception, e:
         raise LMError(currargs='Failed to initialize Walker ({})'.format(e))
      else:
         self.potatoes = self._createPotatoMakeflows()
         self.masterPotato = self._createMasterMakeflow()
         # All spuds are written to Potato
         potatoFname = self.masterPotato.getTriageFilename(prefix='potato')
         self.spudArfFile = open(potatoFname, 'w')
         
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
               spud = self.christopher.startWalken()
               self.keepWalken = not self.christopher.complete
               if spud:
                  # Add MF rule for Spud execution to Master MF
                  self._addRuleToMasterPotatoHead(spud, prefix='spud')
                  # Write species Spud target as input to potato triage
                  spudArf = spud.getArfFilename(prefix='spud')
                  self.spudArfFile.write('{}\n'.format(spudArf))               
            except:
               self.log.info('Saving next start {} ...'
                             .format(self.christopher.nextStart))
               self.christopher.saveNextStart()
               raise
            else:
               time.sleep(10)
      finally:
         self.christopher.stopWalken()
      self.log.debug('Stopped Walker')
    
   # .............................
   def onUpdate(self):
      self.log.debug("Update signal caught!")
       
   # .............................
   def onShutdown(self):
      # Stop Walken the archive
      self.christopher.stopWalken()
      # Write each potato MFChain, then add the MFRule to execute it to the Master
      for prjScencode, potato in self.potatoes.iteritems():
         mtx = self.christopher.globalPAMs[prjScencode]
         rules = mtx.computeMe()
         potato.addCommands(rules)
         potato.write()
#          potato.updateStatus(JobStatus.INITIALIZE)
         self._scribe.updateObject(potato)
         self._addRuleToMasterPotatoHead(potato, prefix='potato')
      # Write the masterPotatoHead MFChain
      self.masterPotato.write()
      self.masterPotato.updateStatus(JobStatus.INITIALIZE)
      self._scribe.updateObject(self.masterPotato)
      # Close the spud Arf file (list of spud MFChain targets)
      self.spudArfFile.close()
      
      self.log.debug("Shutdown signal caught!")
      Daemon.onShutdown(self)

# ...............................................
   def _createMasterMakeflow(self):
      meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
              MFChain.META_DESC: 'MasterPotatoHead for User {}, Archive {}'
      .format(self.userId, self.archiveName)}
      newMFC = MFChain(self.userId, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=dt.gmt().mjd)
      mfChain = self._scribe.insertMFChain(newMFC)
      return mfChain

# ...............................................
   def _createPotatoMakeflows(self):
      chains = {}
      for prjScencode in self.christopher.globalPAMs.keys():
         meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
                 MFChain.META_DESC: 'Potato for User {}, Archive {}, Scencode {}'
         .format(self.userId, self.archiveName, prjScencode)}
         newMFC = MFChain(self.userId, priority=self.priority, 
                          metadata=meta, status=JobStatus.GENERAL, 
                          statusModTime=dt.gmt().mjd)
         mfChain = self._scribe.insertMFChain(newMFC)
         chains[prjScencode] = mfChain
      return chains

   # .............................
   def _addRuleToMasterPotatoHead(self, mfchain, prefix='spud'):
      """
      @summary: Create a Spud or Potato rule for the MasterPotatoHead MF 
      """
      targetFname = mfchain.getArfFilename(prefix=prefix)
      outputFname = mfchain.getDLocation()
      cmdArgs = ['LOCAL makeflow',
                 '-T wq', 
                 '-C {}:9097'.format(PUBLIC_FQDN),
                 '-a {}'.format(outputFname)]
      mfCmd = ' '.join(cmdArgs)
      arfCmd = 'touch {}'.format(targetFname)
      cmd = '{} ; {}'.format(mfCmd, arfCmd)
      # Create a rule from the MF and Arf file creation
      rule = MfRule(cmd, [targetFname], dependencies=[])
      self.masterPotato.addCommands([rule])

# .............................................................................
if __name__ == "__main__":
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)

   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-n', '--archive_name', default=PUBLIC_ARCHIVE_NAME,
            help=('Name for the existing archive, gridset, and grid created for ' +
                  'these data.  This name was created in initBoom.'))
   parser.add_argument('-u', '--user', default=PUBLIC_USER,
            help=('Owner of this archive this archive. The default is the '
                  'configured PUBLIC_USER.'))
   parser.add_argument('cmd', choices=['start', 'stop', 'restart'],
              help="The action that should be performed by the Walker daemon")

   args = parser.parse_args()
   archiveName = args.archive_name
   if archiveName is not None:
      archiveName = archiveName.replace(' ', '_')
   userId = args.user
   cmd = args.cmd.lower()
      
   if os.path.exists(BOOM_PID_FILE):
      pid = open(BOOM_PID_FILE).read().strip()
   else:
      pid = os.getpid()
   
   secs = time.time()
   tuple = time.localtime(secs)
   timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", tuple))
   logger = ScriptLogger('archivist.{}'.format(timestamp))
   boomer = Boomer(BOOM_PID_FILE, userId, archiveName, log=logger)
     
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
$PYTHON LmDbServer/boom/boom.py  --archive_name "Heuchera archive" --user ryan start
$PYTHON LmDbServer/boom/boom.py --archive_name "Aimee test archive"  --user aimee start



import mx.DateTime as dt
import os, sys, time
from LmDbServer.boom.boom import *
from LmBackend.common.daemon import Daemon
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmDbServer.common.lmconstants import BOOM_PID_FILE
from LmServer.base.lmobj import LMError
from LmServer.base.utilities import isCorrectUser
from LmServer.common.lmconstants import PUBLIC_ARCHIVE_NAME
from LmServer.common.localconstants import PUBLIC_USER, PUBLIC_FQDN, APP_PATH
from LmServer.common.log import ScriptLogger
from LmServer.legion.processchain import MFChain
from LmServer.makeflow.cmd import MfRule
from LmServer.tools.cwalken import ChristopherWalken

userId = 'ryan'
archiveName = 'Heuchera_archive'
secs = time.time()
tuple = time.localtime(secs)
timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", tuple))
logger = ScriptLogger('archivist.{}'.format(timestamp))

boomer = Boomer(BOOM_PID_FILE, userId, archiveName, log=logger)
boomer.initialize()

spud = boomer.christopher.startWalken()
if spud:
   boomer._addRuleToMasterPotatoHead(spud, prefix='spud')
   spudArf = spud.getArfFilename(prefix='spud')
   boomer.spudArfFile.write('{}\n'.format(spudArf))

for i in range(61):
   spud = boomer.christopher.startWalken()
   if spud:
      boomer._addRuleToMasterPotatoHead(spud, prefix='spud')
      spudArf = spud.getArfFilename(prefix='spud')
      boomer.spudArfFile.write('{}\n'.format(spudArf))
   


boomer.christopher.stopWalken()

pcodes = ['AR5-CCSM4-RCP8.5-2050-10min', 'CMIP5-CCSM4-lgm-10min', 
          'CMIP5-CCSM4-mid-10min', 'observed-10min', 
          'AR5-CCSM4-RCP4.5-2050-10min', 'AR5-CCSM4-RCP4.5-2070-10min', 
          'AR5-CCSM4-RCP8.5-2070-10min']
for prjScencode, potato in boomer.potatoes.iteritems():
   if prjScencode != 'AR5-CCSM4-RCP8.5-2050-10min':
      print prjScencode
      mtx = boomer.christopher.globalPAMs[prjScencode]
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

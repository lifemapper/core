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

from LmCommon.common.lmconstants import JobStatus, LMFormat, ProcessType
from LmCommon.common.readyfile import readyFilename
from LmBackend.common.lmobj import LMError, LMObject
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
class Boomer(LMObject):
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
   def __init__(self, configFname, assemblePams=True, priority=None, log=None):      
      self.name = self.__class__.__name__.lower()
      # Logfile
      if log is None:
         secs = time.time()
         timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
         logname = '{}.{}'.format(self.name, timestamp)
         log = ScriptLogger(logname, level=logging.INFO)
      self.log = log

      self.configFname = configFname
      self.assemblePams = assemblePams
      self.priority = priority
      # Send Database connection
      self._scribe = BorgScribe(self.log)
      # iterator tool for species
      self.christopher = None
      # Dictionary of {scenCode: (potatoChain, triagePotatoFile)}
      self.potatoes = None
      # MFChain for masterPotatoHead MF
      self.masterPotato = None
      # open file for writing Spud Arf filenames for Potato triage
      self.spudArfFnames = None
      # Stop indicator
      self.keepWalken = False
      self.potatoesOpen = False


   # .............................
   def initializeMe(self):
      """
      @summary: Creates objects (OccurrenceSets, SMDModels, SDMProjections, 
                and MatrixColumns (intersections with the default grid)) 
                and job requests for their calculation.
      @note: The argument to this script contains variables to override 
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
      # Start where we left off 
      self.christopher.moveToStart()
      self.log.debug('Starting Chris at location {} ... '
                     .format(self.christopher.currRecnum))
      self.keepWalken = True

      self.spudArfFnames = []
      # potatoes = {scencode: (potatoChain, triagePotatoFile)
      self.potatoes = {}
      # master MF chain
      self.masterPotato = None
      if self.christopher.assemblePams:
         self.rotatePotatoes()
         
   # .............................
   def processSpud(self):
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
               for scencode, (pc, triagePotatoFile) in self.potatoes.iteritems():
                  pavFname = potatoInputs[scencode]
                  triagePotatoFile.write('{}: {}\n'.format(squid, pavFname))
               self.log.info('Wrote spud squid to {} arf files'
                             .format(len(potatoInputs)))
            if len(self.spudArfFnames) >= SPUD_LIMIT:
               self.rotatePotatoes()
      except Exception, e:
         self.log.debug('Exception {} on spud'.format(str(e)))
         self.close()
         raise e

   # .............................
   def rotatePotatoes(self):
      # Finish up existing potatoes
      #   Write triage rule to each. then write potato to Master Potato      
      if self.potatoes:
         self.log.info('Rotate potatoes ...')
         # Write each potato MFChain, then add the MFRule to execute it to the Master
         for scencode, (potatoChain, triagePotatoFile) in self.potatoes.iteritems():
            # Close this potato input file
            triagePotatoFile.close()
            # Create triage command for potato inputs, add to MF chain
            mtx = self.christopher.globalPAMs[scencode]
            
            targetDir = potatoChain.getRelativeDirectory()
            triageIn = os.path.join(targetDir, triagePotatoFile.name)
            triageOut = os.path.join(targetDir, 
                         potatoChain.getTriageFilename(prefix='mashedPotato'))
            rules = mtx.computeMe(triageIn, triageOut, workDir=targetDir)
            potatoChain.addCommands(rules)
            potatoChain.write()
#             potatoChain.updateStatus(JobStatus.INITIALIZE)
            self._scribe.updateObject(potatoChain)
            # Add this potato to MasterPotato
            self._addRuleToMasterPotatoHead(potatoChain, 
                                            dependencies=self.spudArfFnames, 
                                            prefix='potato')
            self.log.info('  Wrote potato {} for scencode {} and added to Master'
                          .format(potatoChain.objId, scencode))
         # Write the masterPotatoHead MFChain
         self.masterPotato.write()
         self.masterPotato.updateStatus(JobStatus.INITIALIZE)
         self._scribe.updateObject(self.masterPotato)
         self.potatoesOpen = False
         self.log.info('   Wrote MasterPotato {} ({} potatoes and {} spuds)'
                       .format(self.masterPotato.objId, len(self.potatoes), 
                               len(self.spudArfFnames)))
      
      # Create new potatoes
      if not self.christopher.complete:
         self.log.info('Create new potatoes')
         # Initialize new potatoes, MasterPotato
         # potatoes = {scencode: (potatoChain, triagePotatoFile)
         self.spudArfFnames = []
         self.potatoes = self._createPotatoMakeflows()
         self.masterPotato = self._createMasterMakeflow()
         self.potatoesOpen = True
            
   # .............................
   def close(self):
      self.keepWalken = False
      self.log.info('Closing boomer ...')
      # Stop walken the archive and saveNextStart
      self.christopher.stopWalken()
      self.rotatePotatoes()

# ...............................................
   def _createMasterMakeflow(self):
      meta = {MFChain.META_CREATED_BY: self.name,
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
      @summary: Create and return dictionary where 
                key: scenarioCode
                value: tuple of 
                         1) MFChain of commands to assemble a global PAM
                         2) open File of inputs for each species in the PAM,
                            squid, PAV filename
      @return:  dict of {scenarioCode: (potatoChain, triagePotatoFile)} 
      """
      potatoes = {}
      for scencode in self.christopher.globalPAMs.keys():
         # Create MFChain for this GPAM
         meta = {MFChain.META_CREATED_BY: self.name,
                 MFChain.META_DESC: 'Potato for User {}, Archive {}, Scencode {}'
         .format(self.christopher.userId, self.christopher.archiveName, scencode)}
         newMFC = MFChain(self.christopher.userId, priority=self.priority, 
                          metadata=meta, status=JobStatus.GENERAL, 
                          statusModTime=dt.gmt().mjd)
         potatoChain = self._scribe.insertMFChain(newMFC)
         # Get triage input file from MFChain
         triagePotatoFname = potatoChain.getTriageFilename(prefix='triage')
         if not readyFilename(triagePotatoFname, overwrite=True):
            raise LMError(currargs='{} is not ready for write (overwrite=True)'
                              .format(triagePotatoFname))
         try:
            triagePotatoFile = open(triagePotatoFname, 'w')
         except Exception, e:
            raise LMError(currargs='Failed to open {} for writing ({})'
                          .format(triagePotatoFname, str(e)))
         potatoes[scencode] = (potatoChain, triagePotatoFile) 
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
      cmdArgs = ['makeflow',
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
      cmd = 'LOCAL {} ; {}'.format(arfCmd, mfCmd)
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
      cmdArgs = ['checkArfFiles'].extend(self.spudArfFnames)
      mfCmd = ' '.join(cmdArgs)
      arfCmd = 'touch {}'.format(targetFname)
      cmd = 'LOCAL {} ; {}'.format(arfCmd, mfCmd)
      # Create a rule from the MF and Arf file creation
      rule = MfRule(cmd, [targetFname], dependencies=self.spudArfFnames)
      self.masterPotato.addCommands([rule])


# .............................................................................
if __name__ == "__main__":
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)
   earl = EarlJr()
   defaultConfigFile = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                           objCode=PUBLIC_ARCHIVE_NAME, 
                                           usr=PUBLIC_USER)
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-', '--config_file', default=defaultConfigFile,
            help=('Configuration file for the archive, gridset, and grid ' +
                  'to be created from these data.'))

   args = parser.parse_args()
   configFname = args.config_file
   if not os.path.exists(configFname):
      raise Exception('Configuration file {} does not exist'.format(configFname))
   
   secs = time.time()
   timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
   
   scriptname = os.path.splitext(os.path.basename(__file__))[0]
   logname = '{}.{}'.format(scriptname, timestamp)
   logger = ScriptLogger(logname, level=logging.INFO)
   boomer = Boomer(configFname, log=logger)
   boomer.initializeMe()

"""
$PYTHON LmDbServer/boom/boom.py --help

import argparse
import mx.DateTime as dt
import os, sys, time

from LmDbServer.boom.boomer import *
from LmBackend.common.daemon import Daemon
from LmDbServer.common.lmconstants import BOOM_PID_FILE
from LmBackend.common.lmobj import LMError
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
         SERVER_BOOM_HEADING, MatrixType) 

secs = time.time()
tuple = time.localtime(secs)
timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", tuple))
scriptname = 'boomerTesting'
logname = '{}.{}'.format(scriptname, timestamp)
logger = ScriptLogger(logname, level=logging.DEBUG)
currtime = dt.gmt().mjd

earl = EarlJr()
pth = earl.createDataPath(PUBLIC_USER, LMFileType.BOOM_CONFIG)
configFname = os.path.join(pth, '{}{}'.format(PUBLIC_ARCHIVE_NAME, 
                                                 LMFormat.CONFIG.ext))
earl = EarlJr()
pth = earl.createDataPath(PUBLIC_USER, LMFileType.BOOM_CONFIG)
configFile = os.path.join(pth, '{}{}'.format(PUBLIC_ARCHIVE_NAME, 
                                                    LMFormat.CONFIG.ext))

boomer = Boomer(configFname, log=logger)

boomer.initializeMe()
chris = boomer.christopher
woc = chris.weaponOfChoice
alg = chris.algs[0]
prjscen = chris.prjScens[0]
mtx = chris.globalPAMs[prjscen.code]
scribe = boomer._scribe
ptype = ProcessType.INTERSECT_RASTER
borg = scribe._borg

spud, potatoInputs = chris.startWalken()
boomer.keepWalken = not chris.complete


spud = self._createSpudMakeflow(spudObjs)


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
      for scencode, (pc, triagePotatoFile) in self.potatoes.iteritems():
         pavFname = potatoInputs[scencode]
         triagePotatoFile.write('{}: {}\n'.format(squid, pavFname))
      self.log.info('Wrote spud squid to {} arf files'
                    .format(len(potatoInputs)))
   if len(self.spudArfFnames) >= SPUD_LIMIT:
      self.rotatePotatoes()

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
      for scencode, f in boomer.triagePotatoFiles.keys():
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

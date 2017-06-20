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
import mx.DateTime
import os
import sys
import time

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import (LMFormat, JobStatus, MatrixType,
   ProcessType)
from LmServer.common.lmconstants import LMFileType, Priority
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe 
from LmServer.legion.processchain import MFChain
from LmServer.legion.lmmatrix import LMMatrix

CURR_MJD = mx.DateTime.gmt().mjd

# .............................................................................
class RADCaller(LMObject):
   """
   Class to populate a Lifemapper database with inputs for a BOOM archive, and 
   write a configuration file for computations on the inputs.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, gridsetId, priority=Priority.NORMAL):
      """
      @summary Constructor for ArchiveFiller class.
      """
      super(RADCaller, self).__init__()
      # Get database
      try:
         self._scribe = self._getDb()
      except: 
         raise
      self.open()
      self._priority = priority
      self._gridset = self._scribe.getGridset(gridsetId=gridsetId, 
                                              fillMatrices=True)
      if self._gridset is None:
         raise LMError(currargs='Failed to retrieve Gridset for Id {}'
                                .format(gridsetId))
      
   # ...............................................
   @property
   def userId(self):
      return self._gridset.getUserId()
   
   # ...............................................
   @property
   def gridsetId(self):
      return self._gridset.getId()
   
   # ...............................................
   @property
   def gridsetName(self):
      return self._gridset.name
   
   # ...............................................
   def open(self):
      success = self._scribe.openConnections()
      if not success: 
         raise LMError('Failed to open database')

      # ...............................................
   def close(self):
      self._scribe.closeConnections()

   # ...............................................
   @property
   def logFilename(self):
      try:
         fname = self._scribe.log.baseFilename
      except:
         fname = None
      return fname
   
   # ...............................................
   def _getDb(self):
      import logging
      loglevel = logging.INFO
      # Logfile
      secs = time.time()
      timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
      logname = '{}.{}'.format(self.__class__.__name__.lower(), timestamp)
      logger = ScriptLogger(logname, level=loglevel)
      # DB connection
      scribe = BorgScribe(logger)
      return scribe
      
   # ...............................................
   def _createMF(self, rules):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by ArchiveFiller.initBoom.
      """
      desc = ('Makeflow for RAD computations on Gridset {}, {} for User {}'
              .format(self.gridsetId, self.gridsetName, self.userId))
      meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
              MFChain.META_DESC: desc }
      
      newMFC = MFChain(self.userId, metadata=meta, priority=self._priority,
                       status=JobStatus.GENERAL, statusModTime=CURR_MJD)
      mfChain = self._scribe.insertMFChain(newMFC)

      mfChain.addCommands(rules)

      mfChain.write()
      self._scribe.updateObject(mfChain)
      return mfChain
   
   # ...............................................
   def analyzeGrid(self, doCalc=False, doMCPA=False):
      # For each PAM
      gridsetId = self._gridset.getId()
      
      pamDict = {}
      # Loop through PAMs and add calculated matrices to dictionary by either
      #    getting the existing matrices or creating new ones
      for pam in self._gridset.getPAMs():
         pamId = pam.getId()
         pd = {}
         
         # Add PAM
         pd[MatrixType.PAM] = pam
         
         
         if doCalc:
            # Sites matrix
            pd[MatrixType.SITES_OBSERVED] = self._getOrInsertMatrix(gridsetId, 
                                                  MatrixType.SITES_OBSERVED, 
                                                  ProcessType.RAD_CALCULATE,
                                                  pam.gcmCode, pam.altpredCode, 
                                                  pam.dateCode)
            # Species matrix
            pd[MatrixType.SPECIES_OBSERVED] = self._getOrInsertMatrix(gridsetId, 
                                                  MatrixType.SPECIES_OBSERVED, 
                                                  ProcessType.RAD_CALCULATE,
                                                  pam.gcmCode, pam.altpredCode, 
                                                  pam.dateCode)
            # Diveristy matrix
            pd[MatrixType.DIVERSITY_OBSERVED] = self._getOrInsertMatrix(gridsetId, 
                                                  MatrixType.DIVERSITY_OBSERVED, 
                                                  ProcessType.RAD_CALCULATE,
                                                  pam.gcmCode, pam.altpredCode, 
                                                  pam.dateCode)
            # TODO: Site covariance, species covariance, schluter
         
         if doMCPA:
            # GRIM
            pd[MatrixType.GRIM] = self._scribe.getMatrix(gridsetId=gridsetId, 
                                   mtxType=MatrixType.GRIM, 
                                   gcmCode=pam.gcmCode, 
                                   altpredCode=pam.altpredCode, 
                                   dateCode=pam.dateCode)

            # Env Adjusted R-squared
            pd[MatrixType.MCPA_ENV_OBS_ADJ_R_SQ] = self._getOrInsertMatrix(
                                             gridsetId, 
                                             MatrixType.MCPA_ENV_OBS_ADJ_R_SQ, 
                                             ProcessType.MCPA_OBSERVED,
                                             pam.gcmCode, pam.altpredCode, 
                                             pam.dateCode)
            # Env Parital Correlation
            pd[MatrixType.MCPA_ENV_OBS_PARTIAL] = self._getOrInsertMatrix(
                                             gridsetId, 
                                             MatrixType.MCPA_ENV_OBS_PARTIAL, 
                                             ProcessType.MCPA_OBSERVED,
                                             pam.gcmCode, pam.altpredCode, 
                                             pam.dateCode)
            # Env F global
            pd[MatrixType.MCPA_ENV_F_GLOBAL] = self._getOrInsertMatrix(
                                             gridsetId, 
                                             MatrixType.MCPA_ENV_F_GLOBAL, 
                                             ProcessType.MCPA_CORRECT_PVALUES,
                                             pam.gcmCode, pam.altpredCode, 
                                             pam.dateCode)
            # Env F semi partial   
            pd[MatrixType.MCPA_ENV_F_SEMI] = self._getOrInsertMatrix(gridsetId, 
                                             MatrixType.MCPA_ENV_F_SEMI, 
                                             ProcessType.MCPA_CORRECT_PVALUES,
                                             pam.gcmCode, pam.altpredCode, 
                                             pam.dateCode)

            # BG Adjusted R-squared
            pd[MatrixType.MCPA_BG_OBS_ADJ_R_SQ] = self._getOrInsertMatrix(
                                             gridsetId, 
                                             MatrixType.MCPA_BG_OBS_ADJ_R_SQ, 
                                             ProcessType.MCPA_OBSERVED,
                                             pam.gcmCode, pam.altpredCode, 
                                             pam.dateCode)
            # BG Parital Correlation
            pd[MatrixType.MCPA_BG_OBS_PARTIAL] = self._getOrInsertMatrix(
                                             gridsetId, 
                                             MatrixType.MCPA_BG_OBS_PARTIAL, 
                                             ProcessType.MCPA_OBSERVED,
                                             pam.gcmCode, pam.altpredCode, 
                                             pam.dateCode)
            # BG F global
            pd[MatrixType.MCPA_BG_F_GLOBAL] = self._getOrInsertMatrix(
                                             gridsetId, 
                                             MatrixType.MCPA_BG_F_GLOBAL, 
                                             ProcessType.MCPA_CORRECT_PVALUES,
                                             pam.gcmCode, pam.altpredCode, 
                                             pam.dateCode)
            # BG F semi partial   
            pd[MatrixType.MCPA_BG_F_SEMI] = self._getOrInsertMatrix(gridsetId, 
                                             MatrixType.MCPA_BG_F_SEMI, 
                                             ProcessType.MCPA_CORRECT_PVALUES,
                                             pam.gcmCode, pam.altpredCode, 
                                             pam.dateCode)
                     
         pamDict[pamId] = pd
         
      # Insert all taxonomic sources for now
      self._scribe.log.info('  Creating Gridset MFRules ...')
      rules = self._gridset.computeMe(doCalc=doCalc, doMCPA=doMCPA, 
                                      pamDict=pamDict)
         
      mfChain = self._createMF(rules)
      mfChain.updateStatus(JobStatus.INITIALIZE)
      self._scribe.updateObject(mfChain)

      self._scribe.log.info('  Wrote Gridset MF file')
      
   # ..............................................
   def _getOrInsertMatrix(self, gsId, mtxType, procType, gcmCode, altpredCode, dateCode):
      """
      @summary: Attempts to find a matrix of the specified type and scenario
                   parameters.  If it cannot be found, create a new matrix
                   for these values
      @param gsId: The id of the gridset to use
      @param mtxType: The matrix type to look for
      """
      #mtx = self._scribe.getMatrix(gridsetId=gsId, mtxType=mtxType, 
      #                             gcmCode=gcmCode, altpredCode=altpredCode, 
      #                             dateCode=dateCode)
      #if mtx is None:
      
      newMtx = LMMatrix(None, matrixType=mtxType, processType=procType, 
                           gcmCode=gcmCode, altpredCode=altpredCode, 
                           dateCode=dateCode, userId=self._gridset.getUserId(), 
                           gridset=self._gridset)
      mtx = self._scribe.findOrInsertMatrix(newMtx)
      mtx.updateStatus(status=JobStatus.INITIALIZE, 
                          modTime=mx.DateTime.gmt().mjd)
      return mtx

# ...............................................
if __name__ == '__main__':
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)

   import argparse
   parser = argparse.ArgumentParser(
            description=('Compute multi-species computations on a Gridset.'))
   parser.add_argument('gridsetId', type=int,
            help=('Database Id for the Gridset on which to compute outputs'))
   parser.add_argument('-c', '--doCalc', action='store_true',
            help=('Compute multi-species matrix outputs for the matrices ' +
                  'in this Gridset.'))
   parser.add_argument('-m', '--doMCPA', action='store_true',
            help=('Compute Meta-Community Phylogenetics computations on the ' +
                  'matrices, phylogenetic tree, and biogeographic hypotheses ' +
                  'in this Gridset.'))
   parser.add_argument('-p', '--priority', type=int, choices=[0,1,2,3,4,5], 
            help=('Priority for these computations; an integer between 0 and 5 ' +
                  'where 0 is the lowest and 5 is the highest.'))
   args = parser.parse_args()
      
   caller = RADCaller(args.gridsetId, priority=args.priority)
   caller.analyzeGrid(doCalc=args.doCalc, doMCPA=args.doMCPA)
   caller.close()
    
"""
import mx.DateTime
import os
import time

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import (LMFormat, JobStatus, MatrixType)
from LmServer.common.lmconstants import LMFileType, Priority
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe 
from LmServer.legion.processchain import MFChain
CURR_MJD = mx.DateTime.gmt().mjd

from LmDbServer.boom.radme import RADCaller

gridsetId = 1
priority=Priority.REQUESTED
doCalc = True
doMCPA=False

caller = RADCaller(gridsetId, priority=priority)
caller.analyzeGrid(doCalc=doCalc, doMCPA=doMCPA)
caller.close()

"""

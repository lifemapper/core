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
      self._gridset = self._scribe.getGridset(gridsetId=gridsetId)
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
   
#    # ...............................................
#    @property
#    def userPath(self):
#       earl = EarlJr()
#       pth = earl.createDataPath(self.usr, LMFileType.BOOM_CONFIG)
#       return pth
         
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
              .format(self._gridsetId, self._gridsetName, self.userId))
      meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
              MFChain.META_DESC: desc }
      
      newMFC = MFChain(self.userId, metadata=meta, priority=self._priority,
                       status=JobStatus.GENERAL, statusModTime=CURR_MJD)
      mfChain = self._scribe.insertMFChain(newMFC)

      mfChain.write()
      self._scribe.updateObject(potatoChain)
      return mfChain
   
   # ...............................................
   def analyzeGrid(self, doCalc=False, doMCPA=False):   
      # Insert all taxonomic sources for now
      self._scribe.log.info('  Creating Gridset MFRules ...')
      rules = self._gridset.computeMe(doCalc=doCalc, doMCPA=doMCPA)
         
      mfChain = self._createMF()
      mfChain.addCommands(rules)
      mfChain.write()

      self._scribe.log.info('  Wrote Gridset MF file')
      
   
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

caller = RADCaller(args.gridsetId, priority=args.priority)
caller.analyzeGrid(doCalc=args.doCalc, doMCPA=args.doMCPA)
caller.close()

"""

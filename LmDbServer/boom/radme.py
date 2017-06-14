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
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (LMFormat, JobStatus, MatrixType)
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType, Priority
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.cmd import MfRule
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.processchain import MFChain

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd
# TODO: Change to new script, not a daemon
BOOM_SCRIPT = 'LmDbServer/boom/boom.py'

# .............................................................................
class RADCaller(LMObject):
   """
   Class to populate a Lifemapper database with inputs for a BOOM archive, and 
   write a configuration file for computations on the inputs.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, gridsetId):
      """
      @summary Constructor for ArchiveFiller class.
      """
      super(RADCaller, self).__init__()
      # Get database
      try:
         self.scribe = self._getDb()
      except: 
         raise
      self.open()
      self.gridset = self.scribe.getGridset(gridsetId=gridsetId)
      if self.gridset is None:
         raise LMError(currargs='Failed to retrieve Gridset for Id {}'
                                .format(gridsetId))
      self.user = self.gridset.getUserId()
      # If running as root, new user filespace must have permissions corrected
      self._warnPermissions()
      
   # ...............................................
   @property
   def userId(self):
      return self.gridset.getUserId()
   
   # ...............................................
   @property
   def gridsetId(self):
      return self.gridset.getId()
   
   # ...............................................
   def open(self):
      success = self.scribe.openConnections()
      if not success: 
         raise LMError('Failed to open database')

      # ...............................................
   def close(self):
      self.scribe.closeConnections()

   # ...............................................
   @property
   def logFilename(self):
      try:
         fname = self.scribe.log.baseFilename
      except:
         fname = None
      return fname
   
   # ...............................................
   @property
   def userPath(self):
      earl = EarlJr()
      pth = earl.createDataPath(self.usr, LMFileType.BOOM_CONFIG)
      return pth

# ...............................................
   def _warnPermissions(self):
      if not isCorrectUser():
         print("""
               When not running this script as `lmwriter`, make sure to fix
               permissions on the newly created shapegrid {}
               """.format(self.gridname))
         
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
   def _getVarValue(self, var):
      try:
         var = int(var)
      except:
         try:
            var = float(var)
         except:
            pass
      return var
      
      
   # ...............................................
   def createMF(self, rules):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by ArchiveFiller.initBoom.
      """
      meta = {MFChain.META_CREATED_BY: os.path.basename(__file__),
              MFChain.META_DESC: 'Gridset RAD computations for User {}, Gridset {}'
      .format(self.userId, self.gridsetId)}
      newMFC = MFChain(self.usr, priority=self.priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=CURR_MJD)
      mfChain = self.scribe.insertMFChain(newMFC)

      mfChain.write()
      return mfChain
   
   # ...............................................
   def analyzeGrid(self, doCalc=False, doMCPA=False):   
      # Insert all taxonomic sources for now
      self.scribe.log.info('  Creating Gridset MFRules ...')
      rules = self.gridset.computeMe(doCalc=doCalc, doMCPA=doMCPA)
         
      mfChain = self.createMF()
      mfChain.addCommands(rules)
      mfChain.write()

      self.scribe.log.info('  Wrote Gridset MF file')
      
   
# ...............................................
if __name__ == '__main__':
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
   args = parser.parse_args()
      
   caller = RADCaller(args.gridsetId)
   caller.analyzeGrid(doCalc=args.doCalc, doMCPA=args.doMCPA)
   caller.close()
    
"""
import mx.DateTime
import os

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, LMFormat, 
                                    JobStatus, MatrixType, SERVER_BOOM_HEADING)
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmDbServer.common.localconstants import (ALGORITHMS, ASSEMBLE_PAMS, 
      GBIF_TAXONOMY_FILENAME, GBIF_PROVIDER_FILENAME, GBIF_OCCURRENCE_FILENAME, 
      BISON_TSN_FILENAME, IDIG_OCCURRENCE_DATA, IDIG_OCCURRENCE_DATA_DELIMITER,
      USER_OCCURRENCE_DATA, USER_OCCURRENCE_DATA_DELIMITER,
      INTERSECT_FILTERSTRING, INTERSECT_VALNAME, INTERSECT_MINPERCENT, 
      INTERSECT_MINPRESENCE, INTERSECT_MAXPRESENCE, SCENARIO_PACKAGE,
      GRID_CELLSIZE, GRID_NUM_SIDES)
from LmBackend.common.lmobj import LMError, LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, LMFileType, ENV_DATA_PATH, 
         GPAM_KEYWORD, ARCHIVE_KEYWORD, PUBLIC_ARCHIVE_NAME, DEFAULT_EMAIL_POSTFIX)
from LmServer.common.localconstants import (PUBLIC_USER, DATASOURCE, 
                                            POINT_COUNT_MIN)
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.scenario import Scenario
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.sdm.algorithm import Algorithm

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd


from LmDbServer.boom.boominput import ArchiveFiller
filler = ArchiveFiller()

filler.writeConfigFile(fname='/tmp/testFillerConfig.ini')
# filler.initBoom()
# filler.close()

borg = filler.scribe._borg


"""

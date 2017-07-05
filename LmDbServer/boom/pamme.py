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
from LmCommon.common.lmconstants import (JobStatus, MatrixType,
   ProcessType,LMFormat)
from LmServer.base.utilities import isCorrectUser
from LmServer.common.lmconstants import Priority
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe 
from LmServer.legion.processchain import MFChain

CURR_MJD = mx.DateTime.gmt().mjd

# .............................................................................
class Pammer(LMObject):
   """
   Class to populate a Lifemapper database with inputs for a BOOM archive, and 
   write a configuration file for computations on the inputs.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, gridsetId=None, gridsetName=None, userId=None, 
                doPAM=True, doGRIM=True, priority=Priority.NORMAL):
      """
      @summary Constructor for ArchiveFiller class.
      """
      super(Pammer, self).__init__()
      self.name = self.__class__.__name__.lower()
      self.PAMs = []
      self.GRIMs = []
      self.doPAM = doPAM 
      self.doGRIM = doGRIM
      # Get database
      try:
         self._scribe = self._getDb()
      except: 
         raise
      self.open()
      self._gridset = self._scribe.getGridset(gridsetId=gridsetId, 
                                              userId=userId, 
                                              name=gridsetName,
                                              fillMatrices=True)
      if self._gridset is None:
         raise LMError(currargs='Failed to retrieve Gridset for Id {}'
                                .format(gridsetId))
      self._priority = priority
      
   # ...............................................
   def initializeInputs(self):
      """
      @summary Initialize configured and stored inputs for Pammer class.
      """
      self.PAMs = self._gridset.getPAMs()
      self.GRIMs = self._gridset.getGRIMs()
      
   # ...............................................
   def _getMCProcessType(self, mtxColumn, mtxType):
      """
      @summary Initialize configured and stored inputs for ArchiveFiller class.
      """
      if LMFormat.isOGR(driver=mtxColumn.layer.dataFormat):
         if mtxType == MatrixType.PAM:
            ptype = ProcessType.INTERSECT_VECTOR
         elif mtxType == MatrixType.GRIM:
            raise LMError('Vector GRIM intersection is not implemented')
      else:
         if mtxType == MatrixType.PAM:
            ptype = ProcessType.INTERSECT_RASTER
         elif mtxType == MatrixType.GRIM:
            ptype = ProcessType.INTERSECT_RASTER_GRIM
      return ptype
   
   # ...............................................
   def _createMatrixMF(self, mtx):
      """
      @summary Initialize configured and stored inputs for ArchiveFiller class.
      """
      postToSolr = False
      if mtx.matrixType == MatrixType.ROLLING_PAM:
         postToSolr = True
      # Create MFChain for this GPAM
      desc = ('Makeflow for Matrix {}, Gridset {}, User {}'
              .format(mtx.getId(), self.gridsetName, self.userId))
      meta = {MFChain.META_CREATED_BY: self.name,
              MFChain.META_DESC: desc}
      newMFC = MFChain(self.userId, priority=self._priority, 
                       metadata=meta, status=JobStatus.GENERAL, 
                       statusModTime=mx.DateTime.gmt().mjd)
      mtxChain = self._scribe.insertMFChain(newMFC)
      # Add layer intersect rules to it
      targetDir = mtxChain.getRelativeDirectory()
      mtxcols = self._scribe.getColumnsForMatrix(mtx.getId())
      colFilenames = []
      for mtxcol in mtxcols:
         mtxcol.postToSolr = postToSolr
         mtxcol.processType = self._getMCProcessType(mtxcol, mtx.matrixType)
         mtxcol.shapegrid = self._gridset.getShapegrid()

         lyrRules = mtxcol.computeMe(workDir=targetDir)
         mtxChain.addCommands(lyrRules)
         
         # Keep track of intersection filenames for matrix concatenation
         relDir = os.path.splitext(mtxcol.layer.getRelativeDLocation())[0]
         outFname = os.path.join(targetDir, relDir, mtxcol.getTargetFilename())
         colFilenames.append(outFname)
         
      # Add Matrix assembly rules (solr post or file concat) to it
      # TODO: This should only Triage SDM-generated matrixColumn layers
      if mtx.matrixType in (MatrixType.PAM, MatrixType.ROLLING_PAM):
         triageIn = os.path.join(targetDir, 
                                 mtxChain.getTriageFilename(prefix='triage'))
         triageOut = os.path.join(targetDir, 
                                  mtxChain.getTriageFilename(prefix='mashedPotato'))
         mtxRules = mtx.computeMe(triageIn, triageOut, workDir=targetDir)
      else:
         mtxRules = mtx.getConcatAndStockpileRules(colFilenames, workDir=targetDir)
                        
      mtxChain.addCommands(mtxRules)
      mtxChain.write()
      mtxChain.updateStatus(JobStatus.INITIALIZE)
      self._scribe.updateObject(mtxChain)
      self._scribe.log.info('  Wrote Matrix Intersect Makeflow {} for {}'
                    .format(mtxChain.objId, desc))
               
      return mtxChain
      
   # ...............................................
   def assembleIntersects(self):
      """
      @summary Initialize configured and stored inputs for Pammer class.
      """
      mfs = []
      if self.doPAM:
         for pam in self.PAMs:
            mtxChain = self._createMatrixMF(pam)
            mfs.append(mtxChain)
      if self.doGRIM:
         for grim in self.GRIMs:
            mtxChain = self._createMatrixMF(grim)
            mfs.append(mtxChain)
      return mfs
   
   # ...............................................
   @property
   def userId(self):
      return self._gridset.getUserId()
   
   # ...............................................
   @property
   def gridsetId(self):
      return self._gridset.getId()
   
   # ...............................................
   def _getGridsetName(self):
      return self._gridset.name
   
   def _setGridsetName(self, value):
      try:
         self._gridset.name = value
      except:
         pass
      
   gridsetName = property(_getGridsetName, _setGridsetName)
   
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
      desc = ('Makeflow for PAM intersect for Gridset {}, {} for User {}'
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
if __name__ == '__main__':
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)

   import argparse
   parser = argparse.ArgumentParser(
            description=('Intersect all PAM and/or GRIM layers for a Gridset.'))
   parser.add_argument('gridsetId', type=int,
            help=('Database Id for the Gridset on which to intersect layers'))
   parser.add_argument('name', type=str,
            help=('Name for the Gridset on which to intersect layers'))
   parser.add_argument('userId', type=str,
            help=('User Id for the Gridset on which to to intersect layers'))
   parser.add_argument('-pam', '--doPam', action='store_true',
            help=('Compute multi-species matrix outputs for the matrices ' +
                  'in this Gridset.'))
   parser.add_argument('-grim', '--doGrim', action='store_true',
            help=('Compute multi-species matrix outputs for the matrices ' +
                  'in this Gridset.'))
   parser.add_argument('-p', '--priority', type=int, choices=[0,1,2,3,4,5], 
            help=('Priority for these computations; an integer between 0 and 5 ' +
                  'where 0 is the lowest and 5 is the highest.'))
   args = parser.parse_args()
   gridsetId = args.gridsetId
   gridsetName = args.name
   userId = args.userId
   doPam = args.doPam
   doGrim = args.doGrim
      
   caller = Pammer(gridsetId=gridsetId, gridsetName=gridsetName, userId=userId, 
                   doPAM=doPam, doGRIM=doGrim, priority=args.priority)
   caller.initializeInputs()
   caller.assembleIntersects()
   caller.close()
    
"""
import mx.DateTime
import os
import sys
import time

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import (JobStatus, MatrixType,
   ProcessType,LMFormat)
from LmServer.base.utilities import isCorrectUser
from LmServer.common.lmconstants import Priority
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe 
from LmServer.legion.processchain import MFChain

CURR_MJD = mx.DateTime.gmt().mjd

from LmDbServer.boom.pamme import Pammer

gridsetId = 5
priority=Priority.REQUESTED

pammer = Pammer(gridsetId=gridsetId, doPAM=False, doGRIM=True, priority=priority)
pammer.initializeInputs()
mfs = pammer.assembleIntersects()

pammer.close()

"""

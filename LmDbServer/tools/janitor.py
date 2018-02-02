"""
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research
 
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
import shutil

from LmBackend.common.lmobj import LMError, LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd

# .............................................................................
class Janitor(LMObject):
   """
   Class to populate a Lifemapper database with inputs for a BOOM archive, and 
   write a configuration file for computations on the inputs.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, userId, doDeleteDBRecords=True, doDeleteFiles=True,
                doDeleteInputs=False):
      """
      @summary Constructor for Janitor class.
      """
      super(Janitor, self).__init__()
      self.usr = userId
      self._earl = EarlJr()
      self.doDeleteDBRecords = doDeleteDBRecords 
      self.doDeleteFiles = doDeleteFiles
      self.doDeleteInputs = doDeleteInputs
      # Get database
      try:
         self.scribe = self._getDb()
      except: 
         raise
      # If running as root, new user filespace must have permissions corrected
      self._warnPermissions()
      
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
   def getUserMakeflowPath(self):
      pth = self._earl.createDataPath(self.usr, LMFileType.MF_DOCUMENT)
      return pth
         
   # ...............................................
   def getUserSDMPaths(self):
      paths = self._earl.getTopLevelUserSDMPaths(self.usr)
      return paths

   # ...............................................
   def _getDb(self):
      basefilename = os.path.basename(__file__)
      basename, _ = os.path.splitext(basefilename)
      logger = ScriptLogger(basename)
      scribe = BorgScribe(logger)
      return scribe
   
   # ...............................................
   def _clearUserComputedDataRecords(self):
      success = self.scribe.deleteComputedUserData(self.usr)
      return success

   # ...............................................
   def _clearUserComputedDataFiles(self):
      mfpath = self.getUserMakeflowPath()
      self.scribe.log.info('Removing {}'.format(mfpath))
      shutil.rmtree(mfpath, ignore_errors=True)
      sdmPaths = self.getUserSDMPaths()
      for pth in sdmPaths:
         self.scribe.log.info('Removing {}'.format(pth))
         shutil.rmtree(pth, ignore_errors=True)

   # ...............................................
   def _clearUserInputDataRecords(self):
      pass

   # ...............................................
   def _clearUserInputDataFiles(self):
      pass

   # ...............................................
   def clearUserData(self):
      if self.doDeleteDBRecords:
         self._clearUserComputedDataRecords()
      if self.doDeleteFiles:
         self._clearUserComputedDataFiles()
      if self.doDeleteInputs:
         self._clearUserInputDataRecords()
         self._clearUserInputDataFiles()
         
      
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=('Clear a Lifemapper archive of SDM data and '
                         'intersections from those SDM projections ' +
                         'makeflows '))
   parser.add_argument('-', '--user', default=PUBLIC_USER,
         help=('UserId for the data to delete'))
   parser.add_argument('-', '--delete_database_records', default=True, 
         options=[True, False], help=('Delete data from database'))
   parser.add_argument('-', '--delete_files', default=True, 
         options=[True, False], help=('Delete data from filesystem'))
   parser.add_argument('-', '--delete_inputs', default=False, 
         options=[True, False], 
         help=('Delete inputs and configuration from database and filesystem'))
   args = parser.parse_args()
   usr = args.user
   doDeleteDBRecords = args.delete_database_records
   doDeleteFiles = args.delete_files
   doDeleteInputs = args.delete_inputs
 
   filler = Janitor(usr, 
                    doDeleteDBRecords=doDeleteDBRecords, 
                    doDeleteFiles=doDeleteFiles,
                    doDeleteInputs=doDeleteInputs)
   filler.open()
   filler.clearUserData()
   filler.close()
     
"""

"""
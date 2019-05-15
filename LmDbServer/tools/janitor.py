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
import os

from LmBackend.common.lmobj import LMError, LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType, LMFormat
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe


# .............................................................................
class Janitor(LMObject):
    """
    Class to populate a Lifemapper database with inputs for a BOOM archive, and 
    write a configuration file for computations on the inputs.
    """
# .............................................................................
# Constructor
# .............................................................................
    def __init__(self):
        """
        @summary Constructor for Janitor class.
        """
        super(Janitor, self).__init__()
        self._earl = EarlJr()
        # Get database
        try:
            self.scribe = self._getDb()
        except: 
            raise
        
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
    def _getDb(self):
        basefilename = os.path.basename(__file__)
        basename, _ = os.path.splitext(basefilename)
        logger = ScriptLogger(basename)
        scribe = BorgScribe(logger)
        return scribe
    
    # ...............................................
    def _clearUserFiles(self, usr):
        usrpth = self._earl.createDataPath(usr, LMFileType.BOOM_CONFIG)
        for root, dirs, files in os.walk(usrpth):
            for fname in files:
                ext = os.path.splitext(fname)
                fullFname = os.path.join(root, fname)
                # Save config files, newick/nexus tree files, 
                # species data csv and metadata json files at top level
                if (root == usrpth and 
                    ext in [LMFormat.CONFIG.ext, LMFormat.CSV, LMFormat.JSON,
                            LMFormat.NEWICK, LMFormat.NEXUS]):
                    self.scribe.log.info('Saving {}'.format(fullFname))
                else:
                    os.remove(fullFname)
                    self.scribe.log.info('Removing {}'.format(fullFname))

    # ...............................................
    def clearUserData(self, usr):
        count = self.scribe.clearUser(usr)
        self.scribe.log.info('Deleted {} objects for user {}'.format(count, usr))
        self._clearUserFiles(usr)
            
    # ...............................................
    def deleteGridset(self, gridsetid):
        filenames = self.scribe.deleteGridsetReturnFilenames(gridsetid)
        for fn in filenames:
            try:
                os.remove(fn)
                self.scribe.log.info('Deleted {} for gridset {}'.format(fn, 
                                                                    gridsetid))
            except:                
                self.scribe.log.info('Failed to delete {} for gridset {}'.format(fn, 
                                                                    gridsetid))
        
# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=('Clear a Lifemapper archive of all data for a user or'
                             'MatrixColumns, Matrices, and Makeflows for a gridset'))
    parser.add_argument('user_or_gridsetid', default=None,
            help=('UserId or GridsetId for the data to delete'))
    args = parser.parse_args()
    gridsetid = usr = None
    
    try:
        gridsetid = int(args.user_or_gridsetid)
    except:
        usr = args.user_or_gridsetid
        
    print('Janitor argument: gridsetid {}; userid {}'.format(gridsetid, usr))
        
    jan = Janitor()
    jan.open()
    if usr is not None:
        jan.clearUserData(usr)
    elif gridsetid is not None:
        jan.deleteGridset(gridsetid)
    else:
        print('No valid option for clearing gridset or user data')
    jan.close()
      
"""

"""
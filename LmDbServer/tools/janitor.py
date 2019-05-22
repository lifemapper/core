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
import mx.DateTime as DT
import shutil

from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.lmconstants import ONE_DAY

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType, LMFormat
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.notifications.email import EmailNotifier

failedFile = "/home/cjgrady/failed.txt"


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
    def reportFailure(self, mesgs):
        notifier = EmailNotifier()
        notifier.sendMessage(['cjgrady@ku.edu'], 
                             "Failed to delete user occurrence sets", 
                             '\n'.join(mesgs))
        
    # ...............................................
    def deleteObsoleteSDMs(self, usr, obsolete_date, max_num):
        # Should be able to just list old occurrence sets and then have the scribe 
        #     delete experiments associated with them
        occ_ids = self.scribe.deleteObsoleteSDMDataReturnIds(usr, obsolete_date, 
                                                             max_num=max_num)    
        earl = EarlJr()
        for oid in occ_ids:
            if oid is not None:
                opth = earl.createDataPath(usr, LMFileType.OCCURRENCE_FILE, 
                                           occsetId=oid)
                if os.path.exists(opth):
                    try:
                        shutil.rmtree(opth)
                    except Exception, e:
                        self.scribe.log.error('Failed to remove {}, {}'.format(opth, str(e)))
                    else:
                        self.scribe.log.info('Removed {} for occset {}'.format(opth, oid))
                else:
                    self.scribe.log.info('Path {} does not exist'.format(opth))
                    
        
    # ...............................................
    def deleteObsoleteGridsets(self, usr, obsolete_date):
        # Should be able to just list old occurrence sets and then have the scribe 
        #     delete experiments associated with them
        gs_fnames = self.scribe.deleteObsoleteUserGridsetsReturnFilenames(usr, obsolete_date)    
        for fname in gs_fnames:
            if fname is not None and os.path.exists(fname):
                try:
                    os.remove(fname)
                except Exception, e:
                    self.scribe.log.error('Failed to remove {}, {}'.format(fname, str(e)))
                else:
                    self.scribe.log.error('Removed {}'.format(fname))
        
        
# ...............................................
if __name__ == '__main__':
    import math
    currtime = DT.gmt().mjd
    future_date = math.ceil(currtime)
    four_weeks_ago = currtime - (ONE_DAY * 28)

    import argparse
    parser = argparse.ArgumentParser(
                description=("""Clear a Lifemapper archive of 
                obsolete or all data for a user 
                or MatrixColumns, Matrices, and Makeflows for a gridset"""))
    parser.add_argument('--gridsetid', type=int, default=None,
            help=('GridsetId for data to delete'))
    parser.add_argument('--user', type=str, default=None,
            help=('UserId for all or old data to delete'))
    parser.add_argument('--obsolete_date', type=float, default=None,
            help=("""Cutoff date as in MJD format for deleting data for this user. 
            Future date (i.e. 12am tomorrow, {}) indicates to clear all data for 
            this user""".format(future_date)))
    parser.add_argument('--count', type=int, default=10,
            help=("""Maximum number of occurrencesets (with dependent SDMs) 
            to delete"""))
    args = parser.parse_args()
    
    gridsetid = args.gridsetid
    usr = args.user
    obsolete_date = args.obsolete_date
    total = args.count
    
    try:    
        datestr = DT.DateTimeFromMJD(obsolete_date).localtime().Format()
    except:
        datestr = str(obsolete_date)
            
    print("""Janitor arguments: 
    gridsetid {}; usr {}; count {}; obsolete_date {}"""
    .format(gridsetid, usr, total, datestr))
    
    jan = Janitor()
    jan.open()
    if gridsetid is not None or usr is not None:
        if gridsetid is not None:
            jan.deleteGridset(gridsetid)
        elif usr is not None:
            if obsolete_date is None:
                import math
                future_date = math.ceil(currtime)
                print("""--obsolete_date argument must be provided, in MJD format,
                to delete {} user data. To clear all data for the user, provide
                a date in the future (i.e. 12am tomorrow, {})"""
                .format(usr, future_date))
            elif obsolete_date > currtime:
                jan.clearUserData(usr)
            else:
                jan.deleteObsoleteGridsets(usr, obsolete_date)
                jan.deleteObsoleteSDMs(usr, obsolete_date, total)

    else:
        print('No valid option for clearing gridset or user data')
    jan.close()
      
"""

"""
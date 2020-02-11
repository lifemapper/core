"""
Todo: Determine if obsolete
"""
import os
import shutil

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import ONE_DAY
from LmCommon.common.time import gmt
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType, LMFormat
from LmServer.common.log import ScriptLogger
from LmServer.common.solr import delete_from_archive_index
from LmServer.db.borgscribe import BorgScribe
from LmServer.notifications.email import EmailNotifier

failedFile = "/home/cjgrady/failed.txt"

# I read that Solr can't delete more than 1000 documents by id at once with the
#    way the query is structure, so chuck it up into groups
DELETE_GROUP_SIZE = 999


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
    def _deleteFiles(self, fnames):
        for fn in fnames:
            if fn is not None and os.path.exists(fn):
                try:
                    os.remove(fn)
                except Exception as e:
                    self.scribe.log.error('Failed to remove {}, {}'
                                          .format(fn, str(e)))
                else:
                    self.scribe.log.info('Removed {}'.format(fn))

    # ...............................................
    def _deletePavsFromSolr(self, pavids):
        try:
            while len(pavids):
                # Get a group of pav ids to delete
                del_pav_ids = pavids[:DELETE_GROUP_SIZE]
                # Shrink the pav ids list
                pavids = pavids[DELETE_GROUP_SIZE:]
                solr_resp = delete_from_archive_index(
                    pav_id=del_pav_ids, user_id=usr)
        except Exception as e:
            print((str(e)))
            self.scribe.log.error(
                'Failed to delete pavs from solr: {}, check solr logs'.format(
                    str(e)))

    # ...............................................
    def reportFailure(self, mesgs):
        notifier = EmailNotifier()
        notifier.sendMessage(['aimee.stewart@ku.edu'],
                             "Failed to delete user data",
                             '\n'.join(mesgs))

    # ...............................................
    def clearUserData(self, usr):
        count, pavids = self.scribe.clearUser(usr)

        # Remove PAVs from Solr
        self._deletePavsFromSolr(pavids)

        # Delete subdirs under user directory
        usrpth = self._earl.createDataPath(usr, LMFileType.BOOM_CONFIG)
        if os.path.exists(usrpth):
            contents = os.listdir(usrpth)
            for c in contents:
                pth = os.path.join(usrpth, c)
                if os.path.isdir(pth):
                    shutil.rmtree(pth)

            # Delete all files under user directory
            contents = os.listdir(usrpth)
            for c in contents:
                fn = os.path.join(usrpth, c)
                os.remove(fn)
                self.scribe.log.info('Removed {}'.format(fn))
        else:
            self.scribe.log.error('User {} dir does not exist'.format(usr))

    # ...............................................
    def deleteGridset(self, gridsetid):
        fnames, pavids = self.scribe.deleteGridsetReturnFilenamesMtxcolids(gridsetid)
        # Remove PAVs from Solr
        self._deletePavsFromSolr(pavids)
        # Delete gridset-related files
        self._deleteFiles(fnames)

    # ...............................................
    def deleteObsoleteSDMs(self, usr, obsolete_date, max_num):
        earl = EarlJr()
        total_obsolete_occs = self.scribe.countOccurrenceSets(userId=usr,
                                beforeTime=obsolete_date)

        for i in range(0, total_obsolete_occs, max_num):
            occids, pavids = self.scribe.deleteObsoleteSDMDataReturnIds(usr,
                                            obsolete_date, max_num=max_num)
            # Remove PAVs from Solr
            self._deletePavsFromSolr(pavids)

            # Delete occurrence SDM files/directories
            for oid in occids:
                if oid is not None:
                    opth = earl.createDataPath(usr, LMFileType.OCCURRENCE_FILE,
                                               occsetId=oid)
                    if os.path.exists(opth):
                        try:
                            shutil.rmtree(opth)
                        except Exception as e:
                            self.scribe.log.error('Failed to remove {}, {}'
                                                  .format(opth, str(e)))
                        else:
                            self.scribe.log.info('Removed {} for occset {}'
                                                 .format(opth, oid))
                    else:
                        self.scribe.log.info('Path {} does not exist'
                                             .format(opth))

    # ...............................................
    def deleteObsoleteGridsets(self, usr, obsolete_date):
        fnames, pavids = self.scribe.deleteObsoleteUserGridsetsReturnFilenamesMtxcolids(usr,
                                                        obsolete_date)
        # Remove PAVs from Solr
        self._deletePavsFromSolr(pavids)

        # Delete Gridset-related makeflows, gridset, matrix files
        self._deleteFiles(fnames)

#     # ...............................................
#     def deleteObsoleteOccdirs(self, usr, currtime):
#         MAX_TEST = 100
#         basename = '{}_occsets.txt'.format(usr)
#         allfname = '/tmp/{}.{}.txt'.format(basename, currtime)
#         basename = '{}_obsoletes.txt'.format(usr)
#         obsfname = '/tmp/{}.{}.txt'.format(basename, currtime)
#         # Delete subdirs under user directory
#         usrpth = self._earl.createDataPath(usr, LMFileType.BOOM_CONFIG)
#         try:
#             allf = open(allfname, 'w')
#             for root, dirs, files in os.walk(usrpth):
#                 for d in dirs:
#                     thispth = os.path.join(root, d)
#                     # relative pathname parts
#                     taildirs = thispth[len(usrpth):]
#                     pts = taildirs.strip(os.sep).split(os.sep)
#                     # if leaf/occ dir, 4 numeric dirs after user dir
#                     if len(pts) == 4 and pts[0] == '000':
#                         occstr = ''.join(pts)
#                         try:
#                             occid = int(occstr)
#                         except:
#                             self.scribe.log.error('Bad directory {}'.format(thispth))
#                         else:
#                             occ = self.scribe.getOccurrenceSet(occId=occid)
#                             if occ is None:
#                                 allf.write(occstr + '\n')
#         except Exception, e:
#             self.scribe.log.error('Exception in walk loop {}'.format(e))
#         finally:
#             allf.close()
#
#         # TODO: Scribe fn to check a list of user/occsets for existence
#         curr_occids = []
#         try:
#             allf = open(allfname, 'r')
#             for i in MAX_TEST:
#                 occst = allf.readline()
#                 try:
#                     occid = int(occst)
#                 except:
#                     scribe.log.error('Bad occsetid {}'.format(occst))
#                 else:
#                     curr_occids.append(occid)
#             obs_occids = self.scribe.testUserOccsets(curr_occids)


# ...............................................
if __name__ == '__main__':
    import math
    currtime = gmt().mjd
    future_date = math.ceil(currtime)
    four_weeks_ago = currtime - (ONE_DAY * 28)

    import argparse
    parser = argparse.ArgumentParser(
                description=("""Clear a Lifemapper archive of 
                obsolete or all data for a user 
                or MatrixColumns, Matrices, and Makeflows for a gridset"""))
    parser.add_argument('gridsetid_or_userid', type=str,
            help=('GridsetId or UserId to delete data for'))
    parser.add_argument('--obsolete_date', type=float, default=None,
            help=("""Cutoff date as in MJD format for deleting data for this user. 
            Future date (i.e. 12am tomorrow, {}) indicates to clear all data for 
            this user""".format(future_date)))
    parser.add_argument('--count', type=int, default=10,
            help=("""Maximum number of occurrencesets (with dependent SDMs) 
            to delete"""))
    args = parser.parse_args()

    gridsetid = usr = None
    gridsetid_or_userid = args.gridsetid_or_userid
    try:
        gridsetid = int(gridsetid_or_userid)
    except:
        usr = gridsetid_or_userid

    obsolete_date = args.obsolete_date
    total = args.count

    print(("""Janitor arguments: 
    gridsetid {}; usr {}; count {}; obsolete_date {}"""
    .format(gridsetid, usr, total, obsolete_date)))

    # TODO: add method to walk files and rmtree dirs for occset absent from DB
    jan = Janitor()
    jan.open()
    if gridsetid is not None or usr is not None:
        if gridsetid is not None:
            jan.deleteGridset(gridsetid)
        elif usr is not None:
            if obsolete_date is None:
                import math
                future_date = math.ceil(currtime)
                print(("""--obsolete_date argument must be provided, in MJD format,
                to delete {} user data. To clear all data for the user, provide
                a date in the future (i.e. 12am tomorrow, {})"""
                .format(usr, future_date)))
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

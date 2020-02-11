"""Deletes old data for a user

Todo: Is this still in use?  Can it be deleted?
"""
import os
import shutil

from LmCommon.common.lmconstants import ONE_DAY, DEFAULT_POST_USER
from LmCommon.common.time import gmt
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.notifications.email import EmailNotifier

USER = "anon"
DAYS_OLD = 14
failedFile = "/home/cjgrady/failed.txt"


def reportFailure(mesgs):
    notifier = EmailNotifier()
    notifier.sendMessage(['cjgrady@ku.edu'],
                         "Failed to delete user occurrence sets",
                         '\n'.join(mesgs))


# ...............................................
def deleteObsoleteSDMs(scribe, userid, obsolete_date, max_num):
    # Should be able to just list old occurrence sets and then have the scribe
    #     delete experiments associated with them
    occ_ids = scribe.deleteObsoleteSDMDataReturnIds(userid, obsolete_date,
                                                    max_num=max_num)
    earl = EarlJr()
    for oid in occ_ids:
        if oid is not None:
            opth = earl.createDataPath(userid, LMFileType.OCCURRENCE_FILE,
                                       occsetId=oid)
            if os.path.exists(opth):
                try:
                    shutil.rmtree(opth)
                except Exception as e:
                    scribe.log.error('Failed to remove {}, {}'.format(opth, str(e)))
                else:
                    scribe.log.info('Removed {} for occset {}'.format(opth, oid))
            else:
                scribe.log.info('Path {} does not exist'.format(opth))


# ...............................................
def deleteObsoleteGridsets(scribe, userid, obsolete_date):
    # Should be able to just list old occurrence sets and then have the scribe
    #     delete experiments associated with them
    gs_fnames = scribe.deleteGridsetReturnFilenames(userid, obsolete_date)
    for fname in gs_fnames:
        if fname is not None and os.path.exists(fname):
            try:
                os.remove(fname)
            except Exception as e:
                scribe.log.error('Failed to remove {}, {}'.format(fname, str(e)))
            else:
                scribe.log.error('Removed {}'.format(fname))


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    anon_obsolesence_date = gmt().mjd - (ONE_DAY * 14)

    import argparse
    parser = argparse.ArgumentParser(
             description=(""""Delete species occurrencesets older than 
             <date in MJD format>, SDM projections, and MatrixColumns computed 
             from those SDM projections for <user>"""))

    parser.add_argument('user_id', default=DEFAULT_POST_USER,
             help=('User to delete data for'))
    parser.add_argument('mjd_date', type=float, default=anon_obsolesence_date,
             help=('Cutoff date in MJD format'))
    parser.add_argument('max_num', type=int, default=10,
                        help=('Maximum number of occurrencesets to delete'))
    parser.add_argument('--logname', type=str, default=None,
             help=('Basename of the logfile, without extension'))

    args = parser.parse_args()
    usr = args.user_id
    mjd_date = args.mjd_date
    max_num = args.max_num
    logname = args.logname

    if logname is None:
        import time
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(scriptname, timestamp)

    log = ScriptLogger(logname)
    scribe = BorgScribe(log)
    try:
        scribe.openConnections()
        if usr == DEFAULT_POST_USER:
            deleteObsoleteGridsets(scribe, usr, mjd_date)
        deleteObsoleteSDMs(scribe, usr, mjd_date, max_num)
    except:
        pass
    finally:
        scribe.closeConnections()

"""
import os
import shutil

from LmCommon.common.lmconstants import ONE_DAY, DEFAULT_POST_USER

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.notifications.email import EmailNotifier

from LmDbServer.tools.deleteObsoleteSDM import *

usr = 'kubi'
mjd_date = 58450
max_num = 10

import time
secs = time.time()
timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
logname = '{}.{}'.format('testdelete', timestamp)
    
log = ScriptLogger(logname)
scribe = BorgScribe(log)
scribe.openConnections()

deleteObsoleteGridsets(scribe, usr, mjd_date)

deleteObsoleteSDMs(scribe, usr, mjd_date, max_num)


scribe.closeConnections()


"""

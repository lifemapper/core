"""
@summary: Deletes old data for a user
@author: CJ Grady

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
import glob
from mx.DateTime import gmt
import os
import shutil

from LmCommon.common.lmconstants import ONE_DAY, DEFAULT_POST_USER

from LmServer.common.localconstants import PUBLIC_USER
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
    occ_fnames = scribe.deleteObsoleteSDMDataReturnFilenames(userid, 
                                                             obsolete_date, 
                                                             max_num=max_num)    
    for fname in occ_fnames:
        if fname is not None:
            pth, base = os.path.split(fname)
            if os.path.exists(pth):
                try:
                    shutil.rmtree(pth)
                except Exception, e:
                    scribe.log.error('Failed to remove {}, {}'.format(pth, str(e)))
                else:
                    scribe.log.error('Removed {} for occset {}'.format(pth, base))
    
# ...............................................
def deleteObsoleteGridsets(scribe, userid, obsolete_date):
    # Should be able to just list old occurrence sets and then have the scribe 
    #     delete experiments associated with them
    gs_fnames = scribe.deleteGridsetReturnFilenames(userid, obsolete_date)    
    for fname in gs_fnames:
        if fname is not None and os.path.exists(fname):
            try:
                os.remove(fname)
            except Exception, e:
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
    usr = args.user
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


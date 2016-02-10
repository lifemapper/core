"""
@summary: Deletes old data

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
import mx.DateTime 
import glob
import os.path
import time

from LmDbServer.common.localconstants import DEFAULT_ALGORITHMS, \
                                  DEFAULT_MODEL_SCENARIO, \
                                  DEFAULT_PROJECTION_SCENARIOS 
                                  
from LmCommon.common.lmconstants import JobStatus

from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import (ARCHIVE_USER, APP_PATH, 
                                            POINT_COUNT_MIN)
from LmServer.common.lmconstants import Priority, ARCHIVE_DELETE_YEAR, \
                                       ARCHIVE_DELETE_MONTH, ARCHIVE_DELETE_DAY
from LmServer.common.log import ThreadLogger
from LmServer.notifications.email import EmailNotifier
from LmServer.sdm.algorithm import Algorithm

DAYS_OLD = 90
STOPFILE = os.path.join(APP_PATH, 'stopcleanup.die')

#.................................................
def report(succeedCount, failList=None):
   notifier = EmailNotifier()
   msg = 'Succeeded %d'
   if failList:
      msg = '\n'.join([msg, 'Failed on: %s' % str(failList)])
   notifier.sendMessage(['aimee.stewart@ku.edu'], 'cleanupData results', msg)

#.................................................
def deleteOldOccData(scribe, olderThan=None):
   failed = []
   total = 0
   limit = 200
   occList = scribe.listOccurrenceSets(0, limit, beforeTime=olderThan, 
                                        userId=ARCHIVE_USER, atom=False)
   while occList:      
      for occ in occList:
         log.info("Deleting occurrence set: %s, %s, date: %s" % 
                  (occ.getId(), occ.displayName, str(occ.statusModTime)))
         success = scribe.completelyRemoveOccurrenceSet(occ)
         if not success:
            failed.append(occ.getId())
         else:
            total += 1
      if os.path.exists(STOPFILE):
         break
      occList = scribe.listOccurrenceSets(0, limit, beforeTime=tooOldTime, 
                                           userId=ARCHIVE_USER, atom=False)
   scribe.log.info('Deleted %d occurrenceSets' % (total))


#.................................................
def reInitializeOccurrenceJobs(scribe, olderThan=None, newerThan=None,
                               status=JobStatus.GENERAL_ERROR):
   jobs = []
   prjScenList = []
   algList = []
   limit = 300
   # Default archive parameters
   for acode in DEFAULT_ALGORITHMS:
      alg = Algorithm(acode)
      alg.fillWithDefaults()
      algList.append(alg)
   mdlScen = scribe.getScenario(DEFAULT_MODEL_SCENARIO)
   for pcode in DEFAULT_PROJECTION_SCENARIOS:
      prjScenList.append(scribe.getScenario(pcode))
   occList = scribe.listOccurrenceSets(0, limit, 
                                       beforeTime=olderThan, 
                                       afterTime=newerThan,
                                       userId=ARCHIVE_USER, 
                                       status=status,
                                       atom=False)
   for occ in occList:
      if (occ.getRawDLocation() is None or 
          not os.path.exists(occ.getRawDLocation())) :
         scribe.log.info('Removing %d %s (time = %s)' % \
                         (occ.getId(), occ.displayName, str(occ.statusModTime)))
         scribe.completelyRemoveOccurrenceSet(occ)
      else:
         jobs = scribe.initSDMChain(ARCHIVE_USER, occ, algList, mdlScen, 
                                    prjScenList, priority=Priority.NORMAL, 
                                    intersectGrid=None, 
                                    minPointCount=POINT_COUNT_MIN)
   scribe.log.info('Initialized %s occsets, total %s chained jobs' % 
                   (str(len(occList)), str(len(jobs))))

#.................................................
def skipAhead(logger, f, lastid):
   line = None
   if lastid < 0:
      logger.debug('Finished deleting data for IDs in %s' % fname)
   elif lastid == 0:
      line = f.readline()
   elif lastid > 0:
      done = False
      while not done:
         line = f.readline()
         try:
            occid = int(line)
         except Exception, e:
            logger.debug('Failed to interpret: %s' % line)
         else:
            if occid == lastid:
               done = True
   return line
   
#.................................................
def deleteData(pth):
   dirdel = 0
   for name in os.listdir(pth):
      deleteme = os.path.join(pth, name)
      if os.path.isfile(deleteme):
         os.remove(deleteme)
      elif name.startswith('pt'):
         # shapefile directory
         for name2 in os.listdir(deleteme):
            deleteme2 = os.path.join(deleteme, name2)
            os.remove(deleteme2)
         os.rmdir(deleteme)
   if len(os.listdir(pth)) == 0:
      logger.debug('Removing directory %s ...' % pth)
      os.rmdir(pth)
      dirdel += 1
   else:
      logger.debug('Path not empty: ', pth) 
   time.sleep(5)

#.................................................
def completelyDeleteObsoleteOccurrenceData(logger, fname, startfname, stopfname):
   dirdel = 0
   if os.path.exists(fname):
      earljr = EarlJr()
      lastid = findStart(logger, startfname)
      f = open(fname, 'r')
      line = skipAhead(logger, f, lastid)
      while line is not None and line != '':
         try:
            occid = int(line)
         except Exception, e:
            logger.debug('Failed to interpret: %s' % line)
         else:
            pth = earljr.createDataPath(ARCHIVE_USER, occsetId=occid)
            if not os.path.exists(pth):
               logger.debug('Path not found: %s' % pth)
            else:
               deleteData(pth)
         if os.path.exists(stopfname):
            writeNextStart(logger, startfname, occid)
            line = None
         else:
            line = f.readline()
      print 'Deleted %d directories' % dirdel
   return dirdel

# ...............................................
def findStart(logger, startfname):
   lastid = 0
   if os.path.exists(startfname):
      with open(startfname, 'r') as f:
         line = f.read()
         try:
            lastid = int(line)
         except:
            print 'Failed to interpret %s' % str(line)               
      os.remove(startfname)
   return lastid
               
# ...............................................
def writeNextStart(logger, startfname, lastid):
   try:
      f = open(startfname, 'w')
      f.write(str(lastid))
      f.close()
   except Exception, e:
      logger.error('Failed to write %d to start file %s' % (lastid, startfname))

#..............................................................................
if __name__ == "__main__":
   """ 
   mal=> \o /home/astewart/notaxOccids.txt
   mal=> select occurrencesetid from occurrenceset where userid = 'lm2' and scientificnameid is null;
   mal=> \o /home/astewart/subtaxOccids.txt
   mal=> select occurrencesetid from lm_fulloccurrenceset where userid = 'lm2' and scientificnameid is not null and taxonomykey != specieskey and taxonomykey != genuskey;
   mal=> \q
   """
#    tooOldTime = mx.DateTime.gmt().mjd
   logger = ThreadLogger('cleanupData')
   tooOldTime = mx.DateTime.DateTime(ARCHIVE_DELETE_YEAR, ARCHIVE_DELETE_MONTH, 
                                     ARCHIVE_DELETE_DAY).mjd
   stopfname = '/home/astewart/cleanupData.die'
   if os.path.exists(stopfname):
      os.remove(stopfname)
   for fname in ('/home/astewart/notaxOccids.txt', 
                 '/home/astewart/subtaxOccids.txt'):
      fnamewoext, ext  = os.path.splitext(fname)
      startfname = fnamewoext + '.start'      
      dirsDeleted = completelyDeleteObsoleteOccurrenceData(logger, fname, 
                                                           startfname, stopfname)
   

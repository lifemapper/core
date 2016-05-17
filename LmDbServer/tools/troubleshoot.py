"""
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import argparse
import mx.DateTime as dt
from types import ListType, TupleType

from LmBackend.common.occparse import OccDataParser

from LmCommon.common.apiquery import BisonAPI, GbifAPI, IdigbioAPI
from LmCommon.common.lmconstants import (BISON_OCC_FILTERS, BISON_HIERARCHY_KEY,
            BISON_MIN_POINT_COUNT, Instances, ProcessType, DEFAULT_EPSG, 
            DEFAULT_POST_USER, JobStatus, ONE_DAY, ONE_HOUR, ONE_MIN,
            IDIGBIO_GBIFID_FIELD)

from LmDbServer.common.localconstants import WORKER_JOB_LIMIT
from LmDbServer.pipeline.pipeline import _Worker

from LmServer.base.lmobj import LMError, LmHTTPError
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import (JobFamily, Priority, 
                                         PrimaryEnvironment, LOG_PATH)
from LmServer.common.localconstants import (ARCHIVE_USER, POINT_COUNT_MIN, 
                                            APP_PATH, DATASOURCE)
from LmServer.db.scribe import Scribe
from LmServer.notifications.email import EmailNotifier
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.omJob import OmProjectionJob, OmModelJob
from LmServer.sdm.meJob import MeProjectionJob, MeModelJob
from LmServer.sdm.sdmJob import SDMOccurrenceJob

TROUBLESHOOT_UPDATE_INTERVAL = 2 * ONE_HOUR
COMPUTE_CHECK_INTERVAL = 12 * ONE_HOUR
GBIF_SERVICE_INTERVAL = 3 * ONE_MIN            

# .............................................................................
class Troubleshooter(object):
   def __init__(self, updateInterval):
      currTime = dt.gmt().mjd
      self.checkJobTime = currTime - COMPUTE_CHECK_INTERVAL
      self.updateTime = None

# ...............................................
   def _organizeProblemObjects(self, objects, objname):
      problems = {}
      for o in objects:
         usr = o.getUserId()
         if not (problems.has_key(usr)):
            problems[usr] = {objname: set([])}
         problems[usr][objname].add(o)
      return problems

# ...............................................
   def _notifyPeople(self, subject, message, recipients=None):
      if recipients is None:
         recipients = self.developers
      elif not (isinstance(recipients, ListType) 
                or isinstance(recipients, TupleType)):
         recipients = [recipients]
      notifier = EmailNotifier()
      try:
         notifier.sendMessage(recipients, subject, message)
      except Exception, e:
         self.log.error('Failed to notify %s about %s' 
                        % (str(recipients), subject))

# ...............................................
   def _notifyOfStalledExperiments(self, oldtime, cmd, startStatus=None, 
                                   endStatus=None):
      try:
         models, projs = self._scribe.findProblemObjects(oldtime, 
                                       startStat=startStatus, endStat=endStatus, 
                                       ignoreUser=ARCHIVE_USER)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
         
      probs = self._organizeProblemObjects(models, 'Model')
      pprobs = self._organizeProblemObjects(projs, 'Projection')
      for usr in pprobs.keys():
         if probs.has_key(usr):
            probs[usr]['Projection'] = pprobs[usr]['Projection']
         
      if probs.keys():
         msg = ('Problem SDM Data started before %s (mjd=%d)' 
                % (dt.DateTimeFromMJD(oldtime).localtime().Format()))
         for usr in probs.keys():
            msg += '%s\n' % usr
            msg += '  ModelId  Status\n'
            for m in probs[usr]['Model']:
               msg += '  %s     %s\n' % (m.getId(), m.status)
         self.log.debug(msg)
         self._notifyPeople('{} experiments'.format(cmd), msg)
      
# ...............................................
   def run(self, commandList):
      currtime = dt.gmt().mjd
      if (self.updateTime is None or
          currtime - self.updateTime > TROUBLESHOOT_UPDATE_INTERVAL): 
         oldtime = currtime - TROUBLESHOOT_UPDATE_INTERVAL
         self.updateTime = currtime
      for cmd in commandList:
         cmd = cmd.lower()
         if cmd == 'limbo':
               self._notifyOfStalledExperiments(self.checkJobTime, cmd,
                                                startStatus=JobStatus.INITIALIZE, 
                                                endStatus=JobStatus.COMPLETE)
         elif cmd == 'error':
               self._notifyOfStalledExperiments(oldtime, cmd,
                                                startStatus=JobStatus.GENERAL_ERROR, 
                                                ignoreUser=ARCHIVE_USER)

### Main ###
# ...............................................
if __name__ == '__main__':
#    parser = argparse.ArgumentParser(
#             description=('Search for stalled, error, or other problems with ' +
#                          'boom pipeline'))
#    parser.add_argument('commands', help="Problem type(s) to solve", nargs="*")
# 
#    args = parser.parse_args()
#    cmdList = args.commands
   cmdList = ['limbo', 'error']
   app = Troubleshooter(cmdList)
   app.run()

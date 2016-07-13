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
import mx.DateTime as dt
from types import ListType, TupleType

from LmCommon.common.lmconstants import JobStatus, ONE_HOUR

from LmServer.base.lmobj import LMError
from LmServer.common.localconstants import ARCHIVE_USER
from LmServer.db.scribe import Scribe
from LmServer.notifications.email import EmailNotifier

TROUBLESHOOT_UPDATE_INTERVAL = 2 * ONE_HOUR

# .............................................................................
class Troubleshooter(object):
   def __init__(self, cmd):
      currTime = dt.gmt().mjd

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
      oldtime = currtime - TROUBLESHOOT_UPDATE_INTERVAL
      for cmd in commandList:
         cmd = cmd.lower()
         if cmd == 'limbo':
            self.log.debug("troubleshoot limbo is not ready for prime time")
            self.log.info('Check for stalled jobs of all users')
            self._notifyOfStalledExperiments(oldtime, cmd,
                                             startStatus=JobStatus.INITIALIZE, 
                                             endStatus=JobStatus.COMPLETE)
         elif cmd == 'error':
            self.log.debug("troubleshoot error is not ready for prime time")
            self.log.info('Check for error jobs of non-archive users')
            self._notifyOfStalledExperiments(oldtime, cmd,
                                             startStatus=JobStatus.GENERAL_ERROR, 
                                             ignoreUser=ARCHIVE_USER)

### Main ###
# ...............................................
if __name__ == '__main__':
   cmdList = ['limbo', 'error']
   app = Troubleshooter(cmdList)
   app.run(cmdList)

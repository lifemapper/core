"""Troubleshooter

Todo:
    * Most likely remove this module.  It is very likely out of date
"""
from LmCommon.common.lmconstants import JobStatus, ONE_HOUR

from LmBackend.common.lmobj import LMError
from LmCommon.common.time import gmt
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.db.borgscribe import BorgScribe
from LmServer.notifications.email import EmailNotifier

TROUBLESHOOT_UPDATE_INTERVAL = 2 * ONE_HOUR

# .............................................................................
class Troubleshooter(object):
    def __init__(self, cmd):
        currTime = gmt().mjd

# ...............................................
    def _organizeProblemObjects(self, objects, objname):
        problems = {}
        for o in objects:
            usr = o.getUserId()
            if not (usr in problems):
                problems[usr] = {objname: set([])}
            problems[usr][objname].add(o)
        return problems

# ...............................................
    def _notifyPeople(self, subject, message, recipients=None):
        if recipients is None:
            recipients = self.developers
        elif not (isinstance(recipients, (list, tuple))):
            recipients = [recipients]
        notifier = EmailNotifier()
        try:
            notifier.sendMessage(recipients, subject, message)
        except Exception as e:
            self.log.error('Failed to notify %s about %s' 
                                % (str(recipients), subject))

# ...............................................
    def _notifyOfStalledExperiments(self, oldtime, cmd, startStatus=None, 
                                              endStatus=None):
        try:
            models, projs = self._scribe.findProblemObjects(oldtime, 
                                                    startStat=startStatus, endStat=endStatus, 
                                                    ignoreUser=PUBLIC_USER)
        except Exception as e:
            if not isinstance(e, LMError):
                e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
            
        probs = self._organizeProblemObjects(models, 'Model')
        pprobs = self._organizeProblemObjects(projs, 'Projection')
        for usr in list(pprobs.keys()):
            if usr in probs:
                probs[usr]['Projection'] = pprobs[usr]['Projection']
            
        if list(probs.keys()):
            msg = ('Problem SDM Data started before {}'.format(oldtime))
            for usr in list(probs.keys()):
                msg += '%s\n' % usr
                msg += '  ModelId  Status\n'
                for m in probs[usr]['Model']:
                    msg += '  %s      %s\n' % (m.getId(), m.status)
            self.log.debug(msg)
            self._notifyPeople('{} experiments'.format(cmd), msg)
        
# ...............................................
    def run(self, commandList):
        currtime = gmt().mjd
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
                                                            ignoreUser=PUBLIC_USER)

### Main ###
# ...............................................
if __name__ == '__main__':
    cmdList = ['limbo', 'error']
    app = Troubleshooter(cmdList)
    app.run(cmdList)

"""
"""
# import argparse
import os

from LmCommon.common.lmconstants import (DEFAULT_POST_USER, 
                                    ONE_MONTH, ONE_DAY, ONE_HOUR, JobStatus)
from LmCommon.common.time import gmt
from LmServer.common.lmconstants import ReferenceType
from LmServer.common.localconstants import PUBLIC_USER, TROUBLESHOOTERS
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.notifications.email import EmailNotifier

oneHourAgo = "{0:.2f}".format((gmt() - ONE_HOUR).mjd)
oneDayAgo = "{0:.2f}".format((gmt() - ONE_DAY).mjd)
oneWeekAgo = "{0:.2f}".format((gmt() - ONE_DAY*7).mjd)
oneMonthAgo = "{0:.2f}".format((gmt() - ONE_MONTH).mjd)

DISPLAY = {oneHourAgo: 'Hour', 
              oneDayAgo: 'Day', 
              oneWeekAgo: 'Week', 
              oneMonthAgo: 'Month', None: 'Total'}
USERS = (DEFAULT_POST_USER, PUBLIC_USER)
TIMES = (oneHourAgo, oneDayAgo, oneWeekAgo, None)

# ...............................................
def notifyPeople(logger, subject, message, recipients=TROUBLESHOOTERS):
    if not isinstance(recipients, (list, tuple)):
        recipients = [recipients]
    notifier = EmailNotifier()
    try:
        notifier.sendMessage(recipients, subject, message)
    except Exception as e:
        logger.error('Failed to notify {} about {}'.format(str(recipients), subject))

# ...............................................
def _getProgress(scribe, usr, aftertime, afterStatus, beforeStatus):
    progress = {}
    if aftertime is not None:
        aftertime = float(aftertime)
    for otype in ReferenceType.progressTypes():
        if otype == ReferenceType.OccurrenceSet:
            count = scribe.countOccurrenceSets(userId=usr, afterTime=aftertime, 
                                               afterStatus=afterStatus, 
                                               beforeStatus=beforeStatus)
        elif otype == ReferenceType.SDMProjection:
            count = scribe.countSDMProjects(userId=usr, afterTime=aftertime, 
                                            afterStatus=afterStatus, 
                                            beforeStatus=beforeStatus)
        elif otype == ReferenceType.MatrixColumn:
            count = scribe.countMatrixColumns(userId=usr, afterTime=aftertime, 
                                              afterStatus=afterStatus, 
                                              beforeStatus=beforeStatus)
        elif otype == ReferenceType.Matrix:
            count = scribe.countMatrices(userId=usr, afterTime=aftertime, 
                                         afterStatus=afterStatus, 
                                         beforeStatus=beforeStatus)
        progress[otype] = count
    return progress

# ...............................................
def getStats(scribe, afterStatus=JobStatus.COMPLETE, beforeStatus=None):
    outputLines = [] 
    if afterStatus == beforeStatus:
        stat = afterStatus
    elif beforeStatus is None:
        stat = '>= {}'.format(afterStatus)
    else:
        stat = '{} - {}'.format(afterStatus, beforeStatus)
    title = 'STATUS {}'.format(stat)
    header = ['', '***********************************', title, 
                        '***********************************']
    outputLines.extend(header)
    for aftertime in TIMES:
        tmHeader = ('', '**************', DISPLAY[aftertime],'**************')
        outputLines.extend(tmHeader)
        print('\n'.join(outputLines))
        for usr in USERS:
            theseLines = []
            theseStats = _getProgress(scribe, usr, aftertime, 
                                      afterStatus, beforeStatus)
            theseLines.append('')
            theseLines.append('User: {}'.format(usr))
            for rt in ReferenceType.progressTypes():
                theseLines.append('    {}: {}'.format(ReferenceType.name(rt), 
                                                      theseStats[rt]))
            print('\n'.join(theseLines))
            outputLines.extend(theseLines)
        outputLines.append('')
    output = '\n'.join(outputLines)
    return output
                

# ...............................................
# ...............................................
if __name__ == '__main__':
    basename = os.path.splitext(os.path.basename(__file__))[0]
    logger = ScriptLogger(basename)
    scribe = BorgScribe(logger)
    scribe.openConnections()
    
    output_success = getStats(scribe, afterStatus=JobStatus.COMPLETE, 
                              beforeStatus=JobStatus.COMPLETE)
    output_fail = getStats(scribe, afterStatus=JobStatus.GENERAL_ERROR)
    output = '\n\n'.join((output_success, output_fail))
    notifyPeople(logger, 'LM database stats', output)
    logger.info(output)
    scribe.closeConnections()
            
    
    
"""
import os

from LmServer.notifications.email import EmailNotifier
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, 
                                ONE_MONTH, ONE_DAY, ONE_HOUR, JobStatus)
from LmServer.common.localconstants import PUBLIC_USER, TROUBLESHOOTERS
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.lmconstants import ReferenceType
from LmServer.tools.checkProgress import *

oneHourAgo = "{0:.2f}".format((gmt() - ONE_HOUR).mjd)
oneDayAgo = "{0:.2f}".format((gmt() - ONE_DAY).mjd)
oneMonthAgo = "{0:.2f}".format((gmt() - ONE_MONTH).mjd)

DISPLAY = {oneHourAgo: 'Hour', 
              oneDayAgo: 'Day', 
              oneMonthAgo: 'Month', None: 'Total'}
USERS = (DEFAULT_POST_USER, PUBLIC_USER)
TIMES = (oneHourAgo, oneMonthAgo, oneDayAgo, None)

basename = 'testit'
logger = ScriptLogger(basename)
scribe = BorgScribe(logger)
scribe.openConnections()

output = getStats(scribe, afterStatus=JobStatus.COMPLETE, 
                        beforeStatus=JobStatus.COMPLETE)
output = getStats(scribe, afterStatus=JobStatus.GENERAL_ERROR)

logger.info(output)
scribe.closeConnections()

"""
    
    
    
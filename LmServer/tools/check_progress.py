"""Check recent progress of job computations.
"""
# import argparse
import os

from LmCommon.common.lmconstants import (
    DEFAULT_POST_USER, JobStatus, ONE_DAY, ONE_HOUR, ONE_MONTH)
from LmCommon.common.time import gmt
from LmServer.common.lmconstants import ReferenceType
from LmServer.common.localconstants import PUBLIC_USER, TROUBLESHOOTERS
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.notifications.email import EmailNotifier

ONE_HOUR_AGO = "{0:.2f}".format((gmt() - ONE_HOUR).mjd)
ONE_DAY_AGO = "{0:.2f}".format((gmt() - ONE_DAY).mjd)
ONE_WEEK_AGO = "{0:.2f}".format((gmt() - ONE_DAY * 7).mjd)
ONE_MONTH_AGO = "{0:.2f}".format((gmt() - ONE_MONTH).mjd)

DISPLAY = {
    ONE_HOUR_AGO: 'Hour',
    ONE_DAY_AGO: 'Day',
    ONE_WEEK_AGO: 'Week',
    ONE_MONTH_AGO: 'Month',
    None: 'Total'
    }

USERS = (DEFAULT_POST_USER, PUBLIC_USER)
TIMES = (ONE_HOUR_AGO, ONE_DAY_AGO, ONE_WEEK_AGO, None)


# ...............................................
def notify_people(logger, subject, message, recipients=TROUBLESHOOTERS):
    """Notify people of job progress
    """
    if not isinstance(recipients, (list, tuple)):
        recipients = [recipients]
    notifier = EmailNotifier()
    try:
        notifier.send_message(recipients, subject, message)
    except Exception:
        logger.error(
            'Failed to notify {} about {}'.format(str(recipients), subject))


# ...............................................
def _get_progress(scribe, usr, after_time, after_status, before_status):
    progress = {}
    if after_time is not None:
        after_time = float(after_time)
    for otype in ReferenceType.progress_types():
        if otype == ReferenceType.OccurrenceSet:
            count = scribe.count_occurrence_sets(
                user_id=usr, after_time=after_time, after_status=after_status,
                before_status=before_status)
        elif otype == ReferenceType.SDMProjection:
            count = scribe.count_sdm_projects(
                user_id=usr, after_time=after_time, after_status=after_status,
                before_status=before_status)
        elif otype == ReferenceType.MatrixColumn:
            count = scribe.count_matrix_columns(
                user_id=usr, after_time=after_time, after_status=after_status,
                before_status=before_status)
        elif otype == ReferenceType.Matrix:
            count = scribe.count_matrices(
                user_id=usr, after_time=after_time, after_status=after_status,
                before_status=before_status)
        progress[otype] = count
    return progress


# ...............................................
def get_stats(scribe, after_status=JobStatus.COMPLETE, before_status=None):
    """Get stats for the specified status range
    """
    output_lines = []
    if after_status == before_status:
        stat = after_status
    elif before_status is None:
        stat = '>= {}'.format(after_status)
    else:
        stat = '{} - {}'.format(after_status, before_status)
    title = 'STATUS {}'.format(stat)
    header = ['', '***********************************', title,
              '***********************************']
    output_lines.extend(header)
    for after_time in TIMES:
        tm_header = (
            '', '**************', DISPLAY[after_time], '**************')
        output_lines.extend(tm_header)
        print('\n'.join(output_lines))
        for usr in USERS:
            these_lines = []
            these_stats = _get_progress(
                scribe, usr, after_time, after_status, before_status)
            these_lines.append('')
            these_lines.append('User: {}'.format(usr))
            for ref_type in ReferenceType.progress_types():
                these_lines.append(
                    '    {}: {}'.format(
                        ReferenceType.name(ref_type), these_stats[ref_type]))
            print('\n'.join(these_lines))
            output_lines.extend(these_lines)
        output_lines.append('')
    output = '\n'.join(output_lines)
    return output


# ...............................................
def main():
    """Main method for script
    """
    basename = os.path.splitext(os.path.basename(__file__))[0]
    logger = ScriptLogger(basename)
    scribe = BorgScribe(logger)
    scribe.open_connections()

    output_success = get_stats(
        scribe, after_status=JobStatus.COMPLETE,
        before_status=JobStatus.COMPLETE)
    output_fail = get_stats(scribe, after_status=JobStatus.GENERAL_ERROR)
    output = '\n\n'.join((output_success, output_fail))
    notify_people(logger, 'LM database stats', output)
    logger.info(output)
    scribe.close_connections()


# ...............................................
if __name__ == '__main__':
    main()

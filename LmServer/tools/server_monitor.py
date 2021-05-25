"""This script runs several monitoring tests on a Lifemapper front end.
"""
import subprocess

from LmServer.common.localconstants import WEBSERVICES_ROOT, TROUBLESHOOTERS
from LmServer.notifications.email import EmailNotifier


# .............................................................................
class ProcKeys:
    """Constants class for process dictionary keys
    """
    USER = 'user'
    PID = 'pid'
    ELAPSED_TIME = 'elapsed_time'
    COMMAND = 'cmd'


# .............................................................................
def _get_time_tuple(etime):
    """Gets a tuple of time information from an elapsed time string from ps.

    Args:
        etime (str): A string representing elapsed time of a process coming
            from the output of ps ao userid,pid,etime,cmd.

    Returns:
        tuple: A tuple with integer values for running time in
            (days, hours, minutes, seconds).
    """
    day_split = etime.split('-')
    hh, mm, ss = day_split[-1].split(':')
    if len(day_split) > 1:
        days = int(day_split[0])
    else:
        days = 0
    return (days, int(hh), int(mm), int(ss))


# .............................................................................
def _get_proc_info(proc_line):
    """Gets a dictionary containing information about a process from ps.

    Args:
        proc_line (str): A single process listed in ps ao userid,pid,etime,cmd
            output.

    Returns:
        dict: A dictionary of process information.
    """
    parts = proc_line.split()
    return {
        ProcKeys.USER: parts[0],
        ProcKeys.PID: int(parts[1]),
        ProcKeys.ELAPSED_TIME: _get_time_tuple(parts[2]),
        ProcKeys.COMMAND: ' '.join(parts[3:])
    }


# .............................................................................
def _get_matching_processes(search_string):
    """Get processes matching the provided search string

    Args:
        search_string (str): A string to look for in the running command output
            by 'ps'.

    Returns:
        list of dict: Returns a list of dictionary objects for each process
            that matches the search string and running time criteria.
    """
    matching_procs = []
    proc_lines = subprocess.Popen(
        ['ps', 'axo', 'user,pid,etime,cmd'], stdout=subprocess.PIPE
        ).communicate(0)[0].split('\n')
    for proc_line in proc_lines[1:]:
        try:
            proc = _get_proc_info(proc_line)
            if proc[ProcKeys.COMMAND].lower().find(search_string.lower()) >= 0:
                matching_procs.append(proc)
        except Exception:
            pass
    return matching_procs


# .............................................................................
def get_long_running_processes(search_string, test_age=(0, 12, 0, 0)):
    """Get long running processes matching the search string

    Args:
        search_string (str): A string to look for in the running command output
            by 'ps'.
        age (tuple): Tuple of (Days, Hours, Minutes, Seconds) for determining
            which processes are long-running.

    Returns:
        list of dict: Returns a list of dictionary objects for each process
            that matches the search string and running time criteria.
    """
    long_running_processes = []
    string_match_processes = _get_matching_processes(search_string)
    for proc in string_match_processes:
        if proc[ProcKeys.ELAPSED_TIME] >= test_age:
            long_running_processes.append(proc)
    return long_running_processes


# .............................................................................
def get_number_of_running_workers():
    """Gets the number of running workers via qstat.

    Returns:
        int: The number of running workers
    """
    qstat_lines = subprocess.Popen(
        ['qstat'], stdout=subprocess.PIPE).communicate(0)[0].split('\n')
    count = 0
    for line in qstat_lines:
        if line.find('makeflow') and not line.find('qw'):
            count += 1
    return count


# .............................................................................
def main():
    """Main method for script
    """
    msgs = []
    is_okay = True
    # If Matt Daemon has been running for longer than 1 second, will be true
    matt_d_running = bool(
        len(get_long_running_processes(
            'mattDaemon', test_age=(0, 0, 0, 1))) > 0)
    if matt_d_running:
        msgs.append('Matt Daemon is running on {}'.format(WEBSERVICES_ROOT))
    else:
        msgs.append(
            'Matt Daemon is NOT running on {}'.format(WEBSERVICES_ROOT))
        is_okay = False

    # Look for makeflows running for more than 12 hours
    long_makeflows = get_long_running_processes(
        'makeflow', test_age=(0, 12, 0, 0))
    for proc in long_makeflows:
        msgs.append(
            'Makeflow at PID: {} has been running for: {}'.format(
                proc[ProcKeys.PID], proc[ProcKeys.ELAPSED_TIME]))

    # Check to see if workers are running
    num_workers = get_number_of_running_workers()
    msgs.append('There are {} workers running'.format(num_workers))

    notifier = EmailNotifier()
    if is_okay:
        subject = 'Compute processes look okay on {}'.format(WEBSERVICES_ROOT)
    else:
        subject = '!!There are problems on {}!!'.format(WEBSERVICES_ROOT)

    notifier.send_message(TROUBLESHOOTERS, subject, '<br /><br />'.join(msgs))


# .............................................................................
if __name__ == '__main__':
    main()

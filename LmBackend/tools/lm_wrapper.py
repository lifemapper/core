"""This class wraps a command and ensures that the outputs are always created
"""
import argparse
import os
import signal

from LmBackend.common.lmobj import LMObject
from LmBackend.common.subprocessManager import SubprocessRunner

# Need this to be module global for handling signals
current_procs = set()
shutdown = False


# .............................................................................
def handle_signal(signum, frame):
    """Handle a signal sent to the process and pass it on to the subprocess.

    Args:
        signum (int): The signal to handle.
        frame: The current frame.
    """
    print(("Received signal: {}".format(signum)))

    for proc in current_procs:
        print(('Signaling: "{}"'.format(proc.cmd)))
        proc.signal(signum)


# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i', dest='input_files', action='append',
        help='These input files must be non-empty')
    parser.add_argument(
        'cmd', type=str, help='This is the command to be wrapped')
    parser.add_argument(
        'touch_files', type=str, nargs='*',
        help='These files will be created by this script if necessary')
    args = parser.parse_args()

    # Install signal handler
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    skip_comps = False
    # Ensure required input files are not empty
    if args.input_files is not None:
        for fn in args.input_files:
            if os.path.getsize(fn) < 1:
                print(('File: {} has 0 length'.format(fn)))
                skip_comps = True

    if not skip_comps:
        try:
            sub_proc_runner = SubprocessRunner(args.cmd)
            current_procs.add(sub_proc_runner)
            exit_code, std_err = sub_proc_runner.run()
            print(('Exit code: {}'.format(exit_code)))
            print(std_err)
        except Exception as e:
            print(str(e))
    else:
        print(
            'One or more required input files had zero length, skipping.')

    # Don't try this if we were told to shut down
    if not shutdown:
        for fn in args.touch_files:
            if not os.path.exists(fn):
                lmo = LMObject()
                lmo.ready_filename(fn)
                with open(fn, 'a') as outF:
                    os.utime(fn, None)

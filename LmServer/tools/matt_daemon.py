"""Deamon script in charge of job computation control

This module contains the job controller daemon that will run as a daemon
process and continually provide Makeflow processes that workers can connect to

Todo:
    * Catch failed makeflows
"""
import argparse
import glob
import os
import shutil
import signal
import subprocess
import sys
from time import sleep
import traceback

from LmBackend.common.daemon import Daemon, DaemonCommands
from LmCommon.common.lmconstants import LM_USER, JobStatus, ENCODING
from LmCommon.common.time import gmt
from LmServer.base.utilities import is_lm_user
from LmServer.common.lmconstants import (
    CATALOG_SERVER_BIN, CS_OPTIONS, LOG_PATH, MAKEFLOW_BIN, MAKEFLOW_OPTIONS,
    MAKEFLOW_WORKSPACE, MATT_DAEMON_PID_FILE, RM_OLD_WORKER_DIRS_CMD,
    WORKER_FACTORY_BIN, WORKER_FACTORY_OPTIONS)
from LmServer.common.localconstants import MAX_MAKEFLOWS, WORKER_PATH
from LmServer.common.log import LmServerLogger
from LmServer.db.borg_scribe import BorgScribe


# .............................................................................
class KeepAction:
    """Constants class for determining delete action level
    """
    NONE = 0
    ERROR = 1
    ALL = 2

    @staticmethod
    def options():
        """Retrieve possible options
        """
        return [KeepAction.NONE, KeepAction.ERROR, KeepAction.ALL]


# .............................................................................
class MattDaemon(Daemon):
    """Class to manage makeflow subprocesses

    The MattDaemon class manages a pool of Makeflow subprocesses that workers
    connect to.  Once one of the Makeflow processes completes, it is replaced
    by the next available job chain.  Workers can be located anywhere, but at
    least one should probably run locally for local processes like database
    updates.
    """
    keep_logs = KeepAction.NONE
    keep_makeflows = KeepAction.NONE
    keep_outputs = KeepAction.NONE
    log_dir = LOG_PATH

    # .............................
    def initialize(self):
        """Initialize the MattDaemon
        """
        # Makeflow pool
        self._mf_pool = []
        self.cs_proc = None
        self.wf_proc = None

        self.sleep_time = 30
        self.max_makeflows = MAX_MAKEFLOWS
        self.workspace = MAKEFLOW_WORKSPACE

        # Establish db connection
        self.scribe = BorgScribe(self.log)
        self.scribe.open_connections()

        # Start catalog server
        self.start_catalog_server()

        # Start worker factory
        self.start_worker_factory()

    # .............................
    def run(self):
        """Runs workflows until told to stop

        This method will continue to run while the self.keep_running attribute
        is true
        """
        try:
            self.log.info("Running")

            while self.keep_running and os.path.exists(self.pidfile):

                # Check if catalog server and factory are running
                # TODO: Should we attempt to restart these if they are stopped?
                if self.cs_proc.poll() is not None:
                    raise Exception('Catalog server has stopped')

                if self.wf_proc.poll() is not None:
                    raise Exception('Worker factory has stopped')

                # Check if there are any empty slots
                num_running = self.get_number_of_running_processes()

                #  Add mf processes for empty slots
                for mf_obj, mf_doc_fn in self.get_makeflows(
                        self.max_makeflows - num_running):

                    if os.path.exists(mf_doc_fn):
                        if mf_obj.priority is not None:
                            priority = mf_obj.priority
                        else:
                            priority = 1
                        cmd = self._get_makeflow_command(
                            'lifemapper-{0}'.format(mf_obj.get_id()),
                            mf_doc_fn, priority=priority)
                        self.log.debug(cmd)

                        # File outputs
                        out_log_filename = os.path.join(
                            self.log_dir, 'mf_{}.out'.format(mf_obj.get_id()))
                        err_log_filename = os.path.join(
                            self.log_dir, 'mf_{}.err'.format(mf_obj.get_id()))
                        proc_out = open(out_log_filename, 'a', encoding=ENCODING)
                        proc_err = open(err_log_filename, 'a', encoding=ENCODING)

                        mf_proc = subprocess.Popen(
                            cmd, shell=True, stdout=proc_out, stderr=proc_err,
                            preexec_fn=os.setsid)

                        self._mf_pool.append(
                            [mf_obj, mf_doc_fn, mf_proc, proc_out, proc_err])
                    else:
                        self._cleanup_makeflow(
                            mf_obj, mf_doc_fn, 2,
                            lm_status=JobStatus.IO_GENERAL_ERROR)
                # Sleep
                self.log.info('Sleep for {} seconds'.format(self.sleep_time))
                sleep(self.sleep_time)

            self.log.debug('Exiting')
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error('An error occurred')
            self.log.error(str(e))
            self.log.error(tb)
            self.stop_worker_factory()
            self.stop_catalog_server()

    # .............................
    def get_makeflows(self, count):
        """Get available makeflow documents and move to workspace

        Use the scribe to get available makeflow documents and moves DAG files
        to workspace

        Args:
            count (int): The number of Makeflows to retrieve

        Note:
            * If the DAG exists in the workspace, assume that things failed and
                we should try to continue
        """
        raw_mfs = self.scribe.find_mf_chains(count)

        mfs = []
        for mf_obj in raw_mfs:
            # New filename
            orig_loc = mf_obj.get_dlocation()
            new_loc = os.path.join(self.workspace, os.path.basename(orig_loc))

            # Attempt to move the file to the workspace if needed
            try:
                # Move to workspace if it does not exist (see note)
                if not os.path.exists(new_loc):
                    shutil.copyfile(orig_loc, new_loc)
                # Add to mfs list
                mfs.append((mf_obj, new_loc))
            except Exception as e:
                # Could fail if original dlocation does not exist
                self.log.debug('Could not move mf doc: {}'.format(str(e)))

        return mfs

    # .............................
    def get_number_of_running_processes(self):
        """Returns the number of running processes
        """
        num_running = 0
        for idx in range(len(self._mf_pool)):
            if self._mf_pool[idx] is not None:
                result = self._mf_pool[idx][2].poll()
                if result is None:
                    num_running += 1
                else:
                    exit_status = self._mf_pool[idx][2].returncode
                    mf_obj = self._mf_pool[idx][0]
                    mf_doc_fn = self._mf_pool[idx][1]
                    # Close log files
                    proc_out = self._mf_pool[idx][3]
                    proc_err = self._mf_pool[idx][4]
                    log_files = [proc_out.name, proc_err.name]
                    proc_out.close()
                    proc_err.close()
                    self._mf_pool[idx] = None

                    self._cleanup_makeflow(
                        mf_obj, mf_doc_fn, exit_status, log_files=log_files)

        self._mf_pool = [_f for _f in self._mf_pool if _f]
        return num_running

    # .............................
    def on_update(self):
        """Called on Daemon update request
        """
        # Read configuration
        # self.read_configuration()

        self.log.debug('Update signal caught!')

    # .............................
    def on_shutdown(self):
        """Called on Daemon shutdown request
        """
        self.log.debug('Shutdown signal caught!')
        self.scribe.close_connections()
        Daemon.on_shutdown(self)

        # Wait for makeflows to finish
        # 60 * 3 -- CJG: Changed to 30 seconds, need to send time via signal or
        #    something
        max_time = 30
        time_waited = 0
        num_running = self.get_number_of_running_processes()
        self.log.debug(
            'Waiting on {} makeflow processes to finish'.format(num_running))
        while num_running > 0 and time_waited < max_time:
            self.log.debug(
                'Waiting on {} makeflow processes to finish'.format(
                    num_running))
            sleep(self.sleep_time)
            time_waited += self.sleep_time
            try:
                num_running = self.get_number_of_running_processes()
            except Exception:
                num_running = 0

        if time_waited >= max_time:
            self.log.debug(
                'Waited for {} seconds.  Stopping.'.format(time_waited))
            for running_proc in self._mf_pool:
                try:
                    (mf_obj, _, mf_proc, proc_std_out, proc_std_err
                     ) = running_proc
                    self.log.debug(
                        'Killing process group: {}'.format(
                            os.getpgid(mf_proc.pid)))
                    os.killpg(os.getpgid(mf_proc.pid), signal.SIGKILL)
                    proc_std_out.close()
                    proc_std_err.close()
                    # TODO: Set up a pause status or some way to recover from
                    #    where it was at
                    mf_obj.update_status(
                        JobStatus.INITIALIZE, mod_time=gmt().mjd)
                    self.scribe.update_object(mf_obj)
                except Exception as e:
                    self.log.debug(str(e))

        # Stop worker factory
        try:
            self.stop_worker_factory()
        except Exception:
            pass

        # Stop catalog server
        try:
            self.stop_catalog_server()
        except Exception:
            pass

    # .............................
    def set_debug(self, debug_logs, debug_makeflows, debug_outputs, log_dir):
        """Set debugging options

        Args:
            debug_logs (int): Keep logs - 0: None, 1: Failures, 2: All
            debug_makeflows (int): Keep makeflows - 0: None, 1: Failures,
                2: All
            debug_outputs (int): Keep outputs - 0: None, 1: Failures, 2: All
            log_dir (str): A directory to keep log files
        """
        self.keep_logs = debug_logs
        self.keep_makeflows = debug_makeflows
        self.keep_outputs = debug_outputs
        if log_dir is not None:
            self.log_dir = log_dir

    # .............................
    def start_catalog_server(self):
        """Start the local catalog server
        """
        self.log.debug('Starting catalog server')
        cmd = '{} {}'.format(CATALOG_SERVER_BIN, CS_OPTIONS)
        self.cs_proc = subprocess.Popen(
            cmd, shell=True, preexec_fn=os.setsid, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

    # .............................
    def stop_catalog_server(self):
        """Stop the local catalog server
        """
        self.log.debug('Stopping catalog server')
        os.killpg(os.getpgid(self.cs_proc.pid), signal.SIGTERM)

    # .............................
    def start_worker_factory(self):
        """Start worker factory
        """
        self.log.debug('Clean up old worker directories')
        rm_proc = subprocess.Popen(RM_OLD_WORKER_DIRS_CMD, shell=True)
        # Sleep until we remove all of the old work directories
        while rm_proc.poll() is None:
            sleep(1)

        self.log.debug('Starting worker factory')
        cmd = '{} {}'.format(WORKER_FACTORY_BIN, WORKER_FACTORY_OPTIONS)
        self.wf_proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)

    # .............................
    def stop_worker_factory(self):
        """Kill worker factory
        """
        self.log.debug('Kill worker factory')
        os.killpg(os.getpgid(self.wf_proc.pid), signal.SIGTERM)

    # .............................
    @staticmethod
    def _get_makeflow_command(name, mf_doc_fn, priority=1):
        """Assemble Makeflow command

        Args:
            name (str): The name of the Makeflow job
            mf_doc_fn (str): The Makeflow DAG file to run
            priority (int): The priority that this makeflow should run.  The
                greater the number the higher the priority for this makeflow.
        """
        mf_cmd = '{} {} -N {} -P {} {}'.format(
            MAKEFLOW_BIN, MAKEFLOW_OPTIONS, name, priority, mf_doc_fn)
        return mf_cmd

    # .............................
    def _cleanup_failure_dirs(self):
        """Cleans up 'makeflow.failed.*' directories
        """
        bad_dirs = glob.glob('{}/makeflow.failed.*'.format(WORKER_PATH))
        for dir_name in bad_dirs:
            self.log.debug('Removing error directory: {}'.format(dir_name))
            shutil.rmtree(dir_name)

    # .............................
    def _cleanup_makeflow(self, mf_obj, mf_doc_fn, exit_status, lm_status=None,
                          log_files=None):
        """Clean up a makeflow that has finished

        Args:
            mf_obj (:obj:`MFChain`): The workflow chain object.
            mf_doc_fn (str): The file location of the DAG (in the workspace).
            exit_status (int): Unix exit status (negative: killed by signal,
                zero: successful, positive: error).
            lm_status (:obj:`int`, optional): If provided, update the database
                with this status.
            log_files (:obj:`list` of :obj:`str`, optional): A list of log
                files generated by a makeflow.
        """
        self.log.debug('Cleaning up: {}'.format(mf_doc_fn))
        mf_rel_dir = mf_obj.get_relative_directory()

        # Determine what we should clean up
        delete_log = True
        delete_makeflow = True
        delete_output = True

        # Log files
        if self.keep_logs == KeepAction.ALL or (
                self.keep_logs == KeepAction.ERROR and exit_status != 0):

            delete_log = False

        # Output files
        if self.keep_outputs == KeepAction.ALL or (
                self.keep_outputs == KeepAction.ERROR and exit_status != 0):

            delete_output = False

        # Makeflow (original)
        if self.keep_makeflows == KeepAction.ALL or (
                self.keep_makeflows == KeepAction.ERROR and exit_status != 0):

            delete_makeflow = False

        # Delete output files
        if mf_rel_dir is not None and delete_output:
            mf_ws_dir = os.path.join(WORKER_PATH, mf_rel_dir)
            self.log.debug('Attempting to delete: {}'.format(mf_ws_dir))
            try:
                shutil.rmtree(mf_ws_dir)
            except Exception as e:
                self.log.debug(
                    'Could not delete: {} - {}'.format(mf_ws_dir, str(e)))

        # Delete log files
        if delete_log and log_files is not None:
            for log_fn in log_files:
                os.remove(log_fn)

        # Delete makeflow
        if delete_makeflow:
            orig_mf = mf_obj.get_dlocation()
            self.scribe.delete_object(mf_obj)
            # Remove original makeflow file
            try:
                os.remove(orig_mf)
            except Exception as e:
                self.log.debug(
                    'Could not remove makeflow file: {}, {}'.format(
                        orig_mf, str(e)))
        else:
            # Get update status
            if lm_status is None:
                if exit_status < 0:
                    # Killed by signal
                    lm_status = JobStatus.INITIALIZE
                elif exit_status > 0:
                    # Error
                    lm_status = JobStatus.GENERAL_ERROR
                else:
                    # Success
                    lm_status = JobStatus.COMPLETE
            mf_obj.update_status(lm_status, mod_time=gmt().mjd)
            self.scribe.update_object(mf_obj)

        # Remove makeflow files from workspace
        del_files = glob.glob('{}*'.format(mf_doc_fn))
        for file_name in del_files:
            os.remove(file_name)

        # Clean up any failed makeflows
        self._cleanup_failure_dirs()


# .............................................................................
def main():
    """Main method for script.
    """
    if not is_lm_user():
        print(("Run this script as `{}`".format(LM_USER)))
        sys.exit(2)

    parser = argparse.ArgumentParser(
        prog='Lifemapper Makeflow Daemon (Matt Daemon)',
        description='Controls a pool of Makeflow processes')

    # Keep logs, makeflows, data, log dir
    parser.add_argument(
        '-l', '--log_dir', type=str,
        help='Directory to store log files if not default')
    parser.add_argument(
        '-dl', '--debug_log', type=int, choices=KeepAction.options(),
        default=KeepAction.NONE,
        help=('Should logs be kept. 0: Delete all logs. (default).'
              '1: Keep logs for failures. 2: Keep all logs.'))
    parser.add_argument(
        '-dm', '--debug_makeflows', type=int, choices=KeepAction.options(),
        default=KeepAction.ERROR,
        help=('Should makeflows be kept. 0: Delete all makeflows. '
              '1: Keep failed makeflows (default). 2: Keep all makeflows.'))
    parser.add_argument(
        '-do', '--debug_outputs', type=int, choices=KeepAction.options(),
        default=KeepAction.NONE,
        help=('Should outputs be kept. 0: Delete all (default).'
              '1: Keep outputs from failures. 2: Keep all outputs.'))

    parser.add_argument(
        'cmd', choices=[
            DaemonCommands.START, DaemonCommands.STOP, DaemonCommands.RESTART],
        help="The action that should be performed by the makeflow daemon")

    args = parser.parse_args()

    mf_daemon = MattDaemon(
        MATT_DAEMON_PID_FILE,
        log=LmServerLogger("mattDaemon", add_console=True, add_file=True))

    # Set debugging
    mf_daemon.set_debug(
        args.debug_log, args.debug_makeflows, args.debug_outputs, args.log_dir)

    if args.cmd.lower() == DaemonCommands.START:
        print('Start')
        mf_daemon.start()
    elif args.cmd.lower() == DaemonCommands.STOP:
        print('Stop')
        mf_daemon.stop()
    elif args.cmd.lower() == DaemonCommands.RESTART:
        mf_daemon.restart()
    else:
        print(('Unknown command: {}'.format(args.cmd.lower())))
        sys.exit(2)


# .............................................................................
if __name__ == "__main__":
    main()

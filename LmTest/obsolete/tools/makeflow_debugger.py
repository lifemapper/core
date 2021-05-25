# !/bin/bash
"""Debugging tool used to test a Lifemapper makeflow
"""
import argparse
import glob
import os
import shutil
import subprocess

from LmServer.common.lmconstants import MAKEFLOW_OPTIONS, MAKEFLOW_BIN
from LmServer.common.log import ConsoleLogger
from LmServer.db.borg_scribe import BorgScribe


# .............................................................................
def cleanup_makeflow(mf_dag, workspace, cleanup_val, success, log):
    """
    @summary: Clean up a Makeflow
    """
    if cleanup_val in [-1, 1, 2] or (cleanup_val == 0 and success):
        clean_cmd = get_makeflow_clean_command(mf_dag, workspace)
        try:
            subprocess.check_call(clean_cmd, shell=True)
        except subprocess.CalledProcessError as err:
            log.debug('Could not clean up Makeflow:')
            log.debug(str(err))

    if cleanup_val == 2:
        try:
            shutil.rmtree(workspace)
        except Exception as err:
            log.debug('Could not delete {} - {}'.format(workspace, str(err)))


# .............................................................................
def delete_makeflow(scribe, mf_obj, mf_dag, log):
    """
    @summary: Delete the Makeflow DAG / database object
    """
    if mf_obj is not None:
        log.debug('Deleting Makeflow object')
        scribe.deleteObject(mf_obj)

    delFiles = glob.glob('{}*'.format(mf_dag))
    for fn in delFiles:
        log.debug('Trying to delete {}'.format(fn))
        os.remove(fn)


# .............................................................................
def get_makeflow_clean_command(dag_fn, workspace):
    """Get a cleanup command for a Makeflow
    """
    return '{} -c -X {} {}'.format(MAKEFLOW_BIN, workspace, dag_fn)


# .............................................................................
def get_makeflow_document(scribe, mf_id, log):
    """Get a makeflow document
    """
    if os.path.exists(mf_id):
        log.debug(
            ('{} exists on file system, using this DAG instead of DB'
             ).format(mf_id))
        mf_obj = None
        mf_dag = mf_id
        workspace = None
    else:
        mf_obj = scribe.get_mf_chain(int(mf_id))
        if mf_obj is not None:
            mf_dag = mf_obj.get_dlocation()
            workspace = mf_obj.get_relative_directory()
        else:
            raise Exception(
                'Could not find Makeflow process id: {}'.format(mf_id))

    return mf_obj, mf_dag, workspace


# .............................................................................
def get_makeflow_command(dag_fn, options):
    """Get the command to run the Makeflow DAG

    Args:
        dag_fn: The file location of the Makeflow DAG to run
        options: A string of Makeflow options
    """
    return '{} {} {}'.format(MAKEFLOW_BIN, options, dag_fn)


# .............................................................................
def process_mf_options(options, workspace, log, name='lifemapper-job',
                       log_file=None):
    """Processes inputs to create a string of options to pass to Makeflow

    Args:
        options: Raw options from either defaults or user input
        workspace: The provided workspace to use
        log: A LmLogger instance to use for script logging
        log_file: An optional file location for logging
    """
    option_parts = options.split('-')
    processed_options = ''
    for opt in option_parts:
        # Drop workspace and logging options
        if any([opt.startswith('X '),
                opt.startswith('d '),
                opt.startswith('o ')]):
            log.debug('Dropping option "-{}"'.format(opt))
        else:
            processed_options += '-{}'.format(opt)

    # Add workspace
    processed_options += ' -X {}'.format(workspace)

    # Add name
    if name is not None:
        processed_options += ' -N {}'.format(name)

    # Add log file if specified
    if log_file is not None:
        processed_options += ' -d a -o {}'.format(log_file)

    log.debug('Processed Makeflow options: "{}"'.format(processed_options))

    return processed_options


# .............................................................................
def run_makeflow(dag_fn, pre_options, workspace, log, name=None,
                 log_file=None):
    """Run a makeflow and return a boolean indicating if it was successful

    Args:
        dag_fn: The file path to the Makeflow DAG file
        pre_options: Makeflow options from the script arguments (unclean)
        workspace: The workspace were the Makeflow shoudl run
        log: LmLogger instance to be used for logging
        name: The name of the Makeflow (for use with a catalog server)
        log_file: The file location for Makeflow to log
    """
    options = process_mf_options(
        pre_options, workspace, log, name=name, log_file=log_file)
    mf_cmd = get_makeflow_command(dag_fn, options)

    try:
        _ = subprocess.check_output(mf_cmd, shell=True)
        success = True
    except subprocess.CalledProcessError as err:
        log.debug("Process failed")
        log.debug(err.output)
        success = False

    return success


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(
        description=(
            'This is a debugging tool to run individual Lifemapper Makeflows'))

    # Positional
    # -------------------
    # Database ID or filename for Makeflow DAG
    parser.add_argument(
        'id_or_filename',
        help=('Either a database ID of a mfprocess or a filename to an '
              'existing Makeflow DAG'))

    # Optional arguments
    # -------------------
    # Work space
    parser.add_argument(
        '-w', '--workspace', type=str,
        help=('A directory to use as a workspace.  Will try to create if it '
              'does not exist and, if not provided, will try to get it from '
              'the MF object or will assign one and create in the current '
              'working directory'))
    # Log file
    parser.add_argument(
        '-l', '--log_file', type=str,
        help='File location for makeflow to log to')

    # Cleanup
    #  -1 - Run makeflow clean command without running makeflow
    #    0 - Run makeflow clean command if successful (leaves undeclared files)
    #    1 - Run makeflow clean command no matter what (leaves undeclared
    #        files)
    #    2 - Delete workspace to ensure all created files are removed
    parser.add_argument(
        '-c', '--cleanup', type=int, choices=[-1, 0, 1, 2],
        help=('Setting this flag to 0 runs makeflow -c if makeflow is '
              'successful, 1 - runs makeflow -c no matter what, 2 - Deletes '
              'workspace, -1 - Only runs cleanup command and deletes '
              'workspace'))

    # Delete
    parser.add_argument(
        '-d', '--delete', action='store_true',
        help=('If this flag is set, delete the makeflow dag file from the file'
              ' system and remove from database (if dbid)'))

    # Makeflow flags
    parser.add_argument(
        '-flags', '--mf_flags', type=str,
        help='Flags to pass to makeflow (overrides def_flags)')
    parser.add_argument(
        '-def', '--def_flags', action='store_true',
        help=('Use the Lifemapper default Makeflow flags, "{}", except '
              'workspace and log').format(MAKEFLOW_OPTIONS))

    # Makeflow name
    parser.add_argument(
        '-n', '--name', type=str,
        help='The name of this Makeflow (if using a catalog server)')

    args = parser.parse_args()

    log = ConsoleLogger()
    scribe = BorgScribe(log)
    scribe.open_connections()

    try:
        # Get Makeflow DAG / object / workspace
        mf_obj, mf_dag, workspace = get_makeflow_document(
            scribe, args.id_or_filename, log)

        # Set up workspace
        # Check for workspace parameter
        if args.workspace is not None:
            workspace = args.workspace

        if workspace is None:
            i = 0
            workspace = os.path.join('.', 'workspace_{}'.format(i))
            # Look for a new workspace
            while os.path.exists(workspace):
                i += 1
                workspace = os.path.join('.', 'workspace_{}'.format(i))

        # Create workspace directory if needed
        if not os.path.exists(workspace):
            log.debug('Creating workspace directory: {}'.format(workspace))
            os.mkdir(workspace)

        # Should we run Makeflow or just cleanup?
        if args.cleanup is not None and args.cleanup == -1:
            run_mf = False
        else:
            run_mf = True

        if run_mf:
            if args.mf_flags is not None:
                options = args.mf_flags
            elif args.def_flags:
                options = MAKEFLOW_OPTIONS
            else:
                options = ''
            success = run_makeflow(
                mf_dag, options, workspace, log, name=args.name,
                log_file=args.log_file)

        if args.cleanup is not None:
            cleanup_makeflow(mf_dag, workspace, args.cleanup, success, log)

        if args.delete and success:
            delete_makeflow(scribe, mf_obj, mf_dag, log)

    except Exception as e:
        print(str(e))
    finally:
        scribe.close_connections()


# .............................................................................
if __name__ == '__main__':
    main()

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
from LmServer.db.borgscribe import BorgScribe


# .............................................................................
def cleanupMakeflow(mfDag, workspace, cleanupVal, success, log):
    """
    @summary: Clean up a Makeflow
    """
    if cleanupVal in [-1, 1, 2] or (cleanupVal == 0 and success):
        cleanCmd = getMakeflowCleanCommand(mfDag, workspace)
        try:
            subprocess.check_call(cleanCmd, shell=True)
        except subprocess.CalledProcessError as e:
            log.debug('Could not clean up Makeflow:')
            log.debug(str(e))

    if cleanupVal == 2:
        try:
            shutil.rmtree(workspace)
        except Exception as e:
            log.debug('Could not delete {} - {}'.format(workspace, str(e)))


# .............................................................................
def deleteMakeflow(scribe, mfObj, mfDag, log):
    """
    @summary: Delete the Makeflow DAG / database object
    """
    if mfObj is not None:
        log.debug('Deleting Makeflow object')
        scribe.deleteObject(mfObj)

    delFiles = glob.glob('{}*'.format(mfDag))
    for fn in delFiles:
        log.debug('Trying to delete {}'.format(fn))
        os.remove(fn)


# .............................................................................
def getMakeflowCleanCommand(dagFn, workspace):
    """
    @summary: Get a cleanup command for a Makeflow
    """
    cleanCmd = '{} -c -X {} {}'.format(MAKEFLOW_BIN, workspace, dagFn)
    return cleanCmd


# .............................................................................
def getMakeflowDocument(scribe, mfId, log):
    """
    @summary: If the parameter provided is an integer, treat it as a mfprocessid
                     and get the Makeflow DAG filename from the database.  If it is
                     a file path, use it directly.
    """
    if os.path.exists(mfId):
        log.debug(
            '{} exists on file system, using this DAG instead of DB'.format(mfId))
        mfObj = None
        mfDag = mfId
        ws = None
    else:
        mfObj = scribe.getMFChain(int(mfId))
        if mfObj is not None:
            mfDag = mfObj.get_dlocation()
            ws = mfObj.getRelativeDirectory()
        else:
            raise Exception('Could not find Makeflow process id: {}'.format(mfId))

    return mfObj, mfDag, ws


# .............................................................................
def getMakeflowCommand(dagFn, options):
    """
    @summary: Get the command to run the Makeflow DAG
    @param dagFn: The file location of the Makeflow DAG to run
    @param options: A string of Makeflow options
    """
    mfCmd = '{mfBin} {mfOptions} {mfDoc}'.format(
                                    mfBin=MAKEFLOW_BIN, mfOptions=options,
                                    mfDoc=dagFn)
    return mfCmd


# .............................................................................
def processMfOptions(options, workspace, log, name='lifemapper-job',
                            logFile=None):
    """
    @summary: Processes inputs to create a string of options to pass to Makeflow
    @param options: Raw options from either defaults or user input
    @param workspace: The provided workspace to use
    @param log: A LmLogger instance to use for script logging
    @param logFile: An optional file location for logging
    """
    optionParts = options.split('-')
    processedOptions = ''
    for op in optionParts:
        # Drop workspace and logging options
        if op.startswith('X ') or op.startswith('d ') or op.startswith('o '):
            log.debug('Dropping option "-{}"'.format(op))
        else:
            processedOptions += '-{}'.format(op)

    # Add workspace
    processedOptions += ' -X {}'.format(workspace)

    # Add name
    if name is not None:
        processedOptions += ' -N {}'.format(name)

    # Add log file if specified
    if logFile is not None:
        processedOptions += ' -d a -o {}'.format(logFile)

    log.debug('Processed Makeflow options: "{}"'.format(processedOptions))

    return processedOptions


# .............................................................................
def runMakeflow(dagFn, pre_options, ws, log, name=None, logFile=None):
    """
    @summary: Run a makeflow and return a boolean indicating if it was successful
    @param dagFn: The file path to the Makeflow DAG file
    @param pre_options: Makeflow options from the script arguments (unclean)
    @param ws: The workspace were the Makeflow shoudl run
    @param log: LmLogger instance to be used for logging
    @param name: The name of the Makeflow (for use with a catalog server)
    @param logFile: The file location for Makeflow to log
    """
    options = processMfOptions(pre_options, ws, log, name=name, logFile=logFile)
    mfCmd = getMakeflowCommand(dagFn, options)

    try:
        cmdOutput = subprocess.check_output(mfCmd, shell=True)
        success = True
    except subprocess.CalledProcessError as e:
        log.debug("Process failed")
        log.debug(e.output)
        success = False

    return success


# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='This is a debugging tool to run individual Lifemapper Makeflows')

    # Positional
    # -------------------
    # Database ID or filename for Makeflow DAG
    parser.add_argument('idOrFilename', help='Either a database ID of a mfprocess or a filename to an existing Makeflow DAG')

    # Optional arguments
    # -------------------
    # Work space
    parser.add_argument('-w', '--workspace', type=str,
            help='A directory to use as a workspace.  Will try to create if it does not exist and, if not provided, will try to get it from the MF object or will assign one and create in the current working directory')
    # Log file
    parser.add_argument('-l', '--log_file', type=str,
                              help='File location for makeflow to log to')

    # Cleanup
    #  -1 - Run makeflow clean command without running makeflow
    #    0 - Run makeflow clean command if successful (leaves undeclared files)
    #    1 - Run makeflow clean command no matter what (leaves undeclared files)
    #    2 - Delete workspace to ensure all created files are removed
    parser.add_argument('-c', '--cleanup', type=int, choices=[-1, 0, 1, 2],
            help='Setting this flag to 0 runs makeflow -c if makeflow is successful, 1 - runs makeflow -c no matter what, 2 - Deletes workspace, -1 - Only runs cleanup command and deletes workspace')

    # Delete
    parser.add_argument('-d', '--delete', action='store_true',
            help='If this flag is set, delete the makeflow dag file from the file system and remove from database (if dbid)')

    # Makeflow flags
    parser.add_argument('-flags', '--mf_flags', type=str,
            help='Flags to pass to makeflow (overrides def_flags)')
    parser.add_argument('-def', '--def_flags', action='store_true',
        help='Use the Lifemapper default Makeflow flags, "{}", except workspace and log'.format(MAKEFLOW_OPTIONS))

    # Makeflow name
    parser.add_argument('-n', '--name', type=str,
                        help='The name of this Makeflow (if using a catalog server)')

    args = parser.parse_args()

    log = ConsoleLogger()
    scribe = BorgScribe(log)
    scribe.openConnections()

    try:
        # Get Makeflow DAG / object / workspace
        mfObj, mfDag, ws = getMakeflowDocument(scribe, args.idOrFilename, log)

        # Set up workspace
        # Check for workspace parameter
        if args.workspace is not None:
            ws = args.workspace

        if ws is None:
            i = 0
            ws = os.path.join('.', 'workspace_{}'.format(i))
            # Look for a new workspace
            while os.path.exists(ws):
                i += 1
                ws = os.path.join('.', 'workspace_{}'.format(i))

        # Create workspace directory if needed
        if not os.path.exists(ws):
            log.debug('Creating workspace directory: {}'.format(ws))
            os.mkdir(ws)

        # Should we run Makeflow or just cleanup?
        if args.cleanup is not None and args.cleanup == -1:
            runMF = False
        else:
            runMF = True

        if runMF:
            if args.mf_flags is not None:
                options = args.mf_flags
            elif args.def_flags:
                options = MAKEFLOW_OPTIONS
            else:
                options = ''
            success = runMakeflow(mfDag, options, ws, log, name=args.name,
                                         logFile=args.log_file)

        if args.cleanup is not None:
            cleanupMakeflow(mfDag, ws, args.cleanup, success, log)

        if args.delete and success:
            deleteMakeflow(scribe, mfObj, mfDag, log)

    except Exception as e:
        print(str(e))
    finally:
        scribe.closeConnections()

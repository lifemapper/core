"""This script will reinitialize objects that have been abandoned

Note:
    This script will create new Makeflows for all matching objects even if they 
        are in another Makeflow.  It does not determine what has been
        abandoned.
"""
import argparse
from mx.DateTime import gmt
import os

from LmCommon.common.lmconstants import JobStatus

from LmServer.common.lmconstants import Priority
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain

# .............................................................................
def reinitialize_projections(gridset_id, begin_status, end_status, group_size,
                             log, processing_parameters=None,
                             user_id=PUBLIC_USER):
    """Reinitializes projections for computation

    Args:
        gridset_id: The database ID of the gridset to look for projections to
            re-initialize.
        begin_status: The lower bound of projection statuses to re-initialize.
        end_status: The upper bound of projection statuses to re-initialize.
        group_size: The number of projections to include in each
            re-initialization workflow.
        log: A logger instance to use for logging output.
        processing_parameters: A dictionary of processing parameters
        user_id: The user id to use when searching for projections.

    Todo:
        Remove user_id?  It may be redundant since we now require gridset id.
    """
    scribe = BorgScribe(log)
    scribe.openConnections()

    # TODO: Get procParams
    if processing_parameters:
        mask_layer = scribe.getLayer(
            userId=user_id, lyrName=processing_parameters['mask']['mask_name'])
        processing_parameters['mask']['mask_layer'] = mask_layer

    count = scribe.countSDMProjects(
        user_id, afterStatus=begin_status, beforeStatus=end_status,
        gridsetId=gridset_id)

    i = 0
    while i < count:
        prjs = scribe.listSDMProjects(
            i, group_size, userId=user_id, afterStatus=begin_status,
            beforeStatus=end_status, gridsetId=gridset_id, atom=False)

        description = 'Makeflow for abandoned projections'
        meta = {
            MFChain.META_CREATED_BY : os.path.basename(__file__),
            MFChain.META_DESCRIPTION : description
        }

        new_mfc = MFChain(user_id, metadata=meta, priority=Priority.REQUESTED,
                         status=JobStatus.GENERAL, statusModTime=gmt().mjd)
        mf_chain = scribe.insertMFChain(new_mfc, gridset_id)
        scribe.updateObject(mf_chain)

        work_dir = 'mf_{}'.format(mf_chain.getId())

        rules = []
        for prj in prjs:
            rules.extend(prj.computeMe(
                workDir=work_dir, procParams=processing_parameters, 
                addOccRules=True))

        mf_chain.addCommands(rules)
        mf_chain.write()
        mf_chain.updateStatus(JobStatus.INITIALIZE)
        scribe.updateObject(mf_chain)
        log.debug('Wrote makeflow {} to {}'.format(mf_chain.getId(), 
                                                   mf_chain.getDLocation()))

        i += group_size

    scribe.closeConnections()

# .............................................................................
if __name__ == '__main__':
    log = ScriptLogger('abandons')

    # TODO: Get this from an argument
    processing_parameters = {
        'mask' : {
            'mask_name' : 'ecoreg_30sec_na',
            'code' : 'hull_region_intersect',
            'buffer' : 0.5
        }
    }

    parser = argparse.ArgumentParser(
        description='This script finds and reinitializes projections')
    parser.add_argument(
        'begin_status', type=int,
        help='Reset projections that have at least this status')
    parser.add_argument(
        'end_status', type=int,
        help='Reset projections that have at most this status')
    parser.add_argument(
        'group_size', type=int,
        help='Group this many projections together for computation')
    parser.add_argument(
        'gridset_id', type=int, help='Reset projections from this gridset')
    parser.add_argument(
        '-u', '--user_id', type=str, default=PUBLIC_USER,
        help='Reset projections for this user, defaults to public')

    args = parser.parse_args()

    reinitialize_projections(args.gridset_id, args.begin_status, 
                             args.end_status, args.group_size, log, 
                             processing_parameters=processing_parameters, 
                             user_id=args.user_id)

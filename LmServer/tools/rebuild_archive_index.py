#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script creates a workflow to rebuild the solr index for a gridset
"""
import argparse
import os

from mx.DateTime.DateTime import gmt

from LmBackend.command.common import SystemCommand
from LmBackend.command.server import LmTouchCommand
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmServer.common.lmconstants import Priority
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain

# The number of matrix columns to pull at a time
GROUP_SIZE = 1000

# .............................................................................
def rebuild_index_for_gridset(gridset_id):
    """Creates a solr index rebuild workflow for a gridset

    Args:
        gridset_id: The identifier for the gridset to use.
    """
    log = ScriptLogger('rebuild_solr_gs_{}'.format(gridset_id))
    scribe = BorgScribe(log)
    scribe.openConnections()
    
    # Get gridset and fill PAMs
    gs = scribe.getGridset(gridsetId=gridset_id, fillMatrices=True)
    shapegrid = gs.getShapegrid()
    user_id = gs.getUserId()
    
    # TODO: Should this only be rolling PAMs?
    for pam in gs.getPAMs():
        # Create makeflow
        wfMeta = {
            MFChain.META_CREATED_BY : os.path.basename(__file__),
            MFChain.META_DESCRIPTION : \
                'Reindex PAMs for gridset {}, PAM {}'.format(
                    gridset_id, pam.getId())
        }
        new_wf = MFChain(user_id, priority=Priority.REQUESTED, metadata=wfMeta,
                             status=JobStatus.GENERAL, statusModTime=gmt().mjd)
        my_wf = scribe.insertMFChain(new_wf, gridset_id)
        
        # TODO: Determine what work directory should be
        work_dir = my_wf.getRelativeDirectory()
    
        matrix_id = pam.getId()
        num_columns = scribe.countMatrixColumns(userId=user_id, 
                                                matrixId=matrix_id)
        i = 0
        while i < num_columns:
            mcs = scribe.listMatrixColumns(
                i, GROUP_SIZE, userId=user_id, matrixId=matrix_id, atom=False)
            i += GROUP_SIZE
            for mc in mcs:
                # Get layer
                lyr = scribe.getLayer(lyrId=mc.getLayerId())
                if os.path.exists(lyr.getDLocation()):
                    if lyr.minVal == lyr.maxVal and lyr.minVal == 0.0:
                        print('No prediction for layer {}, skipping'.format(
                            lyr.getId()))
                    else:
                        mc.layer = lyr
                        mc.shapegrid = shapegrid
                        mc.processType = ProcessType.INTERSECT_RASTER
                        
                        mc.updateStatus(JobStatus.INITIALIZE)
                        scribe.updateObject(mc)
                        my_wf.addCommands(mc.computeMe(workDir=work_dir))
                        
                        prj_target_dir = os.path.join(
                             work_dir, os.path.splitext(
                                 lyr.getRelativeDLocation())[0])
                        prj_touch_fn = os.path.join(prj_target_dir, 
                                                    'touch.out')
                        touch_cmd = LmTouchCommand(prj_touch_fn)
                        my_wf.addCommands(
                            touch_cmd.getMakeflowRule(local=True))
                        
                        prj_name = os.path.basename(
                             os.path.splitext(lyr.getDLocation())[0])

                        prj_status_filename = os.path.join(
                            prj_target_dir, '{}.status'.format(prj_name))
                        touchStatusCommand = SystemCommand(
                            'echo', '{} > {}'.format(
                                JobStatus.COMPLETE, prj_status_filename), 
                            inputs=[prj_touch_fn], 
                            outputs=[prj_status_filename])
                        my_wf.addCommands(
                            touchStatusCommand.getMakeflowRule(local=True))
    
        my_wf.write()
        my_wf.updateStatus(JobStatus.INITIALIZE)
        scribe.updateObject(my_wf)
    
    scribe.closeConnections()


# .............................................................................
if __name__ == '__main__':
    desc = ''.join(['This script creates a workflow to reintersect all of the',
                         ' columns for each PAM in a gridset'])
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('gridset_id', type=int, 
                              help='The ID of the gridset to reintersect')
    # TODO: Consider if we need parameter indicating that we should clear index

    args = parser.parse_args()

    rebuild_index_for_gridset(args.gridset_id)

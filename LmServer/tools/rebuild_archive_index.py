"""This script creates a workflow to rebuild the solr index for a gridset
"""
import argparse
import os

from LmBackend.command.common import SystemCommand
from LmBackend.command.server import TouchFileCommand
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.time import gmt
from LmServer.common.lmconstants import Priority
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.process_chain import MFChain

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
    scribe.open_connections()

    # Get gridset and fill PAMs
    gridset = scribe.get_gridset(gridset_id=gridset_id, fill_matrices=True)
    shapegrid = gridset.get_shapegrid()
    user_id = gridset.get_user_id()

    # TODO: Should this only be rolling PAMs?
    for pam in gridset.get_all_pams():
        # Create makeflow
        wf_meta = {
            MFChain.META_CREATED_BY: os.path.basename(__file__),
            MFChain.META_DESCRIPTION:
                'Reindex PAMs for gridset {}, PAM {}'.format(
                    gridset_id, pam.get_id())
            }
        new_wf = MFChain(
            user_id, priority=Priority.REQUESTED, metadata=wf_meta,
            status=JobStatus.GENERAL, status_mod_time=gmt().mjd)
        my_wf = scribe.insert_mf_chain(new_wf, gridset_id)

        # TODO: Determine what work directory should be
        work_dir = my_wf.get_relative_directory()

        matrix_id = pam.get_id()
        num_columns = scribe.count_matrix_columns(
            user_id=user_id, matrix_id=matrix_id)
        i = 0
        while i < num_columns:
            matrix_columns = scribe.list_matrix_columns(
                i, GROUP_SIZE, user_id=user_id, matrix_id=matrix_id,
                atom=False)
            i += GROUP_SIZE
            for mtx_col in matrix_columns:
                # Get layer
                lyr = scribe.get_layer(lyr_id=mtx_col.get_layer_id())
                if os.path.exists(lyr.get_dlocation()):
                    if lyr.min_val == lyr.max_val and lyr.min_val == 0.0:
                        print(('No prediction for layer {}, skipping'.format(
                            lyr.get_id())))
                    else:
                        mtx_col.layer = lyr
                        mtx_col.shapegrid = shapegrid
                        mtx_col.process_type = ProcessType.INTERSECT_RASTER

                        mtx_col.update_status(JobStatus.INITIALIZE)
                        scribe.update_object(mtx_col)
                        my_wf.add_commands(
                            mtx_col.compute_me(workDir=work_dir))

                        prj_target_dir = os.path.join(
                            work_dir, os.path.splitext(
                                lyr.get_relative_dlocation())[0])
                        prj_touch_fn = os.path.join(
                            prj_target_dir, 'touch.out')
                        touch_cmd = TouchFileCommand(prj_touch_fn)
                        my_wf.add_commands(
                            touch_cmd.get_makeflow_rule(local=True))

                        prj_name = os.path.basename(
                            os.path.splitext(lyr.get_dlocation())[0])

                        prj_status_filename = os.path.join(
                            prj_target_dir, '{}.status'.format(prj_name))
                        touch_status_command = SystemCommand(
                            'echo', '{} > {}'.format(
                                JobStatus.COMPLETE, prj_status_filename),
                            inputs=[prj_touch_fn],
                            outputs=[prj_status_filename])
                        my_wf.add_commands(
                            touch_status_command.get_makeflow_rule(local=True))

        my_wf.write()
        my_wf.update_status(JobStatus.INITIALIZE)
        scribe.update_object(my_wf)

    scribe.close_connections()


# .............................................................................
def main():
    """Main method for script
    """
    desc = (
        'This script creates a workflow to reintersect all of the columns '
        'for each PAM in a gridset')
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument(
        'gridset_id', type=int, help='The ID of the gridset to reintersect')
    # TODO: Consider if we need parameter indicating that we should clear index

    args = parser.parse_args()

    rebuild_index_for_gridset(args.gridset_id)


# .............................................................................
if __name__ == '__main__':
    main()

"""Module containing functions for getting the progress response of an object
"""
import cherrypy
import json

from LmCommon.common.lmconstants import JobStatus, LMFormat

from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
def format_gridset(gridset_id, detail=False):
    """Returns a dictionary of progress information for a gridset

    Args:
        gridset_id (:obj:`int`): The gridset id to get progress for
    """
    scribe = BorgScribe(LmPublicLogger())
    scribe.openConnections()
    
    if detail:
        gridset = scribe.getGridset(gridsetId=gridset_id, fillMatrices=True)
        complete_mtxs = 0
        error_mtxs = 0
        running_mtxs = 0
        waiting_mtxs = 0

        # Gridset object has matrix objects so we can count status
        for mtx in gridset.getMatrices():
            if mtx.status == JobStatus.GENERAL:
                waiting_mtxs += 1
            elif mtx.status < JobStatus.COMPLETE:
                running_mtxs += 1
            elif mtx.status == JobStatus.COMPLETE:
                complete_mtxs += 1
            elif mtx.status >= JobStatus.GENERAL_ERROR:
                error_mtxs += 1
        
        # Use scribe to get counts for projections
        
        complete_prjs = scribe.countSDMProjects(
            userId=gridset.getUserId(), afterStatus=JobStatus.COMPLETE - 1,
            beforeStatus=JobStatus.COMPLETE + 1, gridsetId=gridset.getId()) 
        error_prjs = scribe.countSDMProjects(
            userId=gridset.getUserId(), afterStatus=JobStatus.GENERAL_ERROR - 1,
            gridsetId=gridset.getId())
        running_prjs = scribe.countSDMProjects(
            userId=gridset.getUserId(), afterStatus=JobStatus.GENERAL + 1,
            beforeStatus=JobStatus.COMPLETE - 1, gridsetId=gridset.getId())
        waiting_prjs = scribe.countSDMProjects(
            userId=gridset.getUserId(), beforeStatus=JobStatus.GENERAL + 1,
            gridsetId=gridset.getId())
        
        total_prjs = complete_prjs + error_prjs + running_prjs + waiting_prjs
        
        complete_wfs = scribe.countMFChains(
            gridsetId=gridset.getId(), afterStat=JobStatus.COMPLETE - 1,
            beforeStat=JobStatus.COMPLETE + 1)
        error_wfs = scribe.countMFChains(
            gridsetId=gridset.getId(), afterStat=JobStatus.GENERAL_ERROR - 1)
        running_wfs = scribe.countMFChains(
            gridsetId=gridset.getId(), afterStat=JobStatus.GENERAL + 1,
            beforeStat=JobStatus.COMPLETE - 1)
        waiting_wfs = scribe.countMFChains(
            gridsetId=gridset.getId(), beforeStat=JobStatus.GENERAL + 1)
        
        scribe.closeConnections()
        
        # Initialize in case there are zero prj or mtx
        mtx_percent = 1.0
        prj_percent = 1.0
        
        if len(gridset.getMatrices()) > 0:
            mtx_percent = 1.0 * (
                complete_mtxs + error_mtxs) / len(gridset.getMatrices())
        if total_prjs > 0:
            prj_percent = 1.0 * (complete_prjs + error_prjs) / total_prjs
        
        progress = 0.5 * (mtx_percent + prj_percent)
        
        progress_dict = {
            'matrices' : {
                'complete' : complete_mtxs,
                'error' : error_mtxs,
                'running' : running_mtxs,
                'waiting' : waiting_mtxs
            },
            'progress' : progress,
            'projections' : {
                'complete' : complete_prjs,
                'error' : error_prjs,
                'running' : running_prjs,
                'waiting' : waiting_prjs
            },
            'workflows' : {
                'complete' : complete_wfs,
                'error' : error_wfs,
                'running' : running_wfs,
                'waiting' : waiting_wfs
            }
        }
    else:
        gs_mfs = scribe.listMFChains(
            0, 100, gridsetId=gridset_id, atom=False)
        
        mfs_left = 0
        mfs_running = 0
        for mf in gs_mfs:
            if mf.status < JobStatus.GENERAL_ERROR:
                mfs_left += 1
            if mf.status == JobStatus.COMPLETE:
                mfs_running += 1.0
            elif mf.status > JobStatus.INITIALIZE:
                # Add .5 to say it is half done
                mfs_running += 0.5
        
        if mfs_left == 0:
            progress = 1.0
        else:
            progress = float(mfs_running) / mfs_left

        progress_dict = {
            'progress' : progress
        }
    return progress_dict
    
# .............................................................................
def progress_object_formatter(obj_type, obj_id, detail=False):
    """Return a progress interface for an object

    Args:
        obj_type (str): A Lifemapper object type name
        obj_id (int): The database ID for the object
    """
    formatted_obj = _format_object(obj_type, obj_id, detail=detail)
    return json.dumps(formatted_obj, indent=3)

# .............................................................................
def _format_object(obj_type, obj_id, detail=False):
    """Helper function to determine how to get progress of an object

    Args:
        obj_type (str): A Lifemapper object type name
        obj_id (int): The database ID for the object
    """
    # Progress is JSON format, PROGRESS format is work around for accept
    #    headers
    cherrypy.response.headers['Content-Type'] = LMFormat.JSON.getMimeType()
    if obj_type.lower() == 'gridset':
        return format_gridset(obj_id, detail=detail)
    else:
        raise TypeError(
            'Cannot get progress for object of type: {}'.format(obj_type))

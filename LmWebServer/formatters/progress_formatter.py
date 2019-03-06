"""Module containing functions for getting the progress response of an object
"""
import cherrypy
import json

from LmCommon.common.lmconstants import JobStatus, LMFormat

from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.gridset import Gridset

# .............................................................................
def format_gridset(gridset, detail=False):
    """Returns a dictionary of progress information for a gridset

    Args:
        gridset (:obj: `Gridset`): The gridset object to get the progress
    """
    scribe = BorgScribe(LmPublicLogger())
    scribe.openConnections()
    
    if detail:
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
        
        scribe.closeConnections()
        
        # Initialize in case there are zero prj or mtx
        mtx_percent = 1.0
        prj_percent = 1.0
        
        if len(gridset.getMatrices()) > 0:
            mtx_percent = 1.0 * complete_mtxs / len(gridset.getMatrices())
        if total_prjs > 0:
            prj_percent = 1.0 * complete_prjs / total_prjs
        
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
            }
        }
    else:
        gs_mfs = scribe.listMFChains(
            0, 100, gridsetId=gridset.getId(), atom=False)
        
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
def progress_object_formatter(obj):
    """Return a progress interface for an object

    Args:
        obj (:obj: `ServiceObject`): A Lifemapper object to attempt to get the
            progress of
    """
    formatted_obj = _format_object(obj)
    return json.dumps(formatted_obj, indent=3)

# .............................................................................
def _format_object(obj):
    """Helper function to determine how to get progress of an object

    Args:
        obj (:obj: `ServiceObject`): A Lifemapper service object to get the
            progress
    """
    # Progress is JSON format, PROGRESS format is work around for accept
    #    headers
    cherrypy.response.headers['Content-Type'] = LMFormat.JSON.getMimeType()
    if isinstance(obj, Gridset):
        return format_gridset(obj)
    else:
        raise TypeError(
            'Cannot get progress for object of type: {}'.format(type(obj)))

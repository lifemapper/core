"""Module containing functions for getting the progress response of an object
"""
import cherrypy
import json

from LmCommon.common.lmconstants import JobStatus, LMFormat

from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
def summarize_object_statuses(summary):
    """Summarizes a summary

    Args:
        summary (:obj:`list` of :obj:`tuple` of :obj:`int`, :obj:`int`): A list
            of (status, count) tuples for an object type
    """
    complete = 0
    waiting = 0
    running = 0
    error = 0
    total = 0
    for status, count in summary:
        if status <= JobStatus.INITIALIZE:
            waiting += count
        elif status <= JobStatus.COMPLETE:
            running += count
        elif status == JobStatus.COMPLETE:
            complete += count
        else:
            error += count
        total += count
    return (waiting, running, complete, error, total)

# .............................................................................
def format_gridset(gridset_id, detail=False):
    """Returns a dictionary of progress information for a gridset

    Args:
        gridset_id (:obj:`int`): The gridset id to get progress for
    """
    scribe = BorgScribe(LmPublicLogger())
    scribe.openConnections()
    
    prj_summary = scribe.summarizeSDMProjectsForGridset(gridset_id)
    mtx_summary = scribe.summarizeMatricesForGridset(gridset_id)
    mf_summary = scribe.summarizeMFChainsForGridset(gridset_id)
    mc_summary = scribe.summarizeMtxColumnsForGridset(gridset_id)
    occ_summary = scribe.summarizeOccurrenceSetsForGridset(gridset_id)
    
    (waiting_prjs, running_prjs, complete_prjs, error_prjs, total_prjs
     ) = summarize_object_statuses(prj_summary)
    (waiting_mtxs, running_mtxs, complete_mtxs, error_mtxs, total_mtxs
     ) = summarize_object_statuses(mtx_summary)
    (waiting_mfs, running_mfs, complete_mfs, error_mfs, total_mfs
     ) = summarize_object_statuses(mf_summary)
    (waiting_occs, running_occs, complete_occs, error_occs, total_occs
     ) = summarize_object_statuses(occ_summary)
    (waiting_mcs, running_mcs, complete_mcs, error_mcs, total_mcs
     ) = summarize_object_statuses(mc_summary)
    scribe.closeConnections()
    
    # Progress is determined by makeflows.  If all SDMs error, then -1
    if error_occs == total_occs and total_occs > 0:
        progress = -1.0
    elif error_prjs == total_prjs and total_prjs > 0:
        progress = -1.0
    else:
        # 0.5 * number running + 1.0 * number complete + number error / total
        progress = (
            0.5 * running_mfs + 1.0 * (complete_mfs + error_mfs)) / total_mfs
    
    if detail:
        progress_dict = {
            'matrices' : {
                'complete' : complete_mtxs,
                'error' : error_mtxs,
                'running' : running_mtxs,
                'waiting' : waiting_mtxs
            },
            'matrix_columns' : {
                'complete' : complete_mcs,
                'error' : error_mcs,
                'running' : running_mcs,
                'waiting' : waiting_mcs
            },
            'occurrences' : {
                'complete' : complete_occs,
                'error' : error_occs,
                'running' : running_occs,
                'waiting' : waiting_occs
            },
            'progress' : progress,
            'projections' : {
                'complete' : complete_prjs,
                'error' : error_prjs,
                'running' : running_prjs,
                'waiting' : waiting_prjs
            },
            'workflows' : {
                'complete' : complete_mfs,
                'error' : error_mfs,
                'running' : running_mfs,
                'waiting' : waiting_mfs
            }
        }
    else:
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

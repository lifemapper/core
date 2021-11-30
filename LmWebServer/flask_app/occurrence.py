"""This module provides REST services for Occurrence sets"""
import werkzeug.exceptions as WEXC

from LmCommon.common.lmconstants import (JobStatus)
from LmServer.base.atom import Atom
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.common.boom_post import BoomPoster
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class OccurrenceLayerService(LmService):
    """Class for the occurrence sets web service."""

    # ................................
    @lm_formatter
    def delete_occurrence_set(self, user_id, occset_id):
        """Delete an occurrence set
    
        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            occset_id (int): The id of the occurrence set to delete.
        """
        occ = self.scribe.get_occurrence_set(occ_id=int(occset_id))
    
        if occ is None:
            raise WEXC.NotFound('Occurrence set not found')
    
        # If allowed to, delete
        if check_user_permission(user_id, occ, HTTPMethod.DELETE):
            success = self.scribe.delete_object(occ)
            if success:
                return True
    
            # If unsuccessful, fail
            raise WEXC.InternalServerError('Failed to delete occurrence set')
    
        # If no permission to delete, raise HTTP 403
        raise WEXC.Forbidden('User does not have permission to delete this occurrence set')

    # ................................
    @lm_formatter
    def post_boom_data(self, user_id, user_email, boom_data, **params):
        """Post occurrence data to seed a new BOOM archive

        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            user_email (str): The user to be notified of results of this operation.
            boom_data: JSON package of parameters to initialize a new gridset and workflow.
        """    
        boom_post = BoomPoster(user_id, user_email, boom_data, self.scribe)
        gridset = boom_post.init_boom()
    
        # cherrypy.response.status = HTTPStatus.ACCEPTED
        return Atom(
            gridset.get_id(), gridset.name, gridset.metadata_url,
            gridset.mod_time, epsg=gridset.epsg_code)

    # ................................
    @lm_formatter
    def count_occurrence_sets(self, user_id, after_time=None,
                               before_time=None, display_name=None,
                               epsg_code=None, minimum_number_of_points=1,
                               status=None, gridset_id=None):
        """Return a count of occurrence sets matching the specified criteria
        """
        after_status = None
        before_status = None

        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE + 1
                after_status = JobStatus.COMPLETE - 1
            else:
                after_status = status - 1

        occ_count = self.scribe.count_occurrence_sets(
            user_id=user_id, min_occurrence_count=minimum_number_of_points,
            display_name=display_name, after_time=after_time,
            before_time=before_time, epsg=epsg_code,
            before_status=before_status, after_status=after_status,
            gridset_id=gridset_id)
        return {'count': occ_count}

    # ................................
    @lm_formatter
    def get_occurrence_set(self, occset_id, fill_points=False):
        """Attempt to get an occurrence set"""
        occ = self.scribe.get_occurrence_set(occ_id=int(occset_id))

        if occ is None:
            raise WEXC.NotFound('Occurrence set not found')

        # If allowed to, return
        if check_user_permission(self.get_user_id(), occ, HTTPMethod.GET):
            if fill_points:
                occ.read_shapefile()
            return occ

        raise WEXC.Forbidden('User {} does not have permission to GET occurrence set'.format(
                self.get_user_id()))

    # ................................
    @lm_formatter
    def list_occurrence_sets(self, user_id, after_time=None, before_time=None,
                              display_name=None, epsg_code=None,
                              minimum_number_of_points=1, limit=100, offset=0,
                              status=None, gridset_id=None):
        """Return a list of occurrence sets matching the specified criteria
        """
        after_status = None
        before_status = None
    
        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE + 1
                after_status = JobStatus.COMPLETE - 1
            else:
                after_status = status - 1
    
        occ_atoms = self.scribe.list_occurrence_sets(
            offset, limit, user_id=user_id,
            min_occurrence_count=minimum_number_of_points,
            display_name=display_name, after_time=after_time,
            before_time=before_time, epsg=epsg_code,
            before_status=before_status, after_status=after_status,
            gridset_id=gridset_id)
        return occ_atoms
    
    # ................................
    @lm_formatter
    def list_web_occurrence_sets(
            self, user_id, after_time=None, before_time=None,
            display_name=None, epsg_code=None, minimum_number_of_points=1,
            limit=100, offset=0, status=None, gridset_id=None):
        """Return a list of occurrence set web objects matching criteria"""
        after_status = None
        before_status = None
    
        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE + 1
                after_status = JobStatus.COMPLETE - 1
            else:
                after_status = status - 1
    
        occs = self.scribe.list_occurrence_sets(
            offset, limit, user_id=user_id,
            min_occurrence_count=minimum_number_of_points,
            display_name=display_name, after_time=after_time,
            before_time=before_time, epsg=epsg_code,
            before_status=before_status, after_status=after_status,
            gridset_id=gridset_id, atom=False)
        occ_objs = []
        for occ in occs:
            occ_objs.append(
                {
                    'id': occ.get_id(),
                    'metadata_url': occ.metadata_url,
                    'name': occ.display_name,
                    'modification_time': occ.status_mod_time,
                    'epsg': occ.epsg_code,
                    'status': occ.status,
                    'count': occ.query_count
                }
            )
        return occ_objs


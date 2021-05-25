"""This module provides REST services for Occurrence sets"""
import json

import cherrypy

from LmCommon.common.lmconstants import (
    DEFAULT_POST_USER, HTTPStatus, JobStatus)
from LmServer.base.atom import Atom
from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.common.boom_post import BoomPoster
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_occset_id')
class OccurrenceLayerService(LmService):
    """
    @summary: This class is for the occurrence sets service.  The dispatcher is
                     responsible for calling the correct method
    """

    # ................................
    def DELETE(self, path_occset_id):
        """Attempts to delete an occurrence set

        Args:
            path_occset_id (int): The id of the occurrence set to delete.
        """
        occ = self.scribe.get_occurrence_set(occ_id=int(path_occset_id))

        if occ is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Occurrence set not found')

        # If allowed to, delete
        if check_user_permission(self.get_user_id(), occ, HTTPMethod.DELETE):
            success = self.scribe.delete_object(occ)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            # If unsuccessful, fail
            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Failed to delete occurrence set')

        # If no permission to delete, raise HTTP 403
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User does not have permission to delete this occurrence set')

    # ................................
    @lm_formatter
    def GET(self, path_occset_id=None, after_time=None, before_time=None,
            display_name=None, epsg_code=None, minimum_number_of_points=1,
            limit=100, offset=0, url_user=None, status=None, gridset_id=None,
            fill_points=False, **params):
        """GET request.  Either an occurrence set or list of them.
        """
        if path_occset_id is None:
            return self._list_occurrence_sets(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, display_name=display_name,
                epsg_code=epsg_code,
                minimum_number_of_points=minimum_number_of_points, limit=limit,
                offset=offset, gridset_id=gridset_id, status=status)

        if path_occset_id.lower() == 'count':
            return self._count_occurrence_sets(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, display_name=display_name,
                epsg_code=epsg_code,
                minimum_number_of_points=minimum_number_of_points,
                gridset_id=gridset_id, status=status)

        if path_occset_id.lower() == 'web':
            return self._list_web_occurrence_sets(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, display_name=display_name,
                epsg_code=epsg_code,
                minimum_number_of_points=minimum_number_of_points, limit=limit,
                offset=offset, gridset_id=gridset_id, status=status)

        # Fallback to just get an individual occurrence set
        return self._get_occurrence_set(
            path_occset_id, fill_points=fill_points)

    # ................................
    # @cherrypy.tools.json_out
    @lm_formatter
    def POST(self, **params):
        """Posts a new BOOM archive
        """
        projection_data = json.loads(cherrypy.request.body.read())

        if self.get_user_id() == PUBLIC_USER:
            usr = self.scribe.find_user(DEFAULT_POST_USER)
        else:
            usr = self.scribe.find_user(self.get_user_id())

        boom_post = BoomPoster(
            usr.user_id, usr.email, projection_data, self.scribe)
        gridset = boom_post.init_boom()

        cherrypy.response.status = HTTPStatus.ACCEPTED
        return Atom(
            gridset.get_id(), gridset.name, gridset.metadata_url,
            gridset.mod_time, epsg=gridset.epsg_code)

    # ................................
    def _count_occurrence_sets(self, user_id, after_time=None,
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
    def _get_occurrence_set(self, path_occset_id, fill_points=False):
        """Attempt to get an occurrence set
        """
        occ = self.scribe.get_occurrence_set(occ_id=int(path_occset_id))

        if occ is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Occurrence set not found')

        # If allowed to, return
        if check_user_permission(self.get_user_id(), occ, HTTPMethod.GET):
            if fill_points:
                occ.read_shapefile()
            return occ

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to GET occurrence set'.format(
                self.get_user_id()))

    # ................................
    def _list_occurrence_sets(self, user_id, after_time=None, before_time=None,
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
    def _list_web_occurrence_sets(
            self, user_id, after_time=None, before_time=None,
            display_name=None, epsg_code=None, minimum_number_of_points=1,
            limit=100, offset=0, status=None, gridset_id=None):
        """Return a list of occurrence set web objects matching criteria
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

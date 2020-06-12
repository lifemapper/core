"""This module provides REST services for Projections"""
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
@cherrypy.popargs('path_projection_id')
class SdmProjectService(LmService):
    """Class responsible for SDM Projection services
    """

    # ................................
    def DELETE(self, path_projection_id):
        """Attempts to delete a projection

        Args:
            path_projection_id: The id of the projection to delete
        """
        prj = self.scribe.get_sdm_project(int(path_projection_id))

        if prj is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Projection {} not found'.format(
                    path_projection_id))

        if check_user_permission(self.get_user_id(), prj, HTTPMethod.DELETE):
            success = self.scribe.delete_object(prj)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            # If we have permission but cannot delete, error
            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Failed to delete projection {}'.format(path_projection_id))

        # HTTP 403 if no permission to delete
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to delete projection {}'.format(
                self.get_user_id(), path_projection_id))

    # ................................
    @lm_formatter
    def GET(self, path_projection_id=None, after_status=None, after_time=None,
            algorithm_code=None, before_status=None, before_time=None,
            display_name=None, epsg_code=None, limit=100,
            model_scenario_code=None, occurrence_set_id=None, offset=0,
            projection_scenario_code=None, url_user=None, scenario_id=None,
            status=None, gridset_id=None, **params):
        """Perform a GET request. List, count, or get individual projection.
        """
        if path_projection_id is None:
            return self._list_projections(
                self.get_user_id(url_user=url_user), after_status=after_status,
                after_time=after_time, alg_code=algorithm_code,
                before_status=before_status, before_time=before_time,
                display_name=display_name, epsg_code=epsg_code, limit=limit,
                mdl_scenario_code=model_scenario_code,
                occurrence_set_id=occurrence_set_id, offset=offset,
                prj_scenario_code=projection_scenario_code, status=status,
                gridset_id=gridset_id)

        if path_projection_id.lower() == 'count':
            return self._count_projections(
                self.get_user_id(url_user=url_user), after_status=after_status,
                after_time=after_time, alg_code=algorithm_code,
                before_status=before_status, before_time=before_time,
                display_name=display_name, epsg_code=epsg_code,
                mdl_scenario_code=model_scenario_code,
                occurrence_set_id=occurrence_set_id,
                prj_scenario_code=projection_scenario_code, status=status,
                gridset_id=gridset_id)

        # Get individual as fall back
        return self._get_projection(path_projection_id)

    # ................................
    @lm_formatter
    def POST(self, **params):
        """Posts a new projection
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
    def _count_projections(self, user_id, after_status=None, after_time=None,
                           alg_code=None, before_status=None, before_time=None,
                           display_name=None, epsg_code=None,
                           mdl_scenario_code=None, occurrence_set_id=None,
                           prj_scenario_code=None, status=None,
                           gridset_id=None):
        """Return a count of projections matching the specified criteria
        """
        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE + 1
                after_status = JobStatus.COMPLETE - 1
            else:
                after_status = status - 1

        prj_count = self.scribe.count_sdm_projects(
            user_id=user_id, display_name=display_name, after_time=after_time,
            before_time=before_time, epsg=epsg_code, after_status=after_status,
            before_status=before_status, occ_set_id=occurrence_set_id,
            alg_code=alg_code, mdl_scen_code=mdl_scenario_code,
            prj_scen_code=prj_scenario_code, gridset_id=gridset_id)
        return {'count': prj_count}

    # ................................
    def _get_projection(self, path_projection_id):
        """Attempt to get a projection
        """
        prj = self.scribe.get_sdm_project(int(path_projection_id))

        if prj is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Projection {} not found'.format(path_projection_id))

        if check_user_permission(self.get_user_id(), prj, HTTPMethod.GET):
            return prj

        # If no permission, HTTP 403
        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to delete projection {}'.format(
                self.get_user_id(), path_projection_id))

    # ................................
    def _list_projections(self, user_id, after_status=None, after_time=None,
                          alg_code=None, before_status=None, before_time=None,
                          display_name=None, epsg_code=None, limit=100,
                          mdl_scenario_code=None, occurrence_set_id=None,
                          offset=0, prj_scenario_code=None, status=None,
                          gridset_id=None):
        """Return a list of projections matching the specified criteria
        """
        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                before_status = JobStatus.COMPLETE + 1
                after_status = JobStatus.COMPLETE - 1
            else:
                after_status = status - 1

        prj_atoms = self.scribe.list_sdm_projects(
            offset, limit, user_id=user_id, display_name=display_name,
            after_time=after_time, before_time=before_time, epsg=epsg_code,
            after_status=after_status, before_status=before_status,
            occ_set_id=occurrence_set_id, alg_code=alg_code,
            mdl_scen_code=mdl_scenario_code, prj_scen_code=prj_scenario_code,
            gridset_id=gridset_id)
        return prj_atoms

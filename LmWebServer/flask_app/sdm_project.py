"""This module provides REST services for Projections"""
from flask import make_response, Response
from http import HTTPStatus
import werkzeug.exceptions as WEXC

from LmServer.base.atom import Atom

from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.flask_app.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.common.boom_post import BoomPoster
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# ................................................................0.............
class SdmProjectService(LmService):
    """Class responsible for SDM Projection services
    """

    # ................................
    def get_projection(self, user_id, projection_id):
        """Retrieve a projection"""
        prj = self.scribe.get_sdm_project(int(projection_id))

        if prj is None:
            raise WEXC.NotFound('Projection {} not found'.format(projection_id))

        if check_user_permission(user_id, prj, HTTPMethod.GET):
            return prj

        # If no permission, HTTP 403
        raise WEXC.Forbidden('User {} does not have permission to access projection {}'.format(
                user_id, projection_id))


    # ................................
    def delete_projection(self, user_id, projection_id):
        """Attempts to delete a projection

        Args:
            projection_id: The id of the projection to delete
        """
        prj = self.scribe.get_sdm_project(int(projection_id))

        if prj is None:
            raise WEXC.NotFound('Projection {} not found'.format(projection_id))

        if not check_user_permission(user_id, prj, HTTPMethod.DELETE):
            raise WEXC.Forbidden('User {} does not have permission to delete projection {}'.format(
                user_id, projection_id))
        
        else:
            success = self.scribe.delete_object(prj)
            if success:
                return Response(status=HTTPStatus.NO_CONTENT)
            else:
                # If we have permission but cannot delete, error
                raise WEXC.InternalServerError('Failed to delete projection {}'.format(projection_id))

    # ................................
    @lm_formatter
    def GET(self, projection_id=None, after_status=None, after_time=None,
            algorithm_code=None, before_status=None, before_time=None,
            display_name=None, epsg_code=None, limit=100,
            model_scenario_code=None, occurrence_set_id=None, offset=0,
            projection_scenario_code=None, url_user=None, scenario_id=None,
            status=None, gridset_id=None, atom=True, **params):
        """Perform a GET request. List, count, or get individual projection.
        """
        if projection_id is None:
            return self._list_projections(
                self.get_user_id(url_user=url_user), after_status=after_status,
                after_time=after_time, alg_code=algorithm_code,
                before_status=before_status, before_time=before_time,
                display_name=display_name, epsg_code=epsg_code, limit=limit,
                mdl_scenario_code=model_scenario_code,
                occurrence_set_id=occurrence_set_id, offset=offset,
                prj_scenario_code=projection_scenario_code, status=status,
                gridset_id=gridset_id, atom=atom)

        if projection_id.lower() == 'count':
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
        return self._get_projection(projection_id)

    # ................................
    @lm_formatter
    def post_boom_data(self, user_id, user_email, projection_data, **params):
        """Posts a new projection
        """
        boom_post = BoomPoster(user_id, user_email, projection_data, self.scribe)
        gridset = boom_post.init_boom()

        atom = Atom(
            gridset.get_id(), gridset.name, gridset.metadata_url, gridset.mod_time, epsg=gridset.epsg_code)
        return make_response(atom, HTTPStatus.ACCEPTED)

    # ................................
    def count_projections(
            self, user_id, after_time=None, before_time=None, after_status=None, before_status=None, 
            alg_code=None, display_name=None, epsg_code=None, occurrence_set_id=None,
            mdl_scenario_code=None, prj_scenario_code=None, status=None, gridset_id=None):
        """Return a count of projections matching the specified criteria
        """
        # Process status parameter
        if status:
            before_status = status
            after_status = status

        prj_count = self.scribe.count_sdm_projects(
            user_id=user_id, display_name=display_name, after_time=after_time, before_time=before_time, 
            epsg=epsg_code, after_status=after_status, before_status=before_status, 
            occ_set_id=occurrence_set_id, alg_code=alg_code, mdl_scen_code=mdl_scenario_code,
            prj_scen_code=prj_scenario_code, gridset_id=gridset_id)
        return {'count': prj_count}

    # ................................
    def list_projections(
            self, user_id, after_time=None, before_time=None, after_status=None, before_status=None, 
            alg_code=None, display_name=None, epsg_code=None, occurrence_set_id=None,
            mdl_scenario_code=None, prj_scenario_code=None, status=None, gridset_id=None, 
            limit=100, offset=0, atom=True):
        """Return a list of projections matching the specified criteria"""
        # Process status parameter
        if status:
            before_status = status
            after_status = status

        projs = self.scribe.list_sdm_projects(
            offset, limit, user_id=user_id, display_name=display_name, after_time=after_time, before_time=before_time, 
            epsg=epsg_code, after_status=after_status, before_status=before_status, 
            occ_set_id=occurrence_set_id, alg_code=alg_code, mdl_scen_code=mdl_scenario_code,
            prj_scen_code=prj_scenario_code, gridset_id=gridset_id, atom=atom)
        
        return projs

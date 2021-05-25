"""Client library for Lifemapper web services

Todo:
    * Document all parameters
    * Add option to list public content for count and list service calls
"""
import http.cookiejar
import json
from urllib.parse import urlparse
import urllib.request
import urllib.parse

from LmCommon.common.lmconstants import ENCODING
from LmServer.common.localconstants import PUBLIC_FQDN
from LmWebServer.common.lmconstants import HTTPMethod


# .............................................................................
class _SERVICE:
    """Constants class for web service endpoints
    """
    BIOTAPHYNAMES = 'biotaphynames'
    BIOTAPHYPOINTS = 'biotaphypoints'
    ENVLAYER = 'envlayer'
    GBIFPARSER = 'gbifparser'
    GLOBAL_PAM = 'globalpam'
    GRIDSET = 'gridset'
    HINT = 'hint'
    LAYER = 'layer'
    LOGIN = 'login'
    LOGOUT = 'logout'
    MATRIX = 'matrix'
    MATRIX_COLUMN = 'matrixcolumn'
    OCCURRENCE = 'occurrence'
    OGC = 'ogc'
    OPEN_TREE = 'opentree'
    SCENARIO = 'scenario'
    SCENARIO_PACKAGE = 'scenpackage'
    SDM_PROJECT = 'sdmproject'
    SHAPEGRID = 'shapegrid'
    SIGNUP = 'signup'
    SNIPPET = 'snippet'
    TAXONOMY = 'taxonomy'
    TREE = 'tree'
    UPLOAD = 'upload'

    # ............................
    @staticmethod
    def user_services():
        """Get list of user services
        """
        return [_SERVICE.LOGIN, _SERVICE.LOGOUT, _SERVICE.SIGNUP]


# .............................................................................
class LmWebClient:
    """A web client for accessing Lifemapper services
    """
    # ............................
    def __init__(self, server=PUBLIC_FQDN, url_base='api', version='v2'):
        self.server = server
        # TODO: Enable secure connection when available
        if not (self.server.lower().startswith('http://') or
                self.server.lower().startswith('https://')):
            self.server = 'http://{}'.format(self.server)
        self.url_base = url_base
        self.version = version

    # ............................
    def _build_base_url(self, service, object_id=None, parent_object_id=None,
                        response_format=None):
        """Builds the base URL for a service call
        """
        url = '{}/{}'.format(self.server, self.url_base)

        if service in _SERVICE.user_services():
            url = '{}/{}'.format(url, service)
        elif service == _SERVICE.MATRIX:
            if object_id is not None:
                url = '{}/{}/{}/{}/{}/{}'.format(
                    url, self.version, _SERVICE.GRIDSET, parent_object_id,
                    _SERVICE.MATRIX, object_id)
            else:
                url = '{}/{}/{}/{}/{}'.format(
                    url, self.version, _SERVICE.GRIDSET, parent_object_id,
                    _SERVICE.MATRIX)
        else:
            url = '{}/{}/{}'.format(url, self.version, service)
            if object_id is not None:
                url = '{}/{}'.format(url, object_id)
            if response_format is not None:
                url = '{}/{}'.format(url, response_format)
        return url

    # ............................
    @staticmethod
    def _make_request(req_url, method=HTTPMethod.GET, body=None,
                      headers=None, **query_parameters):
        """Submits a request to the server and returns an open file-like object

        Args:
            req_url: The base URL to submit the request to
            method: The HTTP method used to make the request
            body: The body of the request if desired
            headers: A dictionary of headers to send with the request
            query_parameters; Any additional optional parameters sent to this
                function will be wrapped as query parameters for the request
        """
        try:
            q_params = [
                (k, v) for k, v in list(dict(
                    query_parameters).items()) if v is not None]
            url_params = urllib.parse.urlencode(q_params)

            if body is None and len(
                    q_params) > 0 and method.upper() == HTTPMethod.POST:
                body = url_params
            else:
                req_url = '{}?{}'.format(req_url, url_params)

            if headers is None:
                headers = {}
            
            if isinstance(body, str):
                body = body.encode(encoding=ENCODING)
            req = urllib.request.Request(req_url, data=body, headers=headers)
            req.get_method = lambda: method.upper()

            return urllib.request.urlopen(req)
        except Exception as e:
            print(('The failed URL was: {}'.format(req_url)))
            print(('Error: {}'.format(str(e))))
            raise e

    # ========================
    # = Environmental Layers =
    # ========================
    # ............................
    def delete_environmental_layer(self, layer_id):
        """Attempts to delete an environmental layer
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.ENVLAYER, object_id=layer_id), HTTPMethod.DELETE)

    # ............................
    def count_environmental_layers(self, after_time=None, alt_pred_code=None,
                                   before_time=None, date_code=None,
                                   epsg_code=None, env_code=None,
                                   env_type_id=None, gcm_code=None,
                                   scenario_id=None, headers=None,
                                   response_format=None):
        """Sends a request to the server to count environmental layers
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.ENVLAYER, object_id='count',
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            alt_pred_code=alt_pred_code, before_time=before_time,
            date_code=date_code, epsg_code=epsg_code, env_code=env_code,
            env_type_id=env_type_id, gcm_code=gcm_code,
            scenario_id=scenario_id)

    # ............................
    def get_environmental_layer(self, layer_id, headers=None,
                                response_format=None):
        """Gets an environmental layer from the server
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.ENVLAYER, object_id=layer_id,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_environmental_layers(self, after_time=None, alt_pred_code=None,
                                  before_time=None, date_code=None,
                                  epsg_code=None, env_code=None,
                                  env_type_id=None, gcm_code=None,
                                  scenario_id=None, limit=None, offset=None,
                                  headers=None, response_format=None):
        """Lists environmental layers matching the parameters
        """
        return self._make_request(
            self._build_base_url(_SERVICE.ENVLAYER,
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            alt_pred_code=alt_pred_code, before_time=before_time,
            date_code=date_code, epsg_code=epsg_code, env_code=env_code,
            env_type_id=env_type_id, gcm_code=gcm_code,
            scenario_id=scenario_id, limit=limit, offset=offset)

    # ............................
    def post_environmental_layer(self, layer_content, layer_type, epsg_code,
                                 layer_name, env_layer_type_id=None,
                                 additional_metadata=None, val_units=None,
                                 env_code=None, gcm_code=None,
                                 alt_pred_code=None, date_code=None,
                                 headers=None, response_format=None):
        """Posts a new environmental layer to the server
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.ENVLAYER, response_format=response_format),
            method=HTTPMethod.POST, body=layer_content, headers=headers,
            layer_type=layer_type, epsg_code=epsg_code, layer_name=layer_name,
            env_layer_type_id=env_layer_type_id,
            additional_metadata=additional_metadata, val_units=val_units,
            env_code=env_code, gcm_code=gcm_code, alt_pred_code=alt_pred_code,
            date_code=date_code)

    # ====================
    # = GBIF Name Parser =
    # ====================
    # ............................
    def post_gbif_names_parser(self, names_list, headers=None,
                               response_format=None):
        """Requests the GBIF accepted names from the name strings provided
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GBIFPARSER, response_format=response_format),
            method=HTTPMethod.POST, body=str(names_list), headers=headers)

    # ==============
    # = Global PAM =
    # ==============
    # ............................
    def get_global_pam_gridset_facets(self, headers=None,
                                      response_format=None):
        """Gets the gridsets available and their counts in the Global PAM
        """
        url = self._build_base_url(
            _SERVICE.GLOBAL_PAM, object_id='gridset',
            response_format=response_format)
        return self._make_request(url, method=HTTPMethod.GET, headers=headers)

    # ............................
    def get_global_pam_matches(self, algorithm_code=None, bbox=None,
                               display_name=None, gridset_id=None,
                               model_scenario_code=None, point_max=None,
                               point_min=None, prj_scen_code=None, squid=None,
                               taxon_kingdom=None, taxon_phylum=None,
                               taxon_class=None, taxon_order=None,
                               taxon_family=None, taxon_genus=None,
                               taxon_species=None, headers=None,
                               response_format=None):
        """Queries the Global PAM for matching records
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GLOBAL_PAM, response_format=response_format),
            method=HTTPMethod.GET, headers=headers,
            algorithm_code=algorithm_code, bbox=bbox,
            display_name=display_name, gridset_id=gridset_id,
            model_scenario_code=model_scenario_code, point_max=point_max,
            point_min=point_min, prj_scen_code=prj_scen_code, squid=squid,
            taxon_kingdom=taxon_kingdom, taxon_phylum=taxon_phylum,
            taxon_class=taxon_class, taxon_order=taxon_order,
            taxon_family=taxon_family, taxon_genus=taxon_genus,
            taxon_species=taxon_species)

    # ............................
    def post_global_pam_subset(self, archive_name, gridset_id,
                               algorithm_code=None, bbox=None,
                               display_name=None, model_scenario_code=None,
                               point_max=None, point_min=None,
                               prj_scen_code=None, squid=None,
                               taxon_kingdom=None, taxon_phylum=None,
                               taxon_class=None, taxon_order=None,
                               taxon_family=None, taxon_genus=None,
                               taxon_species=None, headers=None,
                               response_format=None):
        """Requests a subset of the Global PAM for matching PAVs
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GLOBAL_PAM, response_format=response_format),
            method=HTTPMethod.POST, headers=headers, archive_name=archive_name,
            gridset_id=gridset_id, algorithm_code=algorithm_code, bbox=bbox,
            display_name=display_name, model_scenario_code=model_scenario_code,
            point_max=point_max, point_min=point_min,
            prj_scen_code=prj_scen_code, squid=squid,
            taxon_kingdom=taxon_kingdom, taxon_phylum=taxon_phylum,
            taxon_class=taxon_class, taxon_order=taxon_order,
            taxon_family=taxon_family, taxon_genus=taxon_genus,
            taxon_species=taxon_species)

    # ============
    # = Gridsets =
    # ============
    # ............................
    def count_gridsets(self, after_time=None, before_time=None, epsg_code=None,
                       meta_string=None, shapegrid_id=None, headers=None,
                       response_format=None):
        """Counts the gridsets matching the query parameters
        """
        return self._make_request(
            self._build_base_url(_SERVICE.GRIDSET, object_id='count',
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            meta_string=meta_string, shapegrid_id=shapegrid_id,
            before_time=before_time, epsg_code=epsg_code)

    # ............................
    def delete_gridset(self, gridset_id):
        """Attempts to delete a gridset
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GRIDSET, object_id=gridset_id), HTTPMethod.DELETE)

    # ............................
    def delete_gridset_tree(self, gridset_id):
        """Attempts to delete the tree for a gridset
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GRIDSET, object_id='{}/tree'.format(gridset_id)),
            HTTPMethod.DELETE)

    # ............................
    def get_gridset(self, gridset_id, headers=None, response_format=None):
        """Gets a the specified gridset
        """
        return self._make_request(
            self._build_base_url(_SERVICE.GRIDSET, object_id=gridset_id,
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def get_gridset_biogeographic_hypotheses(self, gridset_id, headers=None,
                                             response_format=None):
        """Gets the biogeographic hypotheses for a gridset
        """
        return self._make_request(
            self._build_base_url(_SERVICE.GRIDSET,
                                 object_id='{}/biogeo'.format(gridset_id),
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def get_gridset_tree(self, gridset_id, headers=None, response_format=None):
        """Gets the tree for a gridset
        """
        return self._make_request(
            self._build_base_url(_SERVICE.GRIDSET,
                                 object_id='{}/tree'.format(gridset_id),
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_gridsets(self, after_time=None, before_time=None, epsg_code=None,
                      limit=None, meta_string=None, offset=None,
                      shapegrid_id=None, headers=None, response_format=None):
        """Lists gridsets matching query parameters
        """
        return self._make_request(
            self._build_base_url(_SERVICE.GRIDSET,
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            before_time=before_time, epsg_code=epsg_code, limit=limit,
            meta_string=meta_string, offset=offset, shapegrid_id=shapegrid_id)

    # ............................
    def post_gridset(self, boom_post_json, headers=None, response_format=None):
        """Posts a new gridset
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GRIDSET, response_format=response_format),
            method=HTTPMethod.POST, body=boom_post_json, headers=headers)

    # ............................
    def post_gridset_analysis(self, gridset_id, do_calc=False, do_mcpa=False,
                              num_permutations=300, headers=None,
                              response_format=None):
        """Submits a request to analyze a gridset
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GRIDSET, object_id='{}/analysis'.format(gridset_id),
                response_format=response_format),
            method=HTTPMethod.POST, headers=headers, do_calc=do_calc,
            do_mcpa=do_mcpa, num_permutations=num_permutations)

    # ............................
    def post_gridset_biogeographic_hypotheses(self, gridset_id,
                                              hypothesis_json, headers=None,
                                              response_format=None):
        """Adds biogeographic hypotheses to a gridset
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GRIDSET, object_id='{}/biogeo'.format(gridset_id),
                response_format=response_format), method=HTTPMethod.POST,
            headers=headers, body=hypothesis_json)

    # ............................
    def post_gridset_tree(self, gridset_id, tree_id=None, tree_content=None,
                          tree_schema=None, headers=None,
                          response_format=None):
        """Adds a tree to a gridset
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.GRIDSET, object_id='{}/tree'.format(gridset_id),
                response_format=response_format), method=HTTPMethod.POST,
            headers=headers, body=tree_content, tree_schema=tree_schema,
            tree_id=tree_id)

    # ==========
    # = Layers =
    # ==========
    # ............................
    def count_layers(self, after_time=None, before_time=None, epsg_code=None,
                     squid=None, headers=None, response_format=None):
        """Count the layers matching the query parameters
        """
        return self._make_request(
            self._build_base_url(_SERVICE.LAYER, object_id='count',
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            before_time=before_time, epsg_code=epsg_code, squid=squid)

    # ............................
    def delete_layer(self, layer_id, headers=None, response_format=None):
        """Attempts to delete a layer from the server
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.LAYER, object_id=layer_id,
                response_format=response_format),
            method=HTTPMethod.DELETE, headers=headers)

    # ............................
    def get_layer(self, layer_id, headers=None, response_format=None):
        """Attempts to get a layer from the service
        """
        return self._make_request(
            self._build_base_url(_SERVICE.LAYER, object_id=layer_id,
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_layers(self, after_time=None, before_time=None, epsg_code=None,
                    limit=None, offset=None, squid=None, headers=None,
                    response_format=None):
        """Send a request to the server to get a list of layers
        """
        return self._make_request(
            self._build_base_url(_SERVICE.LAYER,
                                 response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            before_time=before_time, epsg_code=epsg_code, squid=squid,
            limit=limit, offset=offset)

    # ............................
    def post_layer(self, layer_content, layer_type, epsg_code, layer_name,
                   env_layer_type_id=None, additional_metadata=None,
                   val_units=None, env_code=None, gcm_code=None,
                   alt_pred_code=None, date_code=None, headers=None,
                   response_format=None):
        """Posts a new layer to the server
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.LAYER, response_format=response_format),
            method=HTTPMethod.POST, body=layer_content, headers=headers,
            layer_type=layer_type, epsg_code=epsg_code, layer_name=layer_name,
            env_layer_type_id=env_layer_type_id,
            additional_metadata=additional_metadata, val_units=val_units,
            env_code=env_code, gcm_code=gcm_code, alt_pred_code=alt_pred_code,
            date_code=date_code)

    # ============
    # = Matrices =
    # ============
    # ............................
    def delete_matrix(self, gridset_id, matrix_id, headers=None,
                      response_format=None):
        """Attempts to delete a matrix from the server
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.MATRIX, object_id=matrix_id,
                parent_object_id=gridset_id, response_format=response_format),
            method=HTTPMethod.DELETE, headers=headers)

    # ............................
    def count_matrices(self, gridset_id, after_time=None, alt_pred_code=None,
                       before_time=None, date_code=None, epsg_code=None,
                       gcm_code=None, keyword=None, matrix_type=None,
                       status=None, headers=None, response_format=None):
        """Send a request to the server to count matrices
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.MATRIX, object_id='count',
                parent_object_id=gridset_id, response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            alt_pred_code=alt_pred_code, before_time=before_time,
            date_code=date_code, epsg_code=epsg_code, keyword=keyword,
            matrix_type=matrix_type, status=status, gcm_code=gcm_code)

    # ............................
    def get_matrix(self, gridset_id, matrix_id, headers=None,
                   response_format=None):
        """Send a request to the server to get a matrix
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.MATRIX, object_id=matrix_id,
                parent_object_id=gridset_id, response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_matrices(self, gridset_id, after_time=None, alt_pred_code=None,
                      before_time=None, date_code=None, epsg_code=None,
                      gcm_code=None, keyword=None, limit=None,
                      matrix_type=None, offset=None, status=None, headers=None,
                      response_format=None):
        """Gets a list of matrices
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.MATRIX, parent_object_id=gridset_id,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            alt_pred_code=alt_pred_code, before_time=before_time,
            date_code=date_code, epsg_code=epsg_code, keyword=keyword,
            matrix_type=matrix_type, gcm_code=gcm_code, status=status,
            limit=limit, offset=offset)

    # ===================
    # = Occurrence sets =
    # ===================
    # ............................
    def count_occurrence_sets(self, after_time=None, before_time=None,
                              display_name=None, epsg_code=None,
                              minimum_number_of_points=None, status=None,
                              gridset_id=None, headers=None,
                              response_format=None):
        """Count occurrence sets matching the specified criteria
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.OCCURRENCE, object_id='count',
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            before_time=before_time, display_name=display_name,
            epsg_code=epsg_code, gridset_id=gridset_id, status=status,
            minimum_number_of_points=minimum_number_of_points)

    # ............................
    def delete_occurrence_set(self, occ_id, headers=None,
                              response_format=None):
        """Attempts to delete an occurrence set from the server
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.OCCURRENCE, object_id=occ_id,
                response_format=response_format),
            method=HTTPMethod.DELETE, headers=headers)

    # ............................
    def get_occurrence_set(self, occ_id, headers=None, response_format=None):
        """Gets an occurrence set
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.OCCURRENCE, object_id=occ_id,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_occurrence_sets(self, after_time=None, before_time=None,
                             display_name=None, epsg_code=None,
                             minimum_number_of_points=None, limit=None,
                             offset=None, status=None, gridset_id=None,
                             fillPoints=None, headers=None,
                             response_format=None):
        """List occurrence sets matching the specified criteria
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.OCCURRENCE, response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            before_time=before_time, display_name=display_name,
            epsg_code=epsg_code,
            minimum_number_of_points=minimum_number_of_points, status=status,
            gridset_id=gridset_id, fillPoints=fillPoints, limit=limit,
            offset=offset)

    # =======
    # = OGC =
    # =======
    # ............................
    def get_ogc(self, map_name, bbox=None, bgcolor=None, color=None,
                coverage=None, crs=None, exceptions=None, height=None,
                layer=None, layers=None, point=None, request=None,
                format_=None, service=None, sld=None, sld_body=None, srs=None,
                styles=None, time=None, transparent=None, version=None,
                width=None, headers=None):
        """Send an OGC request to the server
        """
        return self._make_request(
            self._build_base_url(_SERVICE.OGC), method=HTTPMethod.GET,
            headers=headers, map_name=map_name, bbox=bbox, bgcolor=bgcolor,
            color=color, coverage=coverage, crs=crs, exceptions=exceptions,
            height=height, layer=layer, layers=layers, point=point,
            request=request, format_=format_, service=service, sld=sld,
            sld_body=sld_body, srs=srs, styles=styles, time=time,
            transparent=transparent, version=version, width=width)

    # =============
    # = Open Tree =
    # =============
    # ............................
    def post_open_tree_induce_subtree(self, taxon_ids, headers=None,
                                      response_format=None):
        """Requests an Open Tree subtree from the taxon ids
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.OPEN_TREE, response_format=response_format),
            method=HTTPMethod.POST, body=str(taxon_ids), headers=headers)

    # =============
    # = Scenarios =
    # =============
    # ............................
    def count_scenarios(self, after_time=None, alt_pred_code=None,
                        before_time=None, date_code=None, epsg_code=None,
                        gcm_code=None, headers=None, response_format=None):
        """Count the scenarios matching the query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SCENARIO, object_id='count',
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            alt_pred_code=alt_pred_code, before_time=before_time,
            date_code=date_code, epsg_code=epsg_code, gcm_code=gcm_code)

    # ............................
    def delete_scenario(self, scenario_id, headers=None, response_format=None):
        """Attempts to delete a scenario
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SCENARIO, object_id=scenario_id,
                response_format=response_format),
            method=HTTPMethod.DELETE, headers=headers)

    # ............................
    def get_scenario(self, scenario_id, headers=None, response_format=None):
        """Attempt to get the specified scenario from the server
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SCENARIO, object_id=scenario_id,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_scenarios(self, after_time=None, alt_pred_code=None,
                       before_time=None, date_code=None, epsg_code=None,
                       gcm_code=None, limit=100, offset=0, headers=None,
                       response_format=None):
        """List scenarios that match the specified query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SCENARIO, response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            alt_pred_code=alt_pred_code, before_time=before_time,
            date_code=date_code, epsg_code=epsg_code, gcm_code=gcm_code,
            limit=limit, offset=offset)

    # ............................
    def post_scenario(self, scenario_json, headers=None, response_format=None):
        """Posts a new scenario
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SCENARIO, response_format=response_format),
            method=HTTPMethod.POST, body=scenario_json, headers=headers)

    # =====================
    # = Scenario Packages =
    # =====================
    # ............................
    def count_scenario_packages(self, after_time=None, before_time=None,
                                scenario_id=None, headers=None,
                                response_format=None):
        """Count the scenario packages matching the query paramters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SCENARIO_PACKAGE, object_id='count',
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            before_time=before_time, scenario_id=scenario_id)

    # ............................
    def get_scenario_package(self, scen_package_id, headers=None,
                             response_format=None):
        """Attempt to get the specified scenario package
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SCENARIO_PACKAGE, object_id=scen_package_id,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_scenario_packages(self, after_time=None, before_time=None,
                               limit=100, offset=0, scenario_id=None,
                               headers=None, response_format=None):
        """List the scenario packages matching the query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SCENARIO_PACKAGE, response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            scenario_id=scenario_id, before_time=before_time, limit=limit,
            offset=offset)

    # ===================
    # = SDM Projections =
    # ===================
    # ............................
    def count_sdm_projections(self, after_status=None, after_time=None,
                              algorithm_code=None, before_status=None,
                              before_time=None, display_name=None,
                              epsg_code=None, model_scenario_code=None,
                              occurrence_set_id=None,
                              projection_scenario_code=None, scenario_id=None,
                              status=None, gridset_id=None, headers=None,
                              response_format=None):
        """Count the SDM projections matching the query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SDM_PROJECT, object_id='count',
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            scenario_id=scenario_id, before_time=before_time,
            after_status=after_status, algorithm_code=algorithm_code,
            before_status=before_status, display_name=display_name,
            epsg_code=epsg_code, model_scenario_code=model_scenario_code,
            status=status, occurrence_set_id=occurrence_set_id,
            projection_scenario_code=projection_scenario_code,
            gridset_id=gridset_id)

    # ............................
    def delete_sdm_projection(self, projection_id, headers=None,
                              response_format=None):
        """Attempt to delete a projection
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SDM_PROJECT, object_id=projection_id,
                response_format=response_format),
            method=HTTPMethod.DELETE, headers=headers)

    # ............................
    def get_sdm_projection(self, projection_id, headers=None,
                           response_format=None):
        """Attempt to retrieve an SDM projection
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SDM_PROJECT, object_id=projection_id,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_sdm_projections(self, after_status=None, after_time=None,
                             algorithm_code=None, before_status=None,
                             before_time=None, display_name=None,
                             epsg_code=None, limit=None,
                             model_scenario_code=None, occurrence_set_id=None,
                             offset=None, projection_scenario_code=None,
                             scenario_id=None, status=None, gridset_id=None,
                             headers=None, response_format=None):
        """List SDM projections matching the query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SDM_PROJECT, response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            scenario_id=scenario_id, before_time=before_time, limit=limit,
            offset=offset, after_status=after_status,
            algorithm_code=algorithm_code, before_status=before_status,
            display_name=display_name, epsg_code=epsg_code,
            model_scenario_code=model_scenario_code, status=status,
            occurrence_set_id=occurrence_set_id,
            projection_scenario_code=projection_scenario_code,
            gridset_id=gridset_id)

    # ............................
    def post_sdm_projection(self, boom_post_json, headers=None,
                            response_format=None):
        """Posts a new projection via a BOOM post
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SDM_PROJECT, response_format=response_format),
            method=HTTPMethod.POST, body=boom_post_json, headers=headers)

    # ==============
    # = Shapegrids =
    # ==============
    # ............................
    def count_shapegrids(self, after_time=None, before_time=None,
                         cell_sides=None, cell_size=None, epsg_code=None,
                         headers=None, response_format=None):
        """Count the number of shapegrids that match the query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SHAPEGRID, object_id='count',
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            before_time=before_time, cell_sides=cell_sides,
            cell_size=cell_size, epsg_code=epsg_code)

    # ............................
    def delete_shapegrid(self, shapegrid_id, headers=None,
                         response_format=None):
        """Attempts to delete a shapegrid
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SHAPEGRID, object_id=shapegrid_id,
                response_format=response_format),
            method=HTTPMethod.DELETE, headers=headers)

    # ............................
    def get_shapegrid(self, shapegrid_id, headers=None, response_format=None):
        """Attempt to retrieve a shapegrid from the server
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SHAPEGRID, object_id=shapegrid_id,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_shapegrids(self, after_time=None, before_time=None,
                        cell_sides=None, cell_size=None, epsg_code=None,
                        limit=None, offset=None, headers=None,
                        response_format=None):
        """Lists the shapegrids that match the specified query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SHAPEGRID, response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            before_time=before_time, cell_sides=cell_sides,
            cell_size=cell_size, limit=limit, offset=offset,
            epsg_code=epsg_code)

    # ............................
    def post_shapegrid(self, name, epsg_code, cell_sides, cell_size, map_units,
                       bbox, cutout, headers=None, response_format=None):
        """Posts a new shapegrid
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SHAPEGRID, response_format=response_format),
            method=HTTPMethod.POST, headers=headers, name=name,
            epsg_code=epsg_code, cell_sides=cell_sides, cell_size=cell_size,
            map_units=map_units, bbox=bbox, cutout=cutout)

    # ============
    # = Snippets =
    # ============
    # ............................
    def list_snippets(self, ident1=None, provider=None, collection=None,
                      catalog_number=None, operation=None, after_time=None,
                      before_time=None, ident2=None, url=None, who=None,
                      agent=None, why=None, headers=None,
                      response_format=None):
        """List snippets matching the specified query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.SNIPPET, response_format=response_format),
            method=HTTPMethod.GET, headers=headers, ident1=ident1,
            provider=provider, collection=collection,
            catalog_number=catalog_number, operation=operation,
            after_time=after_time, before_time=before_time, ident2=ident2,
            url=url, who=who, agent=agent, why=why)

    # =================
    # = Species Hints =
    # =================
    # ............................
    def list_species_hints(self, search_string, limit=None, headers=None,
                           response_format=None):
        """List species hints that match the provided search string
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.HINT, object_id=search_string,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, limit=limit)

    # ============
    # = Taxonomy =
    # ============
    # ............................
    def get_taxonomy_hints(self, taxon_kingdom=None, taxon_phylum=None,
                           taxon_class=None, taxon_order=None,
                           taxon_family=None, taxon_genus=None, taxon_key=None,
                           scientific_name=None, canonical_name=None,
                           squid=None, limit=None, headers=None,
                           response_format=None):
        """Get the taxonomy hits matching the specified query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.TAXONOMY, response_format=response_format),
            method=HTTPMethod.GET, headers=headers,
            taxon_kingdom=taxon_kingdom, taxon_phylum=taxon_phylum,
            taxon_class=taxon_class, taxon_order=taxon_order,
            taxon_family=taxon_family, taxon_genus=taxon_genus,
            taxon_key=taxon_key, scientific_name=scientific_name,
            canonical_name=canonical_name, squid=squid, limit=limit)

    # =========
    # = Trees =
    # =========
    # ............................
    def count_trees(self, name=None, is_binary=None, is_ultrametric=None,
                    has_branch_lengths=None, meta_string=None, after_time=None,
                    before_time=None, headers=None, response_format=None):
        """Count the number of trees matching the query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.TREE, object_id='count',
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            name=name, is_binary=is_binary, is_ultrametric=is_ultrametric,
            has_branch_lengths=has_branch_lengths, meta_string=meta_string,
            before_time=before_time)

    # ............................
    def delete_tree(self, tree_id, headers=None, response_format=None):
        """Attempt to delete a tree
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.TREE, object_id=tree_id,
                response_format=response_format),
            method=HTTPMethod.DELETE, headers=headers)

    # ............................
    def get_tree(self, tree_id, headers=None, response_format=None):
        """Attempt to retrieve the specified tree
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.TREE, object_id=tree_id,
                response_format=response_format),
            method=HTTPMethod.GET, headers=headers)

    # ............................
    def list_trees(self, limit=None, offset=None, name=None, is_binary=None,
                   is_ultrametric=None, has_branch_lengths=None,
                   meta_string=None, after_time=None, before_time=None,
                   headers=None, response_format=None):
        """List the trees matching the specified query parameters
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.TREE, response_format=response_format),
            method=HTTPMethod.GET, headers=headers, after_time=after_time,
            name=name, is_binary=is_binary, is_ultrametric=is_ultrametric,
            has_branch_lengths=has_branch_lengths, meta_string=meta_string,
            before_time=before_time, limit=limit, offset=offset)

    # ............................
    def post_tree(self, tree_content, tree_schema, headers=None,
                  response_format=None):
        """Post a new tree
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.TREE, response_format),
            method=HTTPMethod.POST, body=tree_content, headers=headers,
            tree_schema=tree_schema)

    # ===========
    # = Uploads =
    # ===========
    # ............................
    def post_upload_biogeographic_hypotheses(self, hypotheses_content,
                                             file_name, headers=None,
                                             response_format=None):
        """Uploads zipped hypotheses files for booming
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.UPLOAD, response_format=response_format),
            method=HTTPMethod.POST, body=hypotheses_content, headers=headers,
            file_name=file_name, upload_type='biogeo')

    # ............................
    def post_upload_occurrence_data(self, occ_content, file_name, metadata,
                                    headers=None, response_format=None):
        """Uploads occurrence data for booming
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.UPLOAD, response_format=response_format),
            method=HTTPMethod.POST, body=occ_content, headers=headers,
            file_name=file_name, upload_type='occurrence', metadata=metadata)

    # ............................
    def post_upload_scenario_package(self, scen_package_content, file_name,
                                     headers=None, response_format=None):
        """Uploads a scenario package file for booming
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.UPLOAD, response_format=response_format),
            method=HTTPMethod.POST, body=scen_package_content, headers=headers,
            file_name=file_name, upload_type='climate')

    # ............................
    def post_upload_tree(self, tree_content, file_name, headers=None,
                         response_format=None):
        """Uploads a tree file for booming
        """
        return self._make_request(
            self._build_base_url(
                _SERVICE.UPLOAD, response_format=response_format),
            method=HTTPMethod.POST, body=tree_content, headers=headers,
            file_name=file_name, upload_type='tree')

    # =================
    # = User Services =
    # =================
    # ............................
    def login(self, user_id, passwd):
        """Logs in to the server
        """
        policy_server = urlparse(self.server).netloc
        policy = http.cookiejar.DefaultCookiePolicy(
            allowed_domains=(policy_server,))
        self.cookie_jar = http.cookiejar.LWPCookieJar(policy=policy)
        opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar))
        urllib.request.install_opener(opener)

        req = self._make_request(
            self._build_base_url(_SERVICE.LOGIN), user_id=user_id,
            pword=passwd, method=HTTPMethod.POST)
        resp = req.read()
        req.close()
        return resp

    # ............................
    def logout(self):
        """Logs out of the server
        """
        req = self._make_request(self._build_base_url(_SERVICE.LOGOUT))
        resp = req.read()
        req.close()
        return resp

    # ==============
    # = Processing =
    # ==============
    # ............................
    @staticmethod
    def deserialize(response_object):
        """Deserializes a JSON file-like object
        """
        return json.load(response_object)

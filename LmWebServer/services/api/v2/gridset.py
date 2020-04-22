"""This module provides REST services for grid sets
"""
import json
import os
import zipfile

import cherrypy
import dendropy

from lmpy import Matrix

from LmCommon.common.lmconstants import (
    DEFAULT_TREE_SCHEMA, HTTPStatus, JobStatus, LMFormat, MatrixType,
    ProcessType)
from LmCommon.common.time import gmt
from LmCommon.encoding.layer_encoder import LayerEncoder
from LmDbServer.boom.boom_collate import BoomCollate
from LmServer.base.atom import Atom
from LmServer.base.layer import Vector
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import ARCHIVE_PATH
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.tree import Tree
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.api.v2.matrix import MatrixService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.common.boom_post import BoomPoster
from LmWebServer.services.cp_tools.lm_format import lm_formatter

BG_REF_ID_KEY = 'identifier'
BG_REF_KEY = 'hypothesis_package_reference'
BG_REF_TYPE_KEY = 'reference_type'
EVENT_FIELD_KEY = 'event_field'
FILE_NAME_KEY = 'file_name'
HYPOTHESIS_NAME_KEY = 'hypothesis_name'
KEYWORD_KEY = 'keywords'
LAYERS_KEY = 'layers'


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
        elif status < JobStatus.COMPLETE:
            running += count
        elif status == JobStatus.COMPLETE:
            complete += count
        else:
            error += count
        total += count
    return (waiting, running, complete, error, total)


# .............................................................................
@cherrypy.expose
class GridsetAnalysisService(LmService):
    """This class is for the service representing gridset analyses.

    Note:
        The dispatcher is responsible for calling the correct method.

    Todo:
        * Enable DELETE?  Could remove all existing analysis matrices
        * Enable GET?  Could this just be the outputs?
    """

    # ................................
    @lm_formatter
    def POST(self, path_gridset_id, do_mcpa=False, num_permutations=500,
             do_calc=False, **params):
        """Adds a set of biogeographic hypotheses to the gridset
        """
        # Get gridset
        gridset = self._get_gridset(path_gridset_id)

        # Check status of all matrices
        if not all(
                [mtx.status == JobStatus.COMPLETE
                 for mtx in gridset.get_matrices()]):
            raise cherrypy.HTTPError(
                HTTPStatus.CONFLICT,
                ('The gridset is not ready for analysis.  '
                 'All matrices must be complete'))

        if do_mcpa:
            mcpa_possible = len(
                gridset.get_biogeographic_hypotheses()
                ) > 0 and gridset.tree is not None
        if not mcpa_possible:
            raise cherrypy.HTTPError(
                HTTPStatus.CONFLICT,
                ('The gridset must have a tree and biogeographic '
                 'hypotheses to perform MCPA'))

        # If everything is ready and we have analyses to run, do so
        if do_mcpa or do_calc:
            boom_col = BoomCollate(
                gridset, do_pam_stats=do_calc, do_mcpa=do_mcpa,
                num_permutations=num_permutations)
            boom_col.create_workflow()
            boom_col.close()

            cherrypy.response.status = HTTPStatus.ACCEPTED
            return gridset

        raise cherrypy.HTTPError(
            HTTPStatus.BAD_REQUEST,
            'Must specify at least one analysis to perform')

    # ................................
    def _get_gridset(self, path_gridset_id):
        """Attempt to get a GridSet
        """
        gridset = self.scribe.get_gridset(
            gridset_id=path_gridset_id, fill_matrices=True)
        if gridset is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Gridset {} was not found'.format(path_gridset_id))
        if check_user_permission(self.get_user_id(), gridset, HTTPMethod.GET):
            return gridset

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access Gridset {}'.format(
                self.get_user_id(), path_gridset_id))

    # ................................
    def _get_user_dir(self):
        """Get the user's workspace directory

        Todo:
            Change this to use something at a lower level.  This is using the
                same path construction as the getBoomPackage script
        """
        return os.path.join(
            ARCHIVE_PATH, self.get_user_id(), 'uploads', 'biogeo')


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_biogeo_id')
class GridsetBioGeoService(LmService):
    """Service class for gridset biogeographic hypotheses
    """

    # ................................
    @lm_formatter
    def GET(self, path_gridset_id, path_biogeo_id=None, **params):
        """There is not a true service for limiting the biogeographic
               hypothesis matrices in a gridset, but return all when listing
        """
        gridset = self._get_gridset(path_gridset_id)

        bg_hyps = gridset.get_biogeographic_hypotheses()

        if path_biogeo_id is None:
            return bg_hyps

        for hyp in bg_hyps:
            if hyp.get_id() == path_biogeo_id:
                return hyp

        # If not found 404...
        raise cherrypy.HTTPError(
            HTTPStatus.NOT_FOUND,
            'Biogeographic hypothesis mtx {} not found for gridset {}'.format(
                path_biogeo_id, path_gridset_id))

    # ................................
    @lm_formatter
    def POST(self, path_gridset_id, **params):
        """Adds a set of biogeographic hypotheses to the gridset
        """
        # Get gridset
        gridset = self._get_gridset(path_gridset_id)

        # Process JSON
        hypothesis_json = json.loads(cherrypy.request.body.read())

        # Check reference to get file
        ref_obj = hypothesis_json[BG_REF_KEY]

        # If gridset,
        if ref_obj[BG_REF_TYPE_KEY].lower() == 'gridset':
            #      copy hypotheses from gridset
            try:
                ref_gridset_id = int(ref_obj[BG_REF_ID_KEY])
            except Exception:
                # Probably not an integer or something
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST,
                    'Cannot get gridset for reference identfier {}'.format(
                        ref_obj[BG_REF_ID_KEY]))
            ref_gridset = self._get_gridset(ref_gridset_id)

            # Get hypotheses from other gridset
            ret = []
            for bg_hyp in ref_gridset.get_biogeographic_hypotheses():
                new_bg_mtx = LMMatrix(
                    None, matrix_type=MatrixType.BIOGEO_HYPOTHESES,
                    process_type=ProcessType.ENCODE_HYPOTHESES,
                    gcm_code=bg_hyp.gcm_code,
                    alt_pred_code=bg_hyp.alt_pred_code,
                    date_code=bg_hyp.date_code, metadata=bg_hyp.mtx_metadata,
                    user_id=gridset.get_user_id(), gridset=gridset,
                    status=JobStatus.INITIALIZE)
                inserted_bg = self.scribe.find_or_insert_matrix(new_bg_mtx)
                inserted_bg.update_status(JobStatus.COMPLETE)
                self.scribe.update_object(inserted_bg)
                # Save the original grim data into the new location
                bg_mtx = Matrix.load(bg_hyp.get_dlocation())
                bg_mtx.write(inserted_bg.get_dlocation())
                ret.append(inserted_bg)
        elif ref_obj[BG_REF_TYPE_KEY].lower() == 'upload':
            curr_time = gmt().mjd
            # Check for uploaded biogeo package
            package_name = ref_obj[BG_REF_ID_KEY]
            package_filename = os.path.join(
                self._get_user_dir(), '{}{}'.format(
                    package_name, LMFormat.ZIP.ext))

            encoder = LayerEncoder(gridset.get_shapegrid().get_dlocation())
            # TODO(CJ): Pull this from config somewhere
            min_coverage = 0.25

            if os.path.exists(package_filename):
                with open(package_filename) as in_f:
                    with zipfile.ZipFile(in_f, allowZip64=True) as zip_f:
                        # Get file names in package
                        avail_files = zip_f.namelist()

                        for hyp_lyr in ref_obj[LAYERS_KEY]:
                            hyp_filename = hyp_lyr[FILE_NAME_KEY]

                            # Check to see if file is in zip package
                            if hyp_filename in avail_files or \
                                    '{}{}'.format(
                                            hyp_filename, LMFormat.SHAPE.ext
                                        ) in avail_files:
                                if HYPOTHESIS_NAME_KEY in hyp_lyr:
                                    hyp_name = hyp_lyr[HYPOTHESIS_NAME_KEY]
                                else:
                                    hyp_name = os.path.splitext(
                                        os.path.basename(hyp_filename))[0]

                                if EVENT_FIELD_KEY in hyp_lyr:
                                    event_field = hyp_lyr[EVENT_FIELD_KEY]
                                    column_name = '{} - {}'.format(
                                        hyp_name, event_field)
                                else:
                                    event_field = None
                                    column_name = hyp_name

                                int_param_val_key = \
                                    MatrixColumn.INTERSECT_PARAM_VAL_NAME
                                lyr_meta = {
                                    'name': hyp_name,
                                    int_param_val_key.lower(): event_field,
                                    ServiceObject.META_DESCRIPTION.lower():
                                        '{} based on layer {}'.format(
                                            'Biogeographic hypotheses',
                                            hyp_filename),
                                    ServiceObject.META_KEYWORDS.lower(): [
                                        'biogeographic hypothesis'
                                    ]
                                }

                                if KEYWORD_KEY in hyp_lyr:
                                    lyr_meta[
                                        ServiceObject.META_KEYWORDS.lower()
                                        ].extend(hyp_lyr[KEYWORD_KEY])

                                lyr = Vector(
                                    hyp_name, gridset.get_user_id(),
                                    gridset.epsg, dlocation=None,
                                    metadata=lyr_meta,
                                    data_format=LMFormat.SHAPE.driver,
                                    val_attribute=event_field,
                                    mod_time=curr_time)
                                updated_lyr = self.scribe.find_or_insert_layer(
                                    lyr)

                                # Get dlocation
                                # Loop through files to write all matching
                                #    (ext) to out location
                                base_out = os.path.splitext(
                                    updated_lyr.get_dlocation())[0]

                                for ext in LMFormat.SHAPE.get_extensions():
                                    z_fn = '{}{}'.format(hyp_filename, ext)
                                    out_fn = '{}{}'.format(base_out, ext)
                                    if z_fn in avail_files:
                                        zip_f.extract(z_fn, out_fn)

                                # Add it to the list of files to be encoded
                                encoder.encode_biogeographic_hypothesis(
                                    updated_lyr.get_dlocation(), column_name,
                                    min_coverage, event_field=event_field)
                            else:
                                raise cherrypy.HTTPError(
                                    HTTPStatus.BAD_REQUEST,
                                    '{} missing from package'.format(
                                        hyp_filename))

                # Create biogeo matrix
                # Add the matrix to contain biogeo hypotheses layer
                #    intersections
                meta = {
                    ServiceObject.META_DESCRIPTION.lower():
                        'Biogeographic Hypotheses from package {}'.format(
                            package_name),
                    ServiceObject.META_KEYWORDS.lower(): [
                        'biogeographic hypotheses'
                    ]
                }

                tmp_mtx = LMMatrix(
                    None, matrix_type=MatrixType.BIOGEO_HYPOTHESES,
                    process_type=ProcessType.ENCODE_HYPOTHESES,
                    user_id=self.get_user_id(), gridset=gridset, metadata=meta,
                    status=JobStatus.INITIALIZE, status_mod_time=curr_time)
                bg_mtx = self.scribe.find_or_insert_matrix(tmp_mtx)

                # Encode the hypotheses
                enc_mtx = encoder.get_encoded_matrix()
                enc_mtx.write(bg_mtx.get_dlocation())

                # We'll return the newly inserted biogeo matrix
                ret = [bg_mtx]
            else:
                raise cherrypy.HTTPError(
                    HTTPStatus.NOT_FOUND,
                    'Biogeography package: {} was not found'.format(
                        package_name))
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Cannot add hypotheses with reference type: {}'.format(
                    ref_obj[BG_REF_TYPE_KEY]))

        # Return resulting list of matrices
        return ret

    # ................................
    def _get_gridset(self, path_gridset_id):
        """Attempts to get a GridSet
        """
        gridset = self.scribe.get_gridset(
            gridset_id=path_gridset_id, fill_matrices=True)
        if gridset is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'GridSet {} was not found'.format(path_gridset_id))
        if check_user_permission(self.get_user_id(), gridset, HTTPMethod.GET):
            return gridset

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access GridSet {}'.format(
                self.get_user_id(), path_gridset_id))

    # ................................
    def _get_user_dir(self):
        """Get the user's workspace directory

        Todo:
            Change this to use something at a lower level.  This is using the
                same path construction as the getBoomPackage script
        """
        return os.path.join(
            ARCHIVE_PATH, self.get_user_id(), 'uploads', 'biogeo')


# .............................................................................
@cherrypy.expose
class GridsetProgressService(LmService):
    """Service class for gridset progress
    """

    # ................................
    @lm_formatter
    def GET(self, path_gridset_id, detail=False, **params):
        """Get progress for a gridset
        """
        return ('gridset', path_gridset_id, detail)


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_tree_id')
class GridsetTreeService(LmService):
    """Service for the tree of a gridset
    """

    # ................................
    def DELETE(self, path_tree_id):
        """Attempts to delete a tree

        Args:
            path_tree_id: The id of the tree to delete
        """
        tree = self.scribe.get_tree(tree_id=path_tree_id)

        if tree is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Tree {} not found'.format(path_tree_id))

        # If allowed to, delete
        if check_user_permission(self.get_user_id(), tree, HTTPMethod.DELETE):
            success = self.scribe.delete_object(tree)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            # TODO: How can this happen?  Make sure we catch those cases and
            #             respond appropriately.  We don't want 500 errors
            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                'Failed to delete tree')

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User does not have permission to delete this tree')

    # ................................
    @lm_formatter
    def GET(self, path_gridset_id, path_tree_id=None, include_csv=None,
            include_sdms=None, **params):
        """Just return the gridset tree, no listing at this time
        """
        gridset = self._get_gridset(path_gridset_id)
        return gridset.tree

    # ................................
    @lm_formatter
    def POST(self, path_gridset_id, path_tree_id=None, name=None,
             tree_schema=DEFAULT_TREE_SCHEMA, **params):
        """Posts a new tree and adds it to the gridset
        """
        if path_tree_id is not None:
            tree = self.scribe.get_tree(tree_id=path_tree_id)
            if tree is None:
                raise cherrypy.HTTPError(
                    HTTPStatus.NOT_FOUND,
                    'Tree {} was not found'.format(path_tree_id))
            if check_user_permission(self.get_user_id(), tree, HTTPMethod.GET):
                pass
            else:
                # Raise exception if user does not have permission
                raise cherrypy.HTTPError(
                    HTTPStatus.FORBIDDEN,
                    'User {} cannot access tree {}'.format(
                        self.get_user_id(), path_tree_id))
        else:
            if name is None:
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST,
                    'Must provide name for tree')
            tree = dendropy.Tree.get(
                file=cherrypy.request.body, schema=tree_schema)
            new_tree = Tree(name, user_id=self.get_user_id())
            updated_tree = self.scribe.find_or_insert_tree(new_tree)
            updated_tree.set_tree(tree)
            updated_tree.write_tree()
            updated_tree.mod_time = gmt().mjd
            self.scribe.update_object(updated_tree)

        gridset = self._get_gridset(path_gridset_id)
        gridset.add_tree(tree)
        gridset.update_mod_time(gmt().mjd)
        self.scribe.update_object(gridset)

        return updated_tree

    # ................................
    def _get_gridset(self, path_gridset_id):
        """Attempt to get a Gridset
        """
        gridset = self.scribe.get_gridset(
            gridset_id=path_gridset_id, fill_matrices=True)
        if gridset is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'GridSet {} was not found'.format(path_gridset_id))
        if check_user_permission(self.get_user_id(), gridset, HTTPMethod.GET):
            return gridset

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access GridSet {}'.format(
                self.get_user_id(), path_gridset_id))


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('path_gridset_id')
class GridsetService(LmService):
    """Class for gridset services
    """
    analysis = GridsetAnalysisService()
    biogeo = GridsetBioGeoService()
    matrix = MatrixService()
    progress = GridsetProgressService()
    tree = GridsetTreeService()

    # ................................
    def DELETE(self, path_gridset_id):
        """Attempts to delete a grid set

        Args:
            path_gridset_id: The id of the grid set to delete
        """
        gridset = self.scribe.get_gridset(gridset_id=path_gridset_id)

        if gridset is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, "Grid set not found")

        # If allowed to, delete
        if check_user_permission(
                self.get_user_id(), gridset, HTTPMethod.DELETE):
            success = self.scribe.delete_object(gridset)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            # TODO: How can this happen?  Make sure we catch those cases and
            #             respond appropriately.  We don't want 500 errors
            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "Failed to delete grid set")

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            "User does not have permission to delete this grid set")

    # ................................
    @lm_formatter
    def GET(self, path_gridset_id=None, after_time=None, before_time=None,
            epsg_code=None, limit=100, meta_string=None, offset=0,
            url_user=None, shapegrid_id=None, **params):
        """Perform a GET request, either return gridset, list, or count.
        """
        if path_gridset_id is None:
            return self._list_gridsets(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, epsg=epsg_code, limit=limit,
                meta_string=meta_string, offset=offset,
                shapegrid_id=shapegrid_id)
        if path_gridset_id.lower() == 'count':
            return self._count_gridsets(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, epsg=epsg_code,
                meta_string=meta_string, shapegrid_id=shapegrid_id)

        return self._get_gridset(path_gridset_id)

    # ................................
    def HEAD(self, path_gridset_id=None):
        """Perform a HTTP HEAD request to get general status
        """
        if path_gridset_id is not None:
            mf_summary = self.scribe.summarize_mf_chains_for_gridset(
                path_gridset_id)
            (waiting_mfs, running_mfs, _, _, _) = summarize_object_statuses(
                mf_summary)
            if waiting_mfs + running_mfs == 0:
                cherrypy.response.status = HTTPStatus.OK
            else:
                cherrypy.response.status = HTTPStatus.ACCEPTED
        else:
            cherrypy.response.status = HTTPStatus.OK

    # ................................
    def POST(self, **params):
        """Posts a new grid set
        """
        gridset_data = json.loads(cherrypy.request.body.read())

        usr = self.scribe.find_user(self.get_user_id())

        boom_post = BoomPoster(
            usr.user_id, usr.email, gridset_data, self.scribe)
        gridset = boom_post.init_boom()

        # TODO: What do we return?
        cherrypy.response.status = HTTPStatus.ACCEPTED
        return Atom(
            gridset.get_id(), gridset.name, gridset.metadata_url,
            gridset.mod_time, epsg=gridset.epsg_code)

    # ................................
    def _count_gridsets(self, user_id, after_time=None, before_time=None,
                        epsg=None, meta_string=None, shapegrid_id=None):
        """Count GridSet objects matching the specified criteria

        Args:
            user_id: The user to count GridSets for.  Note that this may not
                be the same user logged into the system
            after_time: (optional) Return GridSets modified after this time
                (Modified Julian Day)
            before_time: (optional) Return GridSets modified before this time
                (Modified Julian Day)
            epsg: (optional) Return GridSets with this EPSG code
        """
        gridset_count = self.scribe.count_gridsets(
            user_id=user_id, shapegrid_layer_id=shapegrid_id,
            meta_string=meta_string, after_time=after_time,
            before_time=before_time, epsg=epsg)
        # Format return
        # Set headers
        return {'count': gridset_count}

    # ................................
    def _get_gridset(self, gridset_id):
        """Attempt to get a GridSet
        """
        gridset = self.scribe.get_gridset(
            gridset_id=gridset_id, fill_matrices=True)
        if gridset is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'Gridset {} was not found'.format(gridset_id))
        if check_user_permission(self.get_user_id(), gridset, HTTPMethod.GET):
            return gridset

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access Gridset {}'.format(
                self.get_user_id(), gridset_id))

    # ................................
    def _list_gridsets(self, user_id, after_time=None, before_time=None,
                       epsg=None, limit=100, meta_string=None, offset=0,
                       shapegrid_id=None):
        """Count GridSet objects matching the specified criteria

        Args:
            user_id: The user to count GridSets for.  Note that this may not be
                the same user logged into the system
            after_time: (optional) Return GridSets modified after this time
                (Modified Julian Day)
            before_time: (optional) Return GridSets modified before this time
                (Modified Julian Day)
            epsg: (optional) Return GridSets with this EPSG code
            limit: (optional) Return this number of GridSets, at most
            offset: (optional) Offset the returned GridSets by this number
        """
        gridset_atoms = self.scribe.list_gridsets(
            offset, limit, user_id=user_id, shapegrid_layer_id=shapegrid_id,
            meta_string=meta_string, after_time=after_time,
            before_time=before_time, epsg=epsg)

        return gridset_atoms

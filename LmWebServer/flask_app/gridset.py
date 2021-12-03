"""This module provides REST services for grid sets"""
import dendropy
from flask import Response, make_response
from http import HTTPStatus
import os
import werkzeug.exceptions as WEXC
import zipfile

from lmpy import Matrix

from LmCommon.common.lmconstants import (
    DEFAULT_TREE_SCHEMA, JobStatus, LMFormat, MatrixType, ProcessType)
from LmCommon.common.time import gmt
from LmCommon.encoding.layer_encoder import LayerEncoder

from LmDbServer.boom.boom_collate import BoomCollate

from LmServer.base.atom import Atom
from LmServer.base.layer import Vector
from LmServer.base.service_object import ServiceObject
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.tree import Tree

from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.flask_app.base import LmService
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
def get_gridset(user_id, gridset_id):
    """Attempts to get a GridSet"""
    gridset = self.scribe.get_gridset(
        gridset_id=gridset_id, fill_matrices=True)
    
    if gridset is None:
        raise WEXC.NotFound('GridSet {} was not found'.format(gridset_id))
    
    if check_user_permission(user_id, gridset, HTTPMethod.GET):
        return gridset

    raise WEXC.Forbidden('User {} does not have permission to access GridSet {}'.format(
            user_id, gridset_id))

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
class GridsetAnalysisService(LmService):
    """This class is for the service representing gridset analyses.

    Todo:
        * Enable DELETE?  Could remove all existing analysis matrices
        * Enable GET?  Could this just be the outputs?
    """

    # ................................
    @lm_formatter
    def request_analysis(self, gridset_id, do_mcpa=False, num_permutations=500, do_calc=False, **params):
        """Adds a set of biogeographic hypotheses to the gridset"""
        # Get gridset
        gridset = self.get_gridset(gridset_id)

        # Check status of all matrices
        if not all([mtx.status == JobStatus.COMPLETE for mtx in gridset.get_matrices()]):
            raise WEXC.Conflict(
                'The gridset is not ready for analysis. All matrices must be complete')

        if do_mcpa:
            mcpa_possible = (
                len(gridset.get_biogeographic_hypotheses()) > 0 and gridset.tree is not None)
            if not mcpa_possible:
                raise WEXC.Conflict(
                    'The gridset must have a tree and biogeographic hypotheses to perform MCPA')

        # If everything is ready and we have analyses to run, do so
        if do_mcpa or do_calc:
            boom_col = BoomCollate(
                gridset, do_pam_stats=do_calc, do_mcpa=do_mcpa,
                num_permutations=num_permutations)
            boom_col.create_workflow()
            boom_col.close()

            return make_response(gridset, HTTPStatus.ACCEPTED)
        else:
            raise WEXC.BadRequest('Must specify at least one analysis to perform')


# .............................................................................
class GridsetBioGeoService(LmService):
    """Service class for gridset biogeographic hypotheses"""

    # ................................
    @lm_formatter
    def get_biogeo_hypotheses(self, user_id, gridset_id, biogeo_id=None, **params):
        """There is not a true service for limiting the biogeographic
               hypothesis matrices in a gridset, but return all when listing
        """
        gridset = get_gridset(user_id, gridset_id)
        bg_hyps = gridset.get_biogeographic_hypotheses()

        if biogeo_id is None:
            return bg_hyps

        for hyp in bg_hyps:
            if hyp.get_id() == biogeo_id:
                return hyp

        # If not found 404...
        raise WEXC.NotFound(
            'Biogeographic hypothesis mtx {} not found for gridset {}'.format(biogeo_id, gridset_id))
        
    # ................................
    @lm_formatter
    def _encode_insert_biogeo(self, zip_f, hyp_lyr, gridset, encoder):
        curr_time = gmt().mjd
        min_coverage = 0.25
        hyp_filename = hyp_lyr[FILE_NAME_KEY]
        # Check to see if file is in zip package
        if HYPOTHESIS_NAME_KEY in hyp_lyr:
            hyp_name = hyp_lyr[HYPOTHESIS_NAME_KEY]
        else:
            hyp_name = os.path.splitext(os.path.basename(hyp_filename))[0]
    
        if EVENT_FIELD_KEY in hyp_lyr:
            event_field = hyp_lyr[EVENT_FIELD_KEY]
            column_name = '{} - {}'.format(
                hyp_name, event_field)
        else:
            event_field = None
            column_name = hyp_name
    
        int_param_val_key = MatrixColumn.INTERSECT_PARAM_VAL_NAME
        lyr_meta = {
            'name': hyp_name,
            int_param_val_key.lower(): event_field,
            ServiceObject.META_DESCRIPTION.lower():
                'Biogeographic hypotheses based on layer {}'.format(hyp_filename),
            ServiceObject.META_KEYWORDS.lower(): ['biogeographic hypothesis']
        }
    
        if KEYWORD_KEY in hyp_lyr:
            lyr_meta[ServiceObject.META_KEYWORDS.lower()].extend(hyp_lyr[KEYWORD_KEY])
    
        lyr = Vector(
            hyp_name, gridset.get_user_id(), gridset.epsg, dlocation=None, metadata=lyr_meta,
            data_format=LMFormat.SHAPE.driver, val_attribute=event_field, mod_time=curr_time)
        updated_lyr = self.scribe.find_or_insert_layer(lyr)
    
        # Loop through files to write all matching
        #    (ext) to out location
        base_out = os.path.splitext(updated_lyr.get_dlocation())[0]
    
        for ext in LMFormat.SHAPE.get_extensions():
            z_fn = '{}{}'.format(hyp_filename, ext)
            out_fn = '{}{}'.format(base_out, ext)
            if z_fn in zip_f.namelist():
                zip_f.extract(z_fn, out_fn)
    
        # Add it to the list of files to be encoded
        encoder.encode_biogeographic_hypothesis(
            updated_lyr.get_dlocation(), column_name, min_coverage, event_field=event_field)

                
    # ................................
    @lm_formatter
    def post_biogeo_hypotheses(self, user_id, gridset_id, biogeo_data, **params):
        """Adds a set of biogeographic hypotheses to the gridset"""
        # Get gridset
        gridset = get_gridset(user_id, gridset_id)
        # Check reference to get file
        hypothesis_reference_obj = biogeo_data[BG_REF_KEY]
        # If gridset,
        if hypothesis_reference_obj[BG_REF_TYPE_KEY].lower() == 'gridset':
            # copy hypotheses from gridset
            try:
                ref_gridset_id = int(hypothesis_reference_obj[BG_REF_ID_KEY])
            except Exception:
                # Probably not an integer or something
                raise WEXC.BadRequest('Cannot get gridset for reference identfier {}'.format(
                    hypothesis_reference_obj[BG_REF_ID_KEY]))
                
            ref_gridset = get_gridset(user_id, ref_gridset_id)

            # Get hypotheses from other gridset
            ret = []
            for bg_hyp in ref_gridset.get_biogeographic_hypotheses():
                new_bg_mtx = LMMatrix(
                    None, matrix_type=MatrixType.BIOGEO_HYPOTHESES, process_type=ProcessType.ENCODE_HYPOTHESES,
                    gcm_code=bg_hyp.gcm_code, alt_pred_code=bg_hyp.alt_pred_code, date_code=bg_hyp.date_code, 
                    metadata=bg_hyp.mtx_metadata, user_id=user_id, gridset=gridset, status=JobStatus.INITIALIZE)
                
                inserted_bg = self.scribe.find_or_insert_matrix(new_bg_mtx)
                inserted_bg.update_status(JobStatus.COMPLETE)
                self.scribe.update_object(inserted_bg)
                # Save the original grim data into the new location
                bg_mtx = Matrix.load(bg_hyp.get_dlocation())
                bg_mtx.write(inserted_bg.get_dlocation())
                ret.append(inserted_bg)
                
        elif hypothesis_reference_obj[BG_REF_TYPE_KEY].lower() == 'upload':
            curr_time = gmt().mjd
            # # Check for uploaded biogeo package
            package_name = hypothesis_reference_obj[BG_REF_ID_KEY]
            package_filename = os.path.join(self.get_user_dir(), '{}{}'.format(package_name, LMFormat.ZIP.ext))
            #
            encoder = LayerEncoder(gridset.get_shapegrid().get_dlocation())
            self._encode_insert_biogeo(gridset, hypothesis_reference_obj, encoder, package_filename)
            # TODO(CJ): Pull this from config somewhere
            
            
            if os.path.exists(package_filename):
                with open(package_filename) as in_f:
                    with zipfile.ZipFile(in_f, allowZip64=True) as zip_f:
                        # Get file names in package
                        avail_files = zip_f.namelist()
            
                        for hyp_lyr in hypothesis_reference_obj[LAYERS_KEY]:
                            hyp_filename = hyp_lyr[FILE_NAME_KEY]
                            self._encode_insert_biogeo(hyp_lyr, gridset, hypothesis_reference_obj, encoder, package_filename)
                            
                            # Check to see if file is in zip package
                            if not (
                                hyp_filename in avail_files or '{}{}'.format(hyp_filename, LMFormat.SHAPE.ext) in avail_files):
                                raise WEXC.BadRequest('{} missing from package'.format(hyp_filename))
                            else:
                                self._encode_insert_biogeo(zip_f, hyp_lyr, gridset, encoder)

                # Create biogeo matrix
                # Add the matrix to contain biogeo hypotheses layer
                #    intersections
                meta = {
                    ServiceObject.META_DESCRIPTION.lower():
                        'Biogeographic Hypotheses from package {}'.format(package_name),
                    ServiceObject.META_KEYWORDS.lower(): ['biogeographic hypotheses']}

                tmp_mtx = LMMatrix(
                    None, matrix_type=MatrixType.BIOGEO_HYPOTHESES, process_type=ProcessType.ENCODE_HYPOTHESES,
                    user_id=user_id, gridset=gridset, metadata=meta, status=JobStatus.INITIALIZE, 
                    status_mod_time=curr_time)
                bg_mtx = self.scribe.find_or_insert_matrix(tmp_mtx)

                # Encode the hypotheses
                enc_mtx = encoder.get_encoded_matrix()
                enc_mtx.write(bg_mtx.get_dlocation())

                # We'll return the newly inserted biogeo matrix
                ret = [bg_mtx]
        else:
            raise WEXC.BadRequest('Cannot add hypotheses with reference type: {}'.format(
                    hypothesis_reference_obj[BG_REF_TYPE_KEY]))

        # Return resulting list of matrices
        return ret


# .............................................................................
class GridsetProgressService(LmService):
    """Service class for gridset progress"""

    # ................................
    @lm_formatter
    def get_gridset_progress(self, gridset_id, detail=False, **params):
        """Get progress for a gridset"""
        return ('gridset', gridset_id, detail)


# .............................................................................
class GridsetTreeService(LmService):
    """Service for the tree of a gridset"""

    # ................................
    def delete_tree(self, user_id, tree_id):
        """Attempts to delete a tree

        Args:
            path_tree_id: The id of the tree to delete
        """
        tree = self.scribe.get_tree(tree_id=tree_id)

        if tree is None:
            raise WEXC.NotFound('Tree {} not found'.format(tree_id))

        # If allowed to, delete
        if check_user_permission(user_id, tree, HTTPMethod.DELETE):
            success = self.scribe.delete_object(tree)
            if success:
                return Response(status=HTTPStatus.NO_CONTENT)

            # TODO: How can this happen?  Catch and respond appropriately, avoid 500 errors
            else:
                raise WEXC.InternalServerError('Failed to delete tree')
        
        else:
            raise WEXC.Forbidden('User does not have permission to delete this tree')

    # ................................
    @lm_formatter
    def get_tree(self, user_id, gridset_id, tree_id=None, include_csv=None, include_sdms=None, **params):
        """Just return the gridset tree, no listing at this time
        
        TODO: remove unused args.  How is this called?
        """
        gridset = get_gridset(user_id, gridset_id)
        return gridset.tree

    # ................................
    @lm_formatter
    def post_tree(self, user_id, gridset_id, tree_id=None, name=None, tree_data=None, tree_schema=DEFAULT_TREE_SCHEMA, **params):
        """Posts a new tree and adds it to the gridset
        
        Note: Calling function in routes.py should retrieve tree_data with:
            tree_data = request.get_json()
        """
        if tree_id is not None:
            tree = self.scribe.get_tree(tree_id=tree_id)
            
            if tree is None:
                raise WEXC.NotFound('Tree {} was not found'.format(tree_id))
            
            if not check_user_permission(user_id, tree, HTTPMethod.GET):
                pass
            
            else:
                # Raise exception if user does not have permission
                raise WEXC.Forbidden('User {} cannot access tree {}'.format(user_id, tree_id))
            
        else:
            if name is None:
                raise WEXC.BadRequest('Must provide name for tree')
            
            tree = dendropy.Tree.get(file=tree_data, schema=tree_schema)
            new_tree = Tree(name, user_id=user_id)
            updated_tree = self.scribe.find_or_insert_tree(new_tree)
            updated_tree.set_tree(tree)
            updated_tree.write_tree()
            updated_tree.mod_time = gmt().mjd
            self.scribe.update_object(updated_tree)

        gridset = get_gridset(user_id, gridset_id)
        gridset.add_tree(tree)
        gridset.update_mod_time(gmt().mjd)
        self.scribe.update_object(gridset)

        return updated_tree


# .............................................................................
class GridsetService(LmService):
    """Class for gridset services"""
    analysis = GridsetAnalysisService()
    biogeo = GridsetBioGeoService()
    matrix = MatrixService()
    progress = GridsetProgressService()
    tree = GridsetTreeService()

    # ................................
    def delete_gridset(self, user_id, gridset_id):
        """Attempts to delete a grid set

        Args:
            user_id (str): The user authorized for this operation.
            gridset_id (int): The id of the grid set to delete
        """
        gridset = self.scribe.get_gridset(gridset_id=gridset_id)

        if gridset is None:
            raise WEXC.NotFound('Gridset {} not found'.format(gridset_id))

        # If allowed to, delete
        if check_user_permission(user_id, gridset, HTTPMethod.DELETE):
            success = self.scribe.delete_object(gridset)
            if success:
                return Response(status=HTTPStatus.NO_CONTENT)

            # TODO: How can this happen?  Make sure we catch those cases and
            #             respond appropriately.  We don't want 500 errors
            raise WEXC.InternalServerError('Failed to delete grid set')

        raise WEXC.Forbidden('User does not have permission to delete this grid set')


    # ................................
    def get_gridset_makeflow_status(self, gridset_id=None):
        """Perform a HTTP HEAD request to get general status"""
        if gridset_id is not None:
            mf_summary = self.scribe.summarize_mf_chains_for_gridset(gridset_id)
            (waiting_mfs, running_mfs, _, _, _) = summarize_object_statuses(mf_summary)
            if waiting_mfs + running_mfs == 0:
                response = Response(status=HTTPStatus.OK)
            else:
                response = Response(status=HTTPStatus.ACCEPTED)
        else:
            response = Response(status=HTTPStatus.OK)
        return response

    # ................................
    @lm_formatter
    def post_boom_data(self, user_id, user_email, gridset_data, **params):
        """Posts a new grid set"""
        boom_post = BoomPoster(user_id, user_email, gridset_data, self.scribe)
        gridset = boom_post.init_boom()

        # Return atom of posted gridset
        return Atom(
            gridset.get_id(), gridset.name, gridset.metadata_url,
            gridset.mod_time, epsg=gridset.epsg_code)

    # ................................
    @lm_formatter
    def count_gridsets(
            self, user_id, after_time=None, before_time=None, epsg_code=None, meta_string=None, 
            shapegrid_id=None):
        """Count GridSet objects matching the specified criteria

        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            after_time (float): Time in MJD of the earliest modtime for filtering
            before_time (float): Time in MJD of the latest modtime for filtering
            epsg_code (str): EPSG code for the SRS for filtering layers
        """
        gridset_count = self.scribe.count_gridsets(
            user_id=user_id, shapegrid_layer_id=shapegrid_id, meta_string=meta_string, 
            after_time=after_time, before_time=before_time, epsg=epsg_code)
        return {'count': gridset_count}

    # ................................
    @lm_formatter
    def get_gridset(self, user_id, gridset_id):
        """Attempt to get a GridSet
        """
        gridset = self.scribe.get_gridset(gridset_id=gridset_id, fill_matrices=True)
        if gridset is None:
            raise WEXC.NotFound('Gridset {} was not found'.format(gridset_id))
        
        if check_user_permission(user_id, gridset, HTTPMethod.GET):
            return gridset

        raise WEXC.Forbidden('User {} does not have permission to access Gridset {}'.format(
            user_id, gridset_id))

    # ................................
    @lm_formatter
    def list_gridsets(
        self, user_id, after_time=None, before_time=None, epsg_code=None, meta_string=None, 
        shapegrid_id=None, limit=100, offset=0):
        """List GridSet objects matching the specified criteria

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
            before_time=before_time, epsg=epsg_code)

        return gridset_atoms

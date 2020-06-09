"""This module provides functionality for subsetting a gridset using Solr

Todo:
    Clean up code and rewrite where necessary
"""

import os

import numpy as np
from osgeo import ogr

from LmBackend.command.server import IndexPAVCommand, StockpileCommand
from LmBackend.command.single import IntersectRasterCommand, GrimRasterCommand
from LmCommon.common.lmconstants import (
    JobStatus, LMFormat, MatrixType, ProcessType)
from LmCommon.common.time import gmt
from LmCommon.compression.binary_list import decompress
from LmCommon.encoding.layer_encoder import LayerEncoder
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import SOLR_FIELDS, SubsetMethod, Priority
from LmServer.common.log import WebLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.gridset import Gridset
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.process_chain import MFChain
from LmServer.legion.shapegrid import Shapegrid
from LmServer.legion.tree import Tree
from lmpy import Matrix


# .............................................................................
def subset_global_pam(archive_name, matches, user_id, bbox=None,
                      cell_size=None, scribe=None):
    """Subset the global PAM and use that to create a new gridset.

    Args:
        archive_name (str): The name of the new gridset.
        matches: Solr hits to be used for subsetting
        user_id (str): The user that will own the new gridset

    Todo:
        Rewrite and use multi pav index.  then get rid of IndexPAVCommand
    """
    method = SubsetMethod.COLUMN

    if scribe is None:
        log = WebLogger()
        scribe = BorgScribe(log)
    else:
        log = scribe.log

    # Get metadata
    match_1 = matches[0]
    orig_shp = scribe.get_shapegrid(match_1[SOLR_FIELDS.SHAPEGRID_ID])
    orig_num_rows = orig_shp.feature_count
    epsg = match_1[SOLR_FIELDS.EPSG_CODE]
    orig_gs_id = match_1[SOLR_FIELDS.GRIDSET_ID]
    orig_gs = scribe.get_gridset(gridset_id=orig_gs_id, fill_matrices=True)

    # Initialize variables we'll test
    if bbox is None:
        bbox = orig_shp.bbox
    if cell_size is None:
        cell_size = orig_shp.cell_size

    # TODO: Add these to function so they can be specified by user
    intersect_params = {
        MatrixColumn.INTERSECT_PARAM_FILTER_STRING: None,
        MatrixColumn.INTERSECT_PARAM_VAL_NAME: 'pixel',
        MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: 1,
        MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: 255,
        MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: 25
    }

    orig_row_headers = get_row_headers(orig_shp.get_dlocation())

    # If bounding box, resolution, or user is different, create a new shapegrid
    if bbox != orig_shp.bbox or cell_size != orig_shp.cell_size or \
            user_id != orig_gs.get_user_id():

        my_sg_name = 'Shapegrid_{}_{}'.format(
            str(bbox).replace(' ', '_'), cell_size)
        new_shp = Shapegrid(
            my_sg_name, user_id, orig_gs.epsg_code, orig_shp.cell_sides,
            cell_size, orig_shp.map_units, bbox, status=JobStatus.INITIALIZE,
            status_mod_time=gmt().mjd)

        # Insert our new shapegrid
        my_shp = scribe.find_or_insert_shapegrid(new_shp)

        # Determine how to create the data files
        # ----------------
        # If the cell size is different or the bounding box is not completely
        #     within the original, we need to reintersect
        if cell_size != orig_shp.cell_size or bbox[0] < orig_shp.bbox[0] or \
                bbox[1] < orig_shp.bbox[1] or bbox[2] > orig_shp.bbox[2] or \
                bbox[3] > orig_shp.bbox[3]:
            method = SubsetMethod.REINTERSECT

            # Write shapefile
            my_shp.buildShape()
            my_shp.write_shapefile(my_shp.get_dlocation())

        # Else, if the bounding box is different, we need to spatially subset
        elif bbox != orig_shp.bbox:
            method = SubsetMethod.SPATIAL
            bbox_wkt = \
                'POLYGON(({0} {1},{0} {3},{2} {3},{2} {1},{0} {1}))'.format(
                    *bbox)
            orig_shp.cutout(bbox_wkt, dloc=my_shp.get_dlocation())

            row_headers = get_row_headers(my_shp.get_dlocation())
            orig_row_headers = get_row_headers(orig_shp.get_dlocation())

            keep_sites = []
            for i, hdr in enumerate(orig_row_headers):
                if hdr in row_headers:
                    keep_sites.append(i)

        # Else, we can just subset the PAM columns
        else:
            method = SubsetMethod.COLUMN

            row_headers = get_row_headers(orig_shp.get_dlocation())

            # Copy original shapegrid to new location
            orig_shp.write_shapefile(my_shp.get_dlocation())

    else:
        # We can use the original shapegrid
        my_shp = orig_shp

    gs_meta = {
        ServiceObject.META_DESCRIPTION:
            'Subset of Global PAM, gridset {}'.format(orig_gs_id),
        ServiceObject.META_KEYWORDS: ['subset']
    }

    # TODO: This really needs to be by original matrix (PAM) id
    #    For now, we can get close if we group by scenario id and algorithm
    #    Another option would be to hash a set of the algorithm parameter
    #    values that will split into groups of like algorithms

    # Create a dictionary of matches by scenario and algorithm
    match_groups = {}
    for match in matches:
        scn_id = match[SOLR_FIELDS.PROJ_SCENARIO_ID]
        alg_code = match[SOLR_FIELDS.ALGORITHM_CODE]

        if scn_id not in match_groups:
            match_groups[scn_id] = {}

        if alg_code not in match_groups[scn_id]:
            match_groups[scn_id][alg_code] = []

        match_groups[scn_id][alg_code].append(match)

    # Create grid set
    gridset = Gridset(
        name=archive_name, metadata=gs_meta, shapegrid=my_shp, epsg_code=epsg,
        user_id=user_id, mod_time=gmt().mjd)
    updated_gs = scribe.find_or_insert_gridset(gridset)

    # Copy the tree if available.  It may be subsetted according to the data in
    #     the gridset and therefore should be separate
    if orig_gs.get_tree_object() is not None:

        # Need to get and fill in tree
        otree = orig_gs.get_tree_object()

        try:
            tree_data = otree.get_tree_object()
        except Exception:  # May need to be read
            try:
                otree.read()
                tree_data = otree.get_tree_object()
            # TODO: Remove this
            except Exception:  # Handle bad dlocation from gridset tree
                otree = scribe.get_tree(tree_id=otree.get_id())
                otree.read()
                tree_data = otree.get_tree_object()

        if otree.name:
            tree_name = otree.name
        else:
            tree_name = otree.get_id()
        new_tree = Tree(
            'Copy of {} tree at {}'.format(tree_name, gmt().mjd), metadata={},
            user_id=user_id, gridset_id=updated_gs.get_id(),
            mod_time=gmt().mjd)
        # Set the data to populate boolean attributes
        new_tree.set_tree(tree_data)
        inserted_tree = scribe.find_or_insert_tree(new_tree)
        # Set the data to write file
        inserted_tree.set_tree(tree_data)
        inserted_tree.write_tree()
        updated_gs.add_tree(inserted_tree, do_read=True)
        log.debug(
            'Tree for gridset {} is {}'.format(
                updated_gs.get_id(), updated_gs.get_tree_object().get_id()))
        scribe.update_object(updated_gs)

    # If we can reuse data from Solr index, do it
    if method in [SubsetMethod.COLUMN, SubsetMethod.SPATIAL]:
        # PAMs
        # --------
        for scn_id in list(match_groups.keys()):
            for alg_code, mtx_matches in match_groups[scn_id].items():

                scn_code = mtx_matches[0][SOLR_FIELDS.PROJ_SCENARIO_CODE]
                date_code = alt_pred_code = gcm_code = None

                if SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE in mtx_matches[0]:
                    date_code = mtx_matches[0][
                        SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE]
                if SOLR_FIELDS.PROJ_SCENARIO_GCM in mtx_matches[0]:
                    gcm_code = mtx_matches[0][SOLR_FIELDS.PROJ_SCENARIO_GCM]
                if SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE in mtx_matches[0]:
                    alt_pred_code = mtx_matches[0][
                        SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE]

                scn_meta = {
                    ServiceObject.META_DESCRIPTION:
                        'Subset of grid set {}, scn {}, algorithm {}'.format(
                            orig_gs_id, scn_id, alg_code),
                    ServiceObject.META_KEYWORDS: ['subset', scn_code, alg_code]
                }

                # Assemble full matrix
                pam_data = np.zeros(
                    (orig_num_rows, len(mtx_matches)), dtype=int)
                squids = []

                for i, match_i in enumerate(mtx_matches):
                    pam_data[:, i] = decompress(
                        match_i[SOLR_FIELDS.COMPRESSED_PAV])
                    squids.append(match_i[SOLR_FIELDS.SQUID])

                # Create object
                # NOTE: We use original row headers even though we are going to
                #    slice them immediately after construction.  We do this to
                #    keep consistent with other matrix slicing
                pam_mtx = LMMatrix(
                    pam_data, matrix_type=MatrixType.PAM, gcm_code=gcm_code,
                    alt_pred_code=alt_pred_code, date_code=date_code,
                    metadata=scn_meta, user_id=user_id, gridset=updated_gs,
                    status=JobStatus.GENERAL, status_mod_time=gmt().mjd,
                    headers={'0': orig_row_headers, '1': squids})

                # If we need to spatially subset, slice the matrix
                if method == SubsetMethod.SPATIAL:
                    tmp = pam_mtx.matrix.slice(keep_sites)
                    pam_mtx.set_data(tmp, headers=tmp.headers)

                # Insert it into db
                updated_pam_mtx = scribe.find_or_insert_matrix(pam_mtx)
                updated_pam_mtx.update_status(JobStatus.COMPLETE)
                scribe.update_object(updated_pam_mtx)
                log.debug(
                    'Dlocation for updated pam: {}'.format(
                        updated_pam_mtx.get_dlocation()))
                pam_mtx.matrix.write(updated_pam_mtx.get_dlocation())

        # GRIMs
        # --------
        for grim in orig_gs.getGRIMs():
            new_grim = LMMatrix(
                None, matrix_type=MatrixType.GRIM,
                process_type=ProcessType.RAD_INTERSECT, gcm_code=grim.gcm_code,
                alt_pred_code=grim.alt_pred_code, date_code=grim.date_code,
                metadata=grim.matrix_metadata, user_id=user_id,
                gridset=updated_gs, status=JobStatus.INITIALIZE)
            inserted_grim = scribe.find_or_insert_matrix(new_grim)
            grim_metadata = grim.matrix_metadata
            grim_metadata['keywords'].append('subset')
            grim_metadata['description'] = 'Subset of GRIM {}'.format(
                grim.get_id())

            if 'keywords' in grim.matrix_metadata:
                grim_metadata['keywords'].extend(
                    grim.matrix_metadata['keywords'])

            inserted_grim.update_status(
                JobStatus.COMPLETE, metadata=grim_metadata)
            scribe.update_object(inserted_grim)
            # Save the original grim data into the new location
            # TODO: Add read / load method for LMMatrix
            grim_mtx = Matrix.load(grim.get_dlocation())

            # If we need to spatially subset, slice the matrix
            if method == SubsetMethod.SPATIAL:
                grim_mtx = grim_mtx.slice(keep_sites)

            grim_mtx.write(inserted_grim.get_dlocation())

        # BioGeo
        # --------
        for orig_bg in orig_gs.getBiogeographicHypotheses():
            new_bg = LMMatrix(
                None, matrix_type=MatrixType.BIOGEO_HYPOTHESES,
                process_type=ProcessType.ENCODE_HYPOTHESES,
                gcm_code=orig_bg.gcm_code, alt_pred_code=orig_bg.alt_pred_code,
                date_code=orig_bg.date_code, metadata=orig_bg.matrix_metadata,
                user_id=user_id, gridset=updated_gs,
                status=JobStatus.INITIALIZE)
            inserted_bg = scribe.find_or_insert_matrix(new_bg)
            inserted_bg.update_status(JobStatus.COMPLETE)
            scribe.update_object(inserted_bg)
            # Save the original grim data into the new location
            # TODO: Add read / load method for LMMatrix
            bg_mtx = Matrix.load(orig_bg.get_dlocation())

            # If we need to spatially subset, slice the matrix
            if method == SubsetMethod.SPATIAL:
                bg_mtx = bg_mtx.slice(keep_sites)

            bg_mtx.write(inserted_bg.get_dlocation())

    else:
        # Reintersect everything

        # Create a new Makeflow object
        wf_meta = {
            MFChain.META_CREATED_BY: os.path.basename(__file__),
            MFChain.META_DESCRIPTION:
                'Subset makeflow.  Orig gridset: {}, new gridset: {}'.format(
                    orig_gs.get_id(), updated_gs.get_id())
        }
        new_wf = MFChain(
            user_id, priority=Priority.REQUESTED, metadata=wf_meta,
            status=JobStatus.GENERAL, status_mod_time=gmt().mjd)

        my_wf = scribe.insertMFChain(new_wf, updated_gs.get_id())
        rules = []

        work_dir = my_wf.getRelativeDirectory()

        # TODO: Make this asynchronous
        # Add shapegrid to workflow
        my_shp.buildShape()
        my_shp.write_shapefile(my_shp.get_dlocation())
        my_shp.update_status(JobStatus.COMPLETE)
        scribe.update_object(my_shp)

        # PAMs
        # --------
        for scn_id in list(match_groups.keys()):
            for alg_code, mtx_matches in match_groups[scn_id].items():

                scn_code = mtx_matches[0][SOLR_FIELDS.PROJ_SCENARIO_CODE]
                date_code = alt_pred_code = gcm_code = None

                if SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE in mtx_matches[0]:
                    date_code = mtx_matches[0][
                        SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE]
                if SOLR_FIELDS.PROJ_SCENARIO_GCM in mtx_matches[0]:
                    gcm_code = mtx_matches[0][SOLR_FIELDS.PROJ_SCENARIO_GCM]
                if SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE in mtx_matches[0]:
                    alt_pred_code = mtx_matches[0][
                        SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE]

                scn_meta = {
                    ServiceObject.META_DESCRIPTION:
                        'Subset of grid set {}, scen {}, algorithm {}'.format(
                            orig_gs_id, scn_id, alg_code),
                    ServiceObject.META_KEYWORDS: ['subset', scn_code, alg_code]
                }

                # TODO: Get intersect parameters from Solr or user provided

                # Create PAM
                pam_mtx = LMMatrix(
                    None, matrix_type=MatrixType.PAM, gcm_code=gcm_code,
                    alt_pred_code=alt_pred_code, date_code=date_code,
                    metadata=scn_meta, user_id=user_id, gridset=updated_gs,
                    status=JobStatus.GENERAL, status_mod_time=gmt().mjd)
                pam = scribe.find_or_insert_matrix(pam_mtx)

                # Insert matrix columns for each match
                for i, mtx_match in enumerate(mtx_matches):
                    try:
                        prj = scribe.get_sdm_project(
                            int(mtx_match[SOLR_FIELDS.PROJ_ID]))
                    except Exception as err:
                        prj = None
                        log.debug(
                            'Could not add projection {}: {}'.format(
                                mtx_matches[i][SOLR_FIELDS.PROJ_ID], str(err)))

                    if prj is not None:
                        prj_meta = {}  # TODO: Add something here?

                        tmp_col = MatrixColumn(
                            None, pam.get_id(), user_id, layer=prj,
                            shapegrid=my_shp,
                            intersect_params=intersect_params,
                            squid=prj.squid, ident=prj.ident,
                            process_type=ProcessType.INTERSECT_RASTER,
                            metadata=prj_meta, matrix_column_id=None,
                            post_to_solr=True, status=JobStatus.GENERAL,
                            status_mod_time=gmt().mjd)
                        mtx_col = scribe.find_or_insert_matrix_column(tmp_col)

                        # Need to intersect this projection with the new
                        #    shapegrid, stockpile it, and post to solr
                        pav_fname = os.path.join(
                            work_dir, 'pav_{}.lmm'.format(mtx_col.get_id()))
                        pav_post_fname = os.path.join(
                            work_dir, 'pav_{}_post.xml'.format(
                                mtx_col.get_id()))
                        pav_success_fname = os.path.join(
                            work_dir, 'pav_{}.success'.format(
                                mtx_col.get_id()))

                        # Intersect parameters
                        min_presence = mtx_col.intersect_params[
                            MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE]
                        max_presence = mtx_col.intersect_params[
                            MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE]
                        min_percent = mtx_col.intersect_params[
                            MatrixColumn.INTERSECT_PARAM_MIN_PERCENT]

                        intersect_cmd = IntersectRasterCommand(
                            my_shp.get_dlocation(), prj.get_dlocation(),
                            pav_fname, min_presence, max_presence, min_percent,
                            squid=prj.squid)
                        index_cmd = IndexPAVCommand(
                            pav_fname, mtx_col.get_id(), prj.get_id(),
                            pam.get_id(), pav_post_fname)
                        stockpile_cmd = StockpileCommand(
                            ProcessType.INTERSECT_RASTER, mtx_col.get_id(),
                            pav_success_fname, [pav_fname])
                        # Add rules to list
                        rules.append(intersect_cmd.get_makeflow_rule())
                        rules.append(index_cmd.get_makeflow_rule(local=True))
                        rules.append(
                            stockpile_cmd.get_makeflow_rule(local=True))

                # Initialize PAM after matrix columns inserted
                pam.update_status(JobStatus.INITIALIZE)
                scribe.update_object(pam)

        # GRIMs
        # --------
        for grim in orig_gs.getGRIMs():
            new_grim = LMMatrix(
                None, matrix_type=MatrixType.GRIM,
                process_type=ProcessType.RAD_INTERSECT, gcm_code=grim.gcm_code,
                alt_pred_code=grim.alt_pred_code, date_code=grim.date_code,
                metadata=grim.matrix_metadata, user_id=user_id,
                gridset=updated_gs, status=JobStatus.INITIALIZE)
            inserted_grim = scribe.find_or_insert_matrix(new_grim)
            grim_metadata = grim.matrix_metadata
            grim_metadata['keywords'].append('subset')
            grim_metadata = {
                'keywords': ['subset'],
                'description': 'Reintersection of GRIM {}'.format(
                    grim.get_id())
            }
            if 'keywords' in grim.matrix_metadata:
                grim_metadata['keywords'].extend(
                    grim.matrix_metadata['keywords'])

            # Get corresponding grim layers (scenario)
            old_grim_cols = scribe.get_columns_for_matrix(grim.get_id())

            for old_col in old_grim_cols:
                # TODO: Metadata
                grim_lyr_meta = {}
                tmp_col = MatrixColumn(
                    None, inserted_grim.get_id(), user_id, layer=old_col.layer,
                    shapegrid=my_shp,
                    intersect_params=old_col.intersect_params,
                    squid=old_col.squid, ident=old_col.ident,
                    process_type=ProcessType.INTERSECT_RASTER_GRIM,
                    metadata=grim_lyr_meta, matrix_column_id=None,
                    post_to_solr=False, status=JobStatus.GENERAL,
                    status_mod_time=gmt().mjd)
                mtx_col = scribe.find_or_insert_matrix_column(tmp_col)

                # Add rules to workflow, intersect and stockpile
                grim_col_fname = os.path.join(
                    work_dir, 'grim_col_{}.lmm'.format(mtx_col.get_id()))
                grim_col_success_fname = os.path.join(
                    work_dir, 'grim_col_{}.success'.format(mtx_col.get_id()))
                min_percent = mtx_col.intersect_params[
                    MatrixColumn.INTERSECT_PARAM_MIN_PERCENT]
                intersect_cmd = GrimRasterCommand(
                    my_shp.get_dlocation(), old_col.layer.get_dlocation(),
                    grim_col_fname, minPercent=min_percent,
                    ident=mtx_col.ident)
                stockpile_cmd = StockpileCommand(
                    ProcessType.INTERSECT_RASTER_GRIM, mtx_col.get_id(),
                    grim_col_success_fname, [grim_col_fname])
                # Add rules to list
                rules.append(intersect_cmd.get_makeflow_rule())
                rules.append(stockpile_cmd.get_makeflow_rule(local=True))

            inserted_grim.update_status(JobStatus.INITIALIZE)
            scribe.update_object(inserted_grim)

        # BioGeo
        # --------
        for orig_bg in orig_gs.getBiogeographicHypotheses():
            new_bg = LMMatrix(
                None, matrix_type=MatrixType.BIOGEO_HYPOTHESES,
                process_type=ProcessType.ENCODE_HYPOTHESES,
                gcm_code=orig_bg.gcm_code, alt_pred_code=orig_bg.alt_pred_code,
                date_code=orig_bg.date_code, metadata=orig_bg.matrix_metadata,
                user_id=user_id, gridset=updated_gs,
                status=JobStatus.INITIALIZE)
            inserted_bg = scribe.find_or_insert_matrix(new_bg)
            mtx_cols = []
            old_cols = scribe.get_columns_for_matrix(orig_bg.get_id())

            encoder = LayerEncoder(my_shp.get_dlocation())
            # TODO(CJ): This should be pulled from a default config or the
            #     database or somewhere
            min_coverage = .25
            # Do this for each layer because we need to have the layer object
            #     do create a matrix column
            for old_col in old_cols:
                lyr = old_col.layer

                try:
                    val_attribute = old_col.layer.layer_metadata[
                        MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
                except KeyError:
                    val_attribute = None

                new_cols = encoder.encode_biogeographic_hypothesis(
                    lyr.get_dlocation(), lyr.name, min_coverage,
                    event_field=val_attribute)

                for col in new_cols:
                    if val_attribute is not None:
                        int_params = {
                            MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower():
                                val_attribute,
                            # TODO(CJ): Pick a better name if we need this
                            MatrixColumn.INTERSECT_PARAM_VAL_VALUE.lower():
                                lyr.name
                        }
                    else:
                        int_params = None

                    metadata = {
                        ServiceObject.META_DESCRIPTION.lower(): (
                            'Encoded Helmert contrasts using the Lifemapper'
                            ' bioGeoContrasts module'),
                        ServiceObject.META_TITLE.lower():
                            'Biogeographic hypothesis column ({})'.format(col)
                    }
                    mtx_col = MatrixColumn(
                        len(mtx_cols), inserted_bg.get_id(), user_id,
                        layer=lyr, shapegrid=my_shp,
                        intersect_params=int_params, metadata=metadata,
                        post_to_solr=False, status=JobStatus.COMPLETE,
                        status_mod_time=gmt().mjd)
                    updated_mc = scribe.find_or_insert_matrix_column(mtx_col)
                    mtx_cols.append(updated_mc)

            enc_mtx = encoder.get_encoded_matrix()
            # Write matrix
            # TODO(CJ): Evaluate if this is how we want to do it
            inserted_bg = enc_mtx
            inserted_bg.set_headers(enc_mtx.get_headers())
            inserted_bg.write(overwrite=True)
            inserted_bg.update_status(JobStatus.COMPLETE, mod_time=gmt().mjd)
            scribe.update_object(inserted_bg)

        # Write workflow and update db object
        my_wf.add_commands(rules)
        my_wf.write()
        my_wf.update_status(JobStatus.INITIALIZE)
        scribe.update_object(my_wf)

    return updated_gs


# ............................................................................
def get_row_headers(shapefile_filename):
    """Get a (sorted by feature id) list of row headers for a shapefile

    Todo:
        Move this to LmCommon or LmBackend and use with LmCompute.  This is a
            rough copy of what is now used for rasterIntersect
    """
    ogr.RegisterAll()
    drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
    dataset = drv.Open(shapefile_filename)
    lyr = dataset.GetLayer(0)

    row_headers = []

    for j in range(lyr.GetFeatureCount()):
        cur_feat = lyr.GetFeature(j)
        site_idx = cur_feat.GetFID()
        x_coord, y_coord = cur_feat.geometry().Centroid().GetPoint_2D()
        row_headers.append((site_idx, x_coord, y_coord))

    return sorted(row_headers)

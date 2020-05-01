"""Add a tree and biogeographic hypotheses to a grid set

Todo:
     How to specify multiple hypotheses with different event fields?

"""
import argparse
import logging
import os
import sys
import time

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (
    BoomKeys, ENCODING, JobStatus, LM_USER, MatrixType, ProcessType,
    SERVER_BOOM_HEADING)
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt
from LmCommon.encoding.layer_encoder import LayerEncoder
from LmServer.base.service_object import ServiceObject
from LmServer.base.utilities import is_lm_user
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.localconstants import DEFAULT_EPSG
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn


# .................................
def _get_biogeo_matrix(scribe, usr, gridset, success_fname, layers=None):
    """Get a biogeographic hypotheses matrix
    """
    if layers is None:
        layers = []
    # Create the encoding data
    bg_mtx = None
    try:
        bg_mtx_list = gridset.get_biogeographic_hypotheses()
    except Exception:
        print('No gridset for hypotheses')
    # TODO: There should be only one?!?
    if len(bg_mtx_list) > 0:
        bg_mtx = bg_mtx_list[0]
    else:
        mtx_keywords = ['biogeographic hypotheses']
        for lyr in layers:
            kwds = []
            try:
                kwds = lyr.metadata[ServiceObject.META_KEYWORDS]
            except Exception:
                kwds = []
            mtx_keywords.extend(kwds)
        # Add the matrix to contain biogeo hypotheses layer intersections
        meta = {
            ServiceObject.META_DESCRIPTION.lower():
                'Biogeographic Hypotheses for archive {}'.format(gridset.name),
            ServiceObject.META_KEYWORDS.lower(): mtx_keywords}
        tmp_mtx = LMMatrix(
            None, matrix_type=MatrixType.BIOGEO_HYPOTHESES,
            process_type=ProcessType.ENCODE_HYPOTHESES, user_id=usr,
            gridset=gridset, metadata=meta, status=JobStatus.INITIALIZE,
            status_mod_time=gmt().mjd)
        bg_mtx = scribe.find_or_insert_matrix(tmp_mtx)
        if bg_mtx is None:
            scribe.log.info('  Failed to add biogeo hypotheses matrix')
    return bg_mtx


# .................................
def encode_hypotheses_to_matrix(scribe, usr, gridset, layers=None):
    """Encoding hypotheses to a BioGeo matrix

    Args:
        scribe: An open BorgScribe object connected to the database
        usr: Userid for these data
        gridset: Gridset object for this tree data
        layers: A list of (layer object, event field) tuples.  Event field
            may be None

    Note:
        This adds to existing encoded hypotheses
    """
    if layers is None:
        layers = []
    mtx_cols = []
    # Find or create the matrix
    bg_mtx = _get_biogeo_matrix(scribe, usr, gridset, layers)
    shapegrid = gridset.get_shapegrid()
    encoder = LayerEncoder(shapegrid.get_dlocation())

    # TODO(CJ): Minimum coverage should be pulled from config or database
    min_coverage = 0.25

    for lyr in layers:
        try:
            val_attribute = lyr.layer_metadata[
                MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
            column_name = val_attribute
        except KeyError:
            val_attribute = None
            column_name = lyr.name
        new_cols = encoder.encode_biogeographic_hypothesis(
            lyr.get_dlocation(), column_name, min_coverage,
            event_field=val_attribute)
        print((
            'layer name={}, eventField={}, dloc={}'.format(
                lyr.name, val_attribute, lyr.get_dlocation())))

        # Add matrix columns for the newly encoded layers
        for col_name in new_cols:
            # TODO: Fill in params and metadata
            try:
                ef_value = col_name.split(' - ')[1]
            except Exception:
                ef_value = col_name

            if val_attribute is not None:
                int_params = {
                    MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower():
                        val_attribute,
                    MatrixColumn.INTERSECT_PARAM_VAL_VALUE.lower():
                        ef_value
                    }
            else:
                int_params = None
            metadata = {
                ServiceObject.META_DESCRIPTION.lower():
                    ('Encoded Helmert contrasts using the Lifemapper '
                     'bioGeoContrasts module'),
                ServiceObject.META_TITLE.lower():
                    'Biogeographic hypothesis column ({})'.format(col_name)}
            mtx_col = MatrixColumn(
                len(mtx_cols), bg_mtx.get_id(), usr, layer=lyr,
                shapegrid=shapegrid, intersect_params=int_params,
                metadata=metadata, post_to_solr=False,
                status=JobStatus.COMPLETE, status_mod_time=gmt().mjd)
            updated_mc = scribe.find_or_insert_matrix_column(mtx_col)
            mtx_cols.append(updated_mc)

        enc_mtx = encoder.get_encoded_matrix()

        bg_mtx.set_data(enc_mtx, headers=enc_mtx.get_headers())

    # Save matrix and update record
    bg_mtx.write(overwrite=True)
    bg_mtx.update_status(JobStatus.COMPLETE, mod_time=gmt().mjd)
    _ = scribe.update_object(bg_mtx)
    return bg_mtx


# .............................................................................
def _get_boom_biogeo_params(scribe, grid_name, usr):
    epsg = DEFAULT_EPSG
    layers = []
    earl = EarlJr()
    config_fname = earl.create_filename(
        LMFileType.BOOM_CONFIG, obj_code=grid_name, usr=usr)
    if config_fname is not None and os.path.exists(config_fname):
        cfg = Config(site_fn=config_fname)
    else:
        raise Exception('Missing config file {}'.format(config_fname))

    try:
        epsg = cfg.get(SERVER_BOOM_HEADING, BoomKeys.EPSG)
    except Exception:
        pass

    try:
        var = cfg.get(SERVER_BOOM_HEADING, BoomKeys.BIOGEO_HYPOTHESES_LAYERS)
    except Exception:
        raise Exception('No configured Biogeographic Hypotheses layers')
    else:
        # May be one or more
        lyr_name_list = [v.strip() for v in var.split(',')]
        for lname in lyr_name_list:
            layers.append(
                scribe.get_layer(user_id=usr, lyr_name=lname, epsg=epsg))
    return layers


# ...............................................
def _write_success_file(message, successFname):
    ready_filename(successFname, overwrite=True)
    with open(successFname, 'w', encoding=ENCODING) as in_file:
        in_file.write(message)


# .............................................................................
def main():
    """Main method for script
    """
    if not is_lm_user():
        print(("Run this script as '{}'".format(LM_USER)))
        sys.exit(2)

    parser = argparse.ArgumentParser(
        description="Encode biogeographic hypotheses into a matrix")

    # Required
    parser.add_argument(
        'user_id', type=str, help=('User owner of the tree'))
    parser.add_argument(
        'gridset_name', type=str,
        help="Gridset name for encoding Biogeographic Hypotheses")
    parser.add_argument(
        'success_file', default=None,
        help=('Filename to be written on successful completion of script.'))
    # Optional
    parser.add_argument(
        '--logname', type=str, default=None,
        help=('Basename of the logfile, without extension'))

    args = parser.parse_args()
    usr = args.user_id
    grid_name = args.gridset_name
    success_file = args.success_file
    log_name = args.logname

    if log_name is None:
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = '{}'.format(
            time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        log_name = '{}.{}'.format(scriptname, timestamp)

    logger = ScriptLogger(log_name, level=logging.INFO)
    scribe = BorgScribe(logger)
    try:
        scribe.open_connections()
        layers = _get_boom_biogeo_params(scribe, grid_name, usr)
        gridset = scribe.get_gridset(
            user_id=usr, name=grid_name, fill_matrices=True)
        if gridset and layers:
            encode_hypotheses_to_matrix(
                scribe, usr, gridset, layers=layers)
        else:
            scribe.log.info('No gridset or layers to encode as hypotheses')
    finally:
        scribe.close_connections()


# .............................................................................
if __name__ == '__main__':
    main()

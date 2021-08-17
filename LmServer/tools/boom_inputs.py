"""Add a tree and biogeographic hypotheses to a grid set

@todo: How to specify multiple hypotheses with different event fields?
"""
import argparse
import os
import sys

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (
    LM_USER, JobStatus, PhyloTreeKeys, MatrixType, ProcessType, SERVER_BOOM_HEADING, BoomKeys)
from LmCommon.common.time import gmt
from LmCommon.encoding.layer_encoder import LayerEncoder
from LmServer.base.service_object import ServiceObject
from LmServer.base.utilities import is_lm_user
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.localconstants import DEFAULT_EPSG
from LmServer.common.log import ConsoleLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.tree import Tree


# .................................
def _get_biogeo_matrix(scribe, usr, gridset, layers=None):
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

        bg_mtx.data = enc_mtx.data
        bg_mtx.setHeaders(enc_mtx.get_headers())

    # Save matrix and update record
    bg_mtx.write(overwrite=True)
    bg_mtx.update_status(JobStatus.COMPLETE, mod_time=gmt().mjd)
    _ = scribe.update_object(bg_mtx)
    return bg_mtx


# .................................
def squidify_tree(scribe, usr, tree):
    """Annotate a tree with squids and node ids, then write to disk

    Args:
        scribe: An open BorgScribe object connected to the database
        usr: The user that owns this data
        tree: Tree object

    Note:
        Matching species must be present in the taxon table of the database
    """
    squid_dict = {}
    shrub = tree.get_tree_object()
    for label in shrub.get_labels():
        # TODO: Do we always need to do this?
        tax_label = label.replace(' ', '_')
        sno = scribe.get_taxon(user_id=usr, taxon_name=tax_label)
        if sno is not None:
            squid_dict[label] = sno.squid

    shrub.annotate_tree(PhyloTreeKeys.SQUID, squid_dict)

    print("Adding interior node labels to tree")
    # Add node labels
    shrub.add_node_labels()

    # Update tree properties
    tree.clear_dlocation()
    tree.set_dlocation()
    tree.set_tree(shrub)
    print("Write tree to final location")
    tree.write_tree()
    tree.update_mod_time(gmt().mjd)
    _ = scribe.update_object(tree)
    return tree


# .................................
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


# .............................................................................
def main():
    """Main method for script
    """
    if not is_lm_user():
        print(("Run this script as '{}'".format(LM_USER)))
        sys.exit(2)

    parser = argparse.ArgumentParser(
        description="Annotate a tree with squids and node ids")

    parser.add_argument(
        '-u', '--user', type=str, help="User name")
    parser.add_argument(
        '-g', '--gridset_name', type=str,
        help="Gridset name for encoding Biogeographic Hypotheses")
    parser.add_argument(
        '-t', '--tree_name', type=str,
        help="Tree name for squid, node annotation")

    args = parser.parse_args()
    usr = args.user

    scribe = BorgScribe(ConsoleLogger())
    scribe.open_connections()
    if args.gridset_name is not None:
        layers = _get_boom_biogeo_params(scribe, args.gridset_name, usr)
        gridset = scribe.get_gridset(
            user_id=usr, name=args.gridset_name, fill_matrices=True)
        if gridset and layers:
            encode_hypotheses_to_matrix(scribe, usr, gridset, layers=layers)
        else:
            print('No gridset or layers to encode as hypotheses')

    if args.tree_name:
        bare_tree = Tree(args.tree_name, user_id=args.user)
        tree = scribe.get_tree(tree=bare_tree)
        _ = squidify_tree(scribe, usr, tree)

    scribe.close_connections()


# .............................................................................
if __name__ == '__main__':
    main()

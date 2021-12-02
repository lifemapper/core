"""Module functions for converting object to JSON

Todo:
    Use constants
"""
from hashlib import md5
import json

from LmCommon.common.lmconstants import LMFormat
from LmServer.base.atom import Atom
from LmServer.base.layer import Raster, Vector
from LmServer.base.utilities import format_time_human
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import OGC_SERVICE_URL, LMFileType
from LmServer.legion.env_layer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.occ_layer import OccurrenceLayer
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.sdm_proj import SDMProjection
from LmServer.legion.shapegrid import Shapegrid
from LmServer.legion.tree import Tree


# Format object method looks at object type and calls formatters appropriately
# Provide methods for direct calls to formatters
# .............................................................................
def format_atom(obj):
    """Format an Atom object into a dictionary"""
    return {
        'epsg': obj.epsg_code,
        'id': obj.get_id(),
        'modification_time': format_time_human(obj.mod_time),
        'name': obj.name,
        'url': obj.url
    }


# .............................................................................
def format_env_layer(lyr):
    """Convert an environmental layer into a dictionary

    Todo:
        * Mapping metadata
        * Min val
        * Max val
        * Value units
    """
    layer_dict = _get_lifemapper_metadata(
        'environmental layer', lyr.get_id(), lyr.metadata_url,
        lyr.get_user_id(), metadata=lyr.layer_metadata)
    data_url = lyr.get_data_url()
    min_val = lyr.min_val
    max_val = lyr.max_val
    val_units = lyr.val_units
    data_type = type(lyr.min_val).__name__
    layer_dict['spatial_raster'] = _get_spatial_raster_metadata(
        lyr.epsg_code, lyr.bbox, lyr.map_units, data_url, lyr.verify,
        lyr.gdal_type, lyr.data_format, min_val, max_val, val_units, data_type,
        resolution=lyr.resolution)
    layer_dict['env_code'] = lyr.env_code
    layer_dict['gcm_code'] = lyr.gcm_code
    layer_dict['alt_pred_code'] = lyr.alt_pred_code
    layer_dict['date_code'] = lyr.date_code

    return layer_dict


# .............................................................................
def format_gridset(gridset):
    """Convert a grid set to a dictionary
    """
    gridset_dict = _get_lifemapper_metadata(
        'gridset', gridset.get_id(), gridset.metadata_url,
        gridset.get_user_id(), metadata=gridset.grid_metadata)
    gridset_dict['epsg'] = gridset.epsg_code

    gridset_dict['biogeo_hypotheses'] = []
    gridset_dict['grims'] = []
    gridset_dict['pams'] = []
    gridset_dict['matrices'] = []

    # Bio geo hypotheses
    for mtx in gridset.get_biogeographic_hypotheses():
        gridset_dict['biogeo_hypotheses'].append(
            {
                'id': mtx.get_id(),
                'url': mtx.metadata_url
            }
        )

    # PAMs
    for mtx in gridset.get_all_pams():
        gridset_dict['pams'].append(
            {
                'id': mtx.get_id(),
                'url': mtx.metadata_url
            }
        )

    # GRIMs
    for mtx in gridset.get_grims():
        gridset_dict['grims'].append(
            {
                'id': mtx.get_id(),
                'url': mtx.metadata_url
            }
        )

    # All matrices
    for mtx in gridset.get_matrices():
        gridset_dict['matrices'].append(format_matrix(mtx))

    # Shapegrid
    gridset_dict['shapegrid_url'] = gridset.get_shapegrid().metadata_url
    gridset_dict['shapegrid_id'] = gridset.shapegrid_id

    # Tree
    if gridset.tree is not None:
        gridset_dict['tree'] = gridset.tree.metadata_url

    gridset_dict['name'] = gridset.name
    gridset_dict['mod_time'] = gridset.mod_time
    return gridset_dict


# .............................................................................
def format_matrix(mtx):
    """Convert a matrix object into a dictionary
    """
    matrix_dict = _get_lifemapper_metadata(
        'matrix', mtx.get_id(), mtx.metadata_url, mtx.get_user_id(),
        status=mtx.status, status_mod_time=mtx.status_mod_time,
        metadata=mtx.matrix_metadata)
    matrix_dict['alt_pred_code'] = mtx.alt_pred_code
    matrix_dict['date_code'] = mtx.date_code
    matrix_dict['gcm_code'] = mtx.gcm_code
    matrix_dict['data_url'] = mtx.get_data_url()
    matrix_dict['matrix_type'] = mtx.matrix_type
    matrix_dict['parent_metadata_url'] = mtx.parent_metadata_url
    matrix_dict['gridset_id'] = mtx.gridset_id
    matrix_dict['gridset_url'] = mtx.gridset_url
    matrix_dict['gridset_name'] = mtx.gridset_name

    return matrix_dict


# .............................................................................
def format_occurrence_set(occ):
    """Convert an Occurrence Set object to a dictionary

    Todo:
        * Mapping metadata
        * Taxon id
    """
    occ_dict = _get_lifemapper_metadata(
        'occurrence set', occ.get_id(), occ.metadata_url, occ.get_user_id(),
        status=occ.status, status_mod_time=occ.status_mod_time,
        metadata=occ.layer_metadata)
    map_name = EarlJr().create_basename(
        LMFileType.SDM_MAP, obj_code=occ.get_id(), usr=occ.get_user_id(),
        epsg=occ.epsg_code)
    occ_dict['map'] = _get_map_metadata(OGC_SERVICE_URL, map_name, occ.name)
    data_url = occ.get_data_url()
    occ_dict['spatial_vector'] = _get_spatial_vector_metadata(
        occ.epsg_code, occ.bbox, occ.map_units, data_url, occ.verify,
        occ.ogr_type, occ.data_format, occ.query_count,
        resolution=occ.resolution)
    occ_dict['species_name'] = occ.display_name
    occ_dict['squid'] = occ.squid
    if len(occ.features) > 0:
        occ_dict['features'] = [f.getAttributes() for f in occ.features]

    return occ_dict


# .............................................................................
def format_projection(prj):
    """Converts a projection object into a dictionary

    Todo:
        * Fix map ogc endpoint
        * Fix map name
        * Min value
        * Max value
        * Value units
        * Public algorithm parameters
        * Masks
        * Taxon id
        * Occurrence set metadata url
    """
    prj_dict = _get_lifemapper_metadata(
        'projection', prj.get_id(), prj.metadata_url, prj.get_user_id(),
        status=prj.status, status_mod_time=prj.status_mod_time,
        metadata=prj.layer_metadata)
    occ = prj.occ_layer
    map_name = EarlJr().create_basename(
        LMFileType.SDM_MAP, obj_code=occ.get_id(), usr=occ.get_user_id(),
        epsg=occ.epsg_code)
    prj_dict['map'] = _get_map_metadata(OGC_SERVICE_URL, map_name, prj.name)
    data_url = prj.get_data_url()
    min_val = 0
    max_val = 1
    val_units = 'prediction'
    prj_dict['spatial_raster'] = _get_spatial_raster_metadata(
        prj.epsg_code, prj.bbox, prj.map_units, data_url, prj.verify,
        prj.gdal_type, prj.data_format, min_val, max_val, val_units,
        prj.gdal_type, prj.resolution)

    prj_dict['algorithm'] = {
        'code': prj.algorithm_code,
        'parameters': prj._algorithm._parameters
    }

    prj_dict['model_scenario'] = {
        'code': prj.model_scenario.code,
        'id': prj.model_scenario.get_id(),
        'metadata_url': prj.model_scenario.metadata_url
    }

    prj_dict['projection_scenario'] = {
        'code': prj.proj_scenario.code,
        'id': prj.proj_scenario.get_id(),
        'metadata_url': prj.proj_scenario.metadata_url
    }

    prj_dict['species_name'] = prj.species_name
    prj_dict['squid'] = prj.squid
    prj_dict['occurrence_set'] = {
        'id': prj.get_occ_layer_id(),
        'metadata_url': prj.occ_layer.metadata_url
    }

    return prj_dict


# .............................................................................
def format_raster_layer(lyr):
    """Convert an environmental layer into a dictionary

    Todo:
        * Mapping metadata
        * Min val
        * Max val
        * Value units
    """
    layer_dict = _get_lifemapper_metadata(
        'raster layer', lyr.get_id(), lyr.metadata_url, lyr.get_user_id(),
        metadata=lyr.layer_metadata)
    data_url = lyr.get_data_url()
    min_val = lyr.min_val
    max_val = lyr.max_val
    val_units = lyr.val_units
    data_type = type(lyr.min_val).__name__
    layer_dict['spatial_raster'] = _get_spatial_raster_metadata(
        lyr.epsg_code, lyr.bbox, lyr.map_units, data_url, lyr.verify,
        lyr.gdal_type, lyr.data_format, min_val, max_val, val_units, data_type,
        resolution=lyr.resolution)

    return layer_dict


# .............................................................................
def format_scenario(scn):
    """Converts a scenario object into a dictionary

    Todo:
        * Fix map ogc endpoint
        * GCM / alt pred code / etc
    """
    scenario_dict = _get_lifemapper_metadata(
        'scenario', scn.get_id(), scn.metadata_url, scn.get_user_id(),
        metadata=scn.scen_metadata)
    map_name = EarlJr().create_basename(
        LMFileType.SCENARIO_MAP, obj_code=scn.code, usr=scn.get_user_id(),
        epsg=scn.epsg_code)
    scenario_dict['map'] = _get_map_metadata(
        OGC_SERVICE_URL, map_name, scn.layers)
    scenario_dict['spatial'] = _get_spatial_metadata(
        scn.epsg_code, scn.bbox, scn.map_units, scn.resolution)

    scn_layers = []
    for lyr in scn.layers:
        scn_layers.append(lyr.metadata_url)
    scenario_dict['layers'] = scn_layers
    scenario_dict['code'] = scn.code

    return scenario_dict


# .............................................................................
def format_scenario_package(scen_package):
    """Converts a scenario package object into a dictionary
    """
    scen_package_dict = _get_lifemapper_metadata(
        'scenario package', scen_package.get_id(), scen_package.metadata_url,
        scen_package.get_user_id(),
        metadata=scen_package.scen_package_metadata)
    scen_package_dict['name'] = scen_package.name
    scen_package_dict['scenarios'] = [
        format_scenario(scn) for (_, scn) in scen_package.scenarios.items()]
    return scen_package_dict


# .............................................................................
def format_shapegrid(shapegrid):
    """Convert a shapegrid into a dictionary
    """
    shapegrid_dict = _get_lifemapper_metadata(
        'shapegrid', shapegrid.get_id(), shapegrid.metadata_url,
        shapegrid.get_user_id(), status=shapegrid.status,
        status_mod_time=shapegrid.status_mod_time,
        metadata=shapegrid.layer_metadata)
    shapegrid_dict['spatial_vector'] = _get_spatial_vector_metadata(
        shapegrid.epsg_code, shapegrid.bbox, shapegrid.map_units,
        shapegrid.get_data_url(), shapegrid.verify, shapegrid.ogr_type,
        shapegrid.data_format, shapegrid.feature_count,
        resolution=shapegrid.resolution)
    shapegrid_dict['cell_sides'] = shapegrid.cell_sides
    shapegrid_dict['cell_size'] = shapegrid.cell_size

    return shapegrid_dict


# .............................................................................
def format_tree(tree):
    """Convert a tree into a dictionary

    Todo:
        * CJG - Add more tree metadata.  Check notes from Ryan conversation
    """
    tree_dict = _get_lifemapper_metadata(
        'tree', tree.get_id(), tree.metadata_url, tree.get_user_id(),
        metadata=tree.tree_metadata)
    shrub = tree.get_tree_object()
    tree_dict['ultrametric'] = shrub.is_ultrametric()
    tree_dict['binary'] = shrub.is_binary()
    return tree_dict


# .............................................................................
def format_vector(vector_layer):
    """Convert a vector into a dictionary
    """
    vector_dict = _get_lifemapper_metadata(
        'Vector Layer', vector_layer.get_id(), vector_layer.metadata_url,
        vector_layer.get_user_id(), metadata=vector_layer.layer_metadata)
    vector_dict['spatial_vector'] = _get_spatial_vector_metadata(
        vector_layer.epsg_code, vector_layer.bbox, vector_layer.map_units,
        vector_layer.get_data_url(), vector_layer.verify,
        vector_layer.ogr_type, vector_layer.data_format,
        vector_layer.feature_count)

    return vector_dict


# .............................................................................
def json_object_formatter(obj):
    """Looks at object and converts to JSON based on its type
    """
    if isinstance(obj, list):
        response = []
        for item in obj:
            response.append(_format_object(item))
    else:
        response = _format_object(obj)

    return json.dumps(response, indent=4)


# .............................................................................
def _format_object(obj):
    """Helper method to format an individual object based on its type"""
    # cherrypy.response.headers['Content-Type'] = LMFormat.JSON.get_mime_type()
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, Atom):
        return format_atom(obj)
    if isinstance(obj, SDMProjection):
        return format_projection(obj)
    if isinstance(obj, OccurrenceLayer):
        return format_occurrence_set(obj)
    if isinstance(obj, EnvLayer):
        return format_env_layer(obj)
    if isinstance(obj, Scenario):
        return format_scenario(obj)
    if isinstance(obj, ScenPackage):
        return format_scenario_package(obj)
    if isinstance(obj, Raster):
        return format_raster_layer(obj)
    if isinstance(obj, Gridset):
        return format_gridset(obj)
    if isinstance(obj, LMMatrix):
        return format_matrix(obj)
    if isinstance(obj, Shapegrid):
        return format_shapegrid(obj)
    if isinstance(obj, Tree):
        return format_tree(obj)
    if isinstance(obj, Vector):
        return format_vector(obj)

    # TODO: Expand these and maybe fallback to a generic formatter of public
    #    attributes
    raise TypeError("Cannot format object of type: {}".format(type(obj)))


# .............................................................................
def _get_lifemapper_metadata(object_type, lm_id, url, user_id, status=None,
                             status_mod_time=None, metadata=None):
    """Get general Lifemapper metadata that we want to return for each type
    """
    lm_dict = {
        'object_type': object_type,
        'id': lm_id,
        'url': url,
        'user': user_id
    }
    if status is not None:
        lm_dict['status'] = status
    if status_mod_time is not None:
        lm_dict['status_mod_time'] = format_time_human(status_mod_time)
        lm_dict['etag'] = md5(
            '{}-{}'.format(url, status_mod_time).encode()).hexdigest()
    if metadata is not None:
        lm_dict['metadata'] = metadata

    return lm_dict


# .............................................................................
def _get_map_metadata(base_url, map_name, layers):
    """Get a dictionary of mapping information

    Note:
        * This is very alpha.  We need to discuss exactly how we will implement
            maps going forward
    """
    map_dict = {
        'endpoint': base_url,
        'map_name': map_name
    }
    if isinstance(layers, list):
        lyrs = []
        for lyr in layers:
            lyrs.append({
                'metadata_url': lyr.metadata_url,
                'layer_name': lyr.name
            })
        map_dict['layers'] = lyrs
    else:
        map_dict['layer_name'] = layers
    return map_dict


# .............................................................................
def _get_spatial_metadata(epsg, bbox, map_units, resolution=None):
    """Get dictionary of spatial metadata

    Note:
        This can be expanded by the _getSpatialRasterMetadata and
            _get_spatial_vector_metadata functions if the object has a data
            file
    """
    spatial_dict = {
        'epsg': epsg,
        'bbox': bbox,
        'map_units': map_units
    }
    if resolution is not None:
        spatial_dict['resolution'] = resolution
    return spatial_dict


# .............................................................................
def _get_spatial_raster_metadata(epsg, bbox, map_units, data_url, sha256_val,
                                 gdal_type, data_format, min_val, max_val,
                                 val_units, data_type, resolution=None):
    """Return a dictionary of metadata about a spatial raster

    Todo:
        * Add file size, number of rows, number of columns?
    """
    spatial_raster_dict = _get_spatial_metadata(
        epsg, bbox, map_units, resolution=resolution)
    spatial_raster_dict['data_url'] = data_url
    spatial_raster_dict['sha256'] = sha256_val
    spatial_raster_dict['gdal_type'] = gdal_type
    spatial_raster_dict['data_format'] = data_format
    spatial_raster_dict['min_val'] = min_val
    spatial_raster_dict['max_val'] = max_val
    spatial_raster_dict['value_units'] = val_units
    spatial_raster_dict['data_type'] = data_type

    return spatial_raster_dict


# .............................................................................
def _get_spatial_vector_metadata(epsg, bbox, map_units, data_url, sha256_val,
                                 ogr_type, data_format, num_features,
                                 resolution=None):
    """Return a dictionary of metadata about a spatial vector

    Todo:
        * Add features?
        * Add file size?
    """
    spatial_vector_dict = _get_spatial_metadata(
        epsg, bbox, map_units, resolution=resolution)
    spatial_vector_dict['data_url'] = data_url
    spatial_vector_dict['sha256'] = sha256_val
    spatial_vector_dict['ogr_type'] = ogr_type
    spatial_vector_dict['data_format'] = data_format
    spatial_vector_dict['num_features'] = num_features

    return spatial_vector_dict

"""Module functions for converting object to JSON

Todo:
    Use constants
"""

from hashlib import md5
import json

import cherrypy

from LmCommon.common.lmconstants import LMFormat

from LmServer.base.atom import Atom
from LmServer.base.layer2 import Raster, Vector
from LmServer.base.utilities import formatTimeHuman
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import OGC_SERVICE_URL, LMFileType
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.sdmproj import SDMProjection
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree


# Format object method looks at object type and calls formatters appropriately
# Provide methods for direct calls to formatters
# .............................................................................
def format_atom(obj):
    """Format an Atom object into a dictionary
    """
    return {
        'epsg': obj.epsgcode,
        'id': obj.id,
        'modificationTime': formatTimeHuman(obj.modTime),
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
        'environmental layer', lyr.get_id(), lyr.metadataUrl, lyr.getUserId(),
        metadata=lyr.lyrMetadata)
    data_url = lyr.getDataUrl()
    min_val = lyr.minVal
    max_val = lyr.maxVal
    val_units = lyr.valUnits
    data_type = type(lyr.minVal).__name__
    layer_dict['spatialRaster'] = _get_spatial_raster_metadata(
        lyr.epsgcode, lyr.bbox, lyr.mapUnits, data_url, lyr.verify,
        lyr.gdalType, lyr.dataFormat, min_val, max_val, val_units, data_type,
        resolution=lyr.resolution)
    layer_dict['envCode'] = lyr.envCode
    layer_dict['gcmCode'] = lyr.gcmCode
    layer_dict['altPredCode'] = lyr.altpredCode
    layer_dict['dateCode'] = lyr.dateCode

    return layer_dict


# .............................................................................
def format_gridset(gridset):
    """Convert a grid set to a dictionary
    """
    gridset_dict = _get_lifemapper_metadata(
        'gridset', gridset.get_id(), gridset.metadataUrl, gridset.getUserId(),
        metadata=gridset.grdMetadata)
    gridset_dict['epsg'] = gridset.epsgcode

    gridset_dict['bioGeoHypotheses'] = []
    gridset_dict['grims'] = []
    gridset_dict['pams'] = []
    gridset_dict['matrices'] = []

    # Bio geo hypotheses
    for mtx in gridset.getBiogeographicHypotheses():
        gridset_dict['bioGeoHypotheses'].append(
            {
                'id': mtx.get_id(),
                'url': mtx.metadataUrl
            }
        )

    # PAMs
    for mtx in gridset.getAllPAMs():
        gridset_dict['pams'].append(
            {
                'id': mtx.get_id(),
                'url': mtx.metadataUrl
            }
        )

    # GRIMs
    for mtx in gridset.getGRIMs():
        gridset_dict['grims'].append(
            {
                'id': mtx.get_id(),
                'url': mtx.metadataUrl
            }
        )

    # All matrices
    for mtx in gridset._matrices:
        gridset_dict['matrices'].append(format_matrix(mtx))

    # Shapegrid
    gridset_dict['shapegridUrl'] = gridset.getShapegrid().metadataUrl
    gridset_dict['shapegridId'] = gridset.shapeGridId

    # Tree
    if gridset.tree is not None:
        gridset_dict['tree'] = gridset.tree.metadataUrl

    gridset_dict['name'] = gridset.name
    gridset_dict['modTime'] = gridset.modTime
    return gridset_dict


# .............................................................................
def format_matrix(mtx):
    """Convert a matrix object into a dictionary
    """
    matrix_dict = _get_lifemapper_metadata(
        'matrix', mtx.get_id(), mtx.metadataUrl, mtx.getUserId(),
        status=mtx.status, statusModTime=mtx.statusModTime,
        metadata=mtx.mtxMetadata)
    matrix_dict['altPredCode'] = mtx.altpredCode
    matrix_dict['dateCode'] = mtx.dateCode
    matrix_dict['gcmCode'] = mtx.gcmCode
    matrix_dict['dataUrl'] = mtx.getDataUrl()
    matrix_dict['matrixType'] = mtx.matrixType
    matrix_dict['parentMetadataUrl'] = mtx.parentMetadataUrl
    matrix_dict['gridsetId'] = mtx.gridsetId
    matrix_dict['gridsetUrl'] = mtx.gridsetUrl
    matrix_dict['gridsetName'] = mtx.gridsetName

    return matrix_dict


# .............................................................................
def format_occurrence_set(occ):
    """Convert an Occurrence Set object to a dictionary

    Todo:
        * Mapping metadata
        * Taxon id
    """
    occ_dict = _get_lifemapper_metadata(
        'occurrence set', occ.get_id(), occ.metadataUrl, occ.getUserId(),
        status=occ.status, statusModTime=occ.statusModTime,
        metadata=occ.lyrMetadata)
    map_name = EarlJr().createBasename(
        LMFileType.SDM_MAP, objCode=occ.get_id(), usr=occ.getUserId(),
        epsg=occ.epsgcode)
    occ_dict['map'] = _get_map_metadata(OGC_SERVICE_URL, map_name, occ.name)
    data_url = occ.getDataUrl()
    occ_dict['spatialVector'] = _get_spatial_vector_metadata(
        occ.epsgcode, occ.bbox, occ.mapUnits, data_url, occ.verify,
        occ.ogrType, occ.dataFormat, occ.queryCount, resolution=occ.resolution)
    occ_dict['speciesName'] = occ.displayName
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
        'projection', prj.get_id(), prj.metadataUrl, prj.getUserId(),
        status=prj.status, statusModTime=prj.statusModTime,
        metadata=prj.lyrMetadata)
    occ = prj._occurrenceSet
    map_name = EarlJr().createBasename(
        LMFileType.SDM_MAP, objCode=occ.get_id(), usr=occ.getUserId(),
        epsg=occ.epsgcode)
    prj_dict['map'] = _get_map_metadata(OGC_SERVICE_URL, map_name, prj.name)
    data_url = prj.getDataUrl()
    min_val = 0
    max_val = 1
    val_units = 'prediction'
    prj_dict['spatialRaster'] = _get_spatial_raster_metadata(
        prj.epsgcode, prj.bbox, prj.mapUnits, data_url, prj.verify,
        prj.gdalType, prj.dataFormat, min_val, max_val, val_units,
        prj.gdalType, prj.resolution)

    prj_dict['algorithm'] = {
        'code': prj.algorithmCode,
        'parameters': prj._algorithm._parameters
    }

    prj_dict['modelScenario'] = {
        'code': prj.modelScenario.code,
        'id': prj.modelScenario.get_id(),
        'metadataUrl': prj.modelScenario.metadataUrl
    }

    prj_dict['projectionScenario'] = {
        'code': prj.projScenario.code,
        'id': prj.projScenario.get_id(),
        'metadataUrl': prj.projScenario.metadataUrl
    }

    prj_dict['speciesName'] = prj.speciesName
    prj_dict['squid'] = prj.squid
    prj_dict['occurrenceSet'] = {
        'id': prj.getOccurrenceSetId(),
        'metadataUrl': prj._occurrenceSet.metadataUrl
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
        'raster layer', lyr.get_id(), lyr.metadataUrl, lyr.getUserId(),
        metadata=lyr.lyrMetadata)
    data_url = lyr.getDataUrl()
    min_val = lyr.minVal
    max_val = lyr.maxVal
    val_units = lyr.valUnits
    data_type = type(lyr.minVal).__name__
    layer_dict['spatialRaster'] = _get_spatial_raster_metadata(
        lyr.epsgcode, lyr.bbox, lyr.mapUnits, data_url, lyr.verify,
        lyr.gdalType, lyr.dataFormat, min_val, max_val, val_units, data_type,
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
        'scenario', scn.get_id(), scn.metadataUrl, scn.getUserId(),
        metadata=scn.scenMetadata)
    map_name = EarlJr().createBasename(
        LMFileType.SCENARIO_MAP, objCode=scn.code, usr=scn.getUserId(),
        epsg=scn.epsgcode)
    scenario_dict['map'] = _get_map_metadata(
        OGC_SERVICE_URL, map_name, scn.layers)
    scenario_dict['spatial'] = _get_spatial_metadata(
        scn.epsgcode, scn.bbox, scn.mapUnits, scn.resolution)

    scn_layers = []
    for lyr in scn.layers:
        scn_layers.append(lyr.metadataUrl)
    scenario_dict['layers'] = scn_layers
    scenario_dict['code'] = scn.code

    return scenario_dict


# .............................................................................
def format_scenario_package(scen_package):
    """Converts a scenario package object into a dictionary
    """
    scen_package_dict = _get_lifemapper_metadata(
        'scenario package', scen_package.get_id(), scen_package.metadataUrl,
        scen_package.getUserId(), metadata=scen_package.scenpkgMetadata)
    scen_package_dict['name'] = scen_package.name
    scen_package_dict['scenarios'] = [
        format_scenario(scn) for (_, scn) in scen_package.scenarios.items()]
    return scen_package_dict


# .............................................................................
def format_shapegrid(shapegrid):
    """Convert a shapegrid into a dictionary
    """
    shapegrid_dict = _get_lifemapper_metadata(
        'shapegrid', shapegrid.get_id(), shapegrid.metadataUrl,
        shapegrid.getUserId(), status=shapegrid.status,
        statusModTime=shapegrid.statusModTime, metadata=shapegrid.lyrMetadata)
    shapegrid_dict['spatialVector'] = _get_spatial_vector_metadata(
        shapegrid.epsgcode, shapegrid.bbox, shapegrid.mapUnits,
        shapegrid.getDataUrl(), shapegrid.verify, shapegrid.ogrType,
        shapegrid.dataFormat, shapegrid.featureCount,
        resolution=shapegrid.resolution)
    shapegrid_dict['cellSides'] = shapegrid.cellsides
    shapegrid_dict['cellSize'] = shapegrid.cellsize

    return shapegrid_dict


# .............................................................................
def format_tree(tree):
    """Convert a tree into a dictionary

    Todo:
        * CJG - Add more tree metadata.  Check notes from Ryan conversation
    """
    tree_dict = _get_lifemapper_metadata(
        'tree', tree.get_id(), tree.metadataUrl, tree.getUserId(),
        metadata=tree.treeMetadata)
    tree_dict['ultrametric'] = tree.isUltrametric()
    tree_dict['binary'] = tree.isBinary()
    return tree_dict


# .............................................................................
def format_vector(vector_layer):
    """Convert a vector into a dictionary
    """
    vector_dict = _get_lifemapper_metadata(
        'Vector Layer', vector_layer.get_id(), vector_layer.metadataUrl,
        vector_layer.getUserId(), metadata=vector_layer.lyrMetadata)
    vector_dict['spatialVector'] = _get_spatial_vector_metadata(
        vector_layer.epsgcode, vector_layer.bbox, vector_layer.mapUnits,
        vector_layer.getDataUrl(), vector_layer.verify, vector_layer.ogrType,
        vector_layer.dataFormat, vector_layer.featureCount)

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
    """Helper method to format an individual object based on its type
    """
    cherrypy.response.headers['Content-Type'] = LMFormat.JSON.getMimeType()
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
    if isinstance(obj, ShapeGrid):
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
        'objectType': object_type,
        'id': lm_id,
        'url': url,
        'user': user_id
    }
    if status is not None:
        lm_dict['status'] = status
    if status_mod_time is not None:
        lm_dict['statusModTime'] = formatTimeHuman(status_mod_time)
        lm_dict['etag'] = md5('{}-{}'.format(url, status_mod_time)).hexdigest()
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
        'mapName': map_name
    }
    if isinstance(layers, list):
        lyrs = []
        for lyr in layers:
            lyrs.append({
                'metadataUrl': lyr.metadataUrl,
                'layerName': lyr.name
            })
        map_dict['layers'] = lyrs
    else:
        map_dict['layerName'] = layers
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
        'mapUnits': map_units
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
    spatial_raster_dict['dataUrl'] = data_url
    spatial_raster_dict['sha256'] = sha256_val
    spatial_raster_dict['gdalType'] = gdal_type
    spatial_raster_dict['dataFormat'] = data_format
    spatial_raster_dict['minVal'] = min_val
    spatial_raster_dict['maxVal'] = max_val
    spatial_raster_dict['valueUnits'] = val_units
    spatial_raster_dict['dataType'] = data_type

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
    spatial_vector_dict['dataUrl'] = data_url
    spatial_vector_dict['sha256'] = sha256_val
    spatial_vector_dict['ogrType'] = ogr_type
    spatial_vector_dict['dataFormat'] = data_format
    spatial_vector_dict['numFeatures'] = num_features

    return spatial_vector_dict

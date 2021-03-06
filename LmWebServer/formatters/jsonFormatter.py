"""Module functions for converting object to JSON
@todo: Use constants
@todo: Can we make this more elegant?
"""

import cherrypy
from hashlib import md5
import json
from types import ListType, DictionaryType

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
def formatAtom(obj):
    """Format an Atom object into a dictionary
    """
    return {
        'epsg' : obj.epsgcode,
        'id' : obj.id,
        'modificationTime' : formatTimeHuman(obj.modTime),
        'name' : obj.name,
        'url' : obj.url
    }

# .............................................................................
def formatEnvLayer(lyr):
    """Convert an environmental layer into a dictionary

    Todo:
        * Mapping metadata
        * Min val
        * Max val
        * Value units
    """
    lyrDict = _getLifemapperMetadata(
        'environmental layer', lyr.getId(), lyr.metadataUrl, lyr.getUserId(),
        metadata=lyr.lyrMetadata)
    dataUrl = lyr.getDataUrl()
    minVal = lyr.minVal
    maxVal = lyr.maxVal
    valUnits = lyr.valUnits
    dataType = type(lyr.minVal).__name__
    lyrDict['spatialRaster'] = _getSpatialRasterMetadata(
        lyr.epsgcode, lyr.bbox, lyr.mapUnits, dataUrl, lyr.verify,
        lyr.gdalType, lyr.dataFormat, minVal, maxVal, valUnits, dataType,
        resolution=lyr.resolution)
    lyrDict['envCode'] = lyr.envCode
    lyrDict['gcmCode'] = lyr.gcmCode
    lyrDict['altPredCode'] = lyr.altpredCode
    lyrDict['dateCode'] = lyr.dateCode
    
    return lyrDict

# .............................................................................
def formatGridset(gs):
    """Convert a grid set to a dictionary
    """
    gsDict = _getLifemapperMetadata(
        'gridset', gs.getId(), gs.metadataUrl, gs.getUserId(),
        metadata=gs.grdMetadata)
    gsDict['epsg'] = gs.epsgcode
    
    gsDict['bioGeoHypotheses'] = []
    gsDict['grims'] = []
    gsDict['pams'] = []
    gsDict['matrices'] = []
    
    # Bio geo hypotheses
    for mtx in gs.getBiogeographicHypotheses():
        gsDict['bioGeoHypotheses'].append(
            {
                'id' : mtx.getId(),
                'url' : mtx.metadataUrl
            }
        )
        
    # PAMs
    for mtx in gs.getAllPAMs():
        gsDict['pams'].append(
            {
                'id' : mtx.getId(),
                'url' : mtx.metadataUrl
            }
        )

    # GRIMs
    for mtx in gs.getGRIMs():
        gsDict['grims'].append(
            {
                'id' : mtx.getId(),
                'url' : mtx.metadataUrl
            }
        )

    # All matrices
    for mtx in gs._matrices:
        gsDict['matrices'].append(formatMatrix(mtx))

    # Shapegrid
    gsDict['shapegridUrl'] = gs.getShapegrid().metadataUrl
    gsDict['shapegridId'] = gs.shapeGridId
    
    # Tree
    if gs.tree is not None:
        gsDict['tree'] = gs.tree.metadataUrl
    
    gsDict['name'] = gs.name
    gsDict['modTime'] = gs.modTime    
    return gsDict

# .............................................................................
def formatMatrix(mtx):
    """Convert a matrix object into a dictionary
    """
    mtxDict = _getLifemapperMetadata(
        'matrix', mtx.getId(), mtx.metadataUrl, mtx.getUserId(),
        status=mtx.status, statusModTime=mtx.statusModTime,
        metadata=mtx.mtxMetadata)
    mtxDict['altPredCode'] = mtx.altpredCode
    mtxDict['dateCode'] = mtx.dateCode
    mtxDict['gcmCode'] = mtx.gcmCode
    mtxDict['dataUrl'] = mtx.getDataUrl()
    mtxDict['matrixType'] = mtx.matrixType
    mtxDict['parentMetadataUrl'] = mtx.parentMetadataUrl
    mtxDict['gridsetId'] = mtx.gridsetId
    mtxDict['gridsetUrl'] = mtx.gridsetUrl
    mtxDict['gridsetName'] = mtx.gridsetName
    
    return mtxDict

# .............................................................................
def formatOccurrenceSet(occ):
    """Convert an Occurrence Set object to a dictionary

    Todo:
        * Mapping metadata
        * Taxon id
    """
    occDict = _getLifemapperMetadata(
        'occurrence set', occ.getId(), occ.metadataUrl, occ.getUserId(),
        status=occ.status, statusModTime=occ.statusModTime,
        metadata=occ.lyrMetadata)
    mapName = EarlJr().createBasename(
        LMFileType.SDM_MAP, objCode=occ.getId(), usr=occ.getUserId(),
        epsg=occ.epsgcode)
    occDict['map'] = _getMapMetadata(OGC_SERVICE_URL, mapName, occ.name)
    dataUrl = occ.getDataUrl()
    occDict['spatialVector'] = _getSpatialVectorMetadata(
        occ.epsgcode, occ.bbox, occ.mapUnits, dataUrl, occ.verify, occ.ogrType,
        occ.dataFormat, occ.queryCount, resolution=occ.resolution)
    occDict['speciesName'] = occ.displayName
    occDict['squid'] = occ.squid
    if len(occ.features) > 0:
        occDict['features'] = [f.getAttributes() for f in occ.features]
    
    return occDict

# .............................................................................
def formatProjection(prj):
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
    prjDict = _getLifemapperMetadata(
        'projection', prj.getId(), prj.metadataUrl, prj.getUserId(),
        status=prj.status, statusModTime=prj.statusModTime,
        metadata=prj.lyrMetadata)
    occ = prj._occurrenceSet
    mapName = EarlJr().createBasename(
        LMFileType.SDM_MAP, objCode=occ.getId(), usr=occ.getUserId(),
        epsg=occ.epsgcode)
    prjDict['map'] = _getMapMetadata(OGC_SERVICE_URL, mapName, prj.name)
    dataUrl = prj.getDataUrl()
    minVal = 0
    maxVal = 1
    valUnits = 'prediction'
    prjDict['spatialRaster'] = _getSpatialRasterMetadata(
        prj.epsgcode, prj.bbox, prj.mapUnits, dataUrl, prj.verify,
        prj.gdalType, prj.dataFormat, minVal, maxVal, valUnits, prj.gdalType,
        prj.resolution)
    
    prjDict['algorithm'] = {
        'code' : prj.algorithmCode,
        'parameters' : prj._algorithm._parameters
    }
    
    prjDict['modelScenario'] = {
        'code' : prj.modelScenario.code,
        'id' : prj.modelScenario.getId(),
        'metadataUrl' : prj.modelScenario.metadataUrl
    }
    
    prjDict['projectionScenario'] = {
        'code' : prj.projScenario.code,
        'id' : prj.projScenario.getId(),
        'metadataUrl' : prj.projScenario.metadataUrl
    }
    
    prjDict['speciesName'] = prj.speciesName
    prjDict['squid'] = prj.squid
    prjDict['occurrenceSet'] = {
        'id' : prj.getOccurrenceSetId(),
        'metadataUrl' : prj._occurrenceSet.metadataUrl
    }
    
    return prjDict
    
# .............................................................................
def formatRasterLayer(lyr):
    """Convert an environmental layer into a dictionary

    Todo:
        * Mapping metadata
        * Min val
        * Max val
        * Value units
    """
    lyrDict = _getLifemapperMetadata(
        'raster layer', lyr.getId(), lyr.metadataUrl, lyr.getUserId(),
        metadata=lyr.lyrMetadata)
    dataUrl = lyr.getDataUrl()
    minVal = lyr.minVal
    maxVal = lyr.maxVal
    valUnits = lyr.valUnits
    dataType = type(lyr.minVal).__name__
    lyrDict['spatialRaster'] = _getSpatialRasterMetadata(
        lyr.epsgcode, lyr.bbox, lyr.mapUnits, dataUrl, lyr.verify,
        lyr.gdalType, lyr.dataFormat, minVal, maxVal, valUnits, dataType,
        resolution=lyr.resolution)
    
    return lyrDict

# .............................................................................
def formatScenario(scn):
    """Converts a scenario object into a dictionary

    Todo:
        * Fix map ogc endpoint
        * GCM / alt pred code / etc
    """
    scnDict = _getLifemapperMetadata(
        'scenario', scn.getId(), scn.metadataUrl, scn.getUserId(),
        metadata=scn.scenMetadata)
    mapName = EarlJr().createBasename(
        LMFileType.SCENARIO_MAP, objCode=scn.code, usr=scn.getUserId(),
        epsg=scn.epsgcode)
    scnDict['map'] = _getMapMetadata(OGC_SERVICE_URL, mapName, scn.layers)
    scnDict['spatial'] = _getSpatialMetadata(
        scn.epsgcode, scn.bbox, scn.mapUnits, scn.resolution)

    scnLayers = []
    for lyr in scn.layers:
        scnLayers.append(lyr.metadataUrl)
    scnDict['layers'] = scnLayers
    scnDict['code'] = scn.code
    
    return scnDict

# .............................................................................
def formatScenarioPackage(scnPkg):
    """Converts a scenario package object into a dictionary
    """
    scnPkgDict = _getLifemapperMetadata(
        'scenario package', scnPkg.getId(), scnPkg.metadataUrl,
        scnPkg.getUserId(), metadata=scnPkg.scenpkgMetadata)
    scnPkgDict['name'] = scnPkg.name
    scnPkgDict['scenarios'] = [
        formatScenario(scn) for (_, scn) in scnPkg.scenarios.iteritems()]
    return scnPkgDict

# .............................................................................
def formatShapegrid(sg):
    """Convert a shapegrid into a dictionary
    """
    sgDict = _getLifemapperMetadata(
        'shapegrid', sg.getId(), sg.metadataUrl, sg.getUserId(),
        status=sg.status, statusModTime=sg.statusModTime,
        metadata=sg.lyrMetadata)
    sgDict['spatialVector'] = _getSpatialVectorMetadata(
        sg.epsgcode, sg.bbox, sg.mapUnits, sg.getDataUrl(), sg.verify,
        sg.ogrType, sg.dataFormat, sg.featureCount, resolution=sg.resolution)
    sgDict['cellSides'] = sg.cellsides
    sgDict['cellSize'] = sg.cellsize

    return sgDict

# .............................................................................
def formatTree(tree):
    """Convert a tree into a dictionary

    Todo:
        * CJG - Add more tree metadata.  Check notes from Ryan conversation
    """
    treeDict = _getLifemapperMetadata(
        'tree', tree.getId(), tree.metadataUrl, tree.getUserId(),
        metadata=tree.treeMetadata)
    treeDict['ultrametric'] = tree.isUltrametric()
    treeDict['binaery'] = tree.isBinary()
    return treeDict

# .............................................................................
def formatVector(vlyr):
    """Convert a vector into a dictionary
    """
    vDict = _getLifemapperMetadata(
        'Vector Layer', vlyr.getId(), vlyr.metadataUrl, vlyr.getUserId(),
        metadata=vlyr.lyrMetadata)
    vDict['spatialVector'] = _getSpatialVectorMetadata(
        vlyr.epsgcode, vlyr.bbox, vlyr.mapUnits, vlyr.getDataUrl(),
        vlyr.verify, vlyr.ogrType, vlyr.dataFormat, vlyr.featureCount)

    return vDict

# .............................................................................
def jsonObjectFormatter(obj):
    """Looks at object and converts to JSON based on its type
    """
    if isinstance(obj, ListType):
        response = []
        for o in obj:
            response.append(_formatObject(o))
    else:
        response = _formatObject(obj)
    
    return json.dumps(response, indent=3)

# .............................................................................
def _formatObject(obj):
    """Helper method to format an individual object based on its type
    """
    cherrypy.response.headers['Content-Type'] = LMFormat.JSON.getMimeType()
    if isinstance(obj, DictionaryType):
        return obj
    elif isinstance(obj, Atom):
        return formatAtom(obj)
    elif isinstance(obj, SDMProjection):
        return formatProjection(obj)
    elif isinstance(obj, OccurrenceLayer):
        return formatOccurrenceSet(obj)
    elif isinstance(obj, EnvLayer):
        return formatEnvLayer(obj)
    elif isinstance(obj, Scenario):
        return formatScenario(obj)
    elif isinstance(obj, ScenPackage):
        return formatScenarioPackage(obj)
    elif isinstance(obj, Raster):
        return formatRasterLayer(obj)
    elif isinstance(obj, Gridset):
        return formatGridset(obj)
    elif isinstance(obj, LMMatrix):
        return formatMatrix(obj)
    elif isinstance(obj, ShapeGrid):
        return formatShapegrid(obj)
    elif isinstance(obj, Tree):
        return formatTree(obj)
    elif isinstance(obj, Vector):
        return formatVector(obj)
    else:
        # TODO: Expand these and maybe fallback to a generic formatter of public
        #             attributes
        raise TypeError("Cannot format object of type: {}".format(type(obj)))

# .............................................................................
def _getLifemapperMetadata(objectType, lmId, url, userId, status=None,
                           statusModTime=None, metadata=None):
    """Get general Lifemapper metadata that we want to return for each type
    """
    lmDict = {
        'objectType' : objectType,
        'id' : lmId,
        'url' : url,
        'user' : userId
    }
    if status is not None:
        lmDict['status'] = status
    if statusModTime is not None:
        lmDict['statusModTime'] = formatTimeHuman(statusModTime)
        lmDict['etag'] = md5('{}-{}'.format(url, statusModTime)).hexdigest()
    if metadata is not None:
        lmDict['metadata'] = metadata

    return lmDict

# .............................................................................
def _getMapMetadata(baseUrl, mapName, layers):
    """Get a dictionary of mapping information

    Note:
        * This is very alpha.  We need to discuss exactly how we will implement
            maps going forward
    """
    mapDict = {
        'endpoint' : baseUrl,
        'mapName' : mapName
    }
    if isinstance(layers, ListType):
        lyrs = []
        for lyr in layers:
            lyrs.append({
                'metadataUrl' : lyr.metadataUrl,
                'layerName' : lyr.name
            })
        mapDict['layers'] = lyrs
    else:
        mapDict['layerName'] = layers
    return mapDict

# .............................................................................
def _getSpatialMetadata(epsg, bbox, mapUnits, resolution=None):
    """Get dictionary of spatial metadata

    Note:
        * This can be expanded by the _getSpatialRasterMetadata and
            _getSpatialVectorMetadata functions if the object has a data file
    """
    spatialDict = {
        'epsg' : epsg,
        'bbox' : bbox,
        'mapUnits' : mapUnits
    }
    if resolution is not None:
        spatialDict['resolution'] = resolution
    return spatialDict

# .............................................................................
def _getSpatialRasterMetadata(epsg, bbox, mapUnits, dataUrl, sha256Val,
                              gdalType, dataFormat, minVal, maxVal, valUnits,
                              dataType, resolution=None):
    """Return a dictionary of metadata about a spatial raster

    Todo:
        * Add file size, number of rows, number of columns?
    """
    srDict = _getSpatialMetadata(epsg, bbox, mapUnits, resolution=resolution)
    srDict['dataUrl'] = dataUrl
    srDict['sha256'] = sha256Val
    srDict['gdalType'] = gdalType
    srDict['dataFormat'] = dataFormat
    srDict['minVal'] = minVal
    srDict['maxVal'] = maxVal
    srDict['valueUnits'] = valUnits
    srDict['dataType'] = dataType
    
    return srDict

# .............................................................................
def _getSpatialVectorMetadata(epsg, bbox, mapUnits, dataUrl, sha256Val,
                              ogrType, dataFormat, numFeatures,
                              resolution=None):
    """Return a dictionary of metadata about a spatial vector

    Todo:
        * Add features?
        * Add file size?
    """
    svDict = _getSpatialMetadata(epsg, bbox, mapUnits, resolution=resolution)
    svDict['dataUrl'] = dataUrl
    svDict['sha256'] = sha256Val
    svDict['ogrType'] = ogrType
    svDict['dataFormat'] = dataFormat
    svDict['numFeatures'] = numFeatures

    return svDict

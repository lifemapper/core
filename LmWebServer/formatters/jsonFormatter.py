"""
@summary: Module functions for converting object to JSON
@author: CJ Grady
@version: 2.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
@todo: Use constants
@todo: Can we make this more elegant?
"""
from hashlib import md5
import json
from types import ListType, DictionaryType

from LmServer.base.atom import Atom
from LmServer.base.layer2 import Raster
from LmServer.base.utilities import formatTimeHuman

from LmServer.legion.sdmproj import SDMProjection
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.scenario import Scenario

# Format object method looks at object type and calls formatters appropriately
# Provide methods for direct calls to formatters
# .............................................................................
def formatAtom(obj):
   """
   @summary: Format an Atom object into a dictionary
   """
   return {
      'epsg' : obj.epsgcode,
      'id' : obj.id,
      'modificationTime' : formatTimeHuman(obj.modTime),
      'name' : obj.name,
      'url' : obj.metadataUrl
   }

# .............................................................................
def formatEnvLayer(lyr):
   """
   @summary: Convert an environmental layer into a dictionary
   @todo: Mapping metadata
   @todo: Min val
   @todo: Max val
   @todo: Value units
   """
   lyrDict = _getLifemapperMetadata('environmental layer', lyr.getId(), 
                                    lyr.metadataUrl, lyr.getUserId(), 
                                    metadata=lyr.lyrMetadata)
   lyrDict['map'] = _getMapMetadata('http://svc.lifemapper.org/api/v2/maps', 
                                    'layers', lyr.name)
   dataUrl = '{}/GTiff'.format(lyr.metadataUrl)
   minVal = lyr.minVal
   maxVal = lyr.maxVal
   valUnits = lyr.valUnits
   dataType = type(lyr.minVal).__name__
   lyrDict['spatialRaster'] = _getSpatialRasterMetadata(lyr.epsgcode, lyr.bbox, 
                                      lyr.mapUnits, dataUrl, lyr.verify,
                                      lyr.gdalType, lyr.dataFormat, minVal, 
                                      maxVal, valUnits, dataType, 
                                      resolution=lyr.resolution)
   lyrDict['envCode'] = lyr.envCode
   lyrDict['gcmCode'] = lyr.gcmCode
   lyrDict['altPredCode'] = lyr.altpredCode
   lyrDict['dateCode'] = lyr.dateCode
   
   return lyrDict

# .............................................................................
def formatOccurrenceSet(occ):
   """
   @summary: Convert an Occurrence Set object to a dictionary
   @todo: Mapping metadata
   @todo: Taxon id
   """
   occDict = _getLifemapperMetadata('occurrence set', occ.getId(), 
                           occ.metadataUrl, occ.getUserId(), status=occ.status, 
                           statusModTime=occ.statusModTime, 
                           metadata=occ.metadata)
   occDict['map'] = _getMapMetadata('http://svc.lifemapper.org/api/v2/maps', 
                                    'occurrences', occ.name)
   dataUrl = '{}/shapefile'.format(occ.metadataUrl)
   occDict['spatialVector'] = _getSpatialVectorMetadata(occ.epsgcode, occ.bbox, 
                                    occ.mapUnits, dataUrl, occ.verify, 
                                    occ.ogrType, occ.dataFormat, occ.queryCount,
                                    resolution=occ.resolution)
   occDict['speciesName'] = occ.displayName
   occDict['squid'] = occ.squid
   
   return occDict

# .............................................................................
def formatProjection(prj):
   """
   @summary: Converts a projection object into a dictionary
   @todo: Fix map ogc endpoint
   @todo: Fix map name
   @todo: Min value
   @todo: Max value
   @todo: Value units
   @todo: Public algorithm parameters
   @todo: Masks
   @todo: Taxon id
   @todo: Occurrence set metadata url
   """
   prjDict = _getLifemapperMetadata('projection', prj.getId(), prj.getUserId(), 
                                    prj.metadataUrl, status=prj.status, 
                                    statusModTime=prj.statusModTime, 
                                    metadata=prj.metadata)
   prjDict['map'] = _getMapMetadata('http://svc.lifemapper.org/api/v2/maps', 
                                    'projections', prj.name)
   dataUrl = '{}/GTiff'.format(prj.metadataUrl)
   minVal = 0
   maxVal = 1
   valUnits = 'prediction'
   prjDict['spatialRaster'] = _getSpatialRasterMetadata(prj.epsgcode, prj.bbox, 
               prj.mapUnits, dataUrl, prj.verify, prj.gdalType, prj.dataFormat, 
               minVal, maxVal, valUnits, prj.dataType, prj.resolution)
   
   prjDict['algorithm'] = {
      'code' : prj.algorithmCode,
      'parameters' : prj._algorithm.getAlgorithmParameters()
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
   """
   @summary: Convert an environmental layer into a dictionary
   @todo: Mapping metadata
   @todo: Min val
   @todo: Max val
   @todo: Value units
   """
   lyrDict = _getLifemapperMetadata('raster layer', lyr.getId(), 
                                    lyr.metadataUrl, lyr.getUserId(), 
                                    metadata=lyr.lyrMetadata)
   #lyrDict['map'] = _getMapMetadata('http://svc.lifemapper.org/api/v2/maps', 
   #                                 'layers', lyr.name)
   dataUrl = '{}/GTiff'.format(lyr.metadataUrl)
   minVal = lyr.minVal
   maxVal = lyr.maxVal
   valUnits = lyr.valUnits
   dataType = type(lyr.minVal).__name__
   lyrDict['spatialRaster'] = _getSpatialRasterMetadata(lyr.epsgcode, lyr.bbox, 
                                      lyr.mapUnits, dataUrl, lyr.verify,
                                      lyr.gdalType, lyr.dataFormat, minVal, 
                                      maxVal, valUnits, dataType, 
                                      resolution=lyr.resolution)
   #lyrDict['envCode'] = lyr.envCode
   #lyrDict['gcmCode'] = lyr.gcmCode
   #lyrDict['alternatePredictioCode'] = lyr.altpredCode
   #lyrDict['dateCode'] = lyr.dateCode
   
   return lyrDict

# .............................................................................
def formatScenario(scn):
   """
   @summary: Converts a scenario object into a dictionary
   @todo: Fix map ogc endpoint
   @todo: GCM / alt pred code / etc
   """
   scnDict = _getLifemapperMetadata('scenario', scn.getId(), scn.metadataUrl,
                                    scn.getUserId(), metadata=scn.scenMetadata)
   scnDict['map'] = _getMapMetadata('http://svc.lifemapper.org/api/v2/maps', 
                                    scn.code, scn.layers)
   scnDict['spatial'] = _getSpatialMetadata(scn.epsgcode, scn.bbox, 
                                            scn.units, scn.resolution)

   scnLayers = []
   for lyr in scn.layers:
      scnLayers.append(lyr.metadataUrl)
   scnDict['layers'] = scnLayers
   scnDict['code'] = scn.code
   
   return scnDict
   
# .............................................................................
def jsonObjectFormatter(obj):
   """
   @summary: Looks at object and converts to JSON based on its type
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
   """
   @summary: Helper method to format an individual object based on its type
   """
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
   elif isinstance(obj, Raster):
      return formatRasterLayer(obj)
   else:
      # TODO: Expand these and maybe fallback to a generic formatter of public
      #          attributes
      raise TypeError, "Cannot format object of type: {}".format(type(obj))

# .............................................................................
def _getLifemapperMetadata(objectType, lmId, url, userId, status=None, 
                           statusModTime=None, metadata=None):
   """
   @summary: Get general Lifemapper metadata that we want to return for each 
                full object type
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
   """
   @summary: Get a dictionary of mapping information
   @note: This is very alpha.  We need to discuss exactly how we will implement 
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
   """
   @summary: Get dictionary of spatial metadata
   @note: This can be expanded by the _getSpatialRasterMetadata and 
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
   """
   @summary: Return a dictionary of metadata about a spatial raster
   @todo: Add file size, number of rows, number of columns?
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
   """
   @summary: Return a dictionary of metadata about a spatial vector
   @todo: Add features?
   @todo: Add file size?
   """
   svDict = _getSpatialMetadata(epsg, bbox, mapUnits, resolution=resolution)
   svDict['dataUrl'] = dataUrl
   svDict['sha256'] = sha256Val
   svDict['ogrType'] = ogrType
   svDict['dataFormat'] = dataFormat
   svDict['numFeatures'] = numFeatures

   return svDict

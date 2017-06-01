"""
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
"""
#import collections
import os
from osgeo import gdal, gdalconst, ogr

from LmBackend.common.lmobj import LMError
from LmServer.base.layer2 import _Layer, Raster, Vector
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.colorpalette import colorPalette
from LmServer.common.lmconstants import (MAP_TEMPLATE, QUERY_TEMPLATE, 
         MapPrefix, LMFileType, IMAGE_PATH, BLUE_MARBLE_IMAGE, POINT_SYMBOL, 
         POINT_SIZE, LINE_SYMBOL, LINE_SIZE, POLYGON_SYMBOL, POLYGON_SIZE, 
         QUERY_TOLERANCE, SYMBOL_FILENAME, DEFAULT_POINT_COLOR, 
         DEFAULT_LINE_COLOR, DEFAULT_PROJECTION_PALETTE, LMServiceType,
         DEFAULT_ENVIRONMENTAL_PALETTE, CT_SPECIES_LAYER_STYLES, 
         CT_SPECIES_KEYWORD, PROJ_LIB)
from LmServer.common.localconstants import (PUBLIC_USER, POINT_COUNT_MAX,
                                            SCENARIO_PACKAGE_EPSG, SCENARIO_PACKAGE_MAPUNITS)
from LmServer.common.lmconstants import CT_USER
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.sdmproj import SDMProjection

# .............................................................................
class _LayerSet(LMSpatialObject):
   """
   Superclass of MapLayerSet
   @todo: extend as collections.MutableSequence subclass
   """   
   def __init__(self, name, title=None, keywords=None, epsgcode=None, 
                layers=None, bbox=None, mapunits=None):
      """
      @summary Constructor for the LayerSet class
      @param name: name or code for this layerset
      @param title: (optional) human readable title of this layerset
      @param keywords: (optional) sequence of keywords for this layerset
      @param epsgcode: (optional) integer representing the native EPSG code of this layerset
      @param layers: (optional) list of layers 
      """
      LMSpatialObject.__init__(self, epsgcode, bbox, mapunits)

      ## Name or code identifying this set of layers
      self.name = name
      ## Title for this set of layers
      self.title = title
      # Keywords for the layerset as a whole
      self._setKeywords(keywords)
      self._layers = []
      ## List of Raster or Vector objects for this LayerSet'
      ## Also sets epsg and bbox
      ## If no layers, initializes to empty list 
      self._setLayers(layers)

   def _getUnits(self):
      """
      @todo: add mapunits to Occ table (and Scenario?), 
             handle better on construction.
      """
      if self._mapunits is None and len(self._layers) > 0:
         self._setUnits(self._layers[0].mapUnits)
      return self._mapunits

# ...............................................
   def getSRS(self):
      srs = self.createSRSFromEPSG()
      return srs      
      
## .............................................................................
## MutableSequence methods
## .............................................................................
#   def __iter__(self):
#      return self
#   
#   # For Python 3 compatibility
#   def __next__(self):
#      pass
#
#   # For Python 2 compatibility
#   def next(self):
#      return self.__next__()
#   
#   def __contains__(self, key):
#      pass
#   
#   def __len__(self):
#      pass
#   
#   def __getitem__(self, index):
#      pass
#   
#   def index(self, value): 
#      pass
#   
#   def count(self):
#      return self.__len__()
#   
#   def __setitem__(self, index, value):
#      pass 
   
# .............................................................................
   def getLayer(self, metadataUrl):
      """
      @summary Gets a layer from the LayerSet with the specified mapPrefix
      @param metadataUrl: metadataUrl for which to find matching layer
      @return the layer object with the given metadataUrl, None if there is no 
              matching layer
      """
      for lyr in self._layers:
         if lyr.metadataUrl == metadataUrl:
            return lyr
      return None
   
# .............................................................................
   def addLayer(self, lyr):
      """
      @note: metadataUrl is used for identification - ensuring that a layer is 
             not duplicated in the layerset.  MetadataUrl should be (relatively)
             unique, unlike mapPrefix which is constructed differently for each
             layerset (and mapfile) that contains a layer.
      """
      if isinstance(lyr, _Layer):
         if self.getLayer(lyr.metadataUrl) is None:
            if self._epsg is None or self._epsg == lyr.epsgcode:
               self._layers.append(lyr)
               if self._epsg is None:
                  self._setEPSG(lyr.epsgcode)
            else:
               raise LMError('Invalid layer SRS %s for layerset with SRS %s' 
                             % (lyr.epsgcode, self._epsg))
      else:
         raise LMError(['Cannot add %s as a Layer' % str(type(lyr))])
         
# .............................................................................
   def addKeywords(self, keywordSequence):
      """
      @summary Adds keywords to the LayerSet object
      @param keywordSequence: List of keywords to add
      """
      if keywordSequence is not None:
         for k in keywordSequence:
            self._keywords.add(k)
         
   def addKeyword(self, keyword):
      """
      @summary Adds a keyword to the LayerSet object
      @param keyword: Keyword to add
      """
      if keyword is not None:
         self._keywords.add(keyword)

   def _getKeywords(self):
      """
      @summary Gets the keywords of the LayerSet
      @return List of keywords describing the LayerSet
      """
      return self._keywords
         
   def _setKeywords(self, keywordSequence):
      """
      @summary Sets the keywords of the LayerSet
      @param keywordSequence: List of keywords that will be associated with 
                              the LayerSet
      """
      if keywordSequence is not None:
         self._keywords = set(keywordSequence)
      else:
         self._keywords = set()
            
## .............................................................................
#   def _setEpsgcode(self, epsg=None):
#      if epsg is not None:
#         self._setEPSG(epsg)
#      elif self._layers:
#         codes = [lyr.epsgcode for lyr in self._layers]
#         self._setEPSG(codes)
#      else:
#         self._setEPSG(epsg)
                     
# .............................................................................
   def _intersectLayerKeywords(self):
      """
      @summary Gets keywords common to all layers in the scenario
      @return Set of keywords
      """
      s = set()
      for i in range(len(self._layers)):
         if i == 0:
            s = s.union(self._layers[0].keywords)
         else:
            s = s.intersection(self._layers[i].keywords)
      return s

   def _unionLayerKeywords(self):
      """
      @summary Gets all keywords that occur in layers in the scenario
      @return Set of keywords
      """
      s = set()
      for i in range(len(self._layers)):
         s = s.union(self._layers[i].keywords)
      return s

# .............................................................................
   def _getUnionBounds(self):
      """
      @summary Gets the union of all bounding boxes in the layers of the 
               LayerSet
      @return tuple of (minx, miny, maxx, maxy) 
      """
      lyrBoxes = [lyr.bbox for lyr in self._layers]
      return self.unionBoundingBoxes(lyrBoxes)
   
   def _getIntersectBounds(self):
      """
      @summary Gets the intersection of all associated bounding boxes
      @return tuple of (minx, miny, maxx, maxy) 
      """
      lyrBoxes = [lyr.bbox for lyr in self._layers]
      return self.intersectBoundingBoxes(lyrBoxes)
   
# .............................................................................
   def _getLayers(self):
      return self._layers

   def _setLayers(self, lyrs):
      if lyrs is not None:
         for lyr in lyrs:
            self.addLayer(lyr)
      else:
         self._layers = []
      ## Using Intersection, could be Union, both 
      ## intersectBounds, unionBounds available as properties
      bbox = self._getUnionBounds()
      self._setBBox(bbox)
      
   def _getLayerCount(self):
      count = 0
      if self._layers is not None:
         count = len(self._layers)
      return count
   
# .............................................................................

   ## Set of words describing layerset as a whole
   keywords = property(_getKeywords, _setKeywords)    
   layers = property(_getLayers, _setLayers)
   # property counting the actual layer objects present 
   count = property (_getLayerCount)

   # Read-only properties  
   # union/intersection of the keywords/Boundaries of all
   # layers of the LayerSet
   
   # Return set of keywords
   unionKeywords = property(_unionLayerKeywords)
   intersectKeywords = property(_intersectLayerKeywords)

   # Return tuple of (minx, miny, maxx, maxy)
   unionBounds = property(_getUnionBounds)
   intersectBounds = property(_getIntersectBounds)
   
# .............................................................................
# .............................................................................
class MapLayerSet(_LayerSet, ServiceObject):
   """
   Superclass of Scenario, PresenceAbsenceLayerset.  
   @todo: extend as collections.MutableSequence subclass
   @note: mapcode should be required
   """
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, mapname, title=None, 
                url=None, dlocation=None, keywords=None, epsgcode=None, layers=None, 
                userId=None, dbId=None, createTime=None, modTime=None, 
                bbox=None, mapunits=None,
                serviceType=LMServiceType.LAYERSETS, mapType=LMFileType.OTHER_MAP):
      """
      @summary Constructor for the LayerSet class
      @param name: mapname or code for this layerset
      @param serviceType: string for constructing webservice URL for object
      @param title: (optional) human readable title of this layerset
      @param mapcode:
      @param url:
      @param keywords: (optional) sequence of keywords for this layerset
      @param epsgcode: (optional) integer representing the native EPSG code of this layerset
      @param layers: (optional) list of layers 
      @param userId: id for the owner of these data
      @param dbid: database id of the object, occsetId for SDM_MAP layersets, 
             gridsetId for RAD_MAP layersets, scenCode for Scenarios 
      """
      _LayerSet.__init__(self, mapname, title=title, keywords=keywords, 
                         epsgcode=epsgcode, layers=layers, 
                         bbox=bbox, mapunits=mapunits)
      ServiceObject.__init__(self, userId, dbId, serviceType, metadataUrl=url, 
                             modTime=modTime)
      self._mapFilename = dlocation
      self._mapType = mapType
      self._mapPrefix = None

# # ...............................................         
#    def createMapPrefix(self):
#       """
#       @note: Only calls this method if not overridden by special cases for 
#              Scenario or SDM Data  
#       """
#       mapprefix = self._earlJr.constructOtherMapPrefix(self.getUserId())
#       return mapprefix
      
# ...............................................
   # TODO: remove this property
   @property
   def mapPrefix(self):
      self.setMapPrefix()
      return self._mapPrefix
   
   def setMapPrefix(self, mapprefix=None):
      if mapprefix is None:
         mapprefix = self._earlJr.constructMapPrefixNew(ftype=LMFileType.OTHER_MAP, 
                                    objCode=self.getId(), mapname=self.mapName, 
                                    usr=self._userId, epsg=self.epsgcode)
      self._mapPrefix = mapprefix
      
# ...............................................
   def createLocalMapFilename(self):
      """
      @summary: Full mapfile with path, containing this layer.  
      """
      fname = None
      if self._mapType == LMFileType.SDM_MAP:
         fname = self._earlJr.createFilename(self._mapType, 
                                             occsetId=self.getId(), 
                                             usr=self._userId)
      elif self._mapType == LMFileType.RAD_MAP:
         fname = self._earlJr.createFilename(self._mapType, 
                                             gridsetId=self.getId(),
                                             usr=self._userId)
      elif self._mapType == LMFileType.OTHER_MAP:
         fname = self._earlJr.createFilename(self._mapType, 
                                             usr=self._userId, 
                                             epsg=self._epsg)
      else:
         print('Unsupported mapType {}'.format(self._mapType))
      return fname

# ...............................................
   def setLocalMapFilename(self, mapfname=None):
      """
      @note: Overrides existing _mapFilename
      @summary: Set absolute mapfilename containing all layers for this User/EPSG. 
      """
      if mapfname is None:
         mapfname = self.createLocalMapFilename()
      self._mapFilename = mapfname

# ...............................................
   def clearLocalMapfile(self):
      """
      @summary: Delete the mapfile containing this layer
      """
      if self.mapfilename is None:
         self.setLocalMapFilename()
      success, msg = self.deleteFile(self.mapfilename)

# ...............................................
   @property
   def mapFilename(self):
      if self._mapFilename is None:
         self.setLocalMapFilename()
      return self._mapFilename
   
# ...............................................
   @property
   def mapName(self):
      mapname = None
      if self._mapFilename is not None:
         pth, mapfname = os.path.split(self._mapFilename)
         mapname, ext = os.path.splitext(mapfname)
      return mapname
   
# .............................................................................
   def writeMap(self, template=MAP_TEMPLATE):
      """
      @summary Create a mapfile by replacing strings in a template mapfile 
               with text created for the layer set.
      @param mapcode: Prefix for the mapfilename
      @param template: Template mapfile 
      @return a string representing a mapfile 
      """
      self.setLocalMapFilename()
      # if mapfile does not exist, create service from database, then write file
      if not(os.path.exists(self._mapFilename)):            
         try:
            layers, onlineUrl = self._createLayers()
            mapTemplate = self._earlJr.getMapFilenameFromMapname(template)
            mapstr = self._getBaseMap(mapTemplate)
            mapstr = self._addMapBaseAttributes(mapstr, onlineUrl)
            mapstr = mapstr.replace('##_LAYERS_##', layers)
         except Exception, e:
            raise
         
         try:
            self._writeBaseMap(mapstr)
         except Exception, e:
            raise LMError('Failed to write %s: %s' % (self._mapFilename, str(e)))

# ...............................................
   def _writeBaseMap(self, mapstr):
      dir = os.path.dirname(self._mapFilename)
      self.readyFilename(self._mapFilename, overwrite=True)
      
      try:
         f = open(self._mapFilename, 'w')
         # make sure that group is set correctly
         f.write(mapstr)
         f.close()
         print('Wrote %s' % (self._mapFilename))
      except Exception, e:
         raise LMError('Failed to write %s; %s' % (self._mapFilename, str(e)))
      
# ...............................................
   def _getBaseMap(self, fname):
      # TODO: in python 2.6, use 'with open(fname, 'r'):'
      try:
         f = open(fname, 'r')
         map = f.read()
         f.close()
      except Exception, e:
         raise LMError('Failed to read %s' % fname)
      return map
         
# ...............................................
   def _addMapBaseAttributes(self, mapstr, onlineUrl):
      """
      @summary Set map attributes on the map from the LayerSet
      @param mapstr: string for a mapserver mapfile to modify
      """
      if self.name.startswith(MapPrefix.SDM):
         label = 'Lifemapper Species Map Service'
      elif self.name.startswith(MapPrefix.USER):
         label = 'Lifemapper User Data Map Service'
      elif self.name.startswith(MapPrefix.SCEN):
         label = 'Lifemapper Environmental Data Map Service'
      elif self.name.startswith(MapPrefix.ANC):
         label = 'Lifemapper Ancillary Map Service'
      elif self.name.startswith(MapPrefix.RAD):
         label = 'Lifemapper RAD Map Service'
#      elif self.name.lower() == SPECIES_SERVICENAME:
#         label = 'Lifemapper Species Occurrence Service'
#      elif self.name.lower() == PROJECTION_SERVICENAME:
#         label = 'Lifemapper Species Habitat Projection Service'
      else:
         label = 'Lifemapper Data Service'

      # changed this from self.name (which left 'scen_' prefix off scenarios)
      mapstr = mapstr.replace('##_MAPNAME_##', self.mapName)      
                              
      if self.epsgcode == SCENARIO_PACKAGE_EPSG:
         boundstr = '  -180  -90  180  90'
      else:
         mbbox = self.unionBounds
         boundstr = '  %.2f  %.2f  %.2f  %.2f' % (mbbox[0], mbbox[1],
                                                           mbbox[2], mbbox[3])
      mapstr = mapstr.replace('##_EXTENT_##', boundstr)
      mapunits = SCENARIO_PACKAGE_MAPUNITS
      if self.layers and len(self.layers) > 0:
         mapunits = self.layers[0].mapUnits
      mapstr = mapstr.replace('##_UNITS_##',  mapunits)

      mapstr = mapstr.replace('##_SYMBOLSET_##',  SYMBOL_FILENAME)
      mapstr = mapstr.replace('##_PROJLIB_##',  PROJ_LIB)
      
      mapprj = self._createProjectionInfo(self.epsgcode)
      mapstr = mapstr.replace('##_PROJECTION_##',  mapprj)
      
      # Mapserver 5.6 & 6.0
      meta = ''
      meta = '\n'.join([meta, '      METADATA'])
      meta = '\n'.join([meta, '         ows_srs   \"epsg:%s\"' % self.epsgcode])
      meta = '\n'.join([meta, '         ows_enable_request   \"*\"'])
      meta = '\n'.join([meta, '         ows_label   \"%s\"' % label])
      meta = '\n'.join([meta, '         ows_title   \"%s\"' % self.title])
      meta = '\n'.join([meta, '         ows_onlineresource   \"%s\"' % onlineUrl])
      meta = '\n'.join([meta, '      END'])

      mapstr = mapstr.replace('##_MAP_METADATA_##', meta)
      return mapstr

# ...............................................
   def _createLayers(self):
      topLyrStr = ''
      midLyrStr = ''
      baseLyrStr = ''
      vOnlineUrl = rOnlineUrl = eOnlineUrl = onlineUrl = None
            
      # Vector layers are described first, so drawn on top
      for lyr in self.layers:
#         print 'Getting layer %s/%d from %s ...' % (lyr.name, lyr.getId(), self.name)
         # todo: Check if lyr is OccurrenceLayer, respond differently to other
         #       types of vector layers.  
         #       Maybe use ServiceObject._serviceType for display options
         if isinstance(lyr, Vector):
            vOnlineUrl = lyr.metadataUrl + '/ogc'
            lyrstr = self._createVectorLayer(lyr)
            topLyrStr = '\n'.join([topLyrStr, lyrstr])
            
         elif isinstance(lyr, Raster):
            # projections are below vector layers and above the base layer
            if isinstance(lyr, SDMProjection):
               rOnlineUrl = lyr.metadataUrl + '/ogc'
               palette = DEFAULT_PROJECTION_PALETTE
               lyrstr = self._createRasterLayer(lyr, palette)
               midLyrStr = '\n'.join([midLyrStr, lyrstr])
            else:
               eOnlineUrl = lyr.metadataUrl + '/ogc'
               palette = DEFAULT_ENVIRONMENTAL_PALETTE
               lyrstr = self._createRasterLayer(lyr, palette)
               baseLyrStr = '\n'.join([baseLyrStr, lyrstr])
              
      maplayers = '\n'.join([topLyrStr, midLyrStr, baseLyrStr])
      
      if vOnlineUrl:
         onlineUrl = vOnlineUrl
      elif rOnlineUrl:
         onlineUrl = rOnlineUrl
      elif eOnlineUrl:
         onlineUrl = eOnlineUrl
            
      # Add bluemarble image to Data/Occurrence Map Services
      if self.epsgcode == SCENARIO_PACKAGE_EPSG:
         backlyr = self._createBlueMarbleLayer()
         maplayers = '\n'.join([maplayers, backlyr])
         
      return maplayers, onlineUrl
    
# ...............................................
   def _createVectorLayer(self, sdlLyr):
      attMeta = []
      proj = None
      meta = None
      cls=None
      
      dataspecs = self._getVectorDataSpecs(sdlLyr)

      if dataspecs: 
         proj = self._createProjectionInfo(sdlLyr.epsgcode)
         
         subsetFname = None                      
         meta = self._getLayerMetadata(sdlLyr, metalines=attMeta, 
                                       isVector=True)
         
         if (sdlLyr.getUserId() == CT_USER
             and sdlLyr.title.startswith(CT_SPECIES_KEYWORD)):
            cls = self._createStyleClasses(sdlLyr.name, CT_SPECIES_LAYER_STYLES)
   
         else:         
            if (sdlLyr.ogrType == ogr.wkbPoint 
                or sdlLyr.ogrType == ogr.wkbMultiPoint):
               style = self._createStyle(POINT_SYMBOL, POINT_SIZE, 
                                         colorstr=DEFAULT_POINT_COLOR)
            elif (sdlLyr.ogrType == ogr.wkbLineString 
                  or sdlLyr.ogrType == ogr.wkbMultiLineString):
               style = self._createStyle(LINE_SYMBOL, LINE_SIZE, 
                                         colorstr=DEFAULT_LINE_COLOR)
            elif (sdlLyr.ogrType == ogr.wkbPolygon 
                  or sdlLyr.ogrType == ogr.wkbMultiPolygon):
               style = self._createStyle(POLYGON_SYMBOL, POLYGON_SIZE, 
                                         outlinecolorstr=DEFAULT_LINE_COLOR)
            cls = self._createClass(sdlLyr.name, [style])
         
      lyr = self._createLayer(sdlLyr, dataspecs, proj, meta, cls=cls)
      return lyr

# ...............................................
   def _createRasterLayer(self, sdlLyr, paletteName):
      dataspecs = self._getRasterDataSpecs(sdlLyr, paletteName)
      proj = self._createProjectionInfo(sdlLyr.epsgcode)
      rasterMetadata = [# following 3 required in MS 6.0+
                        'wcs_label  \"%s\"' % sdlLyr.name,
                        'wcs_rangeset_name  \"%s\"' % sdlLyr.name,
                        'wcs_rangeset_label \"%s\"' % sdlLyr.name]
      # TODO: Where/how is this set??
#       if sdlLyr.nodataVal is not None:
#          rasterMetadata.append('rangeset_nullvalue  %s' 
#                                % str(sdlLyr.nodataVal))
         
      meta = self._getLayerMetadata(sdlLyr, metalines=rasterMetadata)
      
      lyr = self._createLayer(sdlLyr, dataspecs, proj, meta)
      return lyr
                
# ...............................................
   def _createLayer(self, sdlLyr, dataspecs, proj, meta, cls=None):
      lyr = ''
      if dataspecs:
         lyr = '\n'.join([lyr, '   LAYER'])
         lyr = '\n'.join([lyr, '      NAME  \"%s\"' % sdlLyr.name])
         lyr = '\n'.join([lyr, '      TYPE  %s' % self._getMSText(sdlLyr)])
         lyr = '\n'.join([lyr, '      STATUS  ON'])
         lyr = '\n'.join([lyr, '      OPACITY 100'])
#          lyr = '\n'.join([lyr, '      DUMP  TRUE'])
         
         ext = sdlLyr.getSSVExtentString()
         if ext is not None:
            lyr = '\n'.join([lyr, '      EXTENT  %s' % ext])
         
         lyr = '\n'.join([lyr, proj])
         lyr = '\n'.join([lyr, meta])
         lyr = '\n'.join([lyr, dataspecs])
         if cls is not None:
            lyr = '\n'.join([lyr, cls])
         lyr = '\n'.join([lyr, '   END'])
      return lyr
            
     
# ...............................................
   def _createBlueMarbleLayer(self):
      fname = os.path.join(IMAGE_PATH, BLUE_MARBLE_IMAGE)
      lyr = ''
      lyr = '\n'.join([lyr, '   LAYER'])
      lyr = '\n'.join([lyr, '      NAME  bmng'])
      lyr = '\n'.join([lyr, '      TYPE  RASTER'])
      lyr = '\n'.join([lyr, '      DATA  \"%s\"' % fname])
      lyr = '\n'.join([lyr, '      STATUS  ON'])
#       lyr = '\n'.join([lyr, '      DUMP  TRUE'])
      lyr = '\n'.join([lyr, '      EXTENT  -180 -90 180 90']) 
      lyr = '\n'.join([lyr, '      METADATA'])
      lyr = '\n'.join([lyr, '         ows_name   \"NASA blue marble\"'])
      lyr = '\n'.join([lyr, '         ows_title  \"NASA Blue Marble Next Generation\"'])
      lyr = '\n'.join([lyr, '         author     \"NASA\"'])
      lyr = '\n'.join([lyr, '      END'])
      lyr = '\n'.join([lyr, '   END'])
      return lyr

# ...............................................
   def _createClass(self, name=None, styles=[], useCTClassGroups=False):
      cls = ''
      cls = '\n'.join([cls, '      CLASS'])
      if name is not None:
         cls = '\n'.join([cls, '         NAME   %s' % name])
      if useCTClassGroups:
         cls = '\n'.join([cls, '         GROUP   %s' % name])
      for stl in styles:
         cls = '\n'.join([cls, stl])
      cls = '\n'.join([cls, '      END'])
      return cls
      
# ...............................................
   def _createStyle(self, symbol, size, colorstr=None, outlinecolorstr=None):
      style = ''  
      style = '\n'.join([style, '         STYLE' ])
      # if NOT polygon
      if symbol is not None:
         style = '\n'.join([style, '            SYMBOL   \"%s\"' % symbol])
         style = '\n'.join([style, '            SIZE   %d' % size])
      else:
         style = '\n'.join([style, '            WIDTH   %d' % size])
         
      if colorstr is not None:
         (r, g, b) = self._HTMLColorToRGB(colorstr)
         style = '\n'.join([style, '            COLOR   %d  %d  %d' % (r, g, b) ])
         
      if outlinecolorstr is not None:
         (r, g, b) = self._HTMLColorToRGB(outlinecolorstr)
         style = '\n'.join([style, '            OUTLINECOLOR   %d  %d  %d' % (r, g, b) ])
      style = '\n'.join([style, '         END' ])
      return style

# ...............................................
   def _createStyleClasses(self, name, styles):
      classes = ''
      for clsgroup, style in styles.iteritems():
         # first class is default 
         if len(classes) == 0: 
            classes = '\n'.join([classes, '      CLASSGROUP \"%s\"' % clsgroup])
         classes = '\n'.join([classes, '      CLASS'])
         classes = '\n'.join([classes, '         NAME   \"%s\"' % name])
         classes = '\n'.join([classes, '         GROUP   \"%s\"' % clsgroup])
         classes = '\n'.join([classes, '         STYLE'])
         classes = '\n'.join([classes, style])
         classes = '\n'.join([classes, '         END'])
         classes = '\n'.join([classes, '      END'])
      return classes

# ...............................................
   def _createProjectionInfo(self, epsgcode):
      prj = ''
      prj = '\n'.join([prj, '      PROJECTION'])
      prj = '\n'.join([prj, '         \"init=epsg:%s\"' % epsgcode])
      prj = '\n'.join([prj, '      END'])
      return prj
      
# ...............................................
   def _getLayerMetadata(self, sdlLyr, metalines=[], isVector=False):
      meta = ''
      meta = '\n'.join([meta, '      METADATA'])
      try:
         lyrTitle = sdlLyr.lyrMetadata[ServiceObject.META_TITLE]
      except:
         lyrTitle = None
      # DUMP True deprecated in Mapserver 6.0, replaced by
      if isVector:
         meta = '\n'.join([meta, '         gml_geometries \"geom\"'])
         meta = '\n'.join([meta, '         gml_geom_type \"point\"'])
         meta = '\n'.join([meta, '         gml_include_items \"all\"'])
      # ows_ used in metadata for multiple OGC services
      meta = '\n'.join([meta, '         ows_name  \"%s\"' % sdlLyr.name])
      if lyrTitle is not None:
         meta = '\n'.join([meta, '         ows_title  \"%s\"' % lyrTitle])
      for line in metalines:
         meta = '\n'.join([meta, '         %s' % line])
      meta = '\n'.join([meta, '      END'])
      return meta
      
# ...............................................
   def _getVectorDataSpecs(self, sdlLyr):
      dataspecs = None
      # limit to 1000 features for archive point data
      if (isinstance(sdlLyr, OccurrenceLayer) and
          sdlLyr.getUserId() == PUBLIC_USER and 
          sdlLyr.queryCount > POINT_COUNT_MAX):
         dlocation = sdlLyr.getDLocation(subset=True)
         if not os.path.exists(dlocation):
            dlocation = sdlLyr.getDLocation()
      else:
         dlocation = sdlLyr.getDLocation()
         
      if dlocation is not None and os.path.exists(dlocation):
         dataspecs = '      CONNECTIONTYPE  OGR'
         dataspecs = '\n'.join([dataspecs, '      CONNECTION  \"%s\"' % dlocation])
         dataspecs = '\n'.join([dataspecs, '      TEMPLATE \"%s\"' % QUERY_TEMPLATE])
         dataspecs = '\n'.join([dataspecs, '      TOLERANCE  %d' % QUERY_TOLERANCE])
         dataspecs = '\n'.join([dataspecs, '      TOLERANCEUNITS  pixels'])
      return dataspecs

# ...............................................
   def _getRasterDataSpecs(self, sdlLyr, paletteName):
#      datafname = os.path.join(sdlLyr.getAbsolutePath(), sdlLyr.filename)
      dataspecs = None
      dlocation = sdlLyr.getDLocation()
      if dlocation is not None and os.path.exists(dlocation):
         dataspecs = '      DATA  \"%s\"' % dlocation
         if sdlLyr.mapUnits is not None:
            dataspecs = '\n'.join([dataspecs, '      UNITS  %s' % 
                                   sdlLyr.mapUnits.upper()])
         dataspecs = '\n'.join([dataspecs, '      OFFSITE  0  0  0'])

         if sdlLyr.nodataVal is None:
            sdlLyr.populateStats()
         dataspecs = '\n'.join([dataspecs, '      PROCESSING \"NODATA=%s\"' 
                                % str(sdlLyr.nodataVal)])
         # SDM projections are always scaled b/w 0 and 100
         if isinstance(sdlLyr, SDMProjection):
            vmin = 0
            vmax = 100
         else: 
            vmin = sdlLyr.minVal
            vmax = sdlLyr.maxVal
         rampClass = self._createColorRamp(vmin, vmax, paletteName)
         dataspecs = '\n'.join([dataspecs, rampClass])

#          # Continuous data
#          if not(sdlLyr.getIsDiscreteData()):
#             rampClass = self._createColorRamp(vmin, vmax, paletteName)
#             dataspecs = '\n'.join([dataspecs, rampClass])
#          # Classified data (8-bit projections)
#          else:
#             vals = sdlLyr.getHistogram()
#             classdata = self._getDiscreteClasses(vals, paletteName)
#             if classdata is not None:
#                dataspecs = '\n'.join([dataspecs, classdata])
                  
      return dataspecs
   
# ...............................................
   def _getDiscreteClasses(self, vals, paletteName):
      if vals is not None:
         bins = self._createDiscreteBins(vals, paletteName)
         classdata = ''
         for b in bins:
            classdata = '\n'.join([classdata, b])
         return classdata
      else:
         return None
            
# ...............................................
   def _getMSText(self, sdllyr):
      if isinstance(sdllyr, Raster):
         return 'RASTER'
      elif isinstance(sdllyr, Vector):
         if (sdllyr.ogrType == ogr.wkbPoint or 
             sdllyr.ogrType == ogr.wkbMultiPoint):
            return 'POINT'
         elif (sdllyr.ogrType == ogr.wkbLineString or
               sdllyr.ogrType == ogr.wkbMultiLineString):
            return 'LINE'
         elif (sdllyr.ogrType == ogr.wkbPolygon or 
               sdllyr.ogrType == ogr.wkbMultiPolygon):
            return 'POLYGON'
      else:
         raise Exception('Unknown _Layer type')
   
# ...............................................
   def _HTMLColorToRGB(self, colorstring):
      """ convert #RRGGBB to an (R, G, B) tuple """
      colorstring = self._checkHTMLColor(colorstring)
      if colorstring is None:
         colorstring = '#777777'
      r, g, b = colorstring[1:3], colorstring[3:5], colorstring[5:]
      r, g, b = [int(n, 16) for n in (r, g, b)]
      return (r, g, b)

# ...............................................
   def _paletteToRGBStartEnd(self, palettename):
      """ 
      @summary: convert named palettes to a start/end (R, G, B, R, G, B) tuple 
      @note: possible palette names are gray, red, green, blue, yellow, fuschia, 
             aqua, bluered, bluegreen, greenred
      """
      if palettename in ('gray', 'red', 'green', 'blue', 'yellow', 'fuschia', 'aqua'):
         startColor = '#000000'
         if palettename == 'gray':
            endColor = '#FFFFFF'
         elif palettename == 'red':
            endColor = '#FF0000'
         elif palettename == 'green':
            endColor = '#00FF00'
         elif palettename == 'blue':
            endColor = '#0000FF'
         elif palettename == 'yellow':
            endColor = '#FFFF00'
         elif palettename == 'fuschia':
            endColor = '#FF00FF'
         elif palettename == 'aqua':
            endColor = '#00FFFF'
      elif palettename in ('bluered', 'bluegreen'):
         startColor = '#0000FF'
         if palettename == 'bluered':
            endColor = '#FF0000'
         elif palettename == 'bluegreen':
            endColor = '#00FF00'
      elif palettename  == 'greenred':
         startColor = '#00FF00'
         endColor == '#FF0000'
          
      r, g, b = startColor[1:3], startColor[3:5], startColor[5:]
      r1, g1, b1 = [int(n, 16) for n in (r, g, b)]
      
      r, g, b = endColor[1:3], endColor[3:5], endColor[5:]
      r2, g2, b2 = [int(n, 16) for n in (r, g, b)]
      
      return (r1, g1, b1, r2, g2, b2)

# ...............................................
   def _checkHTMLColor(self, colorstring):
      """ ensure #RRGGBB format """
      validChars = ['a', 'b', 'c', 'd', 'e', 'f', 'A', 'B', 'C', 'D', 'E', 'F']
      colorstring = colorstring.strip()
      if len(colorstring) == 6:
         colorstring = '#' + colorstring
      if len(colorstring) == 7:
         if colorstring[0] != '#':
            print('input %s is not in #RRGGBB format' % colorstring)
            return None
         
         for i in range(len(colorstring)):
            if i > 0:
               if not(colorstring[i].isdigit()) and validChars.count(colorstring[i]) == 0:
                  print('input %s is not a valid hex color' % colorstring)
                  return None
      else:
         print('input %s is not in #RRGGBB format' % colorstring)
         return None
      return colorstring

# ...............................................
   def _createDiscreteBins(self, vals, paletteName='gray'):
      bins = []
      numBins = len(vals) + 1
      palette = colorPalette(n=numBins, ptype=paletteName)
      for i in range(len(vals)):
         expr = '([pixel] = %g)' % (vals[i])
         name = 'Value = %g' % (vals[i])
         # skip the first color, so that first class is not black
         bins.append(self._createClassBin(expr, name, palette[i+1]))
      return bins

# ...............................................
   def _createColorRamp(self, vmin, vmax, paletteName='gray'):
      rgbs = self._paletteToRGBStartEnd(paletteName)
      colorstr = '%s %s %s %s %s %s' % (rgbs[0],rgbs[1],rgbs[2], rgbs[3],rgbs[4],rgbs[5])
      ramp = ''
      ramp = '\n'.join([ramp, '      CLASS'])
      ramp = '\n'.join([ramp, '         EXPRESSION ([pixel] >= %s AND [pixel] <= %s)' 
                       % (str(vmin), str(vmax))])
      ramp = '\n'.join([ramp, '         STYLE'])
      ramp = '\n'.join([ramp, '            COLORRANGE %s' % colorstr])
      ramp = '\n'.join([ramp, '            DATARANGE %s  %s' % (str(vmin), str(vmax))])
      ramp = '\n'.join([ramp, '            RANGEITEM \"pixel\"'])
      ramp = '\n'.join([ramp, '         END'])      
      ramp = '\n'.join([ramp, '      END'])
      return ramp

# ...............................................
#    def _createContinousBins(self, vmin, vmax, vnodata, paletteName='gray'):
#       bins = ''
#       rng = vmax - vmin
#       if rng < 10:
#          numBins = 10
#       else:
#          # Changed from 128 - unable to visually distinguish that many
#          numBins = min(int(rng), 32)
#       palette = colorPalette(n=numBins, ptype=paletteName)
#       
#       mmscale = 1.0
#       try:
#          mmscale = (rng)/((len(palette)-1)*1.0)
#       except:
#          mmscale = (1.0)/((len(palette)-1)*1.0)
#          
#       # lowest values class
#       expr, name = self._getRangeExpr(None, vmin + mmscale, vmin, vmax)
#       bins = '\n'.join([bins, self._createClassBin(expr, name, palette[0])])
#       # middle classes
#       for i in range(1, numBins-1):
#          lo = vmin + mmscale * (i * 1.0)
#          hi = vmin + mmscale * ((i+1) * 1.0)
#          expr, name = self._getRangeExpr(lo, hi, vmin, vmax)
#          bins = '\n'.join([bins, self._createClassBin(expr, name, palette[i])])
#       # highest values class
#       expr, name = self._getRangeExpr(vmax - mmscale, None, vmin, vmax)
#       bins = '\n'.join([bins, self._createClassBin(expr, name, palette[numBins])])
#       
#       return bins, numBins

# ...............................................
   def _getRangeExpr(self, lo, hi, vmin, vmax):
      if lo is None:
         lo = vmin
         
      if hi is None: 
         expr = '([pixel] >= %g AND [pixel] <= %g)' % (lo, vmax)
         name = '%g <= Value <= %g' % (lo, vmax)
      else:
         expr = '([pixel] >= %g AND [pixel] < %g)' % (lo, hi)
         name = '%g <= Value < %g' % (lo, hi) 

      return expr, name

# ...............................................
   def _createClassBin(self, expr, name, clr):
      rgbstr = '%s %s %s' % (clr[0],clr[1],clr[2])
      bin = ''
      bin = '\n'.join([bin, '      CLASS'])
      bin = '\n'.join([bin, '         NAME \"%s\"' % name])
      bin = '\n'.join([bin, '         EXPRESSION %s' % expr])
      bin = '\n'.join([bin, '         STYLE'])
      bin = '\n'.join([bin, '            COLOR %s' % rgbstr])
      bin = '\n'.join([bin, '         END'])      
      bin = '\n'.join([bin, '      END'])
      return bin

# ...............................................
   def _getRasterInfo(self, srcpath, getHisto=False):
      """
      @summary: Uses GDAL to retrieve the minimum and maximum values from a 
                RASTER data source.  Note that for some types of data source 
                (like ASCII grids), this process can be quite slow.
      @param srcpath: full path to the raster dataset
      @return: list of [min,max,nodata]
      """
      try:
         src = gdal.Open(srcpath, gdalconst.GA_ReadOnly)
      except Exception, e:
         print('Exception opening %s (%s)' % (srcpath,str(e)) )
         return (None, None, None, None)

      if src is None:
         print('%s is not a valid image file' % srcpath )
         return (None, None, None, None)

      srcbnd = src.GetRasterBand(1)
      (vmin, vmax) = srcbnd.ComputeRasterMinMax()
      nodata = srcbnd.GetNoDataValue()
      if nodata is None and vmin >= 0:
         nodata = 0
      vals = []
      
      # Get histogram only for 8bit data (projections)
      if getHisto and srcbnd.DataType == gdalconst.GDT_Byte:
         hist = srcbnd.GetHistogram()
         for i in range(len(hist)):
            if i > 0 and i != nodata and hist[i] > 0:
               vals.append(i)
               
      return (vmin, vmax, nodata, vals)
            
# ...............................................

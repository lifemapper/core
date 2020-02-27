"""
"""
# import collections
import os

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (DEFAULT_GLOBAL_EXTENT, DEFAULT_EPSG)
from LmServer.base.layer import _Layer, Raster, Vector
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.service_object import ServiceObject
from LmServer.common.color_palette import ColorPalette
from LmServer.common.lmconstants import (MAP_TEMPLATE, QUERY_TEMPLATE,
         MapPrefix, LMFileType, IMAGE_PATH, BLUE_MARBLE_IMAGE, POINT_SYMBOL,
         POINT_SIZE, LINE_SYMBOL, LINE_SIZE, POLYGON_SYMBOL, POLYGON_SIZE,
         QUERY_TOLERANCE, SYMBOL_FILENAME, DEFAULT_POINT_COLOR,
         DEFAULT_LINE_COLOR, DEFAULT_PROJECTION_PALETTE, LMServiceType,
         DEFAULT_ENVIRONMENTAL_PALETTE, PROJ_LIB, SCALE_PROJECTION_MINIMUM,
   SCALE_PROJECTION_MAXIMUM)
from LmServer.common.localconstants import (PUBLIC_USER, POINT_COUNT_MAX)
from LmServer.legion.occ_layer import OccurrenceLayer
from LmServer.legion.sdm_proj import SDMProjection
from osgeo import gdal, gdalconst, ogr


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
        @copydoc LmServer.base.lmobj.LMSpatialObject::__init__()
        @param name: name or code for this layerset
        @param title: (optional) human readable title of this layerset
        @param keywords: (optional) sequence of keywords for this layerset
        @param layers: (optional) list of layers 
        """
        LMSpatialObject.__init__(self, epsgcode, bbox, mapunits)

        # # Name or code identifying this set of layers
        self.name = name
        # # Title for this set of layers
        self.title = title
        # Keywords for the layerset as a whole
        self._setKeywords(keywords)
        self._layers = []
        # # List of Raster or Vector objects for this LayerSet'
        # # Also sets epsg and bbox
        # # If no layers, initializes to empty list
        self._setLayers(layers)

    # ...............................................
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
        srs = self.create_srs_from_epsg()
        return srs

# # .............................................................................
# # MutableSequence methods
# # .............................................................................
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
    def getLayer(self, metadata_url):
        """
        @summary Gets a layer from the LayerSet with the specified mapPrefix
        @param metadata_url: metadata_url for which to find matching layer
        @return the layer object with the given metadata_url, None if there is no 
                matching layer
        """
        for lyr in self._layers:
            if lyr.metadata_url == metadata_url:
                return lyr
        return None

    # .............................................................................
    def add_layer(self, lyr):
        """
        @note: metadata_url is used for identification - ensuring that a layer is 
               not duplicated in the layerset.  MetadataUrl should be (relatively)
               unique, unlike mapPrefix which is constructed differently for each
               layerset (and mapfile) that contains a layer.
        """
        if isinstance(lyr, _Layer):
            if self.getLayer(lyr.metadata_url) is None:
                if self._epsg is None or self._epsg == lyr.epsgcode:
                    self._layers.append(lyr)
                    if self._epsg is None:
                        self._setEPSG(lyr.epsgcode)
                else:
                    raise LMError('Invalid layer SRS {} for layerset with SRS {}'
                                  .format(lyr.epsgcode, self._epsg))
        else:
            raise LMError(['Cannot add {} as a Layer'.format(type(lyr))])

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

    # .............................................................................
    @property
    def intersect_keywords(self):
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

    @property
    def union_keywords(self):
        """
        @summary Gets all keywords that occur in layers in the scenario
        @return Set of keywords
        """
        s = set()
        for i in range(len(self._layers)):
            s = s.union(self._layers[i].keywords)
        return s

    # .............................................................................
    def _getLayers(self):
        return self._layers

    def _setLayers(self, lyrs):
        if lyrs is not None:
            for lyr in lyrs:
                self.add_layer(lyr)
        else:
            self._layers = []
        bboxes = [lyr.bbox for lyr in self._layers]
        bbox = self.union_bboxes(bboxes)
        self._set_bbox(bbox)

    def _getLayerCount(self):
        count = 0
        if self._layers is not None:
            count = len(self._layers)
        return count

    # .............................................................................

    # # Set of words describing layerset as a whole
    keywords = property(_getKeywords, _setKeywords)
    layers = property(_getLayers, _setLayers)
    # property counting the actual layer objects present
    count = property (_getLayerCount)

    # Return tuple of (minx, miny, maxx, maxy)
    @property
    def union_bboxes(self):
        bboxes = [lyr.bbox for lyr in self._layers]
        return self.union_bboxes(bboxes)
        
    @property
    def intersect_bboxes(self):
        bboxes = [lyr.bbox for lyr in self._layers]
        return self.intersect_bboxes(bboxes)


# .............................................................................
# .............................................................................
class MapLayerSet(_LayerSet, ServiceObject):
    """
    Superclass of Scenario.  
    @todo: extend as collections.MutableSequence subclass
    @note: mapcode should be required
    """

    # .............................................................................
    # Constructor
    # .............................................................................
    def __init__(self, mapname, title=None, url=None, dlocation=None, 
                 keywords=None, epsgcode=None, layers=None, user_id=None, 
                 db_id=None, mod_time=None, bbox=None, mapunits=None,
                 # This must be specified
                 service_type=LMServiceType.LAYERSETS,
                 mapType=LMFileType.OTHER_MAP):
        """
        @summary Constructor for the LayerSet class
        @copydoc LmServer.base.layerset._LayerSet::__init__()
        @copydoc LmServer.base.service_object.ServiceObject::__init__()
        @param mapname: mapname or code for this layerset
        @param layers: list of layers 
        @param dbid: database id of the object, occsetId for SDM_MAP layersets, 
               gridsetId for RAD_MAP layersets, scen_code for Scenarios 
        """
        if service_type is None:
            raise LMError('Failed to specify service_type for MapLayerSet superclass')
        _LayerSet.__init__(self, mapname, title=title, keywords=keywords,
                           epsgcode=epsgcode, layers=layers, bbox=bbox, 
                           mapunits=mapunits)
        ServiceObject.__init__(self, user_id, db_id, service_type, metadata_url=url,
                               mod_time=mod_time)
        self._map_filename = dlocation
        self._mapType = mapType
        self._map_prefix = None

    # ...............................................
    # TODO: remove this property
    @property
    def mapPrefix(self):
        self.set_map_prefix()
        return self._map_prefix

    def set_map_prefix(self, mapprefix=None):
        if mapprefix is None:
            mapprefix = self._earl_jr.constructMapPrefixNew(
                ftype=LMFileType.OTHER_MAP, objCode=self.get_id(), 
                mapname=self.map_name, usr=self._user_id, epsg=self.epsgcode)
        self._map_prefix = mapprefix

    # ...............................................
    def create_local_map_filename(self):
        """
        @summary: Full mapfile with path, containing this layer.
        @note: This method is overridden in Scenario  
        """
        fname = None
        if self._mapType == LMFileType.SDM_MAP:
            fname = self._earl_jr.create_filename(self._mapType,
                                               occsetId=self.get_id(),
                                               usr=self._user_id)
        # This will not occur here? only in
        # LmServer.base.legion.gridset.Gridset
        elif self._mapType == LMFileType.RAD_MAP:
            fname = self._earl_jr.create_filename(self._mapType,
                                               gridsetId=self.get_id(),
                                               usr=self._user_id)
        elif self._mapType == LMFileType.OTHER_MAP:
            fname = self._earl_jr.create_filename(self._mapType,
                                               usr=self._user_id,
                                               epsg=self._epsg)
        else:
            print(('Unsupported mapType {}'.format(self._mapType)))
        return fname

    # ...............................................
    def set_local_map_filename(self, mapfname=None):
        """
        @note: Overrides existing _map_filename
        @summary: Set absolute mapfilename containing all layers for this User/EPSG. 
        """
        if mapfname is None:
            mapfname = self.create_local_map_filename()
        self._map_filename = mapfname

    # ...............................................
    def clear_local_mapfile(self):
        """
        @summary: Delete the mapfile containing this layer
        """
        if self._map_filename is None:
            self.set_local_map_filename()
        success, _ = self.deleteFile(self._map_filename)

    # ...............................................
    @property
    def mapFilename(self):
        if self._map_filename is None:
            self.set_local_map_filename()
        return self._map_filename

    # ...............................................
    @property
    def mapAbsolutePath(self):
        pth = None
        if self._map_filename is not None:
            pth, _ = os.path.split(self._map_filename)
        return pth

    # ...............................................
    @property
    def map_name(self):
        mapname = None
        if self._map_filename is not None:
            _, mapfname = os.path.split(self._map_filename)
            mapname, _ = os.path.splitext(mapfname)
        return mapname

    # .............................................................................
    def _get_mapset_url(self):
        """
        @note: This method is overridden in Scenario  
        """
        url = None
        self.set_local_map_filename()
        if self._mapType == LMFileType.SDM_MAP:
            for lyr in self.layers:
                if isinstance(lyr, OccurrenceLayer):
                    url = lyr.metadata_url
        elif self._mapType == LMFileType.RAD_MAP:
            print('RAD_MAP is not yet implemented')
        elif self._mapType == LMFileType.OTHER_MAP:
            print('OTHER_MAP is not yet implemented')
        else:
            print(('Unsupported mapType {}'.format(self._mapType)))
        return url

    # .............................................................................
    def writeMap(self, template=MAP_TEMPLATE):
        """
        @summary Create a mapfile by replacing strings in a template mapfile 
                 with text created for the layer set.
        @param mapcode: Prefix for the mapfilename
        @param template: Template mapfile 
        @return a string representing a mapfile 
        """
        self.set_local_map_filename()
        # if mapfile does not exist, create service from database, then write file
        if not(os.path.exists(self._map_filename)):
            try:
                layers = self._createLayers()
                mapTemplate = self._earl_jr.getMapFilenameFromMapname(template)
                mapstr = self._getBaseMap(mapTemplate)
                onlineUrl = self._get_mapset_url()
                mapstr = self._addMapBaseAttributes(mapstr, onlineUrl)
                mapstr = mapstr.replace('##_LAYERS_##', layers)
            except Exception as e:
                raise

            try:
                self._writeBaseMap(mapstr)
            except Exception as e:
                raise LMError('Failed to write {}; {}'.format(self._map_filename, e))

    # ...............................................
    def _writeBaseMap(self, mapstr):
        self.ready_filename(self._map_filename, overwrite=True)

        try:
            f = open(self._map_filename, 'w')
            # make sure that group is set correctly
            f.write(mapstr)
            f.close()
            print(('Wrote {}'.format((self._map_filename))))
        except Exception as e:
            raise LMError('Failed to write {}; {}'.format(self._map_filename, e))

    # ...............................................
    def _getBaseMap(self, fname):
        # TODO: in python 2.6, use 'with open(fname, 'r'):'
        try:
            with open(fname, 'r') as in_file:
                base_map = in_file.read()
        except Exception as e:
            raise LMError('Failed to read {}; {}'.format(fname, e))
        return base_map

# ...............................................
    def _addMapBaseAttributes(self, mapstr, onlineUrl):
        """
        @summary Set map attributes on the map from the LayerSet
        @param mapstr: string for a mapserver mapfile to modify
        """
        if self.epsgcode == DEFAULT_EPSG:
            mbbox = DEFAULT_GLOBAL_EXTENT
        else:
            mbbox = self.unionBounds
        boundstr = LMSpatialObject.get_extent_string(mbbox, separator='  ')
        mapprj = self._createProjectionInfo(self.epsgcode)
        parts = ['  NAME        {}'.format(self.map_name),
                 # All raster/vector filepaths will be relative to mapfile path
                 '  SHAPEPATH \"{}\"'.format(self.mapAbsolutePath),
                 '  EXTENT      {}'.format(boundstr),
                 '  UNITS       {}'.format(self.mapUnits),
                 '  SYMBOLSET \"{}\"'.format(SYMBOL_FILENAME),
                 '  CONFIG \"PROJ_LIB\" \"{}\"'.format(PROJ_LIB),
                 mapprj
                ]
        mapstuff = '\n'.join(parts)
        mapstr = mapstr.replace('##_MAPSTUFF_##', mapstuff)

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
        else:
            label = 'Lifemapper Data Service'
        parts = ['      METADATA',
                 '         ows_srs     \"epsg:{}\"'.format(self.epsgcode),
                 '         ows_enable_request   \"*\"',
                 '         ows_label   \"{}\"'.format(label),
                 '         ows_title   \"{}\"'.format(self.title),
                 '         ows_onlineresource   \"{}\"'.format(onlineUrl),
                 '      END']
        meta = '\n'.join(parts)
        mapstr = mapstr.replace('##_MAP_METADATA_##', meta)
        return mapstr

    # ...............................................
    def _createLayers(self):
        topLyrStr = ''
        midLyrStr = ''
        baseLyrStr = ''

        # Vector layers are described first, so drawn on top
        for lyr in self.layers:
                # todo: Check if lyr is OccurrenceLayer, respond differently to other
                #       types of vector layers.
                #       Maybe use ServiceObject._service_type for display options
            if isinstance(lyr, Vector):
                lyrstr = self._createVectorLayer(lyr)
                topLyrStr = '\n'.join([topLyrStr, lyrstr])

            elif isinstance(lyr, Raster):
                # projections are below vector layers and above the base layer
                if isinstance(lyr, SDMProjection):
                    palette = DEFAULT_PROJECTION_PALETTE
                    lyrstr = self._createRasterLayer(lyr, palette)
                    midLyrStr = '\n'.join([midLyrStr, lyrstr])
                else:
                    palette = DEFAULT_ENVIRONMENTAL_PALETTE
                    lyrstr = self._createRasterLayer(lyr, palette)
                    baseLyrStr = '\n'.join([baseLyrStr, lyrstr])

        maplayers = '\n'.join([topLyrStr, midLyrStr, baseLyrStr])

        # Add bluemarble image to Data/Occurrence Map Services
        if self.epsgcode == DEFAULT_EPSG:
            backlyr = self._createBlueMarbleLayer()
            maplayers = '\n'.join([maplayers, backlyr])

        return maplayers

    # ...............................................
    def _createVectorLayer(self, sdlLyr):
        attMeta = []
        proj = None
        meta = None
        cls = None

        dataspecs = self._getVectorDataSpecs(sdlLyr)

        if dataspecs:
            proj = self._createProjectionInfo(sdlLyr.epsgcode)

            meta = self._getLayerMetadata(sdlLyr, metalines=attMeta,
                                          isVector=True)
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
        rasterMetadata = ['wcs_label  \"{}\"'.format(sdlLyr.name),
                          'wcs_rangeset_name  \"{}\"'.format(sdlLyr.name),
                          'wcs_rangeset_label \"{}\"'.format(sdlLyr.name)]
        # TODO: Where/how is this set??
        #       if sdlLyr.nodata_val is not None:
        #          rasterMetadata.append('rangeset_nullvalue  {}'
        #                               .format(str(sdlLyr.nodata_val))

        meta = self._getLayerMetadata(sdlLyr, metalines=rasterMetadata)

        lyr = self._createLayer(sdlLyr, dataspecs, proj, meta)
        return lyr

    # ...............................................
    def _createLayer(self, sdlLyr, dataspecs, proj, meta, cls=None):
        lyr = ''
        if dataspecs:
            parts = ['   LAYER',
                     '      NAME  \"{}\"'.format(sdlLyr.name),
                     '      TYPE  {}'.format(self._getMSText(sdlLyr)),
                     '      STATUS  OFF',
                     '      OPACITY 100',
                     ]
            lyr = '\n'.join(parts)

            ext = sdlLyr.getSSVExtentString()
            if ext is not None:
                lyr = '\n'.join([lyr, '      EXTENT  {}'.format(ext)])

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
        boundstr = LMSpatialObject.get_extent_string(DEFAULT_GLOBAL_EXTENT,
                                                     separator='  ')
        parts = ['   LAYER',
                 '      NAME  bmng',
                 '      TYPE  RASTER',
                 '      DATA  \"{}\"'.format(fname),
                 '      STATUS  OFF',
                 '      EXTENT  {}'.format(boundstr),
                 '      METADATA',
                 '         ows_name   \"NASA blue marble\"',
                 '         ows_title  \"NASA Blue Marble Next Generation\"',
                 '         author     \"NASA\"',
                 '      END',
                 '   END']
        lyr = '\n'.join(parts)
        return lyr

    # ...............................................
    def _createClass(self, name=None, styles=[], useCTClassGroups=False):
        parts = ['      CLASS']
        if name is not None:
            parts.append('         NAME   {}'.format(name))
        if useCTClassGroups:
            parts.append('         GROUP   {}'.format(name))
        parts.extend(styles)
        parts.append('      END')
        cls = '\n'.join(parts)
        return cls

    # ...............................................
    def _createStyle(self, symbol, size, colorstr=None, outlinecolorstr=None):
        parts = ['         STYLE' ]
        # if NOT polygon
        if symbol is not None:
            parts.extend(['            SYMBOL   \"{}\"'.format(symbol),
                          '            SIZE   {}'.format(size)])
        else:
            parts.append('            WIDTH   {}'.format(size))

        if colorstr is not None:
            (r, g, b) = self._HTMLColorToRGB(colorstr)
            parts.append('            COLOR   {}  {}  {}'.format(r, g, b))

        if outlinecolorstr is not None:
            (r, g, b) = self._HTMLColorToRGB(outlinecolorstr)
            parts.append('            OUTLINECOLOR   {}  {}  {}'.format(r, g, b))
        parts.append('         END')
        style = '\n'.join(parts)
        return style

    # ...............................................
    def _createStyleClasses(self, name, styles):
        parts = []
        for clsgroup, style in styles.items():
            # first class is default
            if len(parts) == 0:
                parts.append('      CLASSGROUP \"{}\"'.format(clsgroup))
            parts.extend(['         CLASS',
                          '            NAME   \"{}\"'.format(name),
                          '            GROUP   \"{}\"'.format(clsgroup),
                          '            STYLE', style,
                          '         END'])
        if len(parts) > 0:
            parts.append('      END')
        classes = '\n'.join(parts)
        return classes

    # ...............................................
    def _createProjectionInfo(self, epsgcode):
        if epsgcode == '4326':
            parts = ['      PROJECTION',
                     '         \"proj=longlat\"',
                     '         \"ellps=WGS84\"',
                     '         \"datum=WGS84\"',
                     '         \"no_defs\"',
                     '      END']
        else:
            parts = ['      PROJECTION',
                     '         \"init=epsg:{}\"'.format(epsgcode),
                     '      END']
        prj = '\n'.join(parts)
        return prj

    # ...............................................
    def _getLayerMetadata(self, sdlLyr, metalines=[], isVector=False):
        parts = ['      METADATA']
        if isVector:
            parts.extend(['         gml_geometries \"geom\"',
                          '         gml_geom_type \"point\"',
                          '         gml_include_items \"all\"'])
        parts.append('         ows_name  \"{}\"'.format(sdlLyr.name))
        try:
            ltitle = sdlLyr.lyr_metadata[ServiceObject.META_TITLE]
            parts.append('         ows_title  \"{}\"'.format(ltitle))
        except:
            pass
        parts.extend(metalines)
        parts.append('      END')
        meta = '\n'.join(parts)
        return meta

    # ...............................................
    def _getRelativePath(self, dlocation):
        os.path.relpath(dlocation, self.mapAbsolutePath)

    # ...............................................
    def _getVectorDataSpecs(self, sdlLyr):
        dataspecs = None
        # limit to 1000 features for archive point data
        if (isinstance(sdlLyr, OccurrenceLayer) and
            sdlLyr.getUserId() == PUBLIC_USER and
            sdlLyr.query_count > POINT_COUNT_MAX):
            dlocation = sdlLyr.get_dlocation(largeFile=False)
            if not os.path.exists(dlocation):
                dlocation = sdlLyr.get_dlocation()
        else:
            dlocation = sdlLyr.get_dlocation()

        if dlocation is not None and os.path.exists(dlocation):
            relpath = os.path.relpath(dlocation, self.mapAbsolutePath)
            parts = ['      CONNECTIONTYPE  OGR',
                     '      CONNECTION    \"{}\"'.format(relpath),
                     '      TEMPLATE      \"{}\"'.format(QUERY_TEMPLATE),
                     '      TOLERANCE       {}'.format(QUERY_TOLERANCE),
                     '      TOLERANCEUNITS  pixels']
            dataspecs = '\n'.join(parts)
        return dataspecs

    # ...............................................
    def _getRasterDataSpecs(self, sdlLyr, paletteName):
        dataspecs = None
        dlocation = sdlLyr.get_dlocation()
        if dlocation is not None and os.path.exists(dlocation):
            relpath = os.path.relpath(dlocation, self.mapAbsolutePath)
            parts = ['      DATA  \"{}\"'.format(relpath)]

            if sdlLyr.mapUnits is not None:
                parts.append('      UNITS  {}'.format(sdlLyr.mapUnits.upper()))
            parts.append('      OFFSITE  0  0  0')

            if sdlLyr.nodata_val is None:
                sdlLyr.populateStats()
            parts.append('      PROCESSING \"NODATA={}\"'.format(sdlLyr.nodata_val))
            # SDM projections are always scaled b/w 0 and 100
            if isinstance(sdlLyr, SDMProjection):
                vmin = SCALE_PROJECTION_MINIMUM + 1
                vmax = SCALE_PROJECTION_MAXIMUM
            else:
                vmin = sdlLyr.minVal
                vmax = sdlLyr.maxVal
            rampClass = self._createColorRamp(vmin, vmax, paletteName)
            parts.append(rampClass)
            dataspecs = '\n'.join(parts)

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
            classdata = '\n'.join(bins)
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
        elif palettename == 'greenred':
            startColor = '#00FF00'
            endColor = '#FF0000'

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
                print(('input {} is not in #RRGGBB format'.format(colorstring)))
                return None

            for i in range(len(colorstring)):
                if i > 0:
                    if not(colorstring[i].isdigit()) and validChars.count(colorstring[i]) == 0:
                        print(('input {} is not a valid hex color'.format(colorstring)))
                        return None
        else:
            print(('input {} is not in #RRGGBB format'.format(colorstring)))
            return None
        return colorstring

    # ...............................................
    def _createDiscreteBins(self, vals, paletteName='gray'):
        bins = []
        numBins = len(vals) + 1
        palette = ColorPalette(n=numBins, ptype=paletteName)
        for i in range(len(vals)):
            expr = '([pixel] = {:g})'.format(vals[i])
            name = 'Value = {:g}'.format(vals[i])
            # skip the first color, so that first class is not black
            bins.append(self._createClassBin(expr, name, palette[i + 1]))
        return bins

    # ...............................................
    def _createColorRamp(self, vmin, vmax, paletteName='gray'):
        rgbs = self._paletteToRGBStartEnd(paletteName)
        colorstr = '{} {} {} {} {} {}'.format(rgbs[0], rgbs[1], rgbs[2],
                                              rgbs[3], rgbs[4], rgbs[5])
        parts = ['      CLASS',
                 '         EXPRESSION ([pixel] >= {} AND [pixel] <= {})'.format(vmin, vmax),
                 '         STYLE',
                 '            COLORRANGE {}'.format(colorstr),
                 '            DATARANGE {}  {}'.format(vmin, vmax),
                 '            RANGEITEM \"pixel\"',
                 '         END',
                 '      END']

        ramp = '\n'.join(parts)
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
            expr = '([pixel] >= {:g} AND [pixel] <= {:g})'.format(lo, vmax)
            name = '{:g} <= Value <= {:g}'.format(lo, vmax)
        else:
            expr = '([pixel] >= {:g} AND [pixel] < {:g})'.format(lo, hi)
            name = '{:g} <= Value < {:g}'.format(lo, hi)

        return expr, name

    # ...............................................
    def _createClassBin(self, expr, name, clr):
        rgb_str = '{} {} {}'.format(clr[0], clr[1], clr[2])
        return """        CLASS
            NAME \"{}\"
            EXPRESSION {}
            STYLE
                COLOR {}
            END
        END""".format(name, expr, rgb_str)

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
        except Exception as e:
            print(('Exception opening {} ({})'.format(srcpath, e)))
            return (None, None, None, None)

        if src is None:
            print(('{} is not a valid image file'.format(srcpath)))
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

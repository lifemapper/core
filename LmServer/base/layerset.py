"""Module containin layerset classes
"""
# import collections
import os

from osgeo import gdal, gdalconst, ogr

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (
    DEFAULT_GLOBAL_EXTENT, DEFAULT_EPSG, ENCODING)
from LmServer.base.layer import Raster, Vector
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.service_object import ServiceObject
from LmServer.common.color_palette import ColorPalette
from LmServer.common.lmconstants import (
    BLUE_MARBLE_IMAGE, DEFAULT_ENVIRONMENTAL_PALETTE, DEFAULT_LINE_COLOR,
    DEFAULT_POINT_COLOR, DEFAULT_PROJECTION_PALETTE, IMAGE_PATH, LINE_SIZE,
    LINE_SYMBOL, LMFileType, LMServiceType, MAP_TEMPLATE, MapPrefix,
    POINT_SIZE, POINT_SYMBOL, POLYGON_SIZE, POLYGON_SYMBOL, PROJ_LIB,
    QUERY_TEMPLATE, QUERY_TOLERANCE, SCALE_PROJECTION_MAXIMUM,
    SCALE_PROJECTION_MINIMUM, SYMBOL_FILENAME, WEB_MERCATOR_EPSG)
from LmServer.common.localconstants import (PUBLIC_USER, POINT_COUNT_MAX)
from LmServer.legion.occ_layer import OccurrenceLayer
from LmServer.legion.sdm_proj import SDMProjection


# .............................................................................
class _LayerSet(LMSpatialObject):
    """Superclass of MapLayerSet

    TODO:
        extend as collections.MutableSequence subclass
    """
    # ................................
    def __init__(self, name, title=None, keywords=None, layers=None,
                 epsg_code=None, bbox=None, map_units=None):
        """Constructor for the _LayerSet class

        Args:
            name: name or code for this layerset
            title: human readable title of this layerset
            keywords: sequence of keywords for this layerset
            layers: list of layers
            epsg_code (int): EPSG code indicating the SRS to use
            bbox: spatial extent of data
                sequence in the form (minX, minY, maxX, maxY)
                or comma-delimited string in the form 'minX, minY, maxX, maxY'
            map_units: units of measurement for the data. These are keywords as
                used in mapserver, choice of LegalMapUnits described in
                    http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
        """
        LMSpatialObject.__init__(self, epsg_code, bbox, map_units)

        # # Name or code identifying this set of layers
        self.name = name
        # # Title for this set of layers
        self.title = title
        # Keywords for the layerset as a whole
        self._set_keyword(keywords)
        self._layers = []
        # # List of Raster or Vector objects for this LayerSet'
        # # Also sets epsg and bbox
        # # If no layers, initializes to empty list
        self._set_layers(layers)

    # ................................
    def _get_units(self):
        """
        Todo:
            Add map_units to Occ table (and Scenario?), handle better on
                construction.
        """
        if self._map_units is None and len(self._layers) > 0:
            self._set_units(self._layers[0].map_units)
        return self._map_units

    # ................................
    def get_srs(self):
        """Get the spatial reference system for the layerset."""
        return self.create_srs_from_epsg()

    # ................................
    def get_layer(self, metadata_url):
        """Gets a layer from the LayerSet with the specified map_prefix

        Args:
            metadata_url: metadata_url for which to find matching layer

        Returns:
            The layer object with the given metadata_url, None if there is no
                matching layer
        """
        for lyr in self._layers:
            if lyr.metadata_url == metadata_url:
                return lyr
        return None

    # ................................
    def add_layer(self, lyr):
        """Adds a layer to the layerset

        Note:
            metadata_url is used for identification - ensuring that a layer is
                not duplicated in the layerset.  MetadataUrl should be
                (relatively) unique, unlike map_prefix which is constructed
                differently for each layerset (and mapfile) that contains a
                layer.
        """
        if isinstance(lyr, Raster) or isinstance(lyr, Vector):
            if self.get_layer(lyr.metadata_url) is None:
                if self._epsg is None or self._epsg == lyr.epsg_code:
                    self._layers.append(lyr)
                    if self._epsg is None:
                        self._set_epsg(lyr.epsg_code)
                else:
                    raise LMError(
                        'Invalid layer SRS {} for layerset with SRS {}'.format(
                            lyr.epsg_code, self._epsg))
        else:
            raise LMError(['Cannot add {} as a Layer'.format(type(lyr))])

    # ................................
    def add_keywords(self, keywords):
        """Adds keywords to the LayerSet object

        Args:
            keywords: List of keywords to add
        """
        if keywords is not None:
            for k in keywords:
                self._keywords.add(k)

    # ................................
    def add_keyword(self, keyword):
        """Adds a keyword to the LayerSet object

        Args:
            keyword: Keyword to add
        """
        if keyword is not None:
            self._keywords.add(keyword)

    # ................................
    def _get_keyword(self):
        """Gets the keywords of the LayerSet

        Returns:
            List of keywords describing the LayerSet
        """
        return self._keywords

    # ................................
    def _set_keyword(self, keywords):
        """Sets the keywords of the LayerSet

        Args:
            keywords: List of keywords that will be associated with the
                LayerSet
        """
        if keywords is not None:
            self._keywords = set(keywords)
        else:
            self._keywords = set()

    # ................................
    @property
    def intersect_keywords(self):
        """Gets keywords common to all layers in the scenario

        Returns:
            Set of keywords
        """
        keyword_set = set()
        for i, layer in enumerate(self._layers):
            if i == 0:
                keyword_set.union(layer.keywords)
            else:
                keyword_set.intersection(layer.keywords)
        return keyword_set

    # ................................
    @property
    def union_keywords(self):
        """Gets all keywords that occur in layers in the scenario

        Returns:
            Set of keywords
        """
        keyword_set = set()
        for layer in self._layers:
            keyword_set = keyword_set.union(layer.keywords)
        return keyword_set

    # ................................
    def _get_layers(self):
        return self._layers

    # ................................
    def _set_layers(self, lyrs):
        if lyrs is not None:
            for lyr in lyrs:
                self.add_layer(lyr)
        else:
            self._layers = []
        # bboxes = [lyr.bbox for lyr in self._layers]
        bbox = self.union_bboxes
        self._set_bbox(bbox)

    # ................................
    def _get_layer_count(self):
        count = 0
        if self._layers is not None:
            count = len(self._layers)
        return count

    # # Set of words describing layerset as a whole
    keywords = property(_get_keyword, _set_keyword)
    layers = property(_get_layers, _set_layers)
    # property counting the actual layer objects present
    count = property(_get_layer_count)

    # ................................
    # Return tuple of (minx, miny, maxx, maxy)
    @property
    def union_bboxes(self):
        """Creates a union of layer bounding boxes."""
        bboxes = [lyr.bbox for lyr in self._layers]
        return LMSpatialObject.union_bboxes(bboxes)

    # ................................
    @property
    def intersect_bboxes(self):
        """Intersects bounding boxes."""
        bboxes = [lyr.bbox for lyr in self._layers]
        newbbox = LMSpatialObject.intersect_bboxes(bboxes)
        return newbbox


# .............................................................................
class MapLayerSet(_LayerSet, ServiceObject):
    """Superclass of Scenario.

    Todo:
        Extend as collections.MutableSequence subclass

    Note:
        mapcode should be required
    """
    # ................................
    def __init__(self, name, title=None, keywords=None, layers=None,
                 epsg_code=None, bbox=None, map_units=None,
                 user_id=None, db_id=None,
                 service_type=LMServiceType.LAYERSETS, metadata_url=None,
                 mod_time=None, dlocation=None, map_type=LMFileType.OTHER_MAP):
        """Constructor for the MapLayerSet class

        Args:
            name: name or code for this layerset
            title: human readable title of this layerset
            keywords: sequence of keywords for this layerset
            layers: list of layers
            epsg_code (int): EPSG code indicating the SRS to use
            bbox: spatial extent of data
                sequence in the form (minX, minY, maxX, maxY)
                or comma-delimited string in the form 'minX, minY, maxX, maxY'
            map_units: units of measurement for the data. These are keywords as
                used in  mapserver, choice of LegalMapUnits described in
                    http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
            user_id: id for the owner of these data
            db_id: database id of the object
            service_type: constant from LMServiceType
            metadata_url: URL for retrieving the metadata
            mod_time: Last modification Time/Date, in MJD format
            dlocation: data location of the mapfile
            map_type: one of LmServer.common.LMFileType.map_types
        """
        if service_type is None:
            raise LMError(
                'Failed to specify service_type for MapLayerSet superclass')
        _LayerSet.__init__(
            self, name, title=title, keywords=keywords, epsg_code=epsg_code,
            layers=layers, bbox=bbox, map_units=map_units)
        ServiceObject.__init__(
            self, user_id, db_id, service_type, metadata_url=metadata_url,
            mod_time=mod_time)
        self._map_filename = dlocation
        self._map_type = map_type
        self._map_prefix = None

    # ................................
    # TODO: remove this property
    @property
    def map_prefix(self):
        """Gets the layerset map prefix."""
        self.set_map_prefix()
        return self._map_prefix

    # ................................
    def set_map_prefix(self, map_prefix=None):
        """Sets the layerset map prefix."""
        if map_prefix is None:
            map_prefix = self._earl_jr.construct_map_prefix_new(
                f_type=LMFileType.OTHER_MAP, obj_code=self.get_id(),
                map_name=self.map_name, usr=self._user_id, epsg=self.epsg_code)
        self._map_prefix = map_prefix

    # ................................
    def create_local_map_filename(self):
        """Full mapfile with path, containing this layer.

        Note:
            This method is overridden in Scenario
        """
        fname = None
        if self._map_type == LMFileType.SDM_MAP:
            fname = self._earl_jr.create_filename(
                self._map_type, occ_set_id=self.get_id(), usr=self._user_id)
        # This will not occur here? only in
        # LmServer.base.legion.gridset.Gridset
        elif self._map_type == LMFileType.RAD_MAP:
            fname = self._earl_jr.create_filename(
                self._map_type, gridset_id=self.get_id(), usr=self._user_id)
        elif self._map_type == LMFileType.OTHER_MAP:
            fname = self._earl_jr.create_filename(
                self._map_type, usr=self._user_id, epsg=self._epsg)
        else:
            print(('Unsupported mapType {}'.format(self._map_type)))
        return fname

    # ................................
    def set_local_map_filename(self, map_fname=None):
        """Set the absolute map filename for all layers for this user / epsg.

        Note:
            Overrides existing _map_filename
        """
        if map_fname is None:
            map_fname = self.create_local_map_filename()
        self._map_filename = map_fname

    # ................................
    def clear_local_mapfile(self):
        """Delete the mapfile containing this layer"""
        if self._map_filename is None:
            self.set_local_map_filename()
        success, _ = self.delete_file(self._map_filename)

    # ................................
    @property
    def map_filename(self):
        """Gets the map filename."""
        if self._map_filename is None:
            self.set_local_map_filename()
        return self._map_filename

    # ................................
    @property
    def map_absolute_path(self):
        """Gets the absolute path to the layer set map."""
        pth = None
        if self._map_filename is not None:
            pth, _ = os.path.split(self._map_filename)
        return pth

    # ................................
    @property
    def map_name(self):
        """Retrieve the layerset map name."""
        mapname = None
        if self._map_filename is not None:
            _, mapfname = os.path.split(self._map_filename)
            mapname, _ = os.path.splitext(mapfname)
        return mapname

    # ................................
    def _get_mapset_url(self):
        url = None
        self.set_local_map_filename()
        if self._map_type == LMFileType.SDM_MAP:
            for lyr in self.layers:
                if isinstance(lyr, OccurrenceLayer):
                    url = lyr.metadata_url
        elif self._map_type == LMFileType.RAD_MAP:
            print('RAD_MAP is not yet implemented')
        elif self._map_type == LMFileType.OTHER_MAP:
            print('OTHER_MAP is not yet implemented')
        else:
            print(('Unsupported mapType {}'.format(self._map_type)))
        return url

    # ................................
    def write_map(self, template=MAP_TEMPLATE):
        """Writes the map file for this layerset.

        Create a mapfile by replacing strings in a template mapfile with text
        created for the layer set.

        Args:
            template: Template mapfile

        Returns:
            A string representing a mapfile
        """
        self.set_local_map_filename()
        # if mapfile does not exist, create service from database, then write
        #    file
        if not os.path.exists(self._map_filename):
            try:
                layers = self._create_layers()
                map_template = self._earl_jr.get_map_filename_from_map_name(
                    template, user_id=self._user_id)
                map_str = self._get_base_map(map_template)
                online_url = self._get_mapset_url()
                map_str = self._add_map_base_attributes(map_str, online_url)
                map_str = map_str.replace('##_LAYERS_##', layers)
            except Exception as err:
                raise LMError(err)

            try:
                self._write_base_map(map_str)
            except Exception as e:
                raise LMError(
                    'Failed to write {}; {}'.format(self._map_filename, e))

    # ................................
    def _write_base_map(self, map_str):
        self.ready_filename(self._map_filename, overwrite=True)

        try:
            # make sure that group is set correctly
            with open(self._map_filename, 'w', encoding=ENCODING) as out_file:
                out_file.write(map_str)
            print(('Wrote {}'.format((self._map_filename))))
        except Exception as e:
            raise LMError(
                'Failed to write {}; {}'.format(self._map_filename, e))

    # ................................
    @staticmethod
    def _get_base_map(fname):
        try:
            with open(fname, 'r', encoding=ENCODING) as in_file:
                base_map = in_file.read()
        except Exception as e:
            raise LMError('Failed to read {}; {}'.format(fname, e))
        return base_map

    # ................................
    def _add_map_base_attributes(self, map_str, online_url):
        """Set map attributes on the map from the LayerSet

        Args:
            map_str: string for a mapserver mapfile to modify
        """
        if self.epsg_code == DEFAULT_EPSG:
            mbbox = DEFAULT_GLOBAL_EXTENT
        else:
            mbbox = self.union_bboxes
        bound_str = LMSpatialObject.get_extent_string(mbbox, separator='  ')
        map_prj = self._create_projection_info(self.epsg_code)
        parts = [
            '    NAME        {}'.format(self.map_name),
            # All raster/vector filepaths will be relative to mapfile path
            '    SHAPEPATH \"{}\"'.format(self.map_absolute_path),
            '    EXTENT      {}'.format(bound_str),
            '    UNITS       {}'.format(self.map_units),
            '    SYMBOLSET \"{}\"'.format(SYMBOL_FILENAME),
            '    CONFIG \"PROJ_LIB\" \"{}\"'.format(PROJ_LIB),
            map_prj
            ]
        map_stuff = '\n'.join(parts)
        map_str = map_str.replace('##_MAPSTUFF_##', map_stuff)

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
        parts = [
            '      METADATA',
            '         ows_srs     \"epsg:{} epsg:{}\"'.format(
                self.epsg_code, WEB_MERCATOR_EPSG),
            '         ows_enable_request   \"*\"',
            '         ows_label   \"{}\"'.format(label),
            '         ows_title   \"{}\"'.format(self.title),
            '         ows_onlineresource   \"{}\"'.format(online_url),
            '      END'
            ]
        meta = '\n'.join(parts)
        map_str = map_str.replace('##_MAP_METADATA_##', meta)
        return map_str

    # ................................
    def _create_layers(self):
        top_lyr_str = ''
        mid_lyr_str = ''
        base_lyr_str = ''

        # Vector layers are described first, so drawn on top
        for lyr in self.layers:
            # todo: Check if lyr is OccurrenceLayer, respond differently to
            #    other types of vector layers.  Maybe use
            #    ServiceObject._service_type for display options
            if isinstance(lyr, Vector):
                lyrstr = self._create_vector_layer(lyr)
                top_lyr_str = '\n'.join([top_lyr_str, lyrstr])

            elif isinstance(lyr, Raster):
                # projections are below vector layers and above the base layer
                if isinstance(lyr, SDMProjection):
                    palette = DEFAULT_PROJECTION_PALETTE
                    lyrstr = self._create_raster_layer(lyr, palette)
                    mid_lyr_str = '\n'.join([mid_lyr_str, lyrstr])
                else:
                    palette = DEFAULT_ENVIRONMENTAL_PALETTE
                    lyrstr = self._create_raster_layer(lyr, palette)
                    base_lyr_str = '\n'.join([base_lyr_str, lyrstr])

        map_layers = '\n'.join([top_lyr_str, mid_lyr_str, base_lyr_str])

        # Add bluemarble image to Data/Occurrence Map Services
        if self.epsg_code == DEFAULT_EPSG:
            back_lyr = self._create_blue_marble_layer()
            map_layers = '\n'.join([map_layers, back_lyr])

        return map_layers

    # ................................
    def _create_vector_layer(self, sdl_lyr):
        att_meta = []
        proj = None
        meta = None
        cls = None

        data_specs = self._get_vector_data_specs(sdl_lyr)

        if data_specs:
            proj = self._create_projection_info(sdl_lyr.epsg_code)

            meta = self._get_layer_metadata(
                sdl_lyr, metalines=att_meta, is_vector=True)
            if (sdl_lyr.ogr_type == ogr.wkbPoint
                    or sdl_lyr.ogr_type == ogr.wkbMultiPoint):
                style = self._create_style(
                    POINT_SYMBOL, POINT_SIZE, color_str=DEFAULT_POINT_COLOR)
            elif (sdl_lyr.ogr_type == ogr.wkbLineString
                  or sdl_lyr.ogr_type == ogr.wkbMultiLineString):
                style = self._create_style(
                    LINE_SYMBOL, LINE_SIZE, color_str=DEFAULT_LINE_COLOR)
            elif (sdl_lyr.ogr_type == ogr.wkbPolygon
                  or sdl_lyr.ogr_type == ogr.wkbMultiPolygon):
                style = self._create_style(
                    POLYGON_SYMBOL, POLYGON_SIZE,
                    outline_color_str=DEFAULT_LINE_COLOR)
            cls = self._create_class(sdl_lyr.name, [style])

        lyr = self._create_layer(sdl_lyr, data_specs, proj, meta, cls=cls)
        return lyr

    # ................................
    def _create_raster_layer(self, sdl_lyr, palette_name):
        data_specs = self._get_raster_data_specs(sdl_lyr, palette_name)
        proj = self._create_projection_info(sdl_lyr.epsg_code)
        raster_metadata = [
            'wcs_label  \"{}\"'.format(sdl_lyr.name),
            'wcs_rangeset_name  \"{}\"'.format(sdl_lyr.name),
            'wcs_rangeset_label \"{}\"'.format(sdl_lyr.name)]
        # TODO: Where/how is this set??
        #       if sdl_lyr.nodata_val is not None:
        #          raster_metadata.append('rangeset_nullvalue  {}'
        #                               .format(str(sdl_lyr.nodata_val))

        meta = self._get_layer_metadata(sdl_lyr, metalines=raster_metadata)

        return self._create_layer(sdl_lyr, data_specs, proj, meta)

    # ................................
    def _create_layer(self, sdl_lyr, data_specs, proj, meta, cls=None):
        lyr = ''
        if data_specs:
            parts = [
                '   LAYER',
                '      NAME  \"{}\"'.format(sdl_lyr.name),
                '      TYPE  {}'.format(self._get_ms_text(sdl_lyr)),
                '      STATUS  OFF',
                '      OPACITY 100'
                ]
            lyr = '\n'.join(parts)

            ext = sdl_lyr.get_ssv_extent_string()
            if ext is not None:
                lyr = '\n'.join([lyr, '      EXTENT  {}'.format(ext)])

            lyr = '\n'.join([lyr, proj])
            lyr = '\n'.join([lyr, meta])
            lyr = '\n'.join([lyr, data_specs])
            if cls is not None:
                lyr = '\n'.join([lyr, cls])
            lyr = '\n'.join([lyr, '   END'])
        return lyr

    # ................................
    @staticmethod
    def _create_blue_marble_layer():
        fname = os.path.join(IMAGE_PATH, BLUE_MARBLE_IMAGE)
        bound_str = LMSpatialObject.get_extent_string(
            DEFAULT_GLOBAL_EXTENT, separator='  ')
        parts = [
            '   LAYER',
            '      NAME  bmng',
            '      TYPE  RASTER',
            '      DATA  \"{}\"'.format(fname),
            '      STATUS  OFF',
            '      EXTENT  {}'.format(bound_str),
            '      METADATA',
            '         ows_name   \"NASA blue marble\"',
            '         ows_title  \"NASA Blue Marble Next Generation\"',
            '         author     \"NASA\"',
            '      END',
            '   END']
        return '\n'.join(parts)

    # ................................
    @staticmethod
    def _create_class(name=None, styles=None, use_ct_class_groups=False):
        parts = ['      CLASS']
        if name is not None:
            parts.append('         NAME   {}'.format(name))
        if use_ct_class_groups:
            parts.append('         GROUP   {}'.format(name))
        if styles and isinstance(styles, list):
            parts.extend(styles)
        parts.append('      END')
        cls = '\n'.join(parts)
        return cls

    # ................................
    def _create_style(self, symbol, size, color_str=None,
                      outline_color_str=None):
        parts = ['         STYLE']
        # if NOT polygon
        if symbol is not None:
            parts.extend([
                '            SYMBOL   \"{}\"'.format(symbol),
                '            SIZE   {}'.format(size)])
        else:
            parts.append('            WIDTH   {}'.format(size))

        if color_str is not None:
            (red, green, blue) = self._html_color_to_rgb(color_str)
            parts.append(
                '            COLOR   {}  {}  {}'.format(red, green, blue))

        if outline_color_str is not None:
            (red, green, blue) = self._html_color_to_rgb(outline_color_str)
            parts.append(
                '            OUTLINECOLOR   {}  {}  {}'.format(
                    red, green, blue))
        parts.append('         END')
        style = '\n'.join(parts)
        return style

    # ................................
    @staticmethod
    def _create_style_classes(name, styles):
        parts = []
        for cls_group, style in styles.items():
            # first class is default
            if len(parts) == 0:
                parts.append('      CLASSGROUP \"{}\"'.format(cls_group))
            parts.extend(
                ['         CLASS',
                 '            NAME   \"{}\"'.format(name),
                 '            GROUP   \"{}\"'.format(cls_group),
                 '            STYLE', style,
                 '         END'])
        if len(parts) > 0:
            parts.append('      END')
        classes = '\n'.join(parts)
        return classes

    # ................................
    @staticmethod
    def _create_projection_info(epsg_code):
        if epsg_code == '4326':
            parts = [
                '      PROJECTION',
                '         \"proj=longlat\"',
                '         \"ellps=WGS84\"',
                '         \"datum=WGS84\"',
                '         \"no_defs\"',
                '      END']
        else:
            parts = [
                '      PROJECTION',
                '         \"init=epsg:{}\"'.format(epsg_code),
                '      END']
        prj = '\n'.join(parts)
        return prj

    # ................................
    @staticmethod
    def _get_layer_metadata(sdl_lyr, metalines=None, is_vector=False):
        parts = ['      METADATA']
        if is_vector:
            parts.extend(['         gml_geometries \"geom\"',
                          '         gml_geom_type \"point\"',
                          '         gml_include_items \"all\"'])
        parts.append('         ows_name  \"{}\"'.format(sdl_lyr.name))
        try:
            ltitle = sdl_lyr.layer_metadata[ServiceObject.META_TITLE]
            parts.append('         ows_title  \"{}\"'.format(ltitle))
        except Exception:
            pass
        if metalines and isinstance(metalines, list):
            parts.extend(metalines)
        parts.append('      END')
        meta = '\n'.join(parts)
        return meta

    # ................................
    def _get_relative_path(self, dlocation):
        os.path.relpath(dlocation, self.map_absolute_path)

    # ................................
    def _get_vector_data_specs(self, sdl_lyr):
        data_specs = None
        # limit to 1000 features for archive point data
        if (isinstance(sdl_lyr, OccurrenceLayer) and
                sdl_lyr.get_user_id() == PUBLIC_USER and
                sdl_lyr.query_count > POINT_COUNT_MAX):
            dlocation = sdl_lyr.get_dlocation(largeFile=False)
            if not os.path.exists(dlocation):
                dlocation = sdl_lyr.get_dlocation()
        else:
            dlocation = sdl_lyr.get_dlocation()

        if dlocation is not None and os.path.exists(dlocation):
            relpath = os.path.relpath(dlocation, self.map_absolute_path)
            parts = [
                '      CONNECTIONTYPE  OGR',
                '      CONNECTION    \"{}\"'.format(relpath),
                '      TEMPLATE      \"{}\"'.format(QUERY_TEMPLATE),
                '      TOLERANCE       {}'.format(QUERY_TOLERANCE),
                '      TOLERANCEUNITS  pixels']
            data_specs = '\n'.join(parts)
        return data_specs

    # ................................
    def _get_raster_data_specs(self, sdl_lyr, palette_name):
        data_specs = None
        dlocation = sdl_lyr.get_dlocation()
        if dlocation is not None and os.path.exists(dlocation):
            relpath = os.path.relpath(dlocation, self.map_absolute_path)
            parts = ['      DATA  \"{}\"'.format(relpath)]

            if sdl_lyr.map_units is not None:
                parts.append(
                    '      UNITS  {}'.format(sdl_lyr.map_units.upper()))
            parts.append('      OFFSITE  0  0  0')

            if sdl_lyr.nodata_val is None:
                sdl_lyr.populate_stats()
            parts.append(
                '      PROCESSING \"NODATA={}\"'.format(sdl_lyr.nodata_val))
            # SDM projections are always scaled b/w 0 and 100
            if isinstance(sdl_lyr, SDMProjection):
                vmin = SCALE_PROJECTION_MINIMUM + 1
                vmax = SCALE_PROJECTION_MAXIMUM
            else:
                vmin = sdl_lyr.min_val
                vmax = sdl_lyr.max_val
            ramp_class = self._create_color_ramp(vmin, vmax, palette_name)
            parts.append(ramp_class)
            data_specs = '\n'.join(parts)

        return data_specs

    # ................................
    def _get_discrete_classes(self, vals, palette_name):
        if vals is not None:
            bins = self._create_discrete_bins(vals, palette_name)
            return '\n'.join(bins)

        return None

    # ................................
    @staticmethod
    def _get_ms_text(sdl_lyr):
        if isinstance(sdl_lyr, Raster):
            return 'RASTER'
        if isinstance(sdl_lyr, Vector):
            if (sdl_lyr.ogr_type == ogr.wkbPoint or
                    sdl_lyr.ogr_type == ogr.wkbMultiPoint):
                return 'POINT'
            if (sdl_lyr.ogr_type == ogr.wkbLineString or
                    sdl_lyr.ogr_type == ogr.wkbMultiLineString):
                return 'LINE'
            if (sdl_lyr.ogr_type == ogr.wkbPolygon or
                    sdl_lyr.ogr_type == ogr.wkbMultiPolygon):
                return 'POLYGON'
        raise Exception('Unknown _Layer type')

    # ................................
    def _html_color_to_rgb(self, color_string):
        """Convert #RRGGBB to an (R, G, B) tuple """
        color_string = self._check_html_color(color_string)
        if color_string is None:
            color_string = '#777777'
        red, green, blue = (
            int(color_string[1:3], 16), int(color_string[3:5], 16),
            int(color_string[5:], 16))
        return (red, green, blue)

    # ................................
    def _palette_to_rgb_start_end(self, palette_name):
        """Convert named palettes to a start/end (R, G, B, R, G, B) tuple

        Note:
            Possible palette names are gray, red, green, blue, yellow, fuschia,
                aqua, bluered, bluegreen, greenred
        """
        if palette_name in (
                'gray', 'red', 'green', 'blue', 'yellow', 'fuschia', 'aqua'):
            start_color = '#000000'
            if palette_name == 'gray':
                end_color = '#FFFFFF'
            elif palette_name == 'red':
                end_color = '#FF0000'
            elif palette_name == 'green':
                end_color = '#00FF00'
            elif palette_name == 'blue':
                end_color = '#0000FF'
            elif palette_name == 'yellow':
                end_color = '#FFFF00'
            elif palette_name == 'fuschia':
                end_color = '#FF00FF'
            elif palette_name == 'aqua':
                end_color = '#00FFFF'
        elif palette_name in ('bluered', 'bluegreen'):
            start_color = '#0000FF'
            if palette_name == 'bluered':
                end_color = '#FF0000'
            elif palette_name == 'bluegreen':
                end_color = '#00FF00'
        elif palette_name == 'greenred':
            start_color = '#00FF00'
            end_color = '#FF0000'

        red_1, green_1, blue_1 = self._html_color_to_rgb(start_color)
        red_2, green_2, blue_2 = self._html_color_to_rgb(end_color)

        return (red_1, green_1, blue_1, red_2, green_2, blue_2)

    # ................................
    @staticmethod
    def _check_html_color(color_string):
        """Ensure #RRGGBB format"""
        valid_chars = 'abcdef'
        color_string = color_string.strip()
        if len(color_string) == 6:
            color_string = '#' + color_string
        if len(color_string) == 7:
            if color_string[0] != '#':
                print((
                    'input {} is not in #RRGGBB format'.format(color_string)))
                return None

            for val in color_string[1:]:
                if not (val.isdigit() or val.lower() in valid_chars):
                    print('Input {} is not a valid hex color'.format(
                        color_string))
                    return None
        else:
            print(('input {} is not in #RRGGBB format'.format(color_string)))
            return None
        return color_string

    # ................................
    def _create_discrete_bins(self, vals, palette_name='gray'):
        bins = []
        num_bins = len(vals) + 1
        palette = ColorPalette(n=num_bins, ptype=palette_name)
        for i, vals_i in enumerate(vals):
            expr = '([pixel] = {:g})'.format(vals_i)
            name = 'Value = {:g}'.format(vals_i)
            # skip the first color, so that first class is not black
            bins.append(self._create_class_bin(expr, name, palette[i + 1]))
        return bins

    # ................................
    def _create_color_ramp(self, vmin, vmax, palette_name='gray'):
        rgbs = self._palette_to_rgb_start_end(palette_name)
        color_str = '{} {} {} {} {} {}'.format(
            rgbs[0], rgbs[1], rgbs[2], rgbs[3], rgbs[4], rgbs[5])
        parts = [
            '      CLASS',
            '         EXPRESSION ([pixel] >= {} AND [pixel] <= {})'.format(
                vmin, vmax),
            '         STYLE',
            '            COLORRANGE {}'.format(color_str),
            '            DATARANGE {}  {}'.format(vmin, vmax),
            '            RANGEITEM \"pixel\"',
            '         END',
            '      END']

        ramp = '\n'.join(parts)
        return ramp

    # ................................
    @staticmethod
    def _get_range_expr(low_val, high_val, v_min, v_max):
        if low_val is None:
            low_val = v_min

        if high_val is None:
            expr = '([pixel] >= {:g} AND [pixel] <= {:g})'.format(
                low_val, v_max)
            name = '{:g} <= Value <= {:g}'.format(low_val, v_max)
        else:
            expr = '([pixel] >= {:g} AND [pixel] < {:g})'.format(
                low_val, high_val)
            name = '{:g} <= Value < {:g}'.format(low_val, high_val)

        return expr, name

    # ................................
    @staticmethod
    def _create_class_bin(expr, name, clr):
        rgb_str = '{} {} {}'.format(clr[0], clr[1], clr[2])
        return """        CLASS
            NAME \"{}\"
            EXPRESSION {}
            STYLE
                COLOR {}
            END
        END""".format(name, expr, rgb_str)

    # ................................
    @staticmethod
    def _get_raster_info(src_path, get_histo=False):
        """Get minimum and maximum values from a data source.

        Uses GDAL to retrieve the minimum and maximum values from a RASTER data
        source.  Note that for some types of data source (like ASCII grids),
        this process can be quite slow.

        Args:
            src_path: Full path to the raster dataset

        Returns:
            List of [min, max, nodata]
        """
        try:
            src = gdal.Open(src_path, gdalconst.GA_ReadOnly)
        except Exception as e:
            print(('Exception opening {} ({})'.format(src_path, e)))
            return (None, None, None, None)

        if src is None:
            print(('{} is not a valid image file'.format(src_path)))
            return (None, None, None, None)

        src_band = src.GetRasterBand(1)
        (vmin, vmax) = src_band.ComputeRasterMinMax()
        nodata = src_band.GetNoDataValue()
        if nodata is None and vmin >= 0:
            nodata = 0
        vals = []

        # Get histogram only for 8bit data (projections)
        if get_histo and src_band.DataType == gdalconst.GDT_Byte:
            hist = src_band.GetHistogram()
            for i, hist_i in enumerate(hist):
                if i > 0 and i != nodata and hist_i > 0:
                    vals.append(i)

        return (vmin, vmax, nodata, vals)

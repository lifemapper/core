"""Lifemapper map module

Todo:
    - Merge with layerset?  Maybe move out of layerset.
"""
import os

from osgeo import gdal, gdalconst, ogr

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (
    DEFAULT_GLOBAL_EXTENT, DEFAULT_EPSG, ENCODING)
from LmServer.base.layer import Raster, Vector
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.service_object import ServiceObject
from LmServer.common.color_palette import ColorPalette
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import (
    MAP_TEMPLATE, QUERY_TEMPLATE, LMFileType, LMFormat, IMAGE_PATH, BLUE_MARBLE_IMAGE,
    POINT_SYMBOL, POINT_SIZE, LINE_SYMBOL, LINE_SIZE, POLYGON_SYMBOL,
    POLYGON_SIZE, QUERY_TOLERANCE, SYMBOL_FILENAME, DEFAULT_POINT_COLOR,
    DEFAULT_LINE_COLOR, DEFAULT_PROJECTION_PALETTE,
    DEFAULT_ENVIRONMENTAL_PALETTE, PROJ_LIB, SCALE_PROJECTION_MINIMUM,
    SCALE_PROJECTION_MAXIMUM)
from LmServer.common.localconstants import (PUBLIC_USER, POINT_COUNT_MAX)
from LmServer.legion.occ_layer import OccurrenceLayer
from LmServer.legion.sdm_proj import SDMProjection


# .............................................................................
class LMMap(LMSpatialObject):
    """Superclass of Scenario, PresenceAbsenceLayerset.

    Todo:
        Extend as collections.MutableSequence subclass

    Note:
        mapcode should be required
    """
    # ................................
    def __init__(self, map_name, title, url, epsg_code, bbox, map_units,
                 layers=None, gridset=None, map_type=LMFileType.OTHER_MAP):
        """Constructor for the LMMap class

        Args:
            map_name: name for OWS map metadata
            title: title for OWS map metadata
            url: onlineUrl for OWS map metadata
            layers: Layers to map
            gridset: Gridset containing shapegrid and site-based matrices that
                can be joined to the shapegrid for map
        """
        LMSpatialObject.__init__(self, epsg_code, bbox, map_units)
        self.map_name = map_name
        self.url = url
        self.title = title
        self.layers = layers if layers is not None else []
        self._gridset = gridset
        self._map_type = map_type
        self._map_prefix = None

    # ................................
    def write_map(self, map_filename, layers=None, shapegrid=None,
                   matrices=None, template=MAP_TEMPLATE):
        """Write a map

        Create a mapfile by replacing strings in a template mapfile with text
        created for the layer set.

        Args:
            map_filename: Filename for mapfile
            template: Template mapfile

        Returns:
            str - A string representing a mapfile
        """
        if not os.path.exists(map_filename):
            all_layers = []
            if layers:
                lyrs = self._create_layers()
                all_layers.append(lyrs)
            if shapegrid is not None and matrices is not None:
                for matrix in matrices:
                    mtx_lyrs = self._create_matrix_join(shapegrid, matrix)
                    all_layers.append(mtx_lyrs)
            lyr_str = '\n'.join(all_layers)
            try:
                map_template = EarlJr.get_map_template_filename()
                map_str = self._get_base_map(map_template)
                map_str = self._add_map_base_attributes(map_str)
                map_str = map_str.replace('##_LAYERS_##', lyr_str)
            except Exception as err:
                raise LMError(err)

            try:
                self._write_base_map(map_str, map_filename)
            except Exception as e:
                raise LMError(
                    'Failed to write {}: {}'.format(map_filename, str(e)))

    # ................................
    def _write_base_map(self, map_str, map_filename):
        self.ready_filename(map_filename, overwrite=True)
        try:
            # make sure that group is set correctly
            with open(map_filename, 'w', encoding=ENCODING) as out_file:
                out_file.write(map_str)
            print(('Wrote {}'.format(map_filename)))
        except Exception as e:
            raise LMError(
                'Failed to write {}: {}'.format(map_filename, str(e)))

    # ................................
    @staticmethod
    def _get_base_map(fname):
        try:
            with open(fname, 'r', encoding=ENCODING) as in_file:
                base_map = in_file.read()
        except Exception as e:
            raise LMError('Failed to read {}: {}'.format(fname, e))
        return base_map

    # ................................
    def _add_map_base_attributes(self, map_str):
        """Set map attributes on the map from the LayerSet

        Args:
            map_str: string for a mapserver mapfile to modify
        """
        if self._map_type == LMFileType.SDM_MAP:
            label = 'Lifemapper Species Map Service'
        elif self._map_type == LMFileType.SCENARIO_MAP:
            label = 'Lifemapper Environmental Data Map Service'
        elif self._map_type == LMFileType.ANCILLARY_MAP:
            label = 'Lifemapper Ancillary Map Service'
        elif self._map_type == LMFileType.RAD_MAP:
            label = 'Lifemapper RAD Map Service'
        else:
            label = 'Lifemapper Data Service'

        # changed this from self.name (which left 'scen_' prefix off scenarios)
        map_str = map_str.replace('##_MAPNAME_##', self.map_name)
        bound_str = LMSpatialObject.get_extent_string(
            self.bbox, separator='  ')
        map_str = map_str.replace('##_EXTENT_##', bound_str)
        map_str = map_str.replace('##_UNITS_##', self.map_units)
        map_str = map_str.replace('##_SYMBOLSET_##', SYMBOL_FILENAME)
        map_str = map_str.replace('##_PROJLIB_##', PROJ_LIB)
        mapprj = self._create_projection_info(self.epsg_code)
        map_str = map_str.replace('##_PROJECTION_##', mapprj)

        # Mapserver 5.6 & 6.0
        meta = """
        METADATA
            ows_srs    \"epsg:{}\"
            ows_enable_request    \"*\"
            ows_label    \"{}\"
            ows_title    \"{}\"
            ows_online_resource    \"{}\"
        END""".format(self.epsg_code, label, self.title, self.url)

        map_str = map_str.replace('##_MAP_METADATA_##', meta)
        return map_str

    # ................................
    def _create_layers(self):
        top_lyr_str = ''
        mid_lyr_str = ''
        base_lyr_str = ''

        # Vector layers are described first, so drawn on top
        for lyr in self.layers:
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
    # TODO: Remove?  This is duplicated in layerset
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
    @staticmethod
    def _create_matrix_join(shapegrid_layer, matrix):
        """Create a matrix / shapegrid join layer.

        Todo:
            Figure out if this is actually used, it looks like it relies on
                obsolete code
        """
        shapegrid_dlocation = shapegrid_layer.get_dlocation()
        mtx_dlocation = matrix.get_csv_dlocation()
        if shapegrid_dlocation and os.path.exists(shapegrid_dlocation):
            join_layers = []
            for join_name in matrix.get_column_headers():
                join_layers.append("""\
                LAYER
                    NAME  \"{}\"
                    TYPE  POLYGON
                    STATUS  DEFAULT
                    DATA  {}
                    OPACITY  100
                    CLASS
                        NAME  {}
                        STYLE
                            OUTLINECOLOR  120 120 120
                            COLOR  255 255 0
                        END
                    END
                    TEMPLATE \"{}\"
                    TOLDERANCE  {}
                    TOLDERANCEUNITS  pixels
                    JOIN
                        NAME  {}
                        CONNECTIONTYPE  CSV
                        TABLE  \"{}\"
                        FROM \"{}\"
                        TO  \"1\"
                        TYPE  ONE-TO-ONE
                    END
                END\n\n""".format(
                    join_name, shapegrid_dlocation, join_name, QUERY_TEMPLATE,
                    QUERY_TOLERANCE, join_name, mtx_dlocation,
                    shapegrid_layer.site_id))
            return '\n'.join(join_layers)
        return ''

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
    @staticmethod
    def _get_vector_data_specs(sdl_lyr):
        data_specs = None
        # limit to 1000 features for archive point data
        if (isinstance(sdl_lyr, OccurrenceLayer) and
                sdl_lyr.get_user_id() == PUBLIC_USER and
                sdl_lyr.query_count > POINT_COUNT_MAX):
            dlocation = sdl_lyr.get_dlocation(large_file=False)
            if not os.path.exists(dlocation):
                dlocation = sdl_lyr.get_dlocation()
        else:
            dlocation = sdl_lyr.get_dlocation()

        if dlocation is not None and os.path.exists(dlocation):
            parts = [
                '      CONNECTIONTYPE  OGR',
                '      CONNECTION    \"{}\"'.format(dlocation),
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
            parts = ['      DATA  \"{}\"'.format(dlocation)]

            if sdl_lyr.map_units is not None:
                parts.append(
                    '      UNITS  {}'.format(sdl_lyr.map_units.upper()))
            parts.append('      OFFSITE  0  0  0')

            if sdl_lyr.nodata_val is None:
                _ = sdl_lyr.populate_stats(dlocation, None, None, None, None, None, None, None, None)
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

"""This module provides OGC services"""
import cherrypy
import mapscript
import os

from LmCommon.common.lmconstants import HTTPStatus
from LmServer.common.color_palette import ColorPalette
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import (
    LINE_SIZE, LINE_SYMBOL, MAP_TEMPLATE, MapPrefix, OCC_NAME_PREFIX,
    POINT_SIZE, POINT_SYMBOL, POLYGON_SIZE, PRJ_PREFIX, WEB_MERCATOR_EPSG)
from LmWebServer.services.api.v2.base import LmService

PALETTES = (
    'gray', 'red', 'green', 'blue', 'safe', 'pretty', 'yellow', 'fuschia',
    'aqua', 'bluered', 'bluegreen', 'greenred')

# .............................................................................
def delete_obsolete_mapfile(self, filename):
    """ Delete obsolete mapfiles without web-mercator EPSG for W*S services """
    if os.path.exists(filename):
        import subprocess
        cmd = '/usr/bin/grep  epsg:{}  {}'.format(WEB_MERCATOR_EPSG, filename)
        info, err = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE).communicate()
        if len(info) == 0:
            try:
                os.remove(filename)
            except Exception as err:
                print('Failed to remove {}, {}'.format(filename, err))


# .............................................................................
@cherrypy.expose
class MapService(LmService):
    """The base mapping service for OGC services.
    """

    # ................................
    def GET(self, map_name, bbox=None, bgcolor=None, color=None, coverage=None,
            crs=None, exceptions=None, height=None, layer=None, layers=None,
            point=None, request=None, format_=None, service=None,
            sld=None, sld_body=None, srs=None, styles=None, time=None,
            transparent=None, version=None, width=None, **params):
        """GET method for all OGC services

        Args:
            map_name: The map name to use for the request
            bbox: A (min x, min y, max x, max y) tuple of bounding parameters
            bgcolor: A background color to use for a map
            color: The color (or color ramp) to use for the map
            crs: The spatial reference system for the map output
            exceptions: The format to report exceptions in
            height: The height (in pixels) of the returned map
            layers: A list of layer names
            request: The request operation name to perform
            format_: The desired response format, query parameter is
                'format'
            service: The OGC service to use (W*S)
            sld: A URL referencing a StyledLayerDescriptor XML file which
                controls or enhances map layers and styling
            sld_body: A URL-encoded StyledLayerDescriptor XML document which
                controls or enhances map layers and styling
            srs: The spatial reference system for the map output.  'crs' for
                version 1.3.0.
            styles: A list of styles for the response
            time: A time or time range for map requests
            transparent: Boolean indicating if the background of the map should
                be transparent
            version: The version of the service to use
            width: The width (in pixels) of the returned map
        """
        self.map_name = map_name
        earl_jr = EarlJr(scribe=self.scribe)
        map_file_name = earl_jr.get_map_filename_from_map_name(map_name)

        # TODO: Remove this after production mapfiles are updated
        delete_obsolete_mapfile(map_file_name)
            
        if not os.path.exists(map_file_name):
            map_svc = self.scribe.get_map_service_from_map_filename(
                map_file_name)

            if map_svc is not None and map_svc.count > 0:
                map_svc.write_map(MAP_TEMPLATE)

        self.ows_req = mapscript.OWSRequest()
        map_params = [
            ('map', map_name),
            ('bbox', bbox),
            ('bgcolor', bgcolor),
            ('coverage', coverage),
            ('crs', crs),
            ('exceptions', exceptions),
            ('height', height),
            ('layer', layer),
            ('layers', layers),
            ('point', point),
            ('request', request),
            ('format', format_),
            ('service', service),
            ('sld', sld),
            ('sld_body', sld_body),
            ('srs', srs),
            ('styles', styles),
            ('time', time),
            ('transparent', transparent),
            ('version', version),
            ('width', width)
        ]

        for k, val in map_params:
            if val is not None:
                self.ows_req.setParameter(k, str(val))

        self.map_obj = mapscript.mapObj(map_file_name)

        if request.lower() in ['getcapabilities', 'describecoverage']:
            content_type, content = self._wxs_get_text()

        elif service is not None and request is not None and (
                service.lower(), request.lower()) in [
                    ('wcs', 'getcoverage'),
                    ('wms', 'getmap'),
                    ('wms', 'getlegendgraphic')]:
            try:
                content_type, content = self._wxs_get_image(
                    layers, color=color)
            except Exception as e:
                content_type, content = self._wxs_get_text(msg=str(e))
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Cannot handle service / request combination: {} / {}'.format(
                    service, request))

        cherrypy.response.headers['Content-Type'] = content_type
        return content

    # ................................
    def _wxs_get_text(self, msg=None):
        """
        @summary: Return a text response to a W*S request
        """
        if msg is not None:
            content = msg
            content_type = 'text/plain'
        else:
            mapscript.msIO_installStdoutToBuffer()
            self.map_obj.OWSDispatch(self.ows_req)
            content_type = mapscript.msIO_stripStdoutBufferContentType()
            content = mapscript.msIO_getStdoutBufferString()
            mapscript.msIO_resetHandlers()

        if content_type.endswith('_xml'):
            content_type = 'text/xml'

        return content_type, content

    # ................................
    def _wxs_get_image(self, layers, color=None, point=None):
        """
        """
        if color is not None:
            self._change_data_color(color)
        if point is not None:
            self._add_data_point(point, color)
        mapscript.msIO_installStdoutToBuffer()
        _result = self.map_obj.OWSDispatch(self.ows_req)
        content_type = mapscript.msIO_stripStdoutBufferContentType()
        # Get the image through msIO_getStdoutBufferBytes which uses GDAL,
        # which is needed to process Float32 geotiff images
        content = mapscript.msIO_getStdoutBufferBytes()
        mapscript.msIO_resetHandlers()
        return content_type, content

    # ................................
    def _change_data_color(self, color):
        # This assumes only one layer will have a user-defined color
        lyrname = self._find_layer_to_color()
        if isinstance(color, (list, tuple)) and len(color) > 0:
            color = color[0]
        # If there is more than one layer, decide which to change
        maplyr = self.map_obj.getLayerByName(lyrname)
        cls = maplyr.getClass(0)
        # In case raster layer has no classes ...
        if cls is None:
            return
        stl = cls.getStyle(0)
        clr = self._get_rgb(color)

        if maplyr.type == mapscript.MS_LAYER_RASTER:
            if maplyr.numclasses == 1:
                _success = stl.updateFromString(
                    'STYLE COLOR {} {} {} END'.format(clr[0], clr[1], clr[2]))
            else:
                palette_name = self._get_palette_name(color)
                pal = ColorPalette(n=maplyr.numclasses + 1, ptype=palette_name)
                for i in range(maplyr.numclasses):
                    stl = maplyr.getClass(i).getStyle(0)
                    clr = pal[i + 1]
                    _success = stl.updateFromString(
                        'STYLE COLOR %d %d %d END' % (clr[0], clr[1], clr[2]))
        else:
            if maplyr.type == mapscript.MS_LAYER_POINT:
                sym = POINT_SYMBOL
                p_size = POINT_SIZE
            elif maplyr.type == mapscript.MS_LAYER_LINE:
                sym = LINE_SYMBOL
                p_size = LINE_SIZE
            _success = stl.updateFromString(
                'STYLE SYMBOL \"{}\" SIZE {} COLOR {} {} {} END'.format(
                    sym, p_size, clr[0], clr[1], clr[2]))

            if maplyr.type == mapscript.MS_LAYER_POLYGON:
                _success = stl.updateFromString(
                    'STYLE WIDTH {} COLOR {} {} {} END'.format(
                        str(POLYGON_SIZE), clr[0], clr[1], clr[2]))

    # ................................
    def _add_data_point(self, point, color):
        if point is not None:
            lyrstr = self.ows_req.getValueByName('layers')
            # Only adds point if layer 'emptypt' is present
            for lyrname in lyrstr.split(','):
                if lyrname == 'emptypt':
                    if color is not None:
                        (r, g, b) = self._get_rgb(color)
                    else:
                        (r, g, b) = (255, 127, 0)
                    lyrtext = '\n'.join(
                        (
                            '  LAYER',
                            '     NAME  \"emptypt\"',
                            '     TYPE  POINT',
                            '     STATUS  ON',
                            '     OPACITY 100',
                            '     DUMP  TRUE',
                            '     FEATURE POINTS %s %s END END' %
                            (str(point[0]), str(point[1])),
                            '        END'))
                    stltext = '\n'.join(
                        ('        STYLE',
                         '          SYMBOL    \"filledcircle\"',
                         '          SIZE    5',
                         '          COLOR    %d  %d  %d' % (r, g, b),
                         '        END'))
                    lyr = self.map_obj.getLayerByName(lyrname)
                    success = lyr.updateFromString(lyrtext)
                    stl = lyr.getClass(0).getStyle(0)
                    success = stl.updateFromString(stltext)

    # ................................
    def _find_layer_to_color(self):
        """
        Note:
            * This assumes that only one layer will be a candidate for change.
                If more than one layer is specified and can be colored, the
                first one will be chosen.

        Todo:
            make this work like 'styles' parameter, with a comma-delimited list
                of colors, each entry applicable to the layer in the same
                position
        """
        lyrnames = self.ows_req.getValueByName('layers').split(',')
        colorme = None
        bluemarblelayer = 'bmng'

        # Archive maps have only OccurrenceSets, Projections, and Blue Marble
        if self.map_name.startswith(MapPrefix.SDM):
            for lyrname in lyrnames:
                if lyrname.startswith(OCC_NAME_PREFIX):
                    colorme = lyrname
                if lyrname.startswith(PRJ_PREFIX):
                    colorme = lyrname

        elif self.map_name.startswith(MapPrefix.USER):
            for lyrname in lyrnames:
                if lyrname.startswith(OCC_NAME_PREFIX):
                    colorme = lyrname
                if lyrname.startswith(PRJ_PREFIX):
                    colorme = lyrname
            if colorme is None:
                for lyrname in lyrnames:
                    if lyrname != bluemarblelayer:
                        colorme = lyrname

        elif (self.map_name.startswith(MapPrefix.SCEN) or
              self.map_name.startswith(MapPrefix.ANC)):
            for lyrname in lyrnames:
                if lyrname != bluemarblelayer:
                    colorme = lyrname
                    break

        return colorme

    # ................................
    def _get_rgb(self, colorstring):
        if colorstring in PALETTES:
            pal = ColorPalette(n=2, ptype=colorstring)
            return pal[1]

        return self._html_color_to_rgb(colorstring)

    # ................................
    def _html_color_to_rgb(self, color_string):
        """ convert #RRGGBB to an (R, G, B) tuple (integers) """
        color_string = self._check_html_color(color_string)
        if color_string is None:
            color_string = '#777777'
        r, g, b = color_string[1:3], color_string[3:5], color_string[5:]
        r, g, b = [int(n, 16) for n in (r, g, b)]
        return (r, g, b)

    # ................................
    def _check_html_color(self, color_string):
        """Ensure #RRGGBB format
        """
        valid_chars = 'abcdef'
        color_string = color_string.strip()
        if len(color_string) == 6:
            color_string = '#' + color_string
        if len(color_string) == 7:
            if color_string[0] != '#':
                self.log.error(
                    'input {} is not in #RRGGBB format'.format(color_string))
                return None

            # Make sure we have a valid color
            for color_char in color_string[1:]:
                if not color_char.isdigit() and\
                        color_char.lower() not in valid_chars:
                    self.log.error('input {} is not a valid hex color'.format(
                        color_string))
                    return None
        else:
            self.log.error('input {} is not in #RRGGBB format'.format(
                color_string))
            return None
        return color_string

    # ................................
    def _get_palette_name(self, color_string):
        if color_string in PALETTES:
            return color_string
        (r, g, b) = self._html_color_to_rgb(color_string)
        if (r > g and r > b):
            return 'red'
        if (g > r and g > b):
            return 'green'
        if (b > r and b > g):
            return 'blue'
        if (r < g and r < b):
            return 'bluegreen'
        if (g < r and g < b):
            return 'bluered'
        if (b < r and b < g):
            return 'greenred'
        return None

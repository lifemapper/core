"""Lifemapper map module
"""
import os

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (DEFAULT_GLOBAL_EXTENT, DEFAULT_EPSG)
from LmServer.base.layer import Raster, Vector
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.service_object import ServiceObject
from LmServer.common.color_palette import ColorPalette
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import (
    MAP_TEMPLATE, QUERY_TEMPLATE, LMFileType, IMAGE_PATH, BLUE_MARBLE_IMAGE,
    POINT_SYMBOL, POINT_SIZE, LINE_SYMBOL, LINE_SIZE, POLYGON_SYMBOL,
    POLYGON_SIZE, QUERY_TOLERANCE, SYMBOL_FILENAME, DEFAULT_POINT_COLOR,
    DEFAULT_LINE_COLOR, DEFAULT_PROJECTION_PALETTE,
    DEFAULT_ENVIRONMENTAL_PALETTE, PROJ_LIB, SCALE_PROJECTION_MINIMUM,
    SCALE_PROJECTION_MAXIMUM)
from LmServer.common.localconstants import (PUBLIC_USER, POINT_COUNT_MAX)
from LmServer.legion.occ_layer import OccurrenceLayer
from LmServer.legion.sdmproj import SDMProjection
from osgeo import gdal, gdalconst, ogr


# .............................................................................
class LMMap(LMSpatialObject):
    """
    Superclass of Scenario, PresenceAbsenceLayerset.  
    @todo: extend as collections.MutableSequence subclass
    @note: mapcode should be required
    """

# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, mapname, title, url, epsgcode, bbox, mapunits,
                     layers=[], gridset=None, mapType=LMFileType.OTHER_MAP):
        """
        @summary Constructor for the LayerSet class
        @copydoc LmServer.base.lmobj.LMSpatialObject::__init__()
        @param mapname: name for OWS map metadata
        @param title: title for OWS map metadata
        @param url: onlineUrl for OWS map metadata
        @param layers: Layers to map
        @param gridset: Gridset containing shapegrid and site-based matrices that 
                             can be joined to the shapegrid for map 
        """
        LMSpatialObject.__init__(self, epsgcode, bbox, mapunits)
        self.mapName = mapname
        self.url = url
        self.title = title
        self.layers = layers
        self._gridset = gridset
        self._mapType = mapType
        self._mapPrefix = None

# .............................................................................
    def writeMap(self, mapfilename, layers=[], shpGrid=None, matrices=None,
                     template=MAP_TEMPLATE):
        """
        @summary Create a mapfile by replacing strings in a template mapfile 
                    with text created for the layer set.
        @param mapfilename: Filename for mapfile
        @param template: Template mapfile 
        @return a string representing a mapfile 
        """
        if not(os.path.exists(mapfilename)):
            allLayers = []
            if layers:
                lyrs = self._createLayers()
                allLayers.append(lyrs)
            if shpGrid is not None and matrices is not None:
                for matrix in matrices:
                    mtxLyrs = self._createMatrixJoin(shpGrid, matrix)
                    allLayers.append(mtxLyrs)
            lyrstr = '\n'.join(allLayers)
            try:
                earlJr = EarlJr()
                mapTemplate = earlJr.getMapFilenameFromMapname(template)
                mapstr = self._getBaseMap(mapTemplate)
                mapstr = self._addMapBaseAttributes(mapstr)
                mapstr = mapstr.replace('##_LAYERS_##', lyrstr)
            except Exception as e:
                raise

            try:
                self._writeBaseMap(mapstr)
            except Exception as e:
                raise LMError('Failed to write {}: {}'.format(mapfilename, str(e)))

# ...............................................
    def _writeBaseMap(self, mapstr, mapfilename):
        self.ready_filename(mapfilename, overwrite=True)
        try:
            f = open(mapfilename, 'w')
            # make sure that group is set correctly
            f.write(mapstr)
            f.close()
            print(('Wrote {}'.format(mapfilename)))
        except Exception as e:
            raise LMError('Failed to write {}: {}'.format(mapfilename, str(e)))

# ...............................................
    def _getBaseMap(self, fname):
        # TODO: in python 2.6, use 'with open(fname, 'r'):'
        try:
            with open(fname, 'r') as in_file:
                base_map = in_file.read()
        except Exception as e:
            raise LMError('Failed to read %s' % fname)
        return base_map

# ...............................................
    def _addMapBaseAttributes(self, mapstr):
        """
        @summary Set map attributes on the map from the LayerSet
        @param mapstr: string for a mapserver mapfile to modify
        """
        if self._mapType == LMFileType.SDM_MAP:
            label = 'Lifemapper Species Map Service'
        elif self._mapType == LMFileType.SCENARIO_MAP:
            label = 'Lifemapper Environmental Data Map Service'
        elif self._mapType == LMFileType.ANCILLARY_MAP:
            label = 'Lifemapper Ancillary Map Service'
        elif self._mapType == LMFileType.RAD_MAP:
            label = 'Lifemapper RAD Map Service'
        else:
            label = 'Lifemapper Data Service'

        # changed this from self.name (which left 'scen_' prefix off scenarios)
        mapstr = mapstr.replace('##_MAPNAME_##', self.mapName)
        boundstr = LMSpatialObject.getExtentAsString(self.bbox, separator='  ')
        mapstr = mapstr.replace('##_EXTENT_##', boundstr)
        mapstr = mapstr.replace('##_UNITS_##', self.mapUnits)
        mapstr = mapstr.replace('##_SYMBOLSET_##', SYMBOL_FILENAME)
        mapstr = mapstr.replace('##_PROJLIB_##', PROJ_LIB)
        mapprj = self._createProjectionInfo(self.epsgcode)
        mapstr = mapstr.replace('##_PROJECTION_##', mapprj)

        # Mapserver 5.6 & 6.0
        meta = ''
        meta = '\n'.join([meta, '        METADATA'])
        meta = '\n'.join([meta, '            ows_srs    \"epsg:%s\"' % self.epsgcode])
        meta = '\n'.join([meta, '            ows_enable_request    \"*\"'])
        meta = '\n'.join([meta, '            ows_label    \"%s\"' % label])
        meta = '\n'.join([meta, '            ows_title    \"%s\"' % self.title])
        meta = '\n'.join([meta, '            ows_onlineresource    \"%s\"' % self.url])
        meta = '\n'.join([meta, '        END'])

        mapstr = mapstr.replace('##_MAP_METADATA_##', meta)
        return mapstr

# ...............................................
    def _createLayers(self):
        joinLyrStr = ''
        topLyrStr = ''
        midLyrStr = ''
        baseLyrStr = ''

        if self._gridset is not None:
            sgLyr = self._gridset.getShapegrid()
            for matrix in self._gridset.getMatrices():
                joinLyrStr = self._createMatrixJoin(sgLyr, matrix)

        # Vector layers are described first, so drawn on top
        if self.layers:
            for lyr in self.layers:
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

        maplayers = '\n'.join([joinLyrStr, topLyrStr, midLyrStr, baseLyrStr])

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
            subsetFname = None
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
    def _createMatrixJoin(self, sgLyr, matrix):
        jlyrs = ''
        shpDlocation = sgLyr.getDLocation()
        mtxDlocation = matrix.getCSVDLocation()
        if shpDlocation is not None and os.path.exists(shpDlocation):
            jlyrsNames = matrix.getColumnHeaders()
            for joinName in jlyrsNames:
                jlyrs = '\n'.join([jlyrs, '    LAYER'])
                jlyrs = '\n'.join([jlyrs, '        NAME  \"{}\"'.format(joinName)])
                jlyrs = '\n'.join([jlyrs, '        TYPE  POLYGON'])
                jlyrs = '\n'.join([jlyrs, '        STATUS  DEFAULT'])
                jlyrs = '\n'.join([jlyrs, '        DATA  {}'.format(shpDlocation)])
                jlyrs = '\n'.join([jlyrs, '        OPACITY 100'])
                jlyrs = '\n'.join([jlyrs, '        CLASS'])
                jlyrs = '\n'.join([jlyrs, '            NAME    {}'.format(joinName)])
                jlyrs = '\n'.join([jlyrs, '            STYLE' ])
                jlyrs = '\n'.join([jlyrs, '                OUTLINECOLOR    120 120 120'])
                jlyrs = '\n'.join([jlyrs, '                COLOR    255 255 0'])
                jlyrs = '\n'.join([jlyrs, '            END' ])
                jlyrs = '\n'.join([jlyrs, '        END'])
                jlyrs = '\n'.join([jlyrs, '        TEMPLATE \"%s\"' % QUERY_TEMPLATE])
                jlyrs = '\n'.join([jlyrs, '        TOLERANCE  %d' % QUERY_TOLERANCE])
                jlyrs = '\n'.join([jlyrs, '        TOLERANCEUNITS  pixels'])
                jlyrs = '\n'.join([jlyrs, '        JOIN'])
                jlyrs = '\n'.join([jlyrs, '            NAME    {}'.format(joinName)])
                jlyrs = '\n'.join([jlyrs, '            CONNECTIONTYPE  CSV'])
                jlyrs = '\n'.join([jlyrs, '            TABLE  \"{}\"'.format(mtxDlocation)])
                jlyrs = '\n'.join([jlyrs, '            FROM  \"{}\"'.format(sgLyr.siteId)])
                jlyrs = '\n'.join([jlyrs, '            TO     \"1\"'])
                jlyrs = '\n'.join([jlyrs, '            TYPE  ONE-TO-ONE'])
                jlyrs = '\n'.join([jlyrs, '        END'])
                jlyrs = '\n'.join([jlyrs, '    END'])
                jlyrs = '\n'.join([jlyrs, ''])
        return jlyrs

# ...............................................
    def _createRasterLayer(self, sdlLyr, paletteName):
        dataspecs = self._getRasterDataSpecs(sdlLyr, paletteName)
        proj = self._createProjectionInfo(sdlLyr.epsgcode)
        rasterMetadata = [  # following 3 required in MS 6.0+
                                'wcs_label  \"%s\"' % sdlLyr.name,
                                'wcs_rangeset_name  \"%s\"' % sdlLyr.name,
                                'wcs_rangeset_label \"%s\"' % sdlLyr.name]
        # TODO: Where/how is this set??
#         if sdlLyr.nodataVal is not None:
#             rasterMetadata.append('rangeset_nullvalue  %s'
#                                          % str(sdlLyr.nodataVal))

        meta = self._getLayerMetadata(sdlLyr, metalines=rasterMetadata)

        lyr = self._createLayer(sdlLyr, dataspecs, proj, meta)
        return lyr

# ...............................................
    def _createLayer(self, sdlLyr, dataspecs, proj, meta, cls=None):
        lyr = ''
        if dataspecs:
            lyr = '\n'.join([lyr, '    LAYER'])
            lyr = '\n'.join([lyr, '        NAME  \"%s\"' % sdlLyr.name])
            lyr = '\n'.join([lyr, '        TYPE  %s' % self._getMSText(sdlLyr)])
            lyr = '\n'.join([lyr, '        STATUS  ON'])
            lyr = '\n'.join([lyr, '        OPACITY 100'])
#             lyr = '\n'.join([lyr, '        DUMP  TRUE'])

            ext = sdlLyr.getSSVExtentString()
            if ext is not None:
                lyr = '\n'.join([lyr, '        EXTENT  %s' % ext])

            lyr = '\n'.join([lyr, proj])
            lyr = '\n'.join([lyr, meta])
            lyr = '\n'.join([lyr, dataspecs])
            if cls is not None:
                lyr = '\n'.join([lyr, cls])
            lyr = '\n'.join([lyr, '    END'])
        return lyr

# ...............................................
    def _createBlueMarbleLayer(self):
        fname = os.path.join(IMAGE_PATH, BLUE_MARBLE_IMAGE)
        boundstr = LMSpatialObject.getExtentAsString(DEFAULT_GLOBAL_EXTENT,
                                                                    separator='  ')
        lyr = ''
        lyr = '\n'.join([lyr, '    LAYER'])
        lyr = '\n'.join([lyr, '        NAME  bmng'])
        lyr = '\n'.join([lyr, '        TYPE  RASTER'])
        lyr = '\n'.join([lyr, '        DATA  \"%s\"' % fname])
        lyr = '\n'.join([lyr, '        STATUS  ON'])
#         lyr = '\n'.join([lyr, '        DUMP  TRUE'])
        lyr = '\n'.join([lyr, '        EXTENT  {}'.format(boundstr)])
        lyr = '\n'.join([lyr, '        METADATA'])
        lyr = '\n'.join([lyr, '            ows_name    \"NASA blue marble\"'])
        lyr = '\n'.join([lyr, '            ows_title  \"NASA Blue Marble Next Generation\"'])
        lyr = '\n'.join([lyr, '            author      \"NASA\"'])
        lyr = '\n'.join([lyr, '        END'])
        lyr = '\n'.join([lyr, '    END'])
        return lyr

# ...............................................
    def _createClass(self, name=None, styles=[], useCTClassGroups=False):
        cls = ''
        cls = '\n'.join([cls, '        CLASS'])
        if name is not None:
            cls = '\n'.join([cls, '            NAME    %s' % name])
        if useCTClassGroups:
            cls = '\n'.join([cls, '            GROUP    %s' % name])
        for stl in styles:
            cls = '\n'.join([cls, stl])
        cls = '\n'.join([cls, '        END'])
        return cls

# ...............................................
    def _createStyle(self, symbol, size, colorstr=None, outlinecolorstr=None):
        style = ''
        style = '\n'.join([style, '            STYLE' ])
        # if NOT polygon
        if symbol is not None:
            style = '\n'.join([style, '                SYMBOL    \"%s\"' % symbol])
            style = '\n'.join([style, '                SIZE    %d' % size])
        else:
            style = '\n'.join([style, '                WIDTH    %d' % size])

        if colorstr is not None:
            (r, g, b) = self._HTMLColorToRGB(colorstr)
            style = '\n'.join([style, '                COLOR    %d  %d  %d' % (r, g, b) ])

        if outlinecolorstr is not None:
            (r, g, b) = self._HTMLColorToRGB(outlinecolorstr)
            style = '\n'.join([style, '                OUTLINECOLOR    %d  %d  %d' % (r, g, b) ])
        style = '\n'.join([style, '            END' ])
        return style

# ...............................................
    def _createStyleClasses(self, name, styles):
        classes = ''
        for clsgroup, style in styles.items():
            # first class is default
            if len(classes) == 0:
                classes = '\n'.join([classes, '        CLASSGROUP \"%s\"' % clsgroup])
            classes = '\n'.join([classes, '        CLASS'])
            classes = '\n'.join([classes, '            NAME    \"%s\"' % name])
            classes = '\n'.join([classes, '            GROUP    \"%s\"' % clsgroup])
            classes = '\n'.join([classes, '            STYLE'])
            classes = '\n'.join([classes, style])
            classes = '\n'.join([classes, '            END'])
            classes = '\n'.join([classes, '        END'])
        return classes

# ...............................................
    def _createProjectionInfo(self, epsgcode):
        prj = ''
        prj = '\n'.join([prj, '        PROJECTION'])
        prj = '\n'.join([prj, '            \"init=epsg:%s\"' % epsgcode])
        prj = '\n'.join([prj, '        END'])
        return prj

# ...............................................
    def _getLayerMetadata(self, sdlLyr, metalines=[], isVector=False):
        meta = ''
        meta = '\n'.join([meta, '        METADATA'])
        try:
            lyrTitle = sdlLyr.lyrMetadata[ServiceObject.META_TITLE]
        except:
            lyrTitle = None
        # DUMP True deprecated in Mapserver 6.0, replaced by
        if isVector:
            meta = '\n'.join([meta, '            gml_geometries \"geom\"'])
            meta = '\n'.join([meta, '            gml_geom_type \"point\"'])
            meta = '\n'.join([meta, '            gml_include_items \"all\"'])
        # ows_ used in metadata for multiple OGC services
        meta = '\n'.join([meta, '            ows_name  \"%s\"' % sdlLyr.name])
        if lyrTitle is not None:
            meta = '\n'.join([meta, '            ows_title  \"%s\"' % lyrTitle])
        for line in metalines:
            meta = '\n'.join([meta, '            %s' % line])
        meta = '\n'.join([meta, '        END'])
        return meta

# ...............................................
    def _getVectorDataSpecs(self, sdlLyr):
        dataspecs = None
        # limit to 1000 features for archive point data
        if (isinstance(sdlLyr, OccurrenceLayer) and
             sdlLyr.getUserId() == PUBLIC_USER and
             sdlLyr.queryCount > POINT_COUNT_MAX):
            dlocation = sdlLyr.getDLocation(largeFile=False)
            if not os.path.exists(dlocation):
                dlocation = sdlLyr.getDLocation()
        else:
            dlocation = sdlLyr.getDLocation()

        if dlocation is not None and os.path.exists(dlocation):
            dataspecs = '        CONNECTIONTYPE  OGR'
            dataspecs = '\n'.join([dataspecs, '        CONNECTION  \"%s\"' % dlocation])
            dataspecs = '\n'.join([dataspecs, '        TEMPLATE \"%s\"' % QUERY_TEMPLATE])
            dataspecs = '\n'.join([dataspecs, '        TOLERANCE  %d' % QUERY_TOLERANCE])
            dataspecs = '\n'.join([dataspecs, '        TOLERANCEUNITS  pixels'])
        return dataspecs

# ...............................................
    def _getRasterDataSpecs(self, sdlLyr, paletteName):
        dataspecs = None
        dlocation = sdlLyr.getDLocation()
        if dlocation is not None and os.path.exists(dlocation):
            dataspecs = '        DATA  \"%s\"' % dlocation
            if sdlLyr.mapUnits is not None:
                dataspecs = '\n'.join([dataspecs, '        UNITS  %s' %
                                              sdlLyr.mapUnits.upper()])
            dataspecs = '\n'.join([dataspecs, '        OFFSITE  0  0  0'])

            if sdlLyr.nodataVal is None:
                sdlLyr.populateStats()
            dataspecs = '\n'.join([dataspecs, '        PROCESSING \"NODATA=%s\"'
                                          % str(sdlLyr.nodataVal)])
            # SDM projections are always scaled b/w 0 and 100
            if isinstance(sdlLyr, SDMProjection):
                vmin = SCALE_PROJECTION_MINIMUM + 1
                vmax = SCALE_PROJECTION_MAXIMUM
            else:
                vmin = sdlLyr.minVal
                vmax = sdlLyr.maxVal
            rampClass = self._createColorRamp(vmin, vmax, paletteName)
            dataspecs = '\n'.join([dataspecs, rampClass])

#             # Continuous data
#             if not(sdlLyr.getIsDiscreteData()):
#                 rampClass = self._createColorRamp(vmin, vmax, paletteName)
#                 dataspecs = '\n'.join([dataspecs, rampClass])
#             # Classified data (8-bit projections)
#             else:
#                 vals = sdlLyr.getHistogram()
#                 classdata = self._getDiscreteClasses(vals, paletteName)
#                 if classdata is not None:
#                     dataspecs = '\n'.join([dataspecs, classdata])

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
        elif palettename == 'greenred':
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
                print(('input %s is not in #RRGGBB format' % colorstring))
                return None

            for i in range(len(colorstring)):
                if i > 0:
                    if not(colorstring[i].isdigit()) and validChars.count(colorstring[i]) == 0:
                        print(('input %s is not a valid hex color' % colorstring))
                        return None
        else:
            print(('input %s is not in #RRGGBB format' % colorstring))
            return None
        return colorstring

# ...............................................
    def _createDiscreteBins(self, vals, paletteName='gray'):
        bins = []
        numBins = len(vals) + 1
        palette = ColorPalette(n=numBins, ptype=paletteName)
        for i in range(len(vals)):
            expr = '([pixel] = %g)' % (vals[i])
            name = 'Value = %g' % (vals[i])
            # skip the first color, so that first class is not black
            bins.append(self._createClassBin(expr, name, palette[i + 1]))
        return bins

# ...............................................
    def _createColorRamp(self, vmin, vmax, paletteName='gray'):
        rgbs = self._paletteToRGBStartEnd(paletteName)
        colorstr = '%s %s %s %s %s %s' % (rgbs[0], rgbs[1], rgbs[2], rgbs[3], rgbs[4], rgbs[5])
        ramp = ''
        ramp = '\n'.join([ramp, '        CLASS'])
        ramp = '\n'.join([ramp, '            EXPRESSION ([pixel] >= %s AND [pixel] <= %s)'
                              % (str(vmin), str(vmax))])
        ramp = '\n'.join([ramp, '            STYLE'])
        ramp = '\n'.join([ramp, '                COLORRANGE %s' % colorstr])
        ramp = '\n'.join([ramp, '                DATARANGE %s  %s' % (str(vmin), str(vmax))])
        ramp = '\n'.join([ramp, '                RANGEITEM \"pixel\"'])
        ramp = '\n'.join([ramp, '            END'])
        ramp = '\n'.join([ramp, '        END'])
        return ramp

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
            print(('Exception opening %s (%s)' % (srcpath, str(e))))
            return (None, None, None, None)

        if src is None:
            print(('%s is not a valid image file' % srcpath))
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


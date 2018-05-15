"""
@summary: This module provides OGC services

@author: CJ Grady
@version: 2.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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

import cherrypy
import mapscript
import os

from LmServer.common.datalocator import EarlJr
from LmServer.common.colorpalette import colorPalette
from LmServer.common.lmconstants import (LINE_SIZE, LINE_SYMBOL, MAP_TEMPLATE,
                                         MapPrefix, OCC_NAME_PREFIX, 
                                         POINT_SIZE, POINT_SYMBOL, 
                                         POLYGON_SIZE, PRJ_PREFIX)
from LmWebServer.services.api.v2.base import LmService
from LmCommon.common.lmconstants import HTTPStatus

PALETTES = ('gray', 'red', 'green', 'blue', 'safe', 'pretty', 'yellow', 
            'fuschia', 'aqua', 'bluered', 'bluegreen', 'greenred')

# .............................................................................
@cherrypy.expose
class MapService(LmService):
   """
   @summary: This is the base mapping service.  It can be called with explicit
                map file names and layer names.  Only the GET HTTP method is
                available
   """
   # ................................
   def GET(self, mapName, bbox=None, bgcolor=None, color=None, coverage=None,
           crs=None, exceptions=None, height=None, layer=None, layers=None, 
           point=None, request=None, respFormat=None, service=None, sld=None, 
           sld_body=None, srs=None, styles=None, time=None, transparent=None, 
           version=None, width=None, **params):
      """
      @summary: GET method for all OGC services
      @param mapName: The map name to use for the request
      @param bbox: A (min x, min y, max x, max y) tuple of bounding parameters
      @param bgcolor: A background color to use for a map
      @param color: The color (or color ramp) to use for the map
      @param crs: The spatial reference system for the map output
      @param exceptions: The format to report exceptions in
      @param height: The height (in pixels) of the returned map
      @param layers: A list of layer names
      @param request: The request operation name to perform
      @param respFormat: The desired response format, query parameter is 'format'
      @param service: The OGC service to use (W*S)
      @param sld: A URL referencing a StyledLayerDescriptor XML file which 
                     controls or enhances map layers and styling
      @param sld_body: A URL-encoded StyledLayerDescriptor XML document which 
                          controls or enhances map layers and styling
      @param srs: The spatial reference system for the map output.  'crs' for 
                     version 1.3.0.
      @param styles: A list of styles for the response
      @param time: A time or time range for map requests
      @param transparent: Boolean indicating if the background of the map 
                             should be transparent
      @param version: The version of the service to use
      @param width: The width (in pixels) of the returned map
      """
      self.mapName = mapName
      earljr = EarlJr(scribe=self.scribe)
      mapFilename = earljr.getMapFilenameFromMapname(mapName)
      
      if not os.path.exists(mapFilename):
         mapSvc = self.scribe.getMapServiceFromMapFilename(mapFilename)
         
         if mapSvc is not None and mapSvc.count > 0:
            mapSvc.writeMap(MAP_TEMPLATE)
      
      # TODO: Check permission
      # TODO: Handle parameters
      self.owsreq = mapscript.OWSRequest()
      mapParams = [
         ('map', mapName),
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
         ('format', respFormat),
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
           
      for k, v in mapParams:
         if v is not None:
            self.owsreq.setParameter(k, str(v))
      
      self.mapObj = mapscript.mapObj(mapFilename)
      
      if request.lower() in ['getcapabilities', 'describecoverage']:
         contentType, content = self._wxsGetText()
         
      elif service is not None and request is not None and \
           (service.lower(), request.lower()) in [('wcs', 'getcoverage'), 
                                                  ('wms', 'getmap'), 
                                                  ('wms', 'getlegendgraphic')]:
         try:
            contentType, content = self._wxsGetImage(layers, color=color)
         except Exception, e:
            contentType, content = self._wxsGetText(msg=str(e))
      else:
         raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST, 
                'Cannot handle service / request combination: {} / {}'.format(
                   service, request))

      cherrypy.response.headers['Content-Type'] = contentType
      return content

   # ................................
   def _wxsGetText(self, msg=None):
      """
      @summary: Return a text response to a W*S request
      """
      if msg is not None:
         content = msg
         content_type = 'text/plain'
      else:
         mapscript.msIO_installStdoutToBuffer()
         self.mapObj.OWSDispatch(self.owsreq)
         content_type = mapscript.msIO_stripStdoutBufferContentType()
         content = mapscript.msIO_getStdoutBufferString()
         mapscript.msIO_resetHandlers()
      
      if content_type.endswith('_xml'):
         content_type = 'text/xml'
      
      return content_type, content
   
   # ................................
   def _wxsGetImage(self, layers, color=None, point=None):
      """
      """
      if color is not None: 
         self._changeDataColor(color)
      if point is not None: 
         self._addDataPoint(point, color)
      mapscript.msIO_installStdoutToBuffer()
      result = self.mapObj.OWSDispatch( self.owsreq )
      content_type = mapscript.msIO_stripStdoutBufferContentType()
      # Get the image through msIO_getStdoutBufferBytes which uses GDAL, 
      # which is needed to process Float32 geotiff images
      content = mapscript.msIO_getStdoutBufferBytes()
      mapscript.msIO_resetHandlers()
      return content_type, content         
      
# ...............................................
   def _changeDataColor(self, color):
      from types import ListType, TupleType
      # This assumes only one layer will have a user-defined color
      lyrname = self._findLayerToColor()
      if ((isinstance(color, ListType) or isinstance(color, TupleType))
          and len(color) > 0):
         color = color[0]
      # If there is more than one layer, decide which to change        
      maplyr = self.mapObj.getLayerByName(lyrname)
      cls = maplyr.getClass(0)
      # In case raster layer has no classes ...
      if cls is None:
         return
      stl = cls.getStyle(0)
      clr = self._getRGB(color)
            
      if maplyr.type == mapscript.MS_LAYER_RASTER:
         if maplyr.numclasses == 1:
            success = stl.updateFromString('STYLE COLOR %d %d %d END' 
                                           % (clr[0], clr[1], clr[2]))
         else:
            palName = self._getPaletteName(color)
            pal = colorPalette(n=maplyr.numclasses+1, ptype=palName)
            for i in range(maplyr.numclasses):
               stl = maplyr.getClass(i).getStyle(0)
               clr = pal[i+1]
               success = stl.updateFromString('STYLE COLOR %d %d %d END' % 
                                              (clr[0], clr[1], clr[2]))
      else:
         if maplyr.type == mapscript.MS_LAYER_POINT:
            sym = POINT_SYMBOL 
            sz = POINT_SIZE
         elif maplyr.type == mapscript.MS_LAYER_LINE:
            sym = LINE_SYMBOL 
            sz = LINE_SIZE
         success = stl.updateFromString(
                 'STYLE SYMBOL \"%s\" SIZE %d COLOR %d %d %d END' 
                  % (sym, sz, clr[0], clr[1], clr[2]))

         if maplyr.type == mapscript.MS_LAYER_POLYGON:
            success = stl.updateFromString(
                 'STYLE WIDTH %s COLOR %d %d %d END' 
                  % (str(POLYGON_SIZE), clr[0], clr[1], clr[2]))

# ...............................................
   def _addDataPoint(self, point, color):
      if point is not None:
         lyrstr = self.owsreq.getValueByName('layers')
         # Only adds point if layer 'emptypt' is present
         for lyrname in lyrstr.split(','):
            if lyrname == 'emptypt':
               if color is not None:
                  (r, g, b) = self._getRGB(color)
               else:
                  (r, g, b) = (255, 127, 0)
               lyrtext = '\n'.join(('  LAYER',
                                    '    NAME  \"emptypt\"',
                                    '    TYPE  POINT',
                                    '    STATUS  ON',
                                    '    OPACITY 100',
                                    '    DUMP  TRUE',
                                    '    FEATURE POINTS %s %s END END' %
                                    (str(point[0]), str(point[1])),
                                    '      END'))
               stltext = '\n'.join(('      STYLE',
                                    '        SYMBOL   \"filledcircle\"',
                                    '        SIZE   5',
                                    '        COLOR   %d  %d  %d' % (r, g, b),
                                    '      END'))               
               lyr = map.getLayerByName(lyrname)
               success = lyr.updateFromString(lyrtext)
               stl = lyr.getClass(0).getStyle(0)
               success = stl.updateFromString(stltext)

# ...............................................
   def _findLayerToColor(self):
      """
      @note: This assumes that only one layer will be a candidate for change. If 
      more than one layer is specified and can be colored, the first one will be
      chosen.
      @todo: make this work like 'styles' parameter, with a comma-delimited list
             of colors, each entry applicable to the layer in the same position
      """
      lyrnames = self.owsreq.getValueByName('layers').split(',')
      colorme = None
      bluemarblelayer = 'bmng'
      
      # Archive maps have only OccurrenceSets, Projections, and Blue Marble
      if self.mapName.startswith(MapPrefix.SDM):
         for lyrname in lyrnames: 
            if lyrname.startswith(OCC_NAME_PREFIX):
               colorme = lyrname
            if lyrname.startswith(PRJ_PREFIX):
               colorme = lyrname
            
      elif self.mapName.startswith(MapPrefix.USER):
         for lyrname in lyrnames: 
            if lyrname.startswith(OCC_NAME_PREFIX):
               colorme = lyrname
            if lyrname.startswith(PRJ_PREFIX):
               colorme = lyrname
         if colorme is None:
            for lyrname in lyrnames: 
               if lyrname != bluemarblelayer:
                  colorme = lyrname

      elif (self.mapName.startswith(MapPrefix.SCEN) or
            self.mapName.startswith(MapPrefix.ANC)):
         for lyrname in lyrnames: 
            if lyrname != bluemarblelayer:
               colorme = lyrname
               break
               
      return colorme
  
# ...............................................
   def _getRGB(self, colorstring):
      if colorstring in PALETTES:
         pal = colorPalette(n=2, ptype=colorstring)
         return pal[1]
      else:
         return self._HTMLColorToRGB(colorstring)
# ...............................................
   def _HTMLColorToRGB(self, colorstring):
      """ convert #RRGGBB to an (R, G, B) tuple (integers) """
      colorstring = self._checkHTMLColor(colorstring)
      if colorstring is None:
         colorstring = '#777777'
      r, g, b = colorstring[1:3], colorstring[3:5], colorstring[5:]
      r, g, b = [int(n, 16) for n in (r, g, b)]
      return (r, g, b)

# ...............................................
   def _checkHTMLColor(self, colorstring):
      """ ensure #RRGGBB format """
      validcolor = None
      validChars = ['a', 'b', 'c', 'd', 'e', 'f', 'A', 'B', 'C', 'D', 'E', 'F']
      colorstring = colorstring.strip()
      if len(colorstring) == 6:
         colorstring = '#' + colorstring
      if len(colorstring) == 7:
         if colorstring[0] != '#':
            self.log.error('input %s is not in #RRGGBB format' % colorstring)
            return None
         
         for i in range(len(colorstring)):
            if i > 0:
               if not(colorstring[i].isdigit()) and validChars.count(colorstring[i]) == 0:
                  self.log.error('input %s is not a valid hex color' % colorstring)
                  return None
      else:
         self.log.error('input %s is not in #RRGGBB format' % colorstring)
         return None
      return colorstring

# ...............................................
   def _getPaletteName(self, colorstring):
      if colorstring in PALETTES:
         return colorstring
      (r,g,b) = self._HTMLColorToRGB(colorstring)
      if (r > g and r > b):
         return 'red'
      elif (g > r and g > b):
         return 'green'
      elif (b > r and b > g):
         return 'blue'
      elif (r < g and r < b):
         return 'bluegreen'
      elif (g < r and g < b):
         return 'bluered'
      elif (b < r and b < g):
         return 'greenred'
      
"""
from LmServer.common.datalocator import EarlJr
from LmServer.common.log import ConsoleLogger
from LmServer.common.lmconstants import FileFix, LMFileType, MAP_TEMPLATE
from LmServer.db.borgscribe import BorgScribe

mapname = 'data_473'

scribe = BorgScribe(ConsoleLogger())
scribe.openConnections()
earljr = EarlJr(scribe=scribe)

fileType, scencode, occsetId, gridsetId, usr, ancillary, epsg = earljr._parseMapname(mapname) 
mapFilename = earljr.getMapFilenameFromMapname(mapname)

mapSvc = scribe.getMapServiceFromMapFilename(mapFilename)
mapSvc.writeMap(MAP_TEMPLATE)
"""
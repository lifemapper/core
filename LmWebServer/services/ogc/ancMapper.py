""" 
Module to write a mapservice which will be cataloged in the SDL

@status: alpha
@author: Aimee Stewart
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import mapscript
import os 

from LmCommon.common.lmconstants import HTTPStatus, OutputFormat

from LmServer.base.lmobj import LmHTTPError, LMError, LMObject
from LmServer.common.colorpalette import colorPalette
from LmServer.common.lmconstants import (LINE_SIZE, LINE_SYMBOL, POINT_SIZE, 
                              POINT_SYMBOL, POLYGON_SIZE, WEB_PATH, MAP_PATH)
from LmServer.common.localconstants import APP_PATH

PALETTES = ('gray', 'red', 'green', 'blue', 'safe', 'pretty', 'yellow', 'fuschia', 'aqua',
            'bluered', 'bluegreen', 'greenred')

# .............................................................................
class MapConstructor2(LMObject):
   """
   Private class to create a mapservice from a LayerSet object
   """
   
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger):
      """
      @summary MapConstructor constructor
      @param logger: Logger to use
      @todo Use lmPublic logger instead
      """
      LMObject.__init__(self)
      self.owsreq = mapscript.OWSRequest()
      self.log = logger
      self.queryString = None
      self._mapFilename = None
      self.serviceType = None
      self.requestType = None
      self.color = None
      
# .............................................................................
# Public functions
# .............................................................................
   def assembleMap(self, parameters, mapFilename=None):
      """
      @summary Create a mapfile by creating an LayerSet with layer/s,
               from the SDL, then replacing strings in a template mapfile 
               with text created for the request.
      @param parameters: 
      @return a string representing a mapfile matching the request
      """
      mfn = self._findMapValues(parameters)
      
      if mapFilename is None:
         self._mapFilename = mfn
      else:
         self._mapFilename = mapFilename
            
      # if mapfile does not exist, create service from database, then write file
      if not (os.path.exists(self._mapFilename)):
         raise LMError('File {} does not exist'.format(self._mapFilename))

# ...............................................
   def returnResponse(self):
      """
      @summary 
      @param map: 
      @param owsreq:  
      @return 
      @todo Use a more specific exception than Exception
      @todo Finish documenting
      """
      content = None
      
      if self._mapFilename and os.path.exists(self._mapFilename):
         try:
            map = mapscript.mapObj(self._mapFilename)
         except Exception, e:
            self.log.reportError('Damn!! Error on %s (Exception: %s)' 
                                 % (self._mapFilename, str(e)))
            raise e
         
         if (self.requestType == 'GetCapabilities' or 
             self.requestType == 'DescribeCoverage' or 
             self.serviceType == 'WFS'):
            content_type, content = self._wxsGetText(map)
         
         elif ((self.serviceType == 'WCS' and self.requestType == 'GetCoverage') 
               or
               (self.serviceType == 'WMS' and self.requestType == 'GetMap')
               or (self.serviceType == 'WMS' and self.requestType == 'GetLegendGraphic')):
            try:
               content_type, content = self._wxsGetImage(map)
            except Exception, e:
               content_type, content = self._wxsGetText(map, msg=str(e))
               
         else:
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                     msg='Invalid service (%s) and request (%s) combination' \
                            % (self.serviceType, self.requestType))
         if content is None:
            raise Exception('No content returned')
      else:
         raise Exception('mapFilename %s does not exist' % self._mapFilename)

      return content_type, content

# .............................................................................
# Private functions
# .............................................................................

# ...............................................
   def _findMapValues(self, args):
      reqtype = None
      mapname = None
      
      for key in args.keys():
         k = key.lower()
#         self.log.debug('key %s = %s' % (key, args[key]))
         if k == 'map':
            mapname = args[key]
         elif k == 'color':
            self.color =  args[key]
         elif k == 'service':
            self.serviceType = args[key]
            self._appendToQuery(k, args[key])
         elif k == 'request':
            reqtype = self._fixRequestCase(args[key])
            self.requestType = reqtype
            self._appendToQuery(k, reqtype)
         else:
            self._appendToQuery(k, args[key])

      # Add mapname with full path
      if mapname is not None:
         if not mapname.endswith(OutputFormat.MAP):
            mapname = mapname+OutputFormat.MAP
         mapFilename = os.path.join(APP_PATH, WEB_PATH, MAP_PATH, mapname)
         self._appendToQuery('map', mapname)
      else:
         mapFilename = None
      
      return mapFilename
   
# ...............................................
   def _appendToQuery(self, key, val):
      self.owsreq.setParameter(key, val)
      pair = '='.join([key, val])
      if self.queryString is None:
         self.queryString = pair
      else:
         self.queryString = '&'.join([self.queryString, pair])

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

      for lyrname in lyrnames: 
         if lyrname != bluemarblelayer:
            colorme = lyrname
            break
               
      return colorme
               
# ...............................................
   def _changeDataColor(self, map):
      from types import ListType, TupleType
      # This assumes only one layer will have a user-defined color
      lyrname = self._findLayerToColor()
      if ((isinstance(self.color, ListType) or isinstance(self.color, TupleType))
          and len(self.color) > 0):
         self.color = self.color[0]
      # If there is more than one layer, decide which to change        
      maplyr = map.getLayerByName(lyrname)
      cls = maplyr.getClass(0)
      # In case raster layer has no classes ...
      if cls is None:
         return
      stl = cls.getStyle(0)
      clr = self._getRGB(self.color)
            
      if maplyr.type == mapscript.MS_LAYER_RASTER:
         if maplyr.numclasses == 1:
            success = stl.updateFromString('STYLE COLOR %d %d %d END' 
                                           % (clr[0], clr[1], clr[2]))
         else:
            palName = self._getPaletteName(self.color)
            pal = colorPalette(n=maplyr.numclasses+1, ptype=palName)
            for i in range(maplyr.numclasses):
               stl = maplyr.getClass(i).getStyle(0)
               clr = pal[i+1]
               success = stl.updateFromString('STYLE COLOR %d %d %d END' % 
                                              (clr[0], clr[1], clr[2]))
      else:
         sym = None
         if maplyr.type == mapscript.MS_LAYER_POINT:
            sym = POINT_SYMBOL 
            sz = POINT_SIZE
         elif maplyr.type == mapscript.MS_LAYER_LINE:
            sym = LINE_SYMBOL 
            sz = LINE_SIZE
         if sym is not None:
            success = stl.updateFromString(
                    'STYLE SYMBOL \"%s\" SIZE %d COLOR %d %d %d END' 
                     % (sym, sz, clr[0], clr[1], clr[2]))

         if maplyr.type == mapscript.MS_LAYER_POLYGON:
            success = stl.updateFromString(
                 'STYLE WIDTH %s COLOR %d %d %d END' 
                  % (str(POLYGON_SIZE), clr[0], clr[1], clr[2]))
      
# ...............................................
   def _wxsGetText(self, map, msg=None):
      """
      @summary: Return XML response to an W*S request
      @param map: mapscript mapObject for which to return information
      @return: XML response string.
      """
      if msg is not None:
         content = msg
         content_type = 'text-plain'
      else:
         mapscript.msIO_installStdoutToBuffer()
         map.OWSDispatch( self.owsreq )
         content_type = mapscript.msIO_stripStdoutBufferContentType()
         content = mapscript.msIO_getStdoutBufferString()
         mapscript.msIO_resetHandlers()
      
      if content_type.endswith('_xml'):
         content_type = 'text/xml' 
      self.log.debug('content_type = ' + str(content_type))
   
      return content_type, content

# ...............................................
   def _wxsGetImage(self, map):
      """
      @summary 
      @param map: 
      @return 
      @todo Finish documenting
      """      
      # OWSDispatch strips the outputformat (only if GTiff?) from the map object, 
      # so then the map cannot be used (map.draw) to create an image - generates
      # "Map outputformat not set!" error
      #
      # Can save the outputformat to another variable, then 
      # re- setOutputFormat on the map before trying to get the  image.
      #
      # Must not use map.draw then img.getBytes because that uses the 
      # GD library (instead of GDAL) and it does not recognize GTiff format.
      if self.requestType == 'GetLegendGraphic':
         lyrstr = self.owsreq.getValueByName('layer')
      else:   
         lyrstr = self.owsreq.getValueByName('layers')
      if lyrstr is None:
         lyrstr = self.owsreq.getValueByName('coverage')
      if lyrstr is not None and len(lyrstr) > 0:
         for lyrname in lyrstr.split(','):
            lyr = map.getLayerByName(lyrname)
            if lyr is None:
               raise LMError('Layer {} does not exist in mapfile {}'
                             .format(lyrname, self._mapFilename))
      else:
         raise LMError('No layer/layers/coverage parameter provided')
      if self.color is not None: 
         self._changeDataColor(map)
      mapscript.msIO_installStdoutToBuffer()
      result = map.OWSDispatch( self.owsreq )
      content_type = mapscript.msIO_stripStdoutBufferContentType()
      # Get the image through msIO_getStdoutBufferBytes which uses GDAL, 
      # which is needed to process Float32 geotiff images
      content = mapscript.msIO_getStdoutBufferBytes()
      mapscript.msIO_resetHandlers()
      return content_type, content
   
# ...............................................
   def _fixRequestCase(self, request):
      """
      @summary returns lower case parameter value (if supported)
      @param request: W*S request parameter  
      @return string
      @todo Use a more specific exception than Exception
      @todo Finish documenting
      """
      cmd = request.lower()
   
      if cmd == 'getcapabilities':
         return 'GetCapabilities'
      # WMS
      elif cmd == 'getmap':
         return 'GetMap'
      # WFS
      elif cmd == 'getfeature':
         return 'GetFeature'
      # WCS
      elif cmd == 'getcoverage':
         return 'GetCoverage'
      elif cmd == 'describecoverage':
         return 'DescribeCoverage'
      elif cmd == 'getlegendgraphic':
         return 'GetLegendGraphic'
      else:
         raise Exception('%s is not implemented on this server' % request)

# ...............................................
   def _getRGB(self, colorstring):
      if colorstring in PALETTES:
         pal = colorPalette(n=2, ptype=colorstring)
         return pal[1]
      else:
         return self._HTMLColorToRGB(colorstring)
      
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

# .............................................................................
# .............................................................................
if __name__ == "__main__":
   url = "http://yeti.lifemapper.org/services/maps?map=ctAncillProduction.map&height=200&width=400&request=GetMap&service=WMS&bbox=-4387050.0,-3732756.479,4073244.205,4704460.0&srs=epsg:2163&format=image/png&color=ffff00&version=1.1.0&styles=&layers=biome"
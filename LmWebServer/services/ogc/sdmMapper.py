""" 
Module to write a mapservice which will be cataloged in the SDL

@status: alpha
@author: Aimee Stewart
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import HTTPStatus

from LmServer.base.lmobj import LmHTTPError, LMError, LMObject
from LmServer.common.colorpalette import colorPalette
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import MapPrefix, DbUser, \
         LINE_SIZE, LINE_SYMBOL, MAP_TEMPLATE, OCC_NAME_PREFIX, POINT_SIZE, \
         POINT_SYMBOL, POLYGON_SIZE, PRJ_PREFIX
from LmServer.db.peruser import Peruser

PALETTES = ('gray', 'red', 'green', 'blue', 'safe', 'pretty', 'yellow', 'fuschia', 'aqua',
            'bluered', 'bluegreen', 'greenred')

"""
http://lifemapper.org/ogc?sdlsvc=pbj&sdllyr=3609357&request=GetMap&service=WMS&version=1.0.0&bbox=-180.0,-90.0,180.0,90.0&layers=pbj&crs=epsg:4326&format=image/png&resx=0.5&resy=0.5&color=00ff00
http://lifemapper.org/ogc?sdlsvc=pbj&sdllyr=3609357&service=WFS&version=1.1.0&request=GetFeature&bbox=-180.0,-90.0,180.0,90.0
http://129.237.201.132/ogc?sdlsvc=CRU_CL_1.0&sdllyr=curr_ann_rng_tmp&request=GetMap&service=WMS&version=1.0.0&bbox=-180.0,-90.0,180.0,90.0&layers=test&srs=epsg:4326&format=image/png
http://boris.nhm.ku.edu/ogc?map=political&request=GetMap&service=WMS&version=1.0.0&bbox=-180.0,-90.0,180.0,90.0&layers=country&styles=&srs=epsg:4326&format=image/png&width=800&height=400

GetFeature
DescribeFeatureType
"""

# .............................................................................
class MapConstructor(LMObject):
   """
   Private class to create a mapservice from a LayerSet object
   """
   
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger=None, overrideDB=None):
      """
      @summary MapConstructor constructor
      @param logger: Logger to use
      @todo Use lmPublic logger instead
      """
      LMObject.__init__(self)
      self.owsreq = mapscript.OWSRequest()
      self.log = logger
      self._peruser = Peruser(self.log, dbUser=DbUser.Map, overrideDB=overrideDB)
      self.queryString = None
      self._mapFilename = None
      self.serviceType = None
      self.requestType = None
      
      self.pointSymbol = 'filledcircle'
      self.lineSymbol = 'filledcircle'
      
# .............................................................................
# Public functions
# .............................................................................
   def assembleMap(self, parameters, template=MAP_TEMPLATE, sessionUser=None, 
                   mapFilename=None):
      """
      @summary Create a mapfile by creating an LayerSet with layer/s,
               from the SDL, then replacing strings in a template mapfile 
               with text created for the request.
      @param parameters: 
      @return a string representing a mapfile matching the request
      """
      [mfn, self.color, self.point] = self._findMapValues(parameters)
      
      if mapFilename is None:
         self._mapFilename = mfn
      else:
         self._mapFilename = mapFilename
      
#      # Temporary to ensure re-creation for files modified before last production update
#      if os.path.exists(self._mapFilename):
#         pth, mapname = os.path.split(self._mapFilename)
#         scencode, occsetId, usr, epsg, ancillary = Earl().parseMapname(mapname)
#         if not ancillary:
#            mtime = mx.DateTime.DateFromTicks(os.path.getmtime(self._mapFilename))
#            goodtime = mx.DateTime.DateTime(PROD_UPDATE_YR, PROD_UPDATE_MO, 
#                                            PROD_UPDATE_DY, 13)
#            if mtime < goodtime:
#               self._deleteFile(self._mapFilename)
            
      # if mapfile does not exist, create service from database, then write file
      if not (os.path.exists(self._mapFilename)):
         self.log.debug('File does not exist')
         if not self._peruser.isOpen:
            self.log.debug('Peruser is not open')
            self._peruser.openConnections()
            self.log.debug('Peruser open %s' % str(self._peruser.isOpen))
         try:
            mapsvc = self._peruser.getMapServiceFromMapFilename(self._mapFilename)
            self.log.debug('MapService retrieved %s' % str(mapsvc))
         except LMError, e:
            raise
         except Exception, e:
            raise LMError('Failed to get requested mapservice for %s: %s' 
                          % (self._mapFilename, str(e)))
         finally:
            self._peruser.closeConnections()
         
#          if sessionUser is not None:
#             if mapsvc.getUserId() not in [ARCHIVE_USER, DEFAULT_POST_USER, "changeThinking"]:
#                if mapsvc.getUserId() != sessionUser:
#                   raise LmHTTPError(HTTPStatus.FORBIDDEN, msg="You do not have permission to access this resource")
         if mapsvc is not None and mapsvc.count > 0:
            try:
               mapsvc.writeMap(template)
            except Exception, e:
                self.log.reportError('Crap! Error writing map (Exception: %s)' 
                                    % (str(e)))
         else:
            self.log.error('Crap! No layers in mapservice')

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
            content_type, content = self._wxsGetImage(map)
               
   #      elif service == 'WFS':
   #         if reqtype == 'GetFeature':
   #            return self._formatArgs(map, owsreq) 
            
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
   def _getAbsoluteMapFilenameAndUser(self, mapname):
      mapFname = None
      if mapname is not None:
#         mapFname = DataAddresser().getAbsoluteMapPath(mapname)
         mapFname, usr = EarlJr().getMapFilenameAndUserFromMapname(mapname)
      return mapFname, usr

# ...............................................
   def _findMapValues(self, args):
      reqtype = None
      mapname = None
      
      # Change color of points or projections if provided
      color = None
      point = None
      for key in args.keys():
         k = key.lower()
#         self.log.debug('key %s = %s' % (key, args[key]))
         if k == 'map':
            mapname = args[key]
         elif k == 'service':
            self.serviceType = args[key]
            self._appendToQuery(k, args[key])
         elif k == 'color':
            color =  args[key]
         elif k == 'point':
            point =  self._getPointVals(args[key])
         elif k == 'request':
            reqtype = self._fixRequestCase(args[key])
            self.requestType = reqtype
            self._appendToQuery(k, reqtype)
         else:
            self._appendToQuery(k, args[key])

      # Add mapname with full path we have not yet appended
      if mapname is not None:
         mapFilename, usr = self._getAbsoluteMapFilenameAndUser(mapname)
         self._appendToQuery('map', mapname)
      else:
         mapFilename = None
         # This is part of a transition from the current URLs to the new ones
         #raise LMError('Must specify a map and an occurrence or projection layer')
      
      return [mapFilename, color, point]
      
# ...............................................
   def _getPointVals(self, value):
      point = None
      vals = value.split(',')
      if len(vals) == 2 and self._isNumeric(vals[0]) and self._isNumeric(vals[1]):
         point = (vals[0], vals[1])
      return point
   
# ...............................................
   def _isNumeric(self, valstr):
      isnum = True
      try:
         int(valstr)
      except:
         try:
            float(valstr)
         except:
            isnum = False
      return isnum
   
# ...............................................
   def _appendToQuery(self, key, val):
      self.owsreq.setParameter(key, val)
      pair = '='.join([key, val])
      if self.queryString is None:
         self.queryString = pair
      else:
         self.queryString = '&'.join([self.queryString, pair])
      
# ...............................................
   def _wxsGetText(self, map):
      """
      @summary: Return XML response to an W*S request
      @param map: mapscript mapObject for which to return information
      @return: XML response string.
      """
      mapscript.msIO_installStdoutToBuffer()
      map.OWSDispatch( self.owsreq )
      content_type = mapscript.msIO_stripStdoutBufferContentType()
      content = mapscript.msIO_getStdoutBufferString()
      mapscript.msIO_resetHandlers()
      
      self.log.debug('content_type = ' + str(content_type))
      if content_type.endswith('_xml'):
         content_type = 'text/xml' 
      self.log.debug('content_type = ' + str(content_type))
   
      return content_type, content

# ...............................................
   def _wxsGetImage(self, map):
      """
      @summary 
      @param map: 
      @param owsreq:  
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
      for lyrname in lyrstr.split(','):
         lyr = map.getLayerByName(lyrname)
         if lyr is None:
            raise LMError('Layer %s does not exist in mapfile %s' 
                          % (lyrname, self._mapFilename))
      if self.color is not None: 
         self._changeDataColor(map)
      if self.point is not None: 
         self._addDataPoint(map)
      mapscript.msIO_installStdoutToBuffer()
      result = map.OWSDispatch( self.owsreq )
      content_type = mapscript.msIO_stripStdoutBufferContentType()
      # Get the image through msIO_getStdoutBufferBytes which uses GDAL, 
      # which is needed to process Float32 geotiff images
      content = mapscript.msIO_getStdoutBufferBytes()
      mapscript.msIO_resetHandlers()
      return content_type, content
   
# ...............................................
   def _findLayerToColor(self):
      """
      @note: This assumes that only one layer will be a candidate for change. If 
      more than one layer is specified and can be colored, the first one will be
      chosen.
      @todo: make this work like 'styles' parameter, with a comma-delimited list
             of colors, each entry applicable to the layer in the same position
      """
      mapname, ext = os.path.splitext(os.path.split(self._mapFilename)[1])
      lyrnames = self.owsreq.getValueByName('layers').split(',')
      colorme = None
      bluemarblelayer = 'bmng'
      
      # Archive maps have only OccurrenceSets, Projections, and Blue Marble
      if mapname.startswith(MapPrefix.SDM):
         for lyrname in lyrnames: 
            if lyrname.startswith(OCC_NAME_PREFIX):
               colorme = lyrname
            if lyrname.startswith(PRJ_PREFIX):
               colorme = lyrname
            
      elif mapname.startswith(MapPrefix.USER):
         for lyrname in lyrnames: 
            if lyrname.startswith(OCC_NAME_PREFIX):
               colorme = lyrname
            if lyrname.startswith(PRJ_PREFIX):
               colorme = lyrname
         if colorme is None:
            for lyrname in lyrnames: 
               if lyrname != bluemarblelayer:
                  colorme = lyrname

      elif (mapname.startswith(MapPrefix.SCEN) or
            mapname.startswith(MapPrefix.ANC)):
         for lyrname in lyrnames: 
            if lyrname != bluemarblelayer:
               colorme = lyrname
               
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
   def _addDataPoint(self, map):
      if self.point is not None:
         lyrstr = self.owsreq.getValueByName('layers')
         # Only adds point if layer 'emptypt' is present
         for lyrname in lyrstr.split(','):
            if lyrname == 'emptypt':
               if self.color is not None:
                  (r, g, b) = self._getRGB(self.color)
               else:
                  (r, g, b) = (255, 127, 0)
               lyrtext = '\n'.join(('  LAYER',
                                    '    NAME  \"emptypt\"',
                                    '    TYPE  POINT',
                                    '    STATUS  ON',
                                    '    OPACITY 100',
                                    '    DUMP  TRUE',
                                    '    FEATURE POINTS %s %s END END' %
                                    (str(self.point[0]), str(self.point[1])),
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
   def _fixRequestCase(self, request):
      """
      @summary 
      @param request:  
      @return 
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


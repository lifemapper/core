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

from LmCommon.common.lmconstants import HTTPStatus

from LmServer.base.lmobj import LmHTTPError, LMError, LMObject
from LmServer.common.colorpalette import colorPalette
from LmServer.common.lmconstants import (MapPrefix, DbUser, LINE_SIZE, 
         LINE_SYMBOL, MAP_TEMPLATE, OCC_NAME_PREFIX, POINT_SIZE, POINT_SYMBOL, 
         POLYGON_SIZE, PRJ_PREFIX, WEB_PATH, MAP_PATH)
from LmServer.common.localconstants import APP_PATH
from LmServer.db.peruser import Peruser

PALETTES = ('gray', 'red', 'green', 'blue', 'safe', 'pretty', 'yellow', 'fuschia', 'aqua',
            'bluered', 'bluegreen', 'greenred')

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
      self.queryString = None
      self._mapFilename = None
      self.serviceType = None
      self.requestType = None
      
# .............................................................................
# Public functions
# .............................................................................
   def assembleMap(self, parameters, template=MAP_TEMPLATE, mapFilename=None):
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
         raise LMError('File does not exist')

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


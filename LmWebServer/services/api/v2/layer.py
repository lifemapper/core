"""
@summary: This module provides REST services for Layers

@author: CJ Grady
@version: 2.0
@status: alpha

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

import cherrypy

from LmServer.common.localconstants import PUBLIC_USER
from LmServer.legion.envlayer import EnvLayer
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathLayerId')
class Layer(LmService):
   """
   @summary: This class is for the layers service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   def DELETE(self, pathLayerId):
      """
      @summary: Attempts to delete a layer
      @param pathLayerId: The id of the layer to delete
      """
      lyr = self.scribe.getLayer(lyrId=pathLayerId)
      if lyr is None:
         raise cherrypy.HTTPError(404, "Layer not found")
      
      # If allowed to, delete
      if checkUserPermission(self.getUserId(), lyr, HTTPMethod.DELETE):
         success = self.scribe.deleteObject(lyr)
         if success:
            cherrypy.response.status = 204
            return 
         else:
            # TODO: Could this happen if this layer is in a scenario that is 
            #    being used?  If so, this should be a 4xx error
            raise cherrypy.HTTPError(500, 
                        "Failed to delete layer")
      else:
         raise cherrypy.HTTPError(403, 
                 "User does not have permission to delete this layer")

   # ................................
   @lmFormatter
   def GET(self, pathLayerId=None, afterTime=None, altPredCode=None, 
           beforeTime=None, dateCode=None, epsgCode=None, envCode=None, 
           envTypeId=None, gcmCode=None, layerType=None, limit=100, offset=0, 
           public=None, scenarioId=None, squid=None):
      """
      @summary: Performs a GET request.  If a layer id is provided,
                   attempt to return that item.  If not, return a list of 
                   layers that match the provided parameters
      """
      # Layer type:
      #   0 - Anything
      #   1 - Environmental layer
      #   2 - ? (Not implemented yet)
      if public:
         userId = PUBLIC_USER
      else:
         userId = self.getUserId()
      
      if layerType is None or layerType == 0:
         if pathLayerId is None:
            return self._listLayers(userId, afterTime=afterTime, 
                                    beforeTime=beforeTime, epsgCode=epsgCode, 
                                    limit=limit, offset=offset, squid=squid)
         elif pathLayerId.lower() == 'count':
            return self._countLayers(userId, afterTime=afterTime, 
                                    beforeTime=beforeTime, epsgCode=epsgCode, 
                                    squid=squid)
         else:
            return self._getLayer(pathLayerId, envLayer=False)
      else:
         if pathLayerId is None:
            return self._listEnvLayers(userId, afterTime=afterTime, 
                               altPredCode=altPredCode, beforeTime=beforeTime, 
                               dateCode=dateCode, envCode=envCode, 
                               envTypeId=envTypeId, epsgCode=epsgCode, 
                               gcmCode=gcmCode, limit=limit, offset=offset, 
                               scenarioId=scenarioId)
         elif pathLayerId.lower() == 'count':
            return self._countEnvLayers(userId, afterTime=afterTime, 
                               altPredCode=altPredCode, beforeTime=beforeTime, 
                               dateCode=dateCode, envCode=envCode, 
                               envTypeId=envTypeId, epsgCode=epsgCode, 
                               gcmCode=gcmCode, scenarioId=scenarioId)
         else:
            return self._getLayer(pathLayerId, envLayer=True)
      
   # ................................
   @lmFormatter
   def POST(self, layerType, epsgCode, layerName, 
            envLayerTypeId=None, additionalMetadata=None, valUnits=None,
            envCode=None, gcmCode=None, alternatePredictionCode=None, 
            dateCode=None):
      """
      @summary: Posts a new layer
      @todo: Add file type parameter or look at headers?
      """
      if layerType is None or layerType == 0:
         # Generic layer
         pass
      elif layerType == 1:
         # Environmental layer0
         lyrContent = cherrypy.request.body
         lyr = EnvLayer(layerName, self.getUserId(), epsgCode, 
                        lyrMetadata=additionalMetadata, valUnits=valUnits, 
                        valAttibut='pixel', envCode=envCode, gcmCode=gcmCode,
                        altpredCode=alternatePredictionCode, dateCode=dateCode,
                        envTypeId=envLayerTypeId)
         lyr.writeRaster(srcData=lyrContent)
         updatedLyr = self.scribe.findOrInsertEnvLayer(lyr)

         return updatedLyr
   
   # ................................
   #@cherrypy.tools.json_out
   #def PUT(self, pathLayerId, epsgCode, envLayerType, name=None, isCategorical=None,
   #         envLayerTypeId=None, additionalMetadata=None, valUnits=None,
   #         gcmCode=None, alternatePredictionCode=None, dateCode=None):
   #   pass
   
   # ................................
   def _countEnvLayers(self, userId, afterTime=None, altPredCode=None, 
                            beforeTime=None, dateCode=None, envCode=None, 
                            envTypeId=None, epsgCode=None, gcmCode=None, 
                            scenarioId=None):
      """
      @summary: Count environmental layer objects matching the specified 
                   criteria
      @param userId: The user to list environmental layers for.  Note that this
                        may not be the same user logged into the system
      @param afterTime: (optional) Return layers modified after this time 
                           (Modified Julian Day)
      @param altPredCode: (optional) Return layers with this alternate 
                             prediction code
      @param beforeTime: (optional) Return layers modified before this time 
                            (Modified Julian Day)
      @param dateCode: (optional) Return layers with this date code
      @param envCode: (optional) Return layers with this environment code
      @param envTypeId: (optional) Return layers with this environmental type
      @param epsgCode: (optional) Return layers with this EPSG code
      @param gcmCode: (optional) Return layers with this GCM code
      @param scenarioId: (optional) Return layers from this scenario
      """
      lyrCount = self.scribe.countEnvLayers(userId=userId, 
                                   envCode=envCode, gcmcode=gcmCode,
                                   altpredCode=altPredCode, dateCode=dateCode, 
                                   afterTime=afterTime, beforeTime=beforeTime,
                                   epsg=epsgCode, envTypeId=envTypeId,
                                   scenarioId=scenarioId)
      # Format return
      # Set headers
      return {"count" : lyrCount}

   # ................................
   def _countLayers(self, userId, afterTime=None, beforeTime=None, epsgCode=None, 
                         squid=None):
      """
      @summary: Return a count of layers matching the specified criteria
      @param userId: The user to list layers for.  Note that this may not be
                        the same user that is logged into the system
      @param afterTime: (optional) List layers modified after this time 
                           (Modified Julian Day)
      @param beforeTime: (optional) List layers modified before this time
                            (Modified Julian Day)
      @param epsgCode: (optional) Return layers that have this EPSG code
      @param limit: (optional) Return this number of layers, at most
      @param offset: (optional) Offset the returned layers by this number
      @param squid: (optional) Return layers with this species identifier
      """
      lyrCount = self.scribe.countLayers(userId=userId, 
                                        squid=squid, afterTime=afterTime, 
                                        beforeTime=beforeTime, epsg=epsgCode)
      # Format return
      # Set headers
      return {"count" : lyrCount}

   # ................................
   def _getLayer(self, pathLayerId, envLayer=False):
      """
      @summary: Attempt to get a layer
      """
      if envLayer:
         lyr = self.scribe.getEnvLayer(lyrId=pathLayerId)
      else:
         lyr = self.scribe.getLayer(lyrId=pathLayerId)
      if lyr is None:
         raise cherrypy.HTTPError(404, 
                        'Environmental layer {} was not found'.format(pathLayerId))
      if checkUserPermission(self.getUserId(), lyr, HTTPMethod.GET):
         return lyr
      else:
         raise cherrypy.HTTPError(403, 
                  'User {} does not have permission to access layer {}'.format(
                     self.getUserId(), pathLayerId))
   
   # ................................
   def _listEnvLayers(self, userId, afterTime=None, altPredCode=None, 
                            beforeTime=None, dateCode=None, envCode=None, 
                            envTypeId=None, epsgCode=None, gcmCode=None, 
                            limit=100, offset=0, scenarioId=None):
      """
      @summary: List environmental layer objects matching the specified 
                   criteria
      @param userId: The user to list environmental layers for.  Note that this
                        may not be the same user logged into the system
      @param afterTime: (optional) Return layers modified after this time 
                           (Modified Julian Day)
      @param altPredCode: (optional) Return layers with this alternate 
                             prediction code
      @param beforeTime: (optional) Return layers modified before this time 
                            (Modified Julian Day)
      @param dateCode: (optional) Return layers with this date code
      @param envCode: (optional) Return layers with this environment code
      @param envTypeId: (optional) Return layers with this environmental type
      @param epsgCode: (optional) Return layers with this EPSG code
      @param gcmCode: (optional) Return layers with this GCM code
      @param limit: (optional) Return this number of layers, at most
      @param offset: (optional) Offset the returned layers by this number
      @param scenarioId: (optional) Return layers from this scenario
      """
      lyrAtoms = self.scribe.listEnvLayers(offset, limit, userId=userId, 
                                   envCode=envCode, gcmcode=gcmCode,
                                   altpredCode=altPredCode, dateCode=dateCode, 
                                   afterTime=afterTime, beforeTime=beforeTime,
                                   epsg=epsgCode, envTypeId=envTypeId,
                                   scenarioId=scenarioId)
      # Format return
      # Set headers
      return lyrAtoms
   
   # ................................
   def _listLayers(self, userId, afterTime=None, beforeTime=None, epsgCode=None, 
                         limit=100, offset=0, squid=None):
      """
      @summary: Return a list of layers matching the specified criteria
      @param userId: The user to list layers for.  Note that this may not be
                        the same user that is logged into the system
      @param afterTime: (optional) List layers modified after this time 
                           (Modified Julian Day)
      @param beforeTime: (optional) List layers modified before this time
                            (Modified Julian Day)
      @param epsgCode: (optional) Return layers that have this EPSG code
      @param limit: (optional) Return this number of layers, at most
      @param offset: (optional) Offset the returned layers by this number
      @param squid: (optional) Return layers with this species identifier
      """
      lyrAtoms = self.scribe.listLayers(offset, limit, userId=userId, 
                                        squid=squid, afterTime=afterTime, 
                                        beforeTime=beforeTime, epsg=epsgCode)
      # Format return
      # Set headers
      return lyrAtoms
   
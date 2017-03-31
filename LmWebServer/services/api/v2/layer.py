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

from LmWebServer.services.api.v2.base import LmService

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('layerId')
class Layer(LmService):
   """
   @summary: This class is for the layers service.  The dispatcher is
                responsible for calling the correct method
   """
   # ................................
   def DELETE(self, layerId):
      """
      @summary: Attempts to delete a layer
      @param layerId: The id of the layer to delete
      @todo: Need user id
      """
      pass

   # ................................
   def GET(self, layerId=None, afterTime=None, beforeTime=None, epsgCode=None, 
           isCategorical=None, limit=100, offset=0, public=None, 
           scenarioId=None):
      """
      @summary: Performs a GET request.  If a layer id is provided,
                   attempt to return that item.  If not, return a list of 
                   layers that match the provided parameters
      """
      if layerId is None:
         return self._listLayers(afterTime=afterTime, 
                beforeTime=beforeTime, epsgCode=epsgCode, 
                isCategorical=isCategorical, limit=limit, offset=offset, 
                public=public, scenarioId=scenarioId)
      else:
         return self._getLayer(layerId)
   
   # ................................
   @cherrypy.json_out
   def POST(self, epsgCode, envLayerType, name=None, isCategorical=None,
            envLayerTypeId=None, additionalMetadata=None, valUnits=None,
            gcmCode=None, alternatePredictionCode=None, dateCode=None):
      """
      @summary: Posts a new layer
      @todo: Add file type parameter or look at headers?
      @todo: User id
      """
      pass
   
   # ................................
   @cherrypy.json_out
   def PUT(self, layerId, epsgCode, envLayerType, name=None, isCategorical=None,
            envLayerTypeId=None, additionalMetadata=None, valUnits=None,
            gcmCode=None, alternatePredictionCode=None, dateCode=None):
      pass
   
   # ................................
   def _getLayer(self, layerId):
      """
      @summary: Attempt to get a layer
      """
      pass
   
   # ................................
   def _listLayers(self, afterTime=None, beforeTime=None, epsgCode=None, 
                  isCategorical=None, limit=100, offset=0, public=None, 
                  scenarioId=None):
      """
      @summary: Return a list of layers matching the specified criteria
      """
      pass
   
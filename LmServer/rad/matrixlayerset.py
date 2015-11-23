""" 
    Module for object to store metadata collected from spatial data for 
    cataloging in the SDL.  The spatial data may be either a mapservice, or
    data files on the filesystem.

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
from LmCommon.common.lmconstants import InputDataType
from LmServer.base.layer import _Layer, _LayerParameters
from LmServer.base.layerset import _LayerSet
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule
from LmServer.rad.anclayer import _AncillaryValue
from LmServer.rad.palayer import _PresenceAbsence

# .............................................................................
# .............................................................................
class MatrixLayerset(_LayerSet, ServiceObject):
   """
   @summary: Object that extends a _LayerSet and is used for organizing 
             layers in a biogeography experiment.  
   @note: The layerset name is constructed with the enclosing experimentId, 
          so will be None until after the experiment has been inserted into
          the database. 
   @note: Each layer in the layerset must have a unique name + parametersId 
   """
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, name, epsgcode=None, lyrIndices=None, url=None,
                keywords=None, description=None, layers=None, 
                userId=None, expId=None, createTime=None, modTime=None, 
                serviceType=LMServiceType.LAYERSETS):
      """
      @summary LayerSet constructor
      @todo: remove name and description since we will not map these layersets
      @param name: Short name of layerset, used as map name and filename
      @param userId: id for the owner of these data
      @param expId: database id of the RADExperiment for this layerset 
      @todo: remove moduleType from here, ServiceObject from _LayerSet
      """
      self._layerIndices = {}
#       MapLayerSet.__init__(self, name, 
#                            url=url, keywords=keywords, 
#                            epsgcode=epsgcode, layers=layers, userId=userId, 
#                            dbId=expId)
      _LayerSet.__init__(self, name, keywords=keywords, 
                         epsgcode=epsgcode, layers=layers)
      ServiceObject.__init__(self, userId, expId, createTime, modTime, 
                             serviceType, moduleType=LMServiceModule.RAD, 
                             metadataUrl=url)
      # layerIndices are a dictionary of 
      #   {layerIndex: (name, layerMetadataUrl, layerId, parametersId, treeIndex)}
      # if provided, layerIndices dictionary entries are added to those of 
      # existing layers, which may not be fully populated
      self.setLayerIndices(lyrIndices)
      
   
# ...............................................
   # layers property code overrides the same methods in layerset.LayerSet
   def _getLayers(self):
      return self._layers
      
   def _setLayers(self, lyrs):
      """
      @note: Overrides LayerSet._setLayers by requiring all layers be the same
             type of object inheriting from _Layer and _LayerParameters
      """
      self._layers = []
      if lyrs:
         for lyr in lyrs:
            self.addLayer(lyr) 
         self._bbox = _LayerSet._getUnionBounds(self)
         
         
# ...............................................
   @property
   def matrixType(self):
      """
      @summary: Identify the type of layerset  
      """
      if len(self._layers) == 0:
         return None
      elif isinstance(self.layers[0], _AncillaryValue):
         return InputDataType.USER_ANCILLARY
      elif isinstance(self.layers[0], _PresenceAbsence):
         return InputDataType.USER_PRESENCE_ABSENCE
   
# .........................................................................
# Public Properties
# .........................................................................
   
   layers = property(_getLayers, _setLayers)
   
# .........................................................................
# Public Methods
# .........................................................................
# .............................................................................
   def _isValidLayer(self, lyr):
      """
      @summary: Check a layer for the MatrixLayerset.  
      @param lyr: Layer to join this MatrixLayerset.
      @raise LMError: on layer that is 
                         a) not a _LayerParameters instance
                         b) not unique in this Layerset
                         c) a different type than existing layers
                         d) a different epsgcode than existing layers  
      """
      success = False
      if not(isinstance(lyr, _LayerParameters) and isinstance(lyr, _Layer)):
         raise LMError('Layers must also inherit from _LayerParameters')
      
      if self.getLayer(lyr.getId(), lyr.getParametersId()) is None:
         if self.layers:
            if self._epsg != lyr.epsgcode:
               raise LMError('New layer must match existing LayerSet epsg %s'
                             % str(self._epsg))
            lyr1 = self.layers[0]
            if (isinstance(lyr1, _AncillaryValue) and 
                not isinstance(lyr, _AncillaryValue)):
               raise LMError('New layer type %s must be type _AncillaryValue'
                             % str(type(lyr)))
            elif (isinstance(lyr1, _PresenceAbsence) and 
                not isinstance(lyr, _PresenceAbsence)):
               raise LMError('New layer type %s must be type _PresenceAbsence'
                             % str(type(lyr)))
         success = True
      else:
         raise LMError('Layer %s with parameters %s already exists in this layerset' 
                       % (lyr.getId(), lyr.getParametersId()))

      return success
   
# .............................................................................
   def addLayer(self, lyr):
      """
      @summary: Add an existing layer to the MatrixLayerset.  
      @param lyr: Layer to add to this MatrixLayerset.
      @note: Since this layer has been added to the MatrixLayerset already, this
             is only used when re-populating the MatrixLayerset; it has been 
             checked for validity and the MatrixIndex has already been set for 
             this layer.
      """
      if self._isValidLayer(lyr):
         self._layers.append(lyr)
         self.addLayerIndex(lyr)

## .............................................................................
#   def updateLayer(self, lyr):
#      """
#      @summary: Update an existing layer in the MatrixLayerset.  
#      @param lyr: Layer to replace in this MatrixLayerset.
#      @note: Since this layer has been added to the MatrixLayerset already, this
#             is only used when re-populating the MatrixLayerset; it has been 
#             checked for validity and the MatrixIndex has already been set for 
#             this layer.
#      """
#      if self._isValidLayer(lyr):
#         self._layers.append(lyr)
#         self.addLayerIndex(lyr)
# .............................................................................
   def setLayers(self, layers):
      """
      To be done with a set of layers already added to the database
      """
      self._layers = []
      self._layerIndices = {}
      for lyr in layers:
         self.addLayer(lyr)
         
# .............................................................................
   def addLayerIndex(self, lyr):
      """
      @summary: Add an existing layer to the MatrixLayerset.  
      @param lyr: Layer to add to this MatrixLayerset.
      @note: Since this layer has been added to the MatrixLayerset already, this
             is only used when re-populating the MatrixLayerset; it has been 
             checked for validity and the MatrixIndex has already been set for 
             this layer.
      """
      idx = lyr.getMatrixIndex()
      if idx >= 0:
         # This overrides an existing entry for the same index
         self._layerIndices[idx] = (lyr.name, lyr.metadataUrl, 
                                    lyr.getId(), lyr.getParametersId(),
                                    lyr.getTreeIndex())
                     
# ...............................................
   def setLayerIndices(self, lyrIndices=None):
      """
      @summary: Add layer matrixIndex and identifiers 
                  (name, metadataUrl, lyrId, parametersId, treeNodeIdx) 
                to the layerIndices dictionary for all layers.  
      """
      self._layerIndices = {}
      if lyrIndices is not None:
         # add new lyrIndices entries to existing dictionary
         for idx, metaTuple in lyrIndices.iteritems():
            if idx >= 0 and not self._layerIndices.has_key(idx):
               self._layerIndices[idx] = metaTuple
      else:
         for lyr in self.layers:
            self.addLayerIndex(lyr)
   
# ...............................................
   def getLayerIndices(self):
      return self._layerIndices
   
# ...............................................
   def getLayerIndicesAsArray(self):
      import numpy as np
      mtype = np.dtype([('name', np.str_, 50), 
                        ('url', np.str_, 512), 
                        ('layerId', np.int_), 
                        ('paramId', np.int_), 
                        ('treeIdx', np.int_)])
      litype = np.dtype([('matrixIdx', np.int_), ('layerMeta', mtype)])
      lilist = [(k,v) for k,v in self._layerIndices.iteritems()]
      npIndices = np.array(lilist, dtype=litype)
      return npIndices

# ...............................................
   def _getLayerCount(self):
      if not self._layerIndices:
         self.setLayerIndices()
      if self._layerIndices:
         return len(self._layerIndices)
      else:
         return 0
      
   count = property(_getLayerCount)

# ...............................................
   def getLayer(self, metaurl, paramid):
      """
      @note: Using the URL instead of the layer.name ensures that we do not 
             have to assume the layer is owned by the Experiment user and we 
             are not restricted to LM-stored layers
      """
      if self.layers and metaurl and paramid:
         for lyr in self._layers:
            if (lyr.metadataUrl == metaurl 
                and lyr.getParametersId() == paramid):
               return lyr
      return None
   
# ...............................................
   def getLayerByIndex(self, idx):
      metaurl, paramid = self._layerIndices[idx]
      lyr = self.getLayer(metaurl, paramid)
      return lyr

# ...............................................
   def getLayerIndex(self, murl, paramid):
      lyridx = None
      if self.layers:
         lyr = self.getLayer(murl, paramid)
         if lyr:
            lyridx = lyr.getMatrixIndex()
      elif self._layerIndices:
         for idx, (metaurl, parameterid) in self._layerIndices.iteritems():
            if metaurl == murl and parameterid == paramid:
               lyridx = idx
         
      return lyridx
      
# ...............................................
   def initLayersPresent(self):
      layersPresent = {}
      if not self._layerIndices:
         self.setLayerIndices()
      if self._layerIndices is not None:
         for l in self._layerIndices.keys():
            layersPresent[l] = True
      return layersPresent   




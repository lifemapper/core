"""
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
import json
from LmServer.base.layerset import MapLayerSet
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import LMFileType, LMServiceType, LMServiceModule
from LmServer.common.localconstants import ARCHIVE_USER
from LmServer.sdm.envlayer import EnvironmentalLayer

# .........................................................................
class Scenario(MapLayerSet):
   """       
   Class to hold a set of Raster object environmental data layers 
   that are used together for creating or projecting a niche model
   """
# .............................................................................
# Constructor
# .............................................................................

   # ...............................................       
   def __init__(self, code, metadata={},
                metadataUrl=None, dlocation=None,
                # TODO: Remove
                startdt=None, enddt=None, 
                title=None, author=None, description=None,
                # new
                units=None, res=None, 
                gcmCode=None, altpredCode=None, dateCode=None,
                bbox=None, modTime=None, keywords=None, epsgcode=None,
                layers=None, userId=ARCHIVE_USER, scenarioid=None):
      """
      @summary Constructor for the scenario class 
      @param code: The code for this set of layers
      @param metadataUrl: Lifemapper metadataUrl of this set of layers
      @param units: units of measurement for pixel size
      @param res: size of each side of a pixel (assumes square pixels)
      @param bbox: (optional) a length 4 tuple of (minX, minY, maxX, maxY)
      @param modTime: (optional) Last modification time of the object (any of 
                        the points (saved in the PBJ) included in this 
                        OccurrenceSet) in modified julian date format; 
                        0 if points have not yet been collected and saved
      @param layers: (optional) list of Raster layers of environmental data
      @param userid: ID of the user associated with this scenario
      @param scenarioid: (optional) The scenario/service id in the database
      """
      self._layers = []
      # layers are set not set in LayerSet or Layerset - done here to check
      # that each layer is an EnvironmentalLayer
      MapLayerSet.__init__(self, code, 
                           title=title, url=metadataUrl, 
                           dlocation=dlocation, keywords=keywords, 
                           epsgcode=epsgcode, userId=userId, dbId=scenarioid,
                           serviceType=LMServiceType.SCENARIOS, moduleType=LMServiceModule.SDM)      
      # aka MapLayerSet.name    
      self.code = code
      # Move to self.metadata
      self.author = author
      self.description = description  
      # obsolete
      self.startDate = startdt
      self.endDate = enddt
      
      self.modTime = modTime
      # new
      self.gcmCode=None
      self.altpredCode=None
      self.dateCode=None
      self.metadata = {}
      self.loadMetadata(metadata)
      
      # Private attributes
      self._scenarioId = scenarioid
      self._setLayers(layers)
      self._setUnits(units)
      self._setRes(res) 
      self._setBBox(bbox)
      self.setMapPrefix()
      self.setLocalMapFilename()
            
   # ...............................................
   def setId(self, id):
      """
      @summary: Sets the database id on the object
      @param id: The database id for the object
      """
      MapLayerSet.setId(self, id)

   # ...............................................
   # layers property code overrides the same methods in layerset.LayerSet
   def _getLayers(self):
      return self._layers
      
   def _setLayers(self, lyrs):
      """
      @todo: Overrides LayerSet._setLayers by requiring identical resolution and 
             mapunits for all layers.
      """
      self._layers = []
      if lyrs:
         for lyr in lyrs:
            self.addLayer(lyr) 
         self._bbox = MapLayerSet._getIntersectBounds(self)

# ...............................................
   def addMetadata(self, metadict):
      for key, val in metadict.iteritems():
         self.metadata[key] = val
         
   def dumpMetadata(self):
      metastring = None
      if self.metadata:
         metastring = json.dumps(self.metadata)
      return metastring

   def loadMetadata(self, meta):
      """
      @note: Adds to dictionary or modifies values for existing keys
      """
      if meta is not None:
         if isinstance(meta, dict): 
            self.addMetadata(meta)
         else:
            try:
               metajson = json.loads(meta)
            except Exception, e:
               print('Failed to load JSON object from {} object {}'
                     .format(type(meta), meta))
            else:
               self.addMetadata(metajson)
   
   # ...............................................
   def _setUnits(self, units):
      if units is not None:
         self._units = units 
      elif len(self._layers) > 0:
         self._units = self._layers[0].mapUnits
      else:
         self._units = None
         
   def _getUnits(self):
      if self._units is None and len(self._layers) > 0:
         self._units = self._layers[0].mapUnits
      return self._units   
      
   # ...............................................       
   def _setRes(self, res):
      if res is not None:
         self._resolution = res 
      elif len(self._layers) > 0:
         self._resolution = self._layers[0].resolution
      else:
         self._resolution = None
         
   def _getRes(self):
      if self._resolution is None and len(self._layers) > 0:
         self._resolution = self._layers[0].resolution
      return self._resolution
   
# .........................................................................
# Public Properties
# .........................................................................
   ## Units of the (1st) Scenario layers
   units = property(_getUnits, _setUnits)
   
   ## Resolution of the (1st) Scenario layers
   resolution = property(_getRes, _setRes)
   
   layers = property(_getLayers, _setLayers)
   
   
# .........................................................................
# Public Methods
# .........................................................................
   
   def addLayer(self, lyr):
      """
      @summary: Add a layer to the scenario.  
      @param lyr: Layer (layer.Raster) to add to this scenario.
      @raise LMError: on layer that is not Raster 
      """
      if lyr is not None:
         if lyr.getId() is None or self.getLayer(lyr.metadataUrl) is None:
            if isinstance(lyr, EnvironmentalLayer):
               self._layers.append(lyr)
               self._bbox = MapLayerSet._getIntersectBounds(self)
               # Set mapPrefix only if does not exist. Could be in multiple 
               # mapfiles, but all should use same url/mapPrefix.
               if lyr.mapPrefix is None:
                  lyr._setMapPrefix(scencode=self.code)
               mapfname = self.createLocalMapFilename()
               lyr.setLocalMapFilename(mapfname=mapfname)
            else:
               raise LMError(['Attempt to add non-Raster layer'])

# ...............................................
   def createMapPrefix(self, lyrname=None):
      """
      @summary Gets the OGC service URL prefix for this object
      @return URL string representing a webservice request for maps of this object
      """
      mapprefix = self._earlJr.constructMapPrefix(ftype=LMFileType.SCENARIO_MAP, 
                     scenarioCode=self.code, lyrname=lyrname, usr=self._userId, 
                     epsg=self._epsg)
      return mapprefix
    
   def _setMapPrefix(self, mapprefix=None):
      if mapprefix is None:
         mapprefix = self.createMapPrefix()
      self._mapPrefix = mapprefix
   
   @property
   def mapPrefix(self): 
      return self._mapPrefix

# ...............................................
   def createLocalMapFilename(self):
      """
      @summary: Find mapfile containing this layer.  
      """
      mapfname = self._earlJr.createFilename(LMFileType.SCENARIO_MAP, 
                    scenarioCode=self.code, usr=self._userId, epsg=self._epsg)
      return mapfname
   

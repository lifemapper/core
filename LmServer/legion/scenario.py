"""
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
from LmBackend.common.lmobj import LMError
from LmServer.base.layerset import MapLayerSet
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import LMFileType, LMServiceType
from LmServer.legion.envlayer import EnvLayer

# .........................................................................
class ScenPackage(ServiceObject, LMSpatialObject):
   """       
   Class to hold a set of Raster object environmental data layers 
   that are used together for creating or projecting a niche model
   """
# .............................................................................
# Constructor
# .............................................................................
   # ...............................................       
   def __init__(self, name, userId, 
                metadata={},
                metadataUrl=None, 
                epsgcode=None, 
                bbox=None, 
                mapunits=None,
                modTime=None, 
                scenarios=None, 
                scenPackageId=None):
      """
      @summary Constructor for the ScenPackage class 
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @param name: The name for this set of scenarios
      @param scenarios: list of Scenario objects
      """
      ServiceObject.__init__(self, userId, scenPackageId, 
                             LMServiceType.SCEN_PACKAGES, 
                             metadataUrl=metadataUrl, 
                             modTime=modTime)
      LMSpatialObject.__init__(self, epsgcode, bbox, mapunits)
      self.name = name
      self.loadScenpkgMetadata(metadata)
      
      self._scenarios = {}
      self.setScenarios(scenarios)
      if self._bbox is None:
         self.resetBBox()
      
# .............................................................................
   def resetBBox(self):
      bboxList = [scen.bbox for scen in self._scenarios.values()]
      minBbox = self.intersectBoundingBoxes(bboxList)
      self._setBBox(minBbox)

# .............................................................................
   def addScenario(self, scen):
      """
      @summary: Add a scenario to an ScenPackage.  
      @note: metadataUrl or scenario code (unique for a user), is used 
             to ensure that a scenario is not duplicated in the ScenPackage.  
      """
      if isinstance(scen, Scenario):
         if scen.getUserId() == self.getUserId():
            if self.getScenario(code=scen.code, 
                                metadataUrl=scen.metadataUrl) is None:
               self._scenarios[scen.code] = scen
         else:
            raise LMError(['Cannot add user {} Scenario to user {} ScenPackage'
                           .format(scen.getUserId(), self.getUserId())])
      else:
         raise LMError(['Cannot add {} as a Scenario'.format(type(scen))])
         
# .............................................................................
   def getScenario(self, code=None, metadataUrl=None):
      """
      @summary Gets a scenario from the ScenPackage with the specified metadataUrl
      @param metadataUrl: metadataUrl for which to find matching scenario
      @param userId: user for which to find matching scenario with code
      @param code: code for which to find matching scenario with userId
      @return the LmServer.legion.Scenario object with the given metadataUrl, 
             or userId/code combination. None if not found.
      """
      for scen in self._scenarios:
         if code is not None:
            try:
               scen = self._scenarios[code]
            except:
               pass
            else:
               return scen
         elif metadataUrl is not None:
            for code, scen in self._scenarios.iteritems():
               if scen.metadataUrl == metadataUrl:
                  return scen
      return None

   # ...............................................
   # layers property code overrides the same methods in layerset.LayerSet
   @property
   def scenarios(self):
      return self._scenarios
      
   def setScenarios(self, scens):
      """
      @summary: sets the scenarios in the ScenPackage
      @param scens: list of scenarios
      """
      self._scenarios = {}
      if scens:
         for scen in scens:
            self.addScenario(scen)

# ...............................................
   def dumpScenpkgMetadata(self):
      return super(ScenPackage, self)._dumpMetadata(self.scenpkgMetadata)
 
# ...............................................
   def loadScenpkgMetadata(self, newMetadata):
      self.scenpkgMetadata = super(ScenPackage, self)._loadMetadata(newMetadata)

# ...............................................
   def addScenpkgMetadata(self, newMetadataDict):
      self.scenpkgMetadata = super(ScenPackage, self)._addMetadata(newMetadataDict, 
                                  existingMetadataDict=self.scenpkgMetadata)


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
   def __init__(self, code, userId, epsgcode, metadata={},
                metadataUrl=None, 
                units=None, res=None, 
                gcmCode=None, altpredCode=None, dateCode=None,
                bbox=None, modTime=None, 
                layers=None, scenarioid=None):
      """
      @summary Constructor for the scenario class 
      @copydoc LmServer.base.layerset.MapLayerSet::__init__()
      @param code: The code for this set of layers
      @param res: size of each side of a pixel (assumes square pixels)
      @param modTime: (optional) Last modification time of the object (any of 
                        the points (saved in the PBJ) included in this 
                        OccurrenceSet) in modified julian date format; 
                        0 if points have not yet been collected and saved
      @param layers: list of Raster layers of environmental data
      @param scenarioid: The scenarioId in the database
      """
      self._layers = []
      # layers are set not set in LayerSet or Layerset - done here to check
      # that each layer is an EnvLayer
      MapLayerSet.__init__(self, code, 
                           url=metadataUrl, 
                           epsgcode=epsgcode, userId=userId, dbId=scenarioid,
                           bbox=bbox, mapunits=units, modTime=modTime,
                           serviceType=LMServiceType.SCENARIOS,
                           mapType=LMFileType.SCENARIO_MAP)
      # aka MapLayerSet.name    
      self.code = code

      self.gcmCode=gcmCode
      self.altpredCode=altpredCode
      self.dateCode=dateCode
      self.loadScenMetadata(metadata)
      
      # Private attributes
      self._scenarioId = scenarioid
      self._setLayers(layers)
      self._setRes(res) 
      self.setMapPrefix()
      self.setLocalMapFilename()
            
   # ...............................................
   def setId(self, scenid):
      """
      @summary: Sets the database id on the object
      @param scenid: The database id for the object
      """
      MapLayerSet.setId(self, scenid)

# ...............................................
   def dumpScenMetadata(self):
      return super(Scenario, self)._dumpMetadata(self.scenMetadata)
 
# ...............................................
   def loadScenMetadata(self, newMetadata):
      self.scenMetadata = super(Scenario, self)._loadMetadata(newMetadata)

# ...............................................
   def addScenMetadata(self, newMetadataDict):
      self.scenMetadata = super(Scenario, self)._addMetadata(newMetadataDict, 
                                  existingMetadataDict=self.scenMetadata)


   # ...............................................
   # layers property code overrides the same methods in layerset.LayerSet
   @property
   def layers(self):
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
   
#    # ...............................................
#    def _setUnits(self, units):
#       if units is not None:
#          self._units = units 
#       elif len(self._layers) > 0:
#          self._units = self._layers[0].mapUnits
#       else:
#          self._units = None
#          
#    @property
#    def units(self):
#       if self._units is None and len(self._layers) > 0:
#          self._units = self._layers[0].mapUnits
#       return self._units   
      
   # ...............................................       
   def _setRes(self, res):
      if res is not None:
         self._resolution = res 
      elif len(self._layers) > 0:
         self._resolution = self._layers[0].resolution
      else:
         self._resolution = None
         
   @property
   def resolution(self):
      if self._resolution is None and len(self._layers) > 0:
         self._resolution = self._layers[0].resolution
      return self._resolution
   
# .............................................................................
   def _getMapsetUrl(self):
      """
      @note: Overrides MapLayerset._getMapsetUrl  
      """
      return self.metadataUrl

# .........................................................................
# Public Methods
# .........................................................................
   
   def addLayer(self, lyr):
      """
      @summary: Add a layer to the scenario.  
      @param lyr: Layer (layer.Raster) to add to this scenario.
      @raise LMError: on layer that is not EnvLayer 
      """
      if lyr is not None:
         if lyr.getId() is None or self.getLayer(lyr.metadataUrl) is None:
            if isinstance(lyr, EnvLayer):
               self._layers.append(lyr)
               self._bbox = MapLayerSet._getIntersectBounds(self)
               # Set mapPrefix only if does not exist. Could be in multiple 
               # mapfiles, but all should use same url/mapPrefix.
               if lyr.mapPrefix is None:
                  lyr._setMapPrefix(scencode=self.code)
               mapfname = self.createLocalMapFilename()
               lyr.setLocalMapFilename(mapfname=mapfname)
            else:
               raise LMError(['Attempt to add non-EnvLayer'])

# ...............................................
   def setLayers(self, lyrs):
      """
      @summary: Add layers to the scenario.  
      @param lyrs: List of Layers (layer.Raster) to add to this scenario.
      @raise LMError: on layer that is not an EnvLayer 
      """
      for lyr in lyrs:
         if not isinstance(lyr, EnvLayer):
            raise LMError('Incompatible Layer type {}'.format(type(lyr)))
      self._layers = lyrs
      self._bbox = MapLayerSet._getIntersectBounds(self)

# ...............................................
   def createMapPrefix(self, lyrname=None):
      """
      @summary Gets the OGC service URL prefix for this object
      @return URL string representing a webservice request for maps of this object
      """
      mapprefix = self._earlJr.constructMapPrefixNew(ftype=LMFileType.SCENARIO_MAP, 
                                          objCode=self.code, lyrname=lyrname, 
                                          usr=self._userId, epsg=self._epsg)
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
      mapfname = self._earlJr.createFilename(self._mapType,
                                             objCode=self.code, 
                                             usr=self._userId, 
                                             epsg=self._epsg)
      return mapfname
   

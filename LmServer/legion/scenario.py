"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import LMFileType, LMServiceType
from LmServer.legion.env_layer import EnvLayer


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
    def __init__(self, name, user_id,
                     metadata={},
                     metadataUrl=None,
                     epsgcode=None,
                     bbox=None,
                     mapunits=None,
                     mod_time=None,
                     scenarios=None,
                     scenPackageId=None):
        """
        @summary Constructor for the ScenPackage class 
        @copydoc LmServer.base.service_object.ServiceObject::__init__()
        @param name: The name for this set of scenarios
        @param scenarios: list of Scenario objects
        """
        ServiceObject.__init__(self, user_id, scenPackageId,
                                      LMServiceType.SCEN_PACKAGES,
                                      metadataUrl=metadataUrl,
                                      mod_time=mod_time)
        LMSpatialObject.__init__(self, epsgcode, bbox, mapunits)
        self.name = name
        self.loadScenpkgMetadata(metadata)

        self._scenarios = {}
        self.set_scenarios(scenarios)
        if self._bbox is None:
            self.reset_bbox()

# .............................................................................
    def reset_bbox(self):
        bboxList = [scen.bbox for scen in list(self._scenarios.values())]
        minBbox = self.intersectBoundingBoxes(bboxList)
        self._set_bbox(minBbox)

# .............................................................................
    def add_scenario(self, scen):
        """
        @summary: Add a scenario to an ScenPackage.  
        @note: metadataUrl or scenario code (unique for a user), is used 
                 to ensure that a scenario is not duplicated in the ScenPackage.  
        """
        if isinstance(scen, Scenario):
            if scen.getUserId() == self.getUserId():
                if self.get_scenario(code=scen.code,
                                          metadataUrl=scen.metadataUrl) is None:
                    self._scenarios[scen.code] = scen
            else:
                raise LMError(['Cannot add user {} Scenario to user {} ScenPackage'
                                    .format(scen.getUserId(), self.getUserId())])
        else:
            raise LMError(['Cannot add {} as a Scenario'.format(type(scen))])

# .............................................................................
    def get_scenario(self, code=None, metadataUrl=None):
        """
        @summary Gets a scenario from the ScenPackage with the specified metadataUrl
        @param metadataUrl: metadataUrl for which to find matching scenario
        @param user_id: user for which to find matching scenario with code
        @param code: code for which to find matching scenario with user_id
        @return the LmServer.legion.Scenario object with the given metadataUrl, 
                 or user_id/code combination. None if not found.
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
                for code, scen in self._scenarios.items():
                    if scen.metadataUrl == metadataUrl:
                        return scen
        return None

    # ...............................................
    # layers property code overrides the same methods in layerset.LayerSet
    @property
    def scenarios(self):
        return self._scenarios

    def set_scenarios(self, scens):
        """
        @summary: sets the scenarios in the ScenPackage
        @param scens: list of scenarios
        """
        self._scenarios = {}
        if scens:
            for scen in scens:
                self.add_scenario(scen)

# ...............................................
    def dump_scenpkg_metadata(self):
        return super(ScenPackage, self)._dump_metadata(self.scenpkg_metadata)

# ...............................................
    def load_scenpkg_metadata(self, new_metadata):
        self.scenpkg_metadata = super(ScenPackage, self)._load_metadata(new_metadata)

# ...............................................
    def add_scenpkg_metadata(self, new_metadata_dict):
        self.scenpkg_metadata = super(ScenPackage, self)._add_metadata(new_metadata_dict,
                                             existing_metadata_dict=self.scenpkg_metadata)


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
    def __init__(self, code, user_id, epsgcode, metadata={},
                     metadataUrl=None,
                     units=None, res=None,
                     gcm_code=None, alt_pred_code=None, date_code=None,
                     bbox=None, mod_time=None,
                     layers=None, scenarioid=None):
        """
        @summary Constructor for the scenario class 
        @copydoc LmServer.base.layerset.MapLayerSet::__init__()
        @param code: The code for this set of layers
        @param res: size of each side of a pixel (assumes square pixels)
        @param mod_time: (optional) Last modification time of the object (any of 
                                the points (saved in the PBJ) included in this 
                                OccurrenceSet) in modified julian date format; 
                                0 if points have not yet been collected and saved
        @param layers: list of Raster layers of environmental data
        @param scenarioid: The scenarioId in the database
        """
        self._layers = []
        # layers are set not set in LayerSet or Layerset - done here to check
        # that each layer is an EnvLayer
        MapLayerSet.__init__(self, code, url=metadataUrl, epsgcode=epsgcode, 
                             user_id=user_id, db_id=scenarioid, bbox=bbox, 
                             mapunits=units, mod_time=mod_time,
                             serviceType=LMServiceType.SCENARIOS,
                             mapType=LMFileType.SCENARIO_MAP)
        # aka MapLayerSet.name
        self.code = code

        self.gcm_code = gcm_code
        self.alt_pred_code = alt_pred_code
        self.date_code = date_code
        self.load_scen_metadata(metadata)

        # Private attributes
        self._scenarioId = scenarioid
        self._setLayers(layers)
        self._setRes(res)
        self.set_map_prefix()
        self.set_local_map_filename()

    # ...............................................
    def set_id(self, scenid):
        """
        @summary: Sets the database id on the object
        @param scenid: The database id for the object
        """
        MapLayerSet.set_id(self, scenid)

# ...............................................
    def dump_scen_metadata(self):
        return super(Scenario, self)._dump_metadata(self.scen_metadata)

# ...............................................
    def load_scen_metadata(self, new_metadata):
        self.scen_metadata = super(Scenario, self)._load_metadata(new_metadata)

# ...............................................
    def add_scen_metadata(self, new_metadata_dict):
        self.scen_metadata = super(Scenario, self)._add_metadata(new_metadata_dict,
                                             existing_metadata_dict=self.scen_metadata)

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
                self.add_layer(lyr)
            self._bbox = MapLayerSet.intersect_bboxes(self)

#     # ...............................................
#     def _setUnits(self, units):
#         if units is not None:
#             self._units = units
#         elif len(self._layers) > 0:
#             self._units = self._layers[0].mapUnits
#         else:
#             self._units = None
#
#     @property
#     def units(self):
#         if self._units is None and len(self._layers) > 0:
#             self._units = self._layers[0].mapUnits
#         return self._units

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
    def _get_mapset_url(self):
        """
        @note: Overrides MapLayerset._get_mapset_url  
        """
        return self.metadataUrl

# .........................................................................
# Public Methods
# .........................................................................

    def add_layer(self, lyr):
        """
        @summary: Add a layer to the scenario.  
        @param lyr: Layer (layer.Raster) to add to this scenario.
        @raise LMError: on layer that is not EnvLayer 
        """
        if lyr is not None:
            if lyr.get_id() is None or self.getLayer(lyr.metadataUrl) is None:
                if isinstance(lyr, EnvLayer):
                    self._layers.append(lyr)
                    self._bbox = MapLayerSet.intersect_bboxes(self)
                    # Set map_prefix only if does not exist. Could be in multiple
                    # mapfiles, but all should use same url/map_prefix.
                    if lyr.map_prefix is None:
                        lyr._set_map_prefix(scencode=self.code)
                    mapfname = self.create_local_map_filename()
                    lyr.set_local_map_filename(mapfname=mapfname)
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
        self._bbox = MapLayerSet.intersect_bboxes(self)

# ...............................................
    def create_map_prefix(self, lyrname=None):
        """
        @summary Gets the OGC service URL prefix for this object
        @return URL string representing a webservice request for maps of this object
        """
        mapprefix = self._earl_jr.constructMapPrefixNew(ftype=LMFileType.SCENARIO_MAP,
                                                        objCode=self.code, lyrname=lyrname,
                                                        usr=self._user_id, epsg=self._epsg)
        return mapprefix

    def _set_map_prefix(self, mapprefix=None):
        if mapprefix is None:
            mapprefix = self.create_map_prefix()
        self._map_prefix = mapprefix

    @property
    def map_prefix(self):
        return self._map_prefix

# ...............................................
    def create_local_map_filename(self):
        """
        @summary: Find mapfile containing this layer.  
        """
        mapfname = self._earl_jr.create_filename(self._mapType,
                                                            objCode=self.code,
                                                            usr=self._user_id,
                                                            epsg=self._epsg)
        return mapfname


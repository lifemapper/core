"""Module containing scenario and scenario package classes
"""
from LmBackend.common.lmobj import LMError
from LmServer.base.layerset import MapLayerSet
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import (
    LMFileType, LMServiceType, ID_PLACEHOLDER)

from LmServer.legion.env_layer import EnvLayer


# .........................................................................
class ScenPackage(ServiceObject, LMSpatialObject):
    """Class containing multiple, related, scenario objects for SDMs."""
    # ................................
    def __init__(self, name, user_id, metadata=None, metadata_url=None,
                 epsg_code=None, bbox=None, map_units=None, mod_time=None,
                 scenarios=None, scen_package_id=None):
        """Constructor."""
        ServiceObject.__init__(
            self, user_id, scen_package_id, LMServiceType.SCEN_PACKAGES,
            metadata_url=metadata_url, mod_time=mod_time)
        LMSpatialObject.__init__(self, epsg_code, bbox, map_units)
        self.name = name
        self.load_scenpkg_metadata(metadata)

        self._scenarios = {}
        self.set_scenarios(scenarios)
        if self._bbox is None:
            self.reset_bbox()

    # ................................
    def reset_bbox(self):
        """Reset the scenario package bounding box."""
        bbox_list = [scen.bbox for scen in list(self._scenarios.values())]
        min_bbox = self.intersect_bboxes(bbox_list)
        self._set_bbox(min_bbox)

    # ................................
    def add_scenario(self, scen):
        """Add a scenario to an ScenPackage.

        Note:
            - metadata_url or scenario code (unique for a user), is used
                to ensure that a scenario is not duplicated in the ScenPackage.
        """
        if isinstance(scen, Scenario):
            if scen.get_user_id() == self.get_user_id():
                if self.get_scenario(
                        code=scen.code, metadata_url=scen.metadata_url
                        ) is None:
                    self._scenarios[scen.code] = scen
            else:
                raise LMError(
                    'Cannot add user {} scenario to {} ScenPackage'.format(
                        scen.get_user_id(), self.get_user_id()))
        else:
            raise LMError('Cannot add {} as a Scenario'.format(type(scen)))

    # ................................
    def get_scenario(self, code=None, metadata_url=None):
        """Return a scenario from the ScenPackage with the specified metadata_url.

        Args:
            metadata_url: metadata_url for which to find matching scenario
            code: code for which to find matching scenario with user_id

        Returns:
            Scenario object with the given metadata_url, or user_id/code
                combination. None if not found.
        """
        for scen in self._scenarios.values():
            if code and scen.code.lower() == code.lower():
                return scen
            elif (metadata_url 
                  and not (scen.metadata_url.endswith(ID_PLACEHOLDER)) 
                  and metadata_url == scen.metadata_url):
                return scen
        return None

    # ................................
    @property
    def scenarios(self):
        """Return the package's scenarios."""
        return self._scenarios

    # ................................
    def set_scenarios(self, scens):
        """Set the scenarios of the package."""
        self._scenarios = {}
        if scens:
            for scen in scens:
                self.add_scenario(scen)

    # ................................
    def dump_scenpkg_metadata(self):
        """Dump the scenario package metadata to a string."""
        return super(ScenPackage, self)._dump_metadata(
            self.scen_package_metadata)

    # ................................
    def load_scenpkg_metadata(self, new_metadata):
        """Load scenario package metadata."""
        self.scen_package_metadata = super(ScenPackage, self)._load_metadata(
            new_metadata)

    # ................................
    def add_scenpkg_metadata(self, new_metadata_dict):
        """Add scenario package metadata."""
        self.scen_package_metadata = super(ScenPackage, self)._add_metadata(
            new_metadata_dict,
            existing_metadata_dict=self.scen_package_metadata)


# .........................................................................
class Scenario(MapLayerSet):
    """Class containing environmental data layers used to create SDMs
    """
    # ................................
    def __init__(self, code, user_id, epsg_code, metadata=None,
                 metadata_url=None, units=None, res=None, gcm_code=None,
                 alt_pred_code=None, date_code=None, bbox=None, mod_time=None,
                 layers=None, scenario_id=None):
        """Constructor

        Args:
            code: The code for this set of layers
            res: size of each side of a pixel (assumes square pixels)
            layers: list of Raster layers of environmental data
            scenario_id: The scenarioId in the database
        """
        self._layers = []
        # layers are set not set in LayerSet or Layerset - done here to check
        # that each layer is an EnvLayer
        MapLayerSet.__init__(
            self, code, metadata_url=metadata_url, epsg_code=epsg_code,
            user_id=user_id, db_id=scenario_id, bbox=bbox, map_units=units,
            mod_time=mod_time, service_type=LMServiceType.SCENARIOS,
            map_type=LMFileType.SCENARIO_MAP)
        # aka MapLayerSet.name
        self.code = code

        self.gcm_code = gcm_code
        self.alt_pred_code = alt_pred_code
        self.date_code = date_code
        self.load_scenario_metadata(metadata)

        # Private attributes
        self._scenario_id = scenario_id
        self._set_layers(layers)
        self._set_res(res)
        self.set_map_prefix()
        self.set_local_map_filename()

    # ................................
    def set_id(self, scenario_id):
        """Set the database id on the object.

        Args:
            scenario_id: The database identifier for the Scenario
        """
        MapLayerSet.set_id(self, scenario_id)

    # ................................
    def dump_scenario_metadata(self):
        """Dump scenario metadata to string."""
        return super(Scenario, self)._dump_metadata(self.scen_metadata)

    # ................................
    def load_scenario_metadata(self, new_metadata):
        """Load scenario metadata."""
        self.scen_metadata = super(Scenario, self)._load_metadata(new_metadata)

    # ................................
    def add_scenario_metadata(self, new_metadata_dict):
        """Add scenario metadata."""
        self.scen_metadata = super(Scenario, self)._add_metadata(
            new_metadata_dict, existing_metadata_dict=self.scen_metadata)

    # ................................
    @property
    def layers(self):
        """Return the layers in the scenario.

        Note:
            layers property code overrides the same methods in LayerSet
        """
        return self._layers

    # ................................
    def _set_layers(self, lyrs):
        """Set the layers of the scenario

        Todo:
            Overrides LayerSet._set_layers by requiring identical resolution
                and mapunits for all layers.
        """
        self._layers = []
        if lyrs:
            for lyr in lyrs:
                self.add_layer(lyr)
            self._bbox = self.intersect_bboxes

    # ................................
    def _set_res(self, res):
        """Set the resolution of the scenario
        """
        if res is not None:
            self._resolution = res
        elif len(self._layers) > 0:
            self._resolution = self._layers[0].resolution
        else:
            self._resolution = None

    # ................................
    @property
    def resolution(self):
        """Return the resolution of the scenario."""
        if self._resolution is None and len(self._layers) > 0:
            self._resolution = self._layers[0].resolution
        return self._resolution

    # ................................
    def _get_mapset_url(self):
        return self.metadata_url

    # ................................
    def add_layer(self, lyr):
        """Add a layer to the scenario.

        Args:
            lyr: Layer (layer.Raster) to add to this scenario.

        Raises:
            LMError: on layer that is not EnvLayer
        """
        if lyr is not None:
            if lyr.get_id() is None or self.get_layer(
                    lyr.metadata_url) is None:
                if isinstance(lyr, EnvLayer):
                    self._layers.append(lyr)
                    # Set map_prefix only if does not exist. Could be in
                    #    multiple mapfiles, but all should use
                    #    same url/map_prefix.
                    if lyr.map_prefix is None:
                        lyr._set_map_prefix(scen_code=self.code)
                    map_fname = self.create_local_map_filename()
                    lyr.set_local_map_filename(map_fname=map_fname)
                else:
                    raise LMError(['Attempt to add non-EnvLayer'])

    # ................................
    def set_layers(self, lyrs):
        """Set the layers for the scenario.

        Args:
            lyrs: List of Layers (layer.Raster) to add to this scenario.

        Raises:
            LMError: on layer that is not an EnvLayer
        """
        for lyr in lyrs:
            if not isinstance(lyr, EnvLayer):
                raise LMError('Incompatible Layer type {}'.format(type(lyr)))
        self._layers = lyrs
        self._bbox = self.intersect_bboxes

    # ................................
    def create_map_prefix(self, lyr_name=None):
        """Return the OGC service URL prefix for this object.

        Returns:
            str - URL representing a webservice request for maps of this object
        """
        return self._earl_jr.construct_map_prefix_new(
            f_type=LMFileType.SCENARIO_MAP, obj_code=self.code,
            lyr_name=lyr_name, usr=self._user_id, epsg=self._epsg)

    # ................................
    def _set_map_prefix(self, map_prefix=None):
        if map_prefix is None:
            map_prefix = self.create_map_prefix()
        self._map_prefix = map_prefix

    # ................................
    @property
    def map_prefix(self):
        return self._map_prefix

    # ................................
    def create_local_map_filename(self):
        """Find mapfile containing this layer."""
        return self._earl_jr.create_filename(
            self._map_type, obj_code=self.code, usr=self._user_id,
            epsg=self._epsg)

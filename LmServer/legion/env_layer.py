"""Module containing Environmental Layer class
"""
from LmBackend.common.lmobj import LMError
from LmServer.base.layer import Raster, _LayerParameters
from LmServer.common.lmconstants import LMServiceType, LMFileType


# .........................................................................
class EnvType(_LayerParameters):
    """Superclass for EnvLayer"""
    # ................................
    def __init__(self, env_code, user_id, gcm_code=None, alt_pred_code=None,
                 date_code=None, metadata=None, mod_time=None,
                 env_type_id=None):
        """Constructor

        Args:
            env_code: Code for the environmental type (i.e. temp, elevation,
                bio7)
            gcm_code: Code for the Global Climate Model used to create these
                data
            alt_pred_code: Code for the alternate prediction (i.e. IPCC
                scenario or Representative Concentration Pathways/RCPs) used to
                create these data
            date_code: Code for the time period for which these data are
                predicted.
        """
        _LayerParameters.__init__(
            self, user_id, param_id=env_type_id, metadata=metadata,
            mod_time=mod_time)
        self.env_code = env_code
        self.gcm_code = gcm_code
        self.alt_pred_code = alt_pred_code
        self.date_code = date_code


# .........................................................................
class EnvLayer(EnvType, Raster):
    """Class to hold a Raster object used for species distribution modeling.
    """
    # ................................
    def __init__(self, name, user_id, epsg_code, scen_code=None, lyr_id=None,
                 squid=None, ident=None, verify=None, dlocation=None,
                 layer_metadata=None, data_format=None, gdal_type=None,
                 val_units=None, val_attribute=None, nodata_val=None,
                 min_val=None, max_val=None, map_units=None, resolution=None,
                 bbox=None, env_layer_id=None, metadata_url=None,
                 parent_metadata_url=None, mod_time=None, env_code=None,
                 gcm_code=None, alt_pred_code=None, date_code=None,
                 env_metadata=None, env_mod_time=None, env_type_id=None):
        """Constructor
        """
        if name is None:
            raise LMError('EnvLayer.name is required')
        EnvType.__init__(
            self, env_code, user_id, gcm_code=gcm_code,
            alt_pred_code=alt_pred_code, date_code=date_code,
            metadata=env_metadata, mod_time=env_mod_time,
            env_type_id=env_type_id)
        self._map_prefix = None
        # Raster metadata_url and service_type override those of EnvType
        # if it is a full EnvLayer
        Raster.__init__(
            self, name, user_id, epsg_code, lyr_id=lyr_id, squid=squid,
            ident=ident, verify=verify, dlocation=dlocation,
            metadata=layer_metadata, data_format=data_format,
            gdal_type=gdal_type, val_units=val_units, nodata_val=nodata_val,
            min_val=min_val, max_val=max_val, map_units=map_units,
            resolution=resolution, bbox=bbox, svc_obj_id=env_layer_id,
            service_type=LMServiceType.ENVIRONMENTAL_LAYERS,
            metadata_url=metadata_url, parent_metadata_url=parent_metadata_url,
            mod_time=mod_time)
        self._scen_code = scen_code
        self._set_map_prefix(scen_code=scen_code)

    # ................................
    @classmethod
    def init_from_parts(cls, raster, env_type, env_layer_id=None,
                        scen_code=None):
        """Initialize an environmental layer from parts
        """
        return EnvLayer(
            raster.name, raster.get_user_id(), raster.epsg_code,
            scen_code=scen_code, lyr_id=raster.get_id(), squid=raster.squid,
            ident=raster.ident, verify=raster.verify,
            dlocation=raster.get_dlocation(),
            layer_metadata=raster.layer_metadata,
            data_format=raster.data_format, gdal_type=raster.gdal_type,
            val_units=raster.val_units, nodata_val=raster.nodata_val,
            min_val=raster.min_val, max_val=raster.max_val,
            map_units=raster.map_units, resolution=raster.resolution,
            bbox=raster.bbox, env_layer_id=env_layer_id,
            metadata_url=raster.metadata_url,
            parent_metadata_url=raster.parent_metadata_url,
            mod_time=raster.mod_time, env_code=env_type.env_code,
            gcm_code=env_type.gcm_code, alt_pred_code=env_type.alt_pred_code,
            date_code=env_type.date_code, env_metadata=env_type.param_metadata,
            env_mod_time=env_type.param_mod_time,
            env_type_id=env_type.get_param_id())

    # ................................
    def _create_map_prefix(self, scen_code=None):
        """Construct the endpoint of a Lifemapper WMS URL for this object.

        Args:
            scen_code: override scenario associated with this layer

        Note:
            - Uses the metatadataUrl for this object, plus 'ogc' format,
                map=<mapname>, and layers=<layername> key/value pairs.
            - If the object has not yet been inserted into the database, a
                 placeholder is used until replacement after database
                 insertion.
        """
        if scen_code is None:
            scen_code = self._scen_code
        return self._earl_jr.construct_map_prefix_new(
            f_type=LMFileType.SCENARIO_MAP, obj_code=scen_code,
            lyr_name=self.name, usr=self._user_id, epsg=self._epsg)

    # ................................
    @property
    def map_layer_name(self):
        """Get the environmental layer layer name
        """
        return self.name

    # ................................
    @property
    def map_prefix(self):
        """Get the map prefix for the environmental layer
        """
        return self._map_prefix

    # ................................
    def _set_map_prefix(self, map_prefix=None, scen_code=None):
        if map_prefix is None:
            map_prefix = self._create_map_prefix(scen_code=self._scen_code)
        self._map_prefix = map_prefix

    # ................................
    @property
    def scen_code(self):
        """Get the code of the scenario this environmental layer belongs to.
        """
        return self._scen_code

    # ................................
    def set_scen_code(self, scen_code=None):
        """Set the scenario code for the environmental layer
        """
        self._scen_code = scen_code

    # ................................
    def create_local_map_filename(self, scen_code=None):
        """Find mapfile containing this layer.

        Args:
            scen_code: override scenario associated with this layer
        """
        if scen_code is None:
            scen_code = self._scen_code
        return self._earl_jr.create_filename(
            LMFileType.SCENARIO_MAP, obj_code=scen_code, usr=self._user_id,
            epsg=self._epsg)

    # ................................
    def set_local_map_filename(self, map_fname=None, scen_code=None):
        """Set the local map filename
        """
        if scen_code is None:
            scen_code = self._scen_code
        if map_fname is None:
            map_fname = self.create_local_map_filename(scen_code=scen_code)
        self._map_filename = map_fname

    # ................................
    def set_layer_param(self, env_type):
        """ Set layer type parameters
        """
        # _LayerParameters
        self.env_code = env_type.env_code
        self.gcm_code = env_type.gcm_code
        self.alt_pred_code = env_type.alt_pred_code
        self.date_code = env_type.date_code
        self.load_param_metadata(env_type.param_metadata)
        self.env_mod_time = env_type.mod_time
        self.set_param_id(env_type.get_param_id())
        self.param_mod_time = env_type.param_mod_time

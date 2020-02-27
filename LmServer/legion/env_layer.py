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
from LmServer.base.layer import Raster, _LayerParameters
from LmServer.common.lmconstants import LMServiceType, LMFileType


# .........................................................................
class EnvType(_LayerParameters):

# .............................................................................
    def __init__(self, env_code, user_id,
                     gcm_code=None, altpred_code=None, date_code=None,
                     metadata={}, mod_time=None, envTypeId=None):
        """
        @summary Initialize the EnvType  class instance
        @copydoc LmServer.base.layer._LayerParameters::__init__()
        @param env_code: Code for the environmental type (i.e. temp, elevation, bio7)
        @param gcm_code: Code for the Global Climate Model used to create these data
        @param altpred_code: Code for the alternate prediction (i.e. IPCC scenario 
                 or Representative Concentration Pathways/RCPs) used to create 
                 these data
        @param date_code: Code for the time period for which these data are predicted.
        """
        _LayerParameters.__init__(self, user_id, param_id=envTypeId,
                                          metadata=metadata, mod_time=mod_time)
        self.env_code = env_code
        self.gcm_code = gcm_code
        self.altpred_code = altpred_code
        self.date_code = date_code


# .........................................................................
class EnvLayer(EnvType, Raster):
    """         
    Class to hold a Raster object used for species distribution modeling.
    """

# .............................................................................
    def __init__(self, name, user_id, epsgcode, scen_code=None, lyr_id=None,
                     squid=None, ident=None, verify=None, dlocation=None,
                     lyr_metadata={}, data_format=None, gdal_type=None,
                     valUnits=None, val_attribute=None,
                     nodata_val=None, min_val=None, max_val=None,
                     mapunits=None, resolution=None,
                     bbox=None, env_layer_id=None, metadata_url=None,
                     parent_metadata_url=None, mod_time=None,
                     # EnvType
                     env_code=None, gcm_code=None, altpred_code=None, date_code=None,
                     envMetadata={}, envModTime=None, envTypeId=None):
        """
        @copydoc LmServer.base.layer.Raster::__init__()
        @copydoc LmServer.base.legion.EnvType::__init__()
        """
        if name is None:
            raise LMError('EnvLayer.name is required')
        EnvType.__init__(self, env_code, user_id,
                     gcm_code=gcm_code, altpred_code=altpred_code, date_code=date_code,
                     metadata=envMetadata, mod_time=envModTime, envTypeId=envTypeId)
        self._map_prefix = None
        # Raster metadata_url and service_type override those of EnvType
        # if it is a full EnvLayer
        Raster.__init__(self, name, user_id, epsgcode, lyr_id=lyr_id,
                     squid=squid, ident=ident, verify=verify, dlocation=dlocation,
                     metadata=lyr_metadata, data_format=data_format, gdal_type=gdal_type,
                     valUnits=valUnits, nodata_val=nodata_val, min_val=min_val, max_val=max_val,
                     mapunits=mapunits, resolution=resolution,
                     bbox=bbox, svc_obj_id=env_layer_id,
                     service_type=LMServiceType.ENVIRONMENTAL_LAYERS,
                     metadata_url=metadata_url, parent_metadata_url=parent_metadata_url,
                     mod_time=mod_time)
        self._scen_code = scen_code
        self._set_map_prefix(scen_code=scen_code)

# ...............................................
    @classmethod
    def init_from_parts(cls, raster, envType, env_layer_id=None, scen_code=None):
        envLyr = EnvLayer(raster.name, raster.get_user_id(), raster.epsgcode,
                        scen_code=scen_code, lyr_id=raster.get_id(), squid=raster.squid,
                        ident=raster.ident, verify=raster.verify, dlocation=raster.get_dlocation(),
                        lyr_metadata=raster.lyr_metadata, data_format=raster.data_format,
                        gdal_type=raster.gdal_type, valUnits=raster.valUnits,
                        nodata_val=raster.nodata_val, min_val=raster.min_val,
                        max_val=raster.max_val, mapunits=raster.mapUnits,
                        resolution=raster.resolution, bbox=raster.bbox,
                        # Join table for EnvironmentalLayer ServiceObject unique id
                        env_layer_id=env_layer_id,
                        metadata_url=raster.metadata_url,
                        parent_metadata_url=raster.parent_metadata_url,
                        mod_time=raster.mod_time,
                        # EnvType
                        env_code=envType.env_code, gcm_code=envType.gcm_code,
                        altpred_code=envType.altpred_code, date_code=envType.date_code,
                        envMetadata=envType.param_metadata, envModTime=envType.paramModTime,
                        envTypeId=envType.getParamId())
        return envLyr

# ...............................................
# other methods
# ...............................................
    def _create_map_prefix(self, scen_code=None):
        """
        @summary: Construct the endpoint of a Lifemapper WMS URL for 
                     this object.
        @param scen_code: override scenario associated with this layer
        @note: Uses the metatadataUrl for this object, plus 'ogc' format, 
                 map=<mapname>, and layers=<layername> key/value pairs.  
        @note: If the object has not yet been inserted into the database, a 
                 placeholder is used until replacement after database insertion.
        """
        if scen_code is None:
            scen_code = self._scen_code
        mapprefix = self._earl_jr.construct_map_prefix_new(
            ftype=LMFileType.SCENARIO_MAP, objCode=scen_code, lyrname=self.name,
            usr=self._user_id, epsg=self._epsg)
        return mapprefix

# ...............................................
    @property
    def map_layername(self):
        return self.name

# ...............................................
    @property
    def map_prefix(self):
        return self._map_prefix

    def _set_map_prefix(self, mapprefix=None, scen_code=None):
        if mapprefix is None:
            mapprefix = self._create_map_prefix(scen_code=self._scen_code)
        self._map_prefix = mapprefix

# ...............................................
    @property
    def scen_code(self):
        return self._scen_code

    def set_scen_code(self, scen_code=None):
        self._scen_code = scen_code

# ...............................................
    def create_local_map_filename(self, scen_code=None):
        """
        @summary: Find mapfile containing this layer.
        @param scen_code: override scenario associated with this layer
        """
        if scen_code is None:
            scen_code = self._scen_code
        mapfname = self._earl_jr.create_filename(LMFileType.SCENARIO_MAP,
                                                            objCode=scen_code,
                                                            usr=self._user_id, epsg=self._epsg)
        return mapfname

# ...............................................
    def set_local_map_filename(self, mapfname=None, scen_code=None):
        """
        @note: Overrides existing _map_filename
        @summary: Find mapfile containing layers for this model's occ_layer.
        @param mapfname: Previously constructed mapfilename
        @param scen_code: override scenario associated with this layer
        """
        if scen_code is None:
            scen_code = self._scen_code
        if mapfname is None:
            mapfname = self.create_local_map_filename(scen_code=scen_code)
        self._map_filename = mapfname

# ...............................................
    def set_layer_param(self, envType):
        """ 
        @param envType: an LmServer.legion.EnvironmentalType object
        """
        # _LayerParameters
        self.env_code = envType.env_code
        self.gcm_code = envType.gcm_code,
        self.altpred_code = envType.altpred_code
        self.date_code = envType.date_code
        self.load_param_metadata(envType.param_metadata)
        self.envModTime = envType.mod_time
        self.setParamId(envType.getParamId())
        self.paramModTime = envType.paramModTime

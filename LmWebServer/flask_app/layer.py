"""This module provides REST services for Layers"""
import werkzeug.exceptions as WEXC

from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class LayerService(LmService):
    """Class for layers web service."""


    # ................................
    @lm_formatter
    def count_env_layers(
            self, user_id, after_time=None, before_time=None, alt_pred_code=None, date_code=None, 
            env_code=None, env_type_id=None, epsg_code=None, gcm_code=None, scenario_code=None):
        """Count environmental layer objects matching the specified criteria

        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            after_time (float): Time in MJD of the earliest modtime for filtering
            before_time (float): Time in MJD of the latest modtime for filtering
            alt_pred_code (str): Code of the GCM scenario for filtering predicted environmental layera
            date_code (str): Code of the date for filtering predicted environmental layers (for past, present, future)
            epsg_code (str): EPSG code for the SRS for filtering layers
            env_code (str): Environmental type code for filtering environmental layers
            env_type_id (int): Database key of the environmental type for filtering environmental layers
            gcm_code (str) = GCM code for filtering environmental layers
            scenario_code (str): Database key for filtering to environmental layers belonging to one scenario
        """
        layer_count = self.scribe.count_env_layers(
            user_id=user_id, after_time=after_time, before_time=before_time, env_code=env_code, gcm_code=gcm_code, 
            alt_pred_code=alt_pred_code, date_code=date_code, epsg=epsg_code, env_type_id=env_type_id, 
            scenario_code=scenario_code)

        return {'count': layer_count}

    # ................................
    @lm_formatter
    def count_layers(self, user_id, after_time=None, before_time=None, epsg_code=None, squid=None):
        """Return a count of layers matching the specified criteria

        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            after_time (float): Time in MJD of the earliest modtime for filtering
            before_time (float): Time in MJD of the latest modtime for filtering
            alt_pred_code (str): Code of the GCM scenario for filtering predicted environmental layera
            date_code (str): Code of the date for filtering predicted environmental layers (for past, present, future)
            epsg_code (str): EPSG code for the SRS for filtering layers
            squid (str): Unique taxon identifier for filtering layers
        """
        layer_count = self.scribe.count_layers(
            user_id=user_id, squid=squid, after_time=after_time, before_time=before_time, epsg=epsg_code)

        return {'count': layer_count}

    # ................................
    @lm_formatter
    def get_layer(self, user_id, layer_id, env_layer=False):
        """Return a layer
        
        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            layer_id (int): A database identifier for a requested layer. 
            env_layer (bool): flag - True restricts the search to environmental layers; False returns non-specific layers
        """
        if env_layer:
            lyr = self.scribe.get_env_layer(lyr_id=layer_id)
        else:
            lyr = self.scribe.get_layer(lyr_id=layer_id)
            
        if lyr is None:
            if env_layer:
                return WEXC.NotFound('Environmental layer {} was not found'.format(layer_id))
            else:
                return WEXC.NotFound('Layer {} was not found'.format(layer_id))
            
        if check_user_permission(user_id, lyr, HTTPMethod.GET):
            return lyr
        else:
            return WEXC.Forbidden('User {} does not have permission to access layer {}'.format(
                user_id, layer_id))

    # ................................
    @lm_formatter
    def list_env_layers(
            self, user_id, after_time=None, before_time=None, alt_pred_code=None, date_code=None, 
            env_code=None, env_type_id=None, epsg_code=None, gcm_code=None, scenario_code=None, 
            limit=100, offset=0):
        """Return a list of environmental layers matching the specified criteria

        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            after_time (float): Time in MJD of the earliest modtime for filtering
            before_time (float): Time in MJD of the latest modtime for filtering
            alt_pred_code (str): Code of the GCM scenario for filtering predicted environmental layera
            date_code (str): Code of the date for filtering predicted environmental layers (for past, present, future)
            epsg_code (str): EPSG code for the SRS for filtering layers
            env_code (str): Environmental type code for filtering environmental layers
            env_type_id (int): Database key of the environmental type for filtering environmental layers
            gcm_code (str) = GCM code for filtering environmental layers
            layer_type (int): Code for filtering on environmental or other layer type.  
                0/None = all; 1 = environmental layer; 2 = Not yet implemented
            scenario_code (str): Code for filtering to environmental layers belonging to one scenario
            limit (int): Number of records to return
            offset  (int): Offset for starting record of records to return 
        """
        lyr_atoms = self.scribe.list_env_layers(
            offset, limit, user_id=user_id, after_time=after_time, before_time=before_time,  
            env_code=env_code, gcm_code=gcm_code, alt_pred_code=alt_pred_code,
            date_code=date_code, epsg=epsg_code, env_type_id=env_type_id, scen_code=scenario_code)

        return lyr_atoms

    # ................................
    def list_layers(
            self, user_id, after_time=None, before_time=None, epsg_code=None, squid=None, 
            limit=100, offset=0):
        """Return a list of layers matching the specified criteria

        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            after_time (float): Time in MJD of the earliest modtime for filtering
            before_time (float): Time in MJD of the latest modtime for filtering
            epsg_code (str): EPSG code for the SRS for filtering layers
            squid (str): Unique identifier or filtering to a taxon associated with the layer
            limit (int): Number of records to return
            offset  (int): Offset for starting record of records to return 
        """
        layer_atoms = self.scribe.list_layers(
            offset, limit, user_id=user_id, after_time=after_time, before_time=before_time, epsg=epsg_code, squid=squid)

        return layer_atoms

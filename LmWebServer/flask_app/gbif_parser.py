"""This module provides a wrapper around GBIF's names service

TODO: Delete?  This and biotaphy_names appear identical
"""
from werkzeug.exceptions import BadRequest

from LmCommon.common.api_query import GbifAPI
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter

# TODO: These need to go into a constants file
ACCEPTED_NAME_KEY = 'accepted_name'
SEARCH_NAME_KEY = 'search_name'
SPECIES_KEY_KEY = 'speciesKey'
SPECIES_NAME_KEY = 'species'
TAXON_ID_KEY = 'taxon_id'


# .............................................................................
class GBIFNamesService(LmService):
    """Service to get GBIF accepted names"""

    # ................................
    @lm_formatter
    def get_gbif_names(self, names_obj):
        """Queries GBIF for accepted names matching the provided list of names
        
        Args:
            names_obj: a JSON list of name strings to match
        """
        if not isinstance(names_obj, list):
            return BadRequest('Name data must be a JSON list')
    
        retval = []
        for name in names_obj:
            try:
                gbif_resp = GbifAPI.get_accepted_names(name)[0]
                retval.append({
                    SEARCH_NAME_KEY: name,
                    ACCEPTED_NAME_KEY: gbif_resp[SPECIES_NAME_KEY],
                    TAXON_ID_KEY: gbif_resp[SPECIES_KEY_KEY]
                })
            except Exception as e:
                self.log.error('Could not get accepted name from GBIF for name {}: {}'.format(name, e))
                retval.append({
                    SEARCH_NAME_KEY: name,
                    ACCEPTED_NAME_KEY: None,
                    TAXON_ID_KEY: None
                })
        return retval

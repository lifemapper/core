"""This module provides a wrapper around GBIF's names service for use in the Biotaphy web application"""
import werkzeug.exceptions as WEXC

from LmCommon.common.api_query import GbifAPI
from LmWebServer.flask_app.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class GBIFTaxonService(LmService):
    """Class to get and filter results from GBIF name-matching service."""

    # ................................
    @lm_formatter
    def get_gbif_results(self, names_obj):
        """Queries GBIF for accepted names matching the provided list of names
        
        Args:
            names_obj: a JSON list of name strings to match
        """
        if not isinstance(names_obj, list):
            return WEXC.BadRequest('Name data must be a JSON list')
    
        retval = []
        for name in names_obj:
            try:
                gbif_resp = GbifAPI.get_accepted_names(name)[0]
            except Exception as e:
                self.log.error('Could not get accepted name from GBIF for name {}: {}'.format(name, e))
                retval.append({
                    GbifAPI.SEARCH_NAME_KEY: name,
                    GbifAPI.ACCEPTED_NAME_KEY: None,
                    GbifAPI.TAXON_ID_KEY: None
                })
            else:
                retval.append({
                    GbifAPI.SEARCH_NAME_KEY: name,
                    GbifAPI.ACCEPTED_NAME_KEY: gbif_resp[
                        GbifAPI.SPECIES_NAME_KEY],
                    GbifAPI.TAXON_ID_KEY: gbif_resp[
                        GbifAPI.SPECIES_KEY_KEY]
                })
        return retval

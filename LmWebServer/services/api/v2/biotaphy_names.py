"""This module provides a wrapper around GBIF's names service
"""
import json

import cherrypy

from LmCommon.common.api_query import GbifAPI
from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
class GBIFTaxonService(LmService):
    """
    """

    # ................................
    @lm_formatter
    def POST(self):
        """Queries GBIF for accepted names matching the provided list of names
        """
        names_obj = json.load(cherrypy.request.body)

        if not isinstance(names_obj, list):
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Names must be provided as a JSON list')

        ret = []
        for name in names_obj:
            try:
                gbif_resp = GbifAPI.get_accepted_names(name)[0]
                ret.append({
                    GbifAPI.SEARCH_NAME_KEY: name,
                    GbifAPI.ACCEPTED_NAME_KEY: gbif_resp[
                        GbifAPI.SPECIES_NAME_KEY],
                    GbifAPI.TAXON_ID_KEY: gbif_resp[
                        GbifAPI.SPECIES_KEY_KEY]
                })
            except Exception as err:
                self.log.error(
                    'Could not get accepted name from GBIF for name {}: {}'
                    .format(name, str(err)), err)
                ret.append({
                    GbifAPI.SEARCH_NAME_KEY: name,
                    GbifAPI.ACCEPTED_NAME_KEY: None,
                    GbifAPI.TAXON_ID_KEY: None
                })
        return ret

#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides a wrapper around GBIF's names service
"""
import cherrypy
import json

from LmCommon.common.apiquery import GbifAPI
from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# TODO: These need to go into a constants file
ACCEPTED_NAME_KEY = 'accepted_name'
SEARCH_NAME_KEY = 'search_name'
SPECIES_KEY_KEY = 'speciesKey'
SPECIES_NAME_KEY = 'species'
TAXON_ID_KEY = 'taxon_id'
# .............................................................................
@cherrypy.expose
class GBIFNamesService(LmService):
    """
    """
    # ................................
    @lmFormatter
    def POST(self):
        """Queries GBIF for accepted names matching the provided list of names
        """
        names_obj = json.load(cherrypy.request.body)
        
        if not isinstance(names_obj, list):
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Names must be provided as a JSON list')
        else:
            ret = []
            for name in names_obj:
                try:
                    gbif_resp = GbifAPI.getAcceptedNames(name)[0]
                    ret.append({
                        SEARCH_NAME_KEY : name,
                        ACCEPTED_NAME_KEY : gbif_resp[SPECIES_NAME_KEY],
                        TAXON_ID_KEY : gbif_resp[SPECIES_KEY_KEY]
                    })
                except Exception as e:
                    self.log.error(
                        'Could not get accepted name from GBIF for name {}: {}'
                        .format(name, str(e)))
                    ret.append({
                        SEARCH_NAME_KEY : name,
                        ACCEPTED_NAME_KEY : None,
                        TAXON_ID_KEY : None
                    })
            return ret

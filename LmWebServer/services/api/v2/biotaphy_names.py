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

# .............................................................................
@cherrypy.expose
class GBIFTaxonService(LmService):
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
                        GbifAPI.SEARCH_NAME_KEY : name,
                        GbifAPI.ACCEPTED_NAME_KEY : gbif_resp[GbifAPI.SPECIES_NAME_KEY],
                        GbifAPI.TAXON_ID_KEY : gbif_resp[GbifAPI.SPECIES_KEY_KEY]
                    })
                except Exception as e:
                    self.log.error(
                        'Could not get accepted name from GBIF for name {}: {}'
                        .format(name, str(e)))
                    ret.append({
                        GbifAPI.SEARCH_NAME_KEY : name,
                        GbifAPI.ACCEPTED_NAME_KEY : None,
                        GbifAPI.TAXON_ID_KEY : None
                    })
            return ret

"""
curl 'http://notyeti-191.lifemapper.org/api/v2/biotaphynames' \
     -H 'Accept: application/json' \
     -H 'Content-Type: application/json' \
     --data-binary '["Acacia bifaria", "Acacia daviesioides"]'

import json

from LmCommon.common.apiquery import GbifAPI
from LmCommon.common.lmconstants import HTTPStatus
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

body = '["Acacia bifaria"]'
userId = DEFAULT_POST_USER


"""
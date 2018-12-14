#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides services for fuzzy search for occurrence sets
"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus
from LmServer.common.solr import queryArchiveIndex

from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
class SpeciesHintService(LmService):
    """This class is responsible for the species hint services
    """
    # ................................
    @lmFormatter
    def GET(self, searchString, limit=20, urlUser=None, **params):
        """Search the index for occurrence sets matching the search string
        """
        if len(searchString) < 3:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Need to provide at least 3 characters for search string')
        else:
            # Split on a space if exists
            parts = searchString.replace('%20', '_').split(' ')
            if len(parts) > 1:
                genus = parts[0]
                sp = '{}*'.format(parts[1])
            else:
                genus = '{}*'.format(parts[0])
                sp = None
        
            matches = queryArchiveIndex(
                taxGenus=genus.title(), taxSpecies=sp,
                userId=self.getUserId(urlUser=urlUser))
            
            occIds = []
            ret = []
            
            for match in matches:
                occId = match['occurrenceId']
                pointCount = match['pointCount']
                displayName = match['displayName']
                binomial = '{} {}'.format(
                    match['taxonGenus'], match['taxonSpecies'])
                if not occId in occIds:
                    occIds.append(occId)
                    ret.append({
                        'binomial' : binomial,
                        'name' : displayName,
                        'numPoints' : pointCount,
                        'occurrenceSet' : occId
                    })
            return ret[:limit]

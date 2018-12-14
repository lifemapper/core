#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides services for querying snippets
"""
import cherrypy

from LmServer.common.solr import querySnippetIndex

from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
class SnippetService(LmService):
    """This class is responsible for the Lifemapper snippet services.
    """
    # ................................
    @lmFormatter
    def GET(self, ident1=None, provider=None, collection=None,
            catalogNumber=None, operation=None, afterTime=None,
            beforeTime=None, ident2=None, url=None, who=None, agent=None,
            why=None, **params):
        """
        @summary: A snippet get request will query the Lifemapper snippet index
                         and return matching entries.
        """
        return self._makeSolrQuery(
            ident1=ident1, provider=provider, collection=collection,
            catalogNumber=catalogNumber, operation=operation,
            afterTime=afterTime, beforeTime=beforeTime, ident2=ident2, url=url,
            who=who, agent=agent, why=why)
    
    # ................................
    def _makeSolrQuery(self, ident1=None, provider=None, collection=None,
                       catalogNumber=None, operation=None, afterTime=None,
                       beforeTime=None, ident2=None, url=None, who=None,
                       agent=None, why=None):
        
        return querySnippetIndex(
            ident1=ident1, provider=provider, collection=collection,
            catalogNumber=catalogNumber, operation=operation,
            afterTime=afterTime, beforeTime=beforeTime, ident2=ident2, url=url,
            who=who, agent=agent, why=why)

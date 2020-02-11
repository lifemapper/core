#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides services for querying snippets
"""
from LmServer.common.solr import querySnippetIndex
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter
import cherrypy


# .............................................................................
@cherrypy.expose
class SnippetService(LmService):
    """This class is responsible for the Lifemapper snippet services.
    """
    # ................................
    @lm_formatter
    def GET(self, ident1=None, provider=None, collection=None,
            catalogNumber=None, operation=None, afterTime=None,
            beforeTime=None, ident2=None, url=None, who=None, agent=None,
            why=None, **params):
        """Query the Lifemapper snippet index and return matches.

        Todo:
            Do I need to send user information?
        """
        return querySnippetIndex(
            ident1=ident1, provider=provider, collection=collection,
            catalogNumber=catalogNumber, operation=operation,
            afterTime=afterTime, beforeTime=beforeTime, ident2=ident2, url=url,
            who=who, agent=agent, why=why)

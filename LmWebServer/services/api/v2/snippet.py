"""This module provides services for querying snippets"""
import cherrypy

from LmServer.common.solr import query_snippet_index
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
class SnippetService(LmService):
    """This class is responsible for the Lifemapper snippet services.
    """

    # ................................
    @lm_formatter
    def GET(self, ident1=None, provider=None, collection=None,
            catalog_number=None, operation=None, after_time=None,
            before_time=None, ident2=None, url=None, who=None, agent=None,
            why=None, **params):
        """Query the Lifemapper snippet index and return matches.

        Todo:
            Do I need to send user information?
        """
        return query_snippet_index(
            ident1=ident1, provider=provider, collection=collection,
            catalog_number=catalog_number, operation=operation,
            after_time=after_time, before_time=before_time, ident2=ident2,
            url=url, who=who, agent=agent, why=why)

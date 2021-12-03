"""This module provides services for querying snippets"""
from LmServer.common.solr import query_snippet_index
from LmWebServer.flask_app.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class SnippetService(LmService):
    """This class is responsible for the Lifemapper snippet services.
    """

    # ................................
    @lm_formatter
    def get_snippet(
        self, ident1=None, provider=None, collection=None, catalog_number=None, operation=None, 
        after_time=None, before_time=None, ident2=None, url=None, who=None, agent=None,
        why=None, **params):
        """Query the Lifemapper snippet index and return matches.

        Args:
            ident1 (int): An identifier for the primary object (probably occurrenceset)
            ident2: A identifier for the secondary object (occurrenceset or projection)
            provider (str): The occurrence point provider
            collection (str): The collection the point belongs to
            catalog_number (str): The catalog number of the occurrence point
            operation (str): A LmServer.common.lmconstants.SnippetOperations
            after_time (float): Return hits after this time (MJD format)
            before_time (float): Return hits before this time (MJD format)
            url: A url for the resulting object
            who: Who initiated the action
            agent: The agent that initiated the action
            why: Why the action was initiated

        Todo: Do I need to send user information?
        Todo: Are provider, collection, catalog_number args for primary object/ident1?
        """
        return query_snippet_index(
            ident1=ident1, provider=provider, collection=collection,
            catalog_number=catalog_number, operation=operation,
            after_time=after_time, before_time=before_time, ident2=ident2,
            url=url, who=who, agent=agent, why=why)

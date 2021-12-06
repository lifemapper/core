"""This module provides a raw interface to solr"""
from LmServer.common.solr import raw_query
from LmWebServer.services.api.v2.base import LmService


# .............................................................................
class RawSolrService(LmService):
    """This class provides a web interface to Solr"""

    # ............................
    def query_collection(self, req_body, **params):
        """Send these raw parameters to solr"""
        collection = req_body['collection']
        query_string = req_body['query_string']
        return raw_query(collection, query_string)

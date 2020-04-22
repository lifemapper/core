"""This module provides a raw interface to solr"""
import json

import cherrypy

from LmServer.common.solr import raw_query
from LmWebServer.services.api.v2.base import LmService


# from LmServer.common.lmconstants import SOLR_TAXONOMY_COLLECTION
# .............................................................................
@cherrypy.expose
class RawSolrService(LmService):
    """This class provides a web interface to Solr
    """

    # ............................
    def POST(self, **params):
        """Send these raw parameters to solr
        """
        # collection = SOLR_TAXONOMY_COLLECTION
        body = json.load(cherrypy.request.body)

        collection = body['collection']
        query_string = body['query_string']
        return raw_query(collection, query_string)

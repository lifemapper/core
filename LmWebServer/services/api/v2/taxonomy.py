#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module contains service code for performing searches on taxonomy.
"""
import cherrypy

from LmServer.common.solr import query_taxonomy_index

from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
class TaxonomyHintService(LmService):
    """This class provides a method for querying available taxonomy.
    """
    # ................................
    @lmFormatter
    def GET(self, taxonKingdom=None, taxonPhylum=None, taxonClass=None, 
            taxonOrder=None, taxonFamily=None, taxonGenus=None, 
            taxonKey=None, scientificName=None, canonicalName=None, squid=None,
            limit=100, urlUser=None, **params):
        docs = query_taxonomy_index(
            taxon_kingdom=taxonKingdom, taxon_phylum=taxonPhylum,
            taxon_class=taxonClass, taxon_order=taxonOrder,
            taxon_family=taxonFamily, taxon_genus=taxonGenus,
            taxon_key=taxonKey, scientific_name=scientificName, 
            canonical_name=canonicalName, squid=squid, user_id=urlUser)
        return docs[:limit]

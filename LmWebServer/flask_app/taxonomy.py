"""This module contains service code for performing searches on taxonomy."""
from LmServer.common.solr import query_taxonomy_index
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class TaxonomyHintService(LmService):
    """This class provides a method for querying available taxonomy."""

    # ................................
    @lm_formatter
    def get_taxonomy(
        self, user_id, kingdom=None, phylum=None, class_=None, order_=None, family=None, genus=None, 
        taxon_key=None, scientific_name=None, canonical_name=None, squid=None, 
        limit=100, **params):
        """Perform a solr search for taxonomy matches."""
        docs = query_taxonomy_index(
            taxon_kingdom=kingdom, taxon_phylum=phylum, taxon_class=class_, taxon_order=order_,
            taxon_family=family, taxon_genus=genus, taxon_key=taxon_key, scientific_name=scientific_name,
            canonical_name=canonical_name, squid=squid, user_id=user_id)
        return docs[:limit]

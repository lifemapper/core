"""This module provides services for fuzzy search for occurrence sets"""
import werkzeug.exceptions as WEXC

from LmServer.common.lmconstants import SOLR_FIELDS
from LmServer.common.solr import query_archive_index
from LmWebServer.flask_app.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class SpeciesHintService(LmService):
    """This class is responsible for the species hint services"""

    # ................................
    @lm_formatter
    def get_hint(self, user_id, search_string, limit=20, **params):
        """Search the index for occurrence sets matching the search string"""
        if len(search_string) < 3:
            raise WEXC.BadRequest('Need to provide at least 3 characters for search string')

        # Split on a space if exists
        parts = search_string.replace('%20', '_').split(' ')
        if len(parts) > 1:
            genus = parts[0]
            species_search = '{}*'.format(parts[1])
        else:
            genus = '{}*'.format(parts[0])
            species_search = None

        matches = query_archive_index(
            tax_genus=genus.title(), tax_species=species_search, user_id=user_id)

        occ_ids = []
        ret = []

        for match in matches:
            occ_id = match[SOLR_FIELDS.OCCURRENCE_ID]
            point_count = match[SOLR_FIELDS.POINT_COUNT]
            display_name = match[SOLR_FIELDS.DISPLAY_NAME]
            binomial = '{} {}'.format(
                match[SOLR_FIELDS.TAXON_GENUS],
                match[SOLR_FIELDS.TAXON_SPECIES])
            if occ_id not in occ_ids:
                occ_ids.append(occ_id)
                ret.append({
                    'binomial': binomial,
                    'name': display_name,
                    'numPoints': point_count,
                    'occurrenceSet': occ_id
                })
        return ret[:limit]

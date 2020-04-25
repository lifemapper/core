"""This module provides services for query and subsetting of global PAMs
"""
import cherrypy

from LmServer.base.atom import Atom
from LmServer.common.lmconstants import SOLR_FIELDS
from LmServer.common.solr import facet_archive_on_gridset, query_archive_index
from LmServer.common.subset import subset_global_pam
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
class GridsetFacetService(LmService):
    """This service retrieves gridsets within the solr index for the user
    """

    # ................................
    @lm_formatter
    def GET(self, url_user=None, **params):
        """Queries the Global PAM for matching results
        """
        facets = facet_archive_on_gridset(user_id=url_user)
        # NOTE: Response is list of id, count but not separated
        i = 0
        counts = []
        while i < len(facets):
            counts.append({
                SOLR_FIELDS.GRIDSET_ID: str(facets[i]),
                'count': int(facets[i + 1])
            })
            i += 2

        return {
            SOLR_FIELDS.GRIDSET_ID: counts
        }


# .............................................................................
@cherrypy.expose
class GlobalPAMService(LmService):
    """This class is responsible for the Global PAM services.

    Note:
        * The dispatcher is responsible for calling the correct method
    """
    gridset = GridsetFacetService()

    # ................................
    @lm_formatter
    def GET(self, algorithm_code=None, bbox=None, display_name=None,
            gridset_id=None, model_scenario_code=None, point_max=None,
            point_min=None, url_user=None, prj_scen_code=None, squid=None,
            taxon_kingdom=None, taxon_phylum=None, taxon_class=None,
            taxon_order=None, taxon_family=None, taxon_genus=None,
            taxon_species=None, **params):
        """Queries the Global PAM and return entries matching the parameters
        """
        return self._make_solr_query(
            algorithm_code=algorithm_code, bbox=bbox,
            display_name=display_name, gridset_id=gridset_id,
            model_scenario_code=model_scenario_code, point_max=point_max,
            point_min=point_min, url_user=url_user,
            projection_scenario_code=prj_scen_code, squid=squid,
            tax_kingdom=taxon_kingdom, tax_phylum=taxon_phylum,
            tax_class=taxon_class, tax_order=taxon_order,
            tax_family=taxon_family, tax_genus=taxon_genus,
            tax_species=taxon_species)

    # ................................
    @lm_formatter
    def POST(self, archive_name, gridset_id, algorithm_code=None, bbox=None,
             cell_size=None, model_scenario_code=None, point_max=None,
             point_min=None, url_user=None, prj_scen_code=None, squid=None,
             taxon_kingdom=None, taxon_phylum=None, taxon_class=None,
             taxon_order=None, taxon_family=None, taxon_genus=None,
             taxon_species=None, display_name=None, **params):
        """A Global PAM post request will create a subset
        """
        matches = self._make_solr_query(
            algorithm_code=algorithm_code, bbox=bbox,
            display_name=display_name, gridset_id=gridset_id,
            model_scenario_code=model_scenario_code, point_max=point_max,
            point_min=point_min, url_user=url_user,
            projection_scenario_code=prj_scen_code, squid=squid,
            tax_kingdom=taxon_kingdom, tax_phylum=taxon_phylum,
            tax_class=taxon_class, tax_order=taxon_order,
            tax_family=taxon_family, tax_genus=taxon_genus,
            tax_species=taxon_species)
        # Make bbox tuple
        if bbox:
            bbox = tuple([float(i) for i in bbox.split(',')])

        gridset = self._subset_global_pam(
            archive_name, matches, bbox=bbox, cell_size=cell_size)
        cherrypy.response.status = 202
        return Atom(
            gridset.get_id(), gridset.name, gridset.metadata_url,
            gridset.mod_time, epsg=gridset.epsg_code)

    # ................................
    def _make_solr_query(self, algorithm_code=None, bbox=None,
                         display_name=None, gridset_id=None,
                         model_scenario_code=None, point_max=None,
                         point_min=None, url_user=None,
                         projection_scenario_code=None, squid=None,
                         tax_kingdom=None, tax_phylum=None, tax_class=None,
                         tax_order=None, tax_family=None, tax_genus=None,
                         tax_species=None):
        return query_archive_index(
            algorithm_code=algorithm_code, bbox=bbox,
            display_name=display_name, gridset_id=gridset_id,
            model_scenario_code=model_scenario_code, point_max=point_max,
            point_min=point_min, tax_class=tax_class,
            projection_scenario_code=projection_scenario_code, squid=squid,
            tax_kingdom=tax_kingdom, tax_phylum=tax_phylum,
            tax_order=tax_order, tax_family=tax_family, tax_genus=tax_genus,
            tax_species=tax_species,
            user_id=self.get_user_id(url_user=url_user))

    # ................................
    def _subset_global_pam(self, archive_name, matches, bbox=None,
                           cell_size=None):
        """Creates a subset of a global PAM and create a new grid set

        Args:
            archive_name (str) : The name of this new grid set
            matches (list) : Solr hits to be used for subsetting
        """
        return subset_global_pam(
            archive_name, matches, self.get_user_id(), bbox=bbox,
            cell_size=cell_size, scribe=self.scribe)

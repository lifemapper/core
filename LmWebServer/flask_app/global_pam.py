"""This module provides services for query and subsetting of global PAMs"""
from flask import make_response
from http import HTTPStatus

from LmServer.base.atom import Atom
from LmServer.common.lmconstants import SOLR_FIELDS
from LmServer.common.solr import facet_archive_on_gridset, query_archive_index
from LmServer.common.subset import subset_global_pam
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class _GridsetFacetService(LmService):
    """This service retrieves gridsets within the solr index for the user"""

    # ................................
    @lm_formatter
    def list_gridsets(self, user_id=None, **params):
        """Queries the Global PAM for matching results"""
        facets = facet_archive_on_gridset(user_id=user_id)
        # NOTE: Response is list of id, count but not separated
        i = 0
        counts = []
        while i < len(facets):
            counts.append(
                {SOLR_FIELDS.GRIDSET_ID: str(facets[i]),
                'count': int(facets[i + 1])})
            i += 2

        return {SOLR_FIELDS.GRIDSET_ID: counts}


# .............................................................................
class GlobalPAMService(LmService):
    """This class is responsible for the Global PAM services."""
    gridset = _GridsetFacetService()

    # ................................
    @lm_formatter
    def retrieve_pam_subset(
        self, user_id, algorithm_code=None, bbox=None, display_name=None, gridset_id=None, 
        model_scenario_code=None, prj_scen_code=None, point_max=None, point_min=None, squid=None,
        taxon_kingdom=None, taxon_phylum=None, taxon_class=None, taxon_order=None, taxon_family=None, 
        taxon_genus=None, taxon_species=None, **params):
        """Queries the Global PAM and returns a subset of intersected layers (PAVs) from Solr, matching the parameters
        
        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            algorithm_code (str): Code for filtering SDM layers modeled with this algorithm to populate a PAM
            bbox (str): Bounding box in format 'minx, miny, maxx, maxy' for subsetting layers to populate a PAM
            display_name (str): Taxonomic name for filtering layers to populate a PAM
            gridset_id (int): Database key for gridset to subset for a PAM
            point_max (int): Maximum number of points for filtering layers to populate a PAM
            point_min (int): Minimum number of points for filtering layers to populate a PAM
            model_scenario_code (str): Code for filtering SDM layers modeled with this scenario to populate a PAM
            prj_scen_code (str): Code for filtering SDM layers projected with this scenario to populate a PAM
            squid (str): Lifemapper unique identifier for filtering layers to populate a PAM 
            taxon_kingdom (str): Kingdom for filtering layers to populate a PAM
            taxon_phylum (str): Phylum for filtering layers to populate a PAM
            taxon_class (str): Class for filtering layers to populate a PAM
            taxon_order (str): Order for filtering layers to populate a PAM 
            taxon_family (str): Family for filtering layers to populate a PAM
            taxon_genus (str): Genus for filtering layers to populate a PAM
            taxon_species (str): Species for filtering layers to populate a PAM
        """
        solr_matches = self._make_solr_query(
            algorithm_code=algorithm_code, bbox=bbox, display_name=display_name, gridset_id=gridset_id,
            model_scenario_code=model_scenario_code, point_max=point_max, point_min=point_min, 
            user_id=user_id, projection_scenario_code=prj_scen_code, squid=squid,
            tax_kingdom=taxon_kingdom, tax_phylum=taxon_phylum, tax_class=taxon_class, tax_order=taxon_order,
            tax_family=taxon_family, tax_genus=taxon_genus, tax_species=taxon_species)
        return solr_matches

    # ................................
    @lm_formatter
    def post_pam_subset(
        self, user_id, archive_name, cell_size=None, algorithm_code=None, bbox=None, display_name=None, 
        gridset_id=None, model_scenario_code=None, prj_scen_code=None, point_max=None, point_min=None, 
        squid=None, taxon_kingdom=None, taxon_phylum=None, taxon_class=None, taxon_order=None, 
        taxon_family=None, taxon_genus=None, taxon_species=None, **params):
        """Queries the Global PAM, and creates a gridset initializing a new PAM from the subset of layers matching the parameters, 

        Args:
            user_id (str): The user authorized for this operation.  Note that this may not be 
                the same user as is logged into the system
            archive_name (str): Name to be associated with the new gridset
            gridset_id (int): Database key for gridset to subset for a PAM
            algorithm_code (str): Code for filtering SDM layers modeled with this algorithm to populate a PAM
            bbox (str): Bounding box in format 'minx, miny, maxx, maxy' for subsetting layers to populate a PAM
            cell_size (float): Size of cells (in map units) to be used for intersections when creating the new PAM
            display_name (str): Taxonomic name for filtering layers to populate a PAM
            point_max (int): Maximum number of points for filtering layers to populate a PAM
            point_min (int): Minimum number of points for filtering layers to populate a PAM
            model_scenario_code (str): Code for filtering SDM layers modeled with this scenario to populate a PAM
            prj_scen_code (str): Code for filtering SDM layers projected with this scenario to populate a PAM
            squid (str): Lifemapper unique identifier for filtering layers to populate a PAM 
            taxon_kingdom (str): Kingdom for filtering layers to populate a PAM
            taxon_phylum (str): Phylum for filtering layers to populate a PAM
            taxon_class (str): Class for filtering layers to populate a PAM
            taxon_order (str): Order for filtering layers to populate a PAM 
            taxon_family (str): Family for filtering layers to populate a PAM
            taxon_genus (str): Genus for filtering layers to populate a PAM
            taxon_species (str): Species for filtering layers to populate a PAM
        """
        solr_matches = self._make_solr_query(
            user_id, algorithm_code=algorithm_code, bbox=bbox, display_name=display_name, gridset_id=gridset_id,
            model_scenario_code=model_scenario_code, projection_scenario_code=prj_scen_code, 
            point_max=point_max, point_min=point_min,  squid=squid, tax_kingdom=taxon_kingdom, tax_phylum=taxon_phylum,
            tax_class=taxon_class, tax_order=taxon_order, tax_family=taxon_family, tax_genus=taxon_genus,
            tax_species=taxon_species)
        # Make bbox tuple from string
        if bbox is not None:
            bbox = tuple([float(i) for i in bbox.split(',')])

        gridset = subset_global_pam(
            archive_name, solr_matches, user_id, bbox=bbox, cell_size=cell_size, scribe=self.scribe)
        gatom = Atom(
            gridset.get_id(), gridset.name, gridset.metadata_url, gridset.mod_time, epsg=gridset.epsg_code)
        return make_response(gatom, HTTPStatus.ACCEPTED)
        

    # ................................
    def _make_solr_query(
        self, user_id, algorithm_code=None, bbox=None, display_name=None, gridset_id=None,
        model_scenario_code=None, projection_scenario_code=None, point_max=None, point_min=None, 
        squid=None, tax_kingdom=None, tax_phylum=None, tax_class=None, tax_order=None, tax_family=None, 
        tax_genus=None, tax_species=None):
        return query_archive_index(
            algorithm_code=algorithm_code, bbox=bbox, display_name=display_name, gridset_id=gridset_id,
            model_scenario_code=model_scenario_code, projection_scenario_code=projection_scenario_code, 
            point_max=point_max, point_min=point_min, squid=squid, tax_kingdom=tax_kingdom, 
            tax_phylum=tax_phylum, tax_class=tax_class, tax_order=tax_order, tax_family=tax_family, 
            tax_genus=tax_genus, tax_species=tax_species, user_id=user_id)

    # # ................................
    # def _subset_global_pam(self, user_id, archive_name, matches, bbox=None, cell_size=None):
    #     """Creates a subset of a global PAM and create a new grid set
    #
    #     Args:
    #         user_id (str): The user authorized for this operation.
    #         archive_name (str) : The name of this new grid set
    #         matches (list) : Solr hits to be used for subsetting
    #         bbox (str): Bounding box in format 'minx, miny, maxx, maxy' for subsetting layers to populate a PAM
    #         cell_size (float): Size of cells (in map units) to be used for intersections when creating the new PAM
    #     """
    #     return subset_global_pam(
    #         archive_name, matches, user_id, bbox=bbox, cell_size=cell_size, scribe=self.scribe)

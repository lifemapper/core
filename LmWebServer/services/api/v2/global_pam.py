#! /usr/bin/env python
# -*- coding: utf-8 -*-
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
    def GET(self, urlUser=None, **params):
        """Queries the Global PAM for matching results
        """
        facets = facet_archive_on_gridset(user_id=urlUser)
        # NOTE: Response is list of id, count but not separated
        i = 0
        counts = []
        while i < len(facets):
            counts.append({
                SOLR_FIELDS.GRIDSET_ID: str(facets[i]),
                'count': int(facets[i+1])
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
    def GET(self, algorithmCode=None, bbox=None, displayName=None,
            gridSetId=None, modelScenarioCode=None, pointMax=None,
            pointMin=None, urlUser=None, prjScenCode=None, squid=None,
            taxonKingdom=None, taxonPhylum=None, taxonClass=None,
            taxonOrder=None, taxonFamily=None, taxonGenus=None,
            taxonSpecies=None, **params):
        """Queries the Global PAM and return entries matching the parameters
        """
        return self._make_solr_query(
            algorithm_code=algorithmCode, bbox=bbox, display_name=displayName,
            gridset_id=gridSetId, model_scenario_code=modelScenarioCode,
            point_max=pointMax, point_min=pointMin, url_user=urlUser,
            projection_scenario_code=prjScenCode, squid=squid,
            tax_kingdom=taxonKingdom, tax_phylum=taxonPhylum,
            tax_class=taxonClass, tax_order=taxonOrder, tax_family=taxonFamily,
            tax_genus=taxonGenus, tax_species=taxonSpecies)

    # ................................
    @lm_formatter
    def POST(self, archiveName, gridSetId, algorithmCode=None, bbox=None,
             cellSize=None, modelScenarioCode=None, pointMax=None,
             pointMin=None, urlUser=None, prjScenCode=None, squid=None,
             taxonKingdom=None, taxonPhylum=None, taxonClass=None,
             taxonOrder=None, taxonFamily=None, taxonGenus=None,
             taxonSpecies=None, displayName=None, **params):
        """A Global PAM post request will create a subset
        """
        matches = self._make_solr_query(
            algorithm_code=algorithmCode, bbox=bbox, display_name=displayName,
            gridset_id=gridSetId, model_scenario_code=modelScenarioCode,
            point_max=pointMax, point_min=pointMin, url_user=urlUser,
            projection_scenario_code=prjScenCode, squid=squid,
            tax_kingdom=taxonKingdom, tax_phylum=taxonPhylum,
            tax_class=taxonClass, tax_order=taxonOrder, tax_family=taxonFamily,
            tax_genus=taxonGenus, tax_species=taxonSpecies)
        # Make bbox tuple
        if bbox:
            bbox = tuple([float(i) for i in bbox.split(',')])

        gridset = self._subset_global_pam(
            archiveName, matches, bbox=bbox, cell_size=cellSize)
        cherrypy.response.status = 202
        return Atom(
            gridset.getId(), gridset.name, gridset.metadataUrl,
            gridset.modTime, epsg=gridset.epsgcode)

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
            user_id=self.get_user_id(urlUser=url_user))

    # ................................
    def _subset_global_pam(self, archive_name, matches, bbox=None,
                           cell_size=None):
        """Creates a subset of a global PAM and create a new grid set

        Args:
            archiveName (str) : The name of this new grid set
            matches (list) : Solr hits to be used for subsetting
        """
        return subset_global_pam(
            archive_name, matches, self.get_user_id(), bbox=bbox,
            cell_size=cell_size, scribe=self.scribe)

#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides services for query and subsetting of global PAMs
"""
import cherrypy

from LmServer.base.atom import Atom
from LmServer.common.lmconstants import SOLR_FIELDS
from LmServer.common.solr import facetArchiveOnGridset, queryArchiveIndex
from LmServer.common.subset import subsetGlobalPAM

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
        facets = facetArchiveOnGridset(userId=urlUser)
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
            algorithmCode=algorithmCode, bbox=bbox, displayName=displayName,
            gridSetId=gridSetId, modelScenarioCode=modelScenarioCode,
            pointMax=pointMax, pointMin=pointMin, urlUser=urlUser,
            projectionScenarioCode=prjScenCode, squid=squid,
            taxKingdom=taxonKingdom, taxPhylum=taxonPhylum,
            taxClass=taxonClass, taxOrder=taxonOrder, taxFamily=taxonFamily,
            taxGenus=taxonGenus, taxSpecies=taxonSpecies)

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
            algorithmCode=algorithmCode, bbox=bbox, displayName=displayName,
            gridSetId=gridSetId, modelScenarioCode=modelScenarioCode,
            pointMax=pointMax, pointMin=pointMin, urlUser=urlUser,
            projectionScenarioCode=prjScenCode, squid=squid,
            taxKingdom=taxonKingdom, taxPhylum=taxonPhylum,
            taxClass=taxonClass, taxOrder=taxonOrder, taxFamily=taxonFamily,
            taxGenus=taxonGenus, taxSpecies=taxonSpecies)
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
    def _make_solr_query(self, algorithmCode=None, bbox=None, displayName=None,
                         gridSetId=None, modelScenarioCode=None, pointMax=None,
                         pointMin=None, urlUser=None,
                         projectionScenarioCode=None, squid=None,
                         taxKingdom=None, taxPhylum=None, taxClass=None,
                         taxOrder=None, taxFamily=None, taxGenus=None,
                         taxSpecies=None):
        return queryArchiveIndex(
            algorithmCode=algorithmCode, bbox=bbox, displayName=displayName,
            gridSetId=gridSetId, modelScenarioCode=modelScenarioCode,
            pointMax=pointMax, pointMin=pointMin,
            projectionScenarioCode=projectionScenarioCode, squid=squid,
            taxKingdom=taxKingdom, taxPhylum=taxPhylum, taxClass=taxClass,
            taxOrder=taxOrder, taxFamily=taxFamily, taxGenus=taxGenus,
            taxSpecies=taxSpecies, userId=self.get_user_id(urlUser=urlUser))

    # ................................
    def _subset_global_pam(self, archive_name, matches, bbox=None,
                           cell_size=None):
        """Creates a subset of a global PAM and create a new grid set

        Args:
            archiveName (str) : The name of this new grid set
            matches (list) : Solr hits to be used for subsetting
        """
        return subsetGlobalPAM(
            archive_name, matches, self.get_user_id(), bbox=bbox,
            cellSize=cell_size, scribe=self.scribe)

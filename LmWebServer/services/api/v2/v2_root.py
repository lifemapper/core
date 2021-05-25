"""This module provides REST services for service objects"""
import cherrypy

from LmWebServer.services.api.v2.biotaphy_names import GBIFTaxonService
from LmWebServer.services.api.v2.biotaphy_points import \
    IDigBioOccurrenceService
from LmWebServer.services.api.v2.env_layer import EnvLayerService
from LmWebServer.services.api.v2.gbif_parser import GBIFNamesService
from LmWebServer.services.api.v2.global_pam import GlobalPAMService
from LmWebServer.services.api.v2.gridset import GridsetService
from LmWebServer.services.api.v2.layer import LayerService
from LmWebServer.services.api.v2.occurrence import OccurrenceLayerService
from LmWebServer.services.api.v2.ogc import MapService
from LmWebServer.services.api.v2.open_tree import OpenTreeService
from LmWebServer.services.api.v2.scenario import ScenarioService
from LmWebServer.services.api.v2.scenario_package import ScenarioPackageService
from LmWebServer.services.api.v2.sdm_project import SdmProjectService
from LmWebServer.services.api.v2.shapegrid import ShapegridService
from LmWebServer.services.api.v2.snippet import SnippetService
from LmWebServer.services.api.v2.solr_raw import RawSolrService
from LmWebServer.services.api.v2.species_hint import SpeciesHintService
from LmWebServer.services.api.v2.taxonomy import TaxonomyHintService
from LmWebServer.services.api.v2.tree import TreeService
from LmWebServer.services.api.v2.upload import UserUploadService


# .............................................................................
@cherrypy.expose
class ApiRootV2:
    """Top level class containing Lifemapper services V2
    """
    biotaphynames = GBIFTaxonService()
    biotaphypoints = IDigBioOccurrenceService()
    biotaphytree = OpenTreeService()
    envlayer = EnvLayerService()
    gbifparser = GBIFNamesService()
    globalpam = GlobalPAMService()
    gridset = GridsetService()
    hint = SpeciesHintService()
    layer = LayerService()
    occurrence = OccurrenceLayerService()
    opentree = OpenTreeService()
    scenario = ScenarioService()
    scenpackage = ScenarioPackageService()
    sdmproject = SdmProjectService()
    shapegrid = ShapegridService()
    snippet = SnippetService()
    rawsolr = RawSolrService()
    taxonomy = TaxonomyHintService()
    tree = TreeService()
    upload = UserUploadService()

    ogc = MapService()

    # ................................
    def __init__(self):
        pass

    # ................................
    def index(self):
        """Service index method.
        """
        return "Index of v2 root"

#!/usr/bin/python
# -*- coding: utf-8 -*-
"""This module provides REST services for service objects
"""

import cherrypy

from LmWebServer.services.api.v2.envLayer import EnvLayerService
from LmWebServer.services.api.v2.gbifparser import GBIFNamesService
from LmWebServer.services.api.v2.globalPam import GlobalPAMService
from LmWebServer.services.api.v2.gridset import GridSetService
from LmWebServer.services.api.v2.layer import LayerService
from LmWebServer.services.api.v2.occurrence import OccurrenceLayerService
from LmWebServer.services.api.v2.ogc import MapService
from LmWebServer.services.api.v2.open_tree import OpenTreeService
from LmWebServer.services.api.v2.scenario import ScenarioService
from LmWebServer.services.api.v2.scenPackage import ScenarioPackageService
from LmWebServer.services.api.v2.sdmProject import SdmProjectService
from LmWebServer.services.api.v2.shapegrid import ShapeGridService
from LmWebServer.services.api.v2.snippet import SnippetService
from LmWebServer.services.api.v2.speciesHint import SpeciesHintService
from LmWebServer.services.api.v2.taxonomy import TaxonomyHintService
from LmWebServer.services.api.v2.tree import TreeService
from LmWebServer.services.api.v2.upload import UserUploadService

# .............................................................................
@cherrypy.expose
class ApiRootV2(object):
    """Top level class containing Lifemapper services V2
    """
    envlayer = EnvLayerService()
    gbifparser = GBIFNamesService()
    globalpam = GlobalPAMService()
    gridset = GridSetService()
    hint = SpeciesHintService()
    layer = LayerService()
    occurrence = OccurrenceLayerService()
    opentree = OpenTreeService()
    scenario = ScenarioService()
    scenpackage = ScenarioPackageService()
    sdmproject = SdmProjectService()
    shapegrid = ShapeGridService()
    snippet = SnippetService()
    taxonomy = TaxonomyHintService()
    tree = TreeService()
    upload = UserUploadService()

    ogc = MapService()

    # ................................
    def __init__(self):
        pass
   
    # ................................
    def index(self):
        return "Index of v2 root"

# .............................................................................
#if __name__ == '__main__':
#conf = {
#      '/v2/' : {
#         'request.dispatch' : cherrypy.dispatch.MethodDispatcher(),
#      }
#}
#cherrypy.quickstart(ApiRootV2(), '/v2/', conf)

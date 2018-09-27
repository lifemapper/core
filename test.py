import mx.DateTime
import os
from osgeo.ogr import wkbPolygon
import time
from types import IntType, FloatType

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, LMFormat, 
   ProcessType, JobStatus, MatrixType, SERVER_PIPELINE_HEADING, 
   SERVER_BOOM_HEADING, SERVER_SDM_MASK_HEADING_PREFIX, DEFAULT_MAPUNITS, DEFAULT_EPSG)
from LmCommon.common.readyfile import readyFilename
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource,
                                           TNCMetadata)
from LmDbServer.common.localconstants import (GBIF_TAXONOMY_FILENAME, 
                                              GBIF_PROVIDER_FILENAME)
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, LMFileType, ENV_DATA_PATH, 
         GPAM_KEYWORD, GGRIM_KEYWORD, ARCHIVE_KEYWORD, PUBLIC_ARCHIVE_NAME, 
         DEFAULT_EMAIL_POSTFIX, Priority, ProcessTool)
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector
from LmServer.base.lmobj import LMSpatialObject
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isLMUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmBackend.common.cmd import MfRule
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.shapegrid import ShapeGrid
from LmDbServer.tools.catalogWriteBoomMakeflows import BOOMFiller
from LmServer.legion.sdmproj import SDMProjection

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd

# cfname='/state/partition1/lmscratch/temp/sax_biotaphy.ini'
# cfname='/state/partition1/lmscratch/temp/heuchera_boom_params.ini'
# cfname = '/state/partition1/lmscratch/temp/taiwan_boom_params.ini'
# filler = BOOMFiller(configFname=cfname)
# filler.initializeInputs()

log = ScriptLogger('fixMetadata')
scribe = BorgScribe(log)

occ = scribe.getOccurrenceSet(occId=244)
mscen = scribe.getScenario('global-10min', userId='nchc')
pscen = scribe.getScenario('taiwan-30sec', userId='nchc')
alg = Algorithm('ATT_MAXENT')

prj = SDMProjection(occ, alg, mscen, pscen, 
                        dataFormat=LMFormat.GTIFF.driver,
                        status=JobStatus.GENERAL, statusModTime=CURR_MJD)


'''


update layer l 
set metadata =  '{"keywords": ["bioclimatic variables", "climate", "elevation", "land cover", "soil", "topography", "observed", "present", "' 
                   || p.displayname || '"],  "isDiscrete": false, "description": "Modeled habitat for '
                   || p.displayname || ' projected onto '
                   || p.prjscenariocode  
                   || ' datalayers", "title": "Taxon '
                   || p.displayname || '  modeled with '
                   || p.algorithmcode || ' and '
                   || p.mdlscenariocode  || ' projected onto ' || p.prjscenariocode || '"}'
from lm_sdmproject p 
where l.layerid = p.layerid and p.prjscenariocode in ('global-10min', 'taiwan-30sec');

update layer l 
set metadata =  '{"keywords": ["bioclimatic variables", "climate", "elevation", "observed", "present", "' 
                   || p.displayname || '"],  "isDiscrete": false, "description": "Modeled habitat for '
                   || p.displayname || ' projected onto '
                   || p.prjscenariocode  
                   || ' datalayers", "title": "Taxon '
                   || p.displayname || '  modeled with '
                   || p.algorithmcode || ' and '
                   || p.mdlscenariocode  || ' projected onto ' || p.prjscenariocode || '"}'
from lm_sdmproject p 
where l.layerid = p.layerid and prjscenariocode = 'observed-10min';


update layer l 
set metadata =  '{"keywords": ["bioclimatic variables", "climate", "elevation", "predicted", "past", "' 
                   || p.displayname || '"],  "isDiscrete": false, "description": "Modeled habitat for '
                   || p.displayname || ' projected onto '
                   || p.prjscenariocode  
                   || ' datalayers", "title": "Taxon '
                   || p.displayname || '  modeled with '
                   || p.algorithmcode || ' and '
                   || p.mdlscenariocode  || ' projected onto ' || p.prjscenariocode || '"}'
from lm_sdmproject p 
where l.layerid = p.layerid and prjscenariocode like 'CMIP%';

update layer l 
set metadata =  '{"keywords": ["bioclimatic variables", "climate", "elevation", "predicted", "future", "' 
                   || p.displayname || '"],  "isDiscrete": false, "description": "Modeled habitat for '
                   || p.displayname || ' projected onto '
                   || p.prjscenariocode  
                   || ' datalayers", "title": "Taxon '
                   || p.displayname || '  modeled with '
                   || p.algorithmcode || ' and '
                   || p.mdlscenariocode  || ' projected onto ' || p.prjscenariocode || '"}'
from lm_sdmproject p 
where l.layerid = p.layerid and prjscenariocode like 'AR5%';
'''
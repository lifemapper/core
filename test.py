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
from LmServer.base.utilities import isCorrectUser
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
from LmDbServer.boom.initboom import BOOMFiller

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd

cfname='/state/partition1/lmscratch/temp/sax_biotaphy.ini'
cfname='/state/partition1/lmscratch/temp/heuchera_boom_params.ini'
cfname = '/state/partition1/lmscratch/temp/taiwan_boom_params.ini'
filler = BOOMFiller(configFname=cfname)
filler.initializeInputs()

              
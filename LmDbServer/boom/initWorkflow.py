"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research
 
          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
    
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
   
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
   
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import ConfigParser
import json
import mx.DateTime
import os
import stat
import types

from LmBackend.command.boom import BoomerCommand
from LmBackend.command.common import (IdigbioQueryCommand ,
    ConcatenateMatricesCommand, SystemCommand, ChainCommand)
from LmBackend.command.server import (
    CatalogTaxonomyCommand, EncodeBioGeoHypothesesCommand, StockpileCommand)
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.apiquery import IdigbioAPI
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, LMFormat, MatrixType, 
      ProcessType, DEFAULT_POST_USER, GBIF, BoomKeys,
      SERVER_BOOM_HEADING, SERVER_SDM_MASK_HEADING_PREFIX, 
      SERVER_SDM_ALGORITHM_HEADING_PREFIX,
      SERVER_DEFAULT_HEADING_POSTFIX)
from LmCommon.common.readyfile import readyFilename

from LmDbServer.common.lmconstants import (SpeciesDatasource, TAXONOMIC_SOURCE)
from LmDbServer.common.localconstants import (GBIF_PROVIDER_FILENAME, 
                                              GBIF_TAXONOMY_FILENAME)
from LmDbServer.tools.catalogScenPkg import SPFiller
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (ARCHIVE_KEYWORD, GGRIM_KEYWORD,
                           GPAM_KEYWORD, LMFileType, Priority, ENV_DATA_PATH,
                           DEFAULT_EMAIL_POSTFIX,
                           SPECIES_DATA_PATH, DEFAULT_NUM_PERMUTATIONS)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isLMUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree
from LmBackend.command.single import GrimRasterCommand

# .............................................................................
class BOOMFiller(LMObject):
    """
    @summary 
    Class to: 
     1) populate a Lifemapper database with inputs for a BOOM archive
        including: user, scenario package, shapegrid, Tree,
                   Biogeographic Hypotheses, gridset
     2) If named scenario package does not exist for the user, add it.
     3) create default matrices for each scenario, 
        PAMs for SDM projections and GRIMs for Scenario layers
     4) Write a configuration file for computations (BOOM daemon) on the inputs
     5) Write a Makeflow to begin the BOOM daemon
    """
# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, paramFname, logname=None):
        """
        @summary Constructor for BOOMFiller class.
        """
        super(BOOMFiller, self).__init__()

        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        self.name = scriptname
        if logname is None:
            import time
            secs = time.time()
            timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}'.format(scriptname, timestamp)
        self.logname = logname
                
        self.inParamFname = paramFname
        # Get database
        try:
            self.scribe = self._getDb(self.logname)
        except: 
            raise
        self.open()

    # ...............................................
    @property
    def log(self):
        return self.scribe.log
    
    # ...............................................
    def initializeInputs(self):
        """
        @summary Initialize configured and stored inputs for BOOMFiller class.
        """      
        (self.userId, self.userIdPath,
         self.userEmail,
         self.user_taxonomy_basefilename,
         self.archiveName,
         self.priority,
         self.scenPackageName,
         mdl_scencode,
         prj_scencodes,
         self.dataSource,
         self.occIdFname,
         self.taxon_name_filename, 
         self.taxon_id_filename, 
         self.occFname,
         self.occSep,   
         self.minpoints,
         self.expdate,
         self.algorithms,
         self.do_assemble_pams,
         self.gridbbox,
         self.cellsides,
         self.cellsize,
         self.gridname, 
         self.intersectParams, 
         self.maskAlg, 
         self.treeFname, 
         self.bghypFnames,
         self.compute_pam_stats, 
         self.compute_mcpa, 
         self.num_permutations, 
         self.other_lyr_names) = self.readParamVals()
        self.woof_time_mjd = mx.DateTime.gmt().mjd
        earl = EarlJr()
        self.outConfigFilename = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                                     objCode=self.archiveName, 
                                                     usr=self.userId)
       
        # # TODO: Decide: do we want to start from beginning of species CSV 
        #         for every woof???
        startFile = earl.createStartWalkenFilename(self.userId, self.archiveName)
        if os.path.exists(startFile):
            os.remove(startFile)

        # Add/find user for this Boom process (should exist)
        self.addUser()
         
        # Find existing scenarios or create from user or public ScenPackage metadata
        self.scenPkg = self.findOrAddScenarioPackage()
        (self.mdl_scencode, self.prj_scencodes, 
         mask_lyrname_scen) = self.findMdlProjScenarios(mdl_scencode, prj_scencodes)
        # TODO: Allow any existing raster with intersecting region
        # TODO: Allow packaging of ancillary layers in data package, 
        #       specify role in params file
        if self.maskAlg:
            if self.maskAlg.code == 'hull_region_intersect':
                mask_lyrname = self.maskAlg.getParameterValue('region')
                # TODO: Delete this from Scenario packages and code
                # Take SDM_MASK_META from old (v2.0) scenario metadata
                if mask_lyrname is None:
                    self.maskAlg.setParameter('region', mask_lyrname_scen)
      
        # Fill grid bbox with scenario package (intersection of all bboxes) if it is absent
        if self.gridbbox is None:
            self.gridbbox = self.scenPkg.bbox
        
        # Created by addArchive
        self.shapegrid = None
        
    # ...............................................
    def findOrAddScenarioPackage(self):
        """
        @summary Find Scenarios from codes 
        @note: Boom parameters must include SCENARIO_PACKAGE, 
                            and optionally, SCENARIO_PACKAGE_MODEL_SCENARIO,
                                            SCENARIO_PACKAGE_PROJECTION_SCENARIOS
               If SCENARIO_PACKAGE_PROJECTION_SCENARIOS is not present, SDMs 
               will be projected onto all scenarios
        @note: This code can only parse scenario metadata marked as version 2.x
        """
        # Make sure Scenario Package exists for this user
        scenPkg = self.scribe.getScenPackage(userId=self.userId, 
                                        scenPkgName=self.scenPackageName, 
                                        fillLayers=True)
        if scenPkg is None:
            # See if metadata exists in user or public environmental directory
            spMetaFname = None
            for pth in (self.userIdPath, ENV_DATA_PATH):
                thisFname = os.path.join(pth, self.scenPackageName + '.py')
                if os.path.exists(thisFname):
                    spMetaFname = thisFname
                    break
        #          spMetaFname = os.path.join(ENV_DATA_PATH, self.scenPackageName + '.py')
            if spMetaFname is None:
                raise LMError("""ScenPackage {} must be authorized for User {} 
                               or all users (with public metadata file {})"""
                               .format(self.scenPackageName, self.userId, spMetaFname))
            else:
                spFiller = SPFiller(spMetaFname, self.userId, scribe=self.scribe)
                scenPkg = spFiller.catalogScenPackages()
           
        return scenPkg
                
    # ...............................................
    def findMdlProjScenarios(self, mdl_scencode, prj_scencodes):
        """
        @summary Find which Scenario for modeling, which (list) for projecting  
        @note: Boom parameters must include SCENARIO_PACKAGE, 
                                may include SCENARIO_PACKAGE_MODEL_SCENARIO,
                                            SCENARIO_PACKAGE_PROJECTION_SCENARIOS
        """
        valid_scencodes = self.scenPkg.scenarios.keys()
        if len(valid_scencodes) == 0 or None in valid_scencodes:
            raise LMError('ScenPackage {} metadata is incorrect, scenario codes = {}'
                         .format(self.scenPackageName, valid_scencodes))
            
        # TODO: Allow alternate masklayer for any Scenario, requires test and/or transform
        base_scencode, mask_lyrname = self._find_scenpkg_base_and_mask(
                                                          self.scenPackageName)      
        if not base_scencode in valid_scencodes:
            raise LMError('ScenPackage {} metadata is incorrect, {} not in scenarios'
                         .format(self.scenPackageName, base_scencode))
            
        # If model Scenarios are not listed, use scenPackage default baseline
        if mdl_scencode is None:
            mdl_scencode = base_scencode
        # If model scenarios does not match scenPackage, error params file
        elif mdl_scencode not in valid_scencodes:
            raise LMError('Params file {} metadata is incorrect, {} not in scenarios {} for package {}'
                         .format(self.inParamFname, mdl_scencode, valid_scencodes, self.scenPackageName))            
        # If projection Scenarios are not listed, use all scenarios in scenPackage
        if not prj_scencodes:
            prj_scencodes = valid_scencodes      
        # If any prj scenario does not match scenPackage, error params file
        else:
            for pcode in prj_scencodes:
                if pcode not in valid_scencodes:
                    raise LMError('Params file {} metadata is incorrect, {} not in scenarios {} for package {}'
                                 .format(self.inParamFname, pcode, valid_scencodes, self.scenPackageName))
        
        return mdl_scencode, prj_scencodes, mask_lyrname
                
    # ...............................................
    def open(self):
        success = self.scribe.openConnections()
        if not success: 
            raise LMError('Failed to open database')

    # ...............................................
    def close(self):
        self.scribe.closeConnections()

    # ...............................................
    @property
    def logFilename(self):
        try:
            fname = self.scribe.log.baseFilename
        except:
            fname = None
        return fname

    # ...............................................
    def _fixPermissions(self, files=[], dirs=[]):
        if isLMUser:
            print('Permissions created correctly by LMUser')
        else:
            dirname = os.path.dirname(self.outConfigFilename)
            stats = os.stat(dirname)
            # item 5 is group id; get for lmwriter
            gid = stats[5]
            if files is not None:
                if not (isinstance(files, list) or 
                        isinstance(files, tuple)):
                    files = [files]
                    for fd in files:
                        try:
                            os.chown(fd, -1, gid)
                            os.chmod(fd, 0664)
                        except Exception, e:
                            print('Failed to fix permissions on {}'.format(fd))
            if dirs is not None:
                if not (isinstance(dirs, list) or 
                        isinstance(dirs, tuple)):
                    dirs = [dirs]
                    for d in dirs:
                        currperms = oct(os.stat(d)[stat.ST_MODE])[-3:]
                        if currperms != '775':
                            try:
                                os.chown(d, -1, gid)
                                os.chmod(d, 0775)
                            except Exception, e:
                                print('Failed to fix permissions on {}'.format(d))
         
    # ...............................................
    def _getDb(self, logname):
        import logging
        logger = ScriptLogger(logname, level=logging.INFO)
        # DB connection
        scribe = BorgScribe(logger)
        return scribe
   
    # .............................................................................
    def _getAlgorithm(self, config, algHeading):
        """
        @note: Returns configured algorithm
        """
        acode =  config.get(algHeading, BoomKeys.ALG_CODE)
        alg = Algorithm(acode)
        alg.fillWithDefaults()
        inputs = {}
        # override defaults with any option specified
        algoptions = config.getoptions(algHeading)
        for name in algoptions:
            pname, ptype = alg.findParamNameType(name)
            if pname is not None:
                if ptype == types.IntType:
                    val = config.getint(algHeading, pname)
                elif ptype == types.FloatType:
                    val = config.getfloat(algHeading, pname)
                else:
                    val = config.get(algHeading, pname)
                    # Some algorithms(mask) may have a parameter indicating a layer,
                    # if so, add name to parameters and object to inputs
                    if acode == 'hull_region_intersect' and pname == 'region':
                    # TODO: re-enable this later.  
                    #       Now, always use layer in SDM_MASK_META in scenario meta
                        pass
#                         if val.endswith(LMFormat.GTIFF.ext):
#                             val = val[:-len(LMFormat.GTIFF.ext)]
#                         inputs[pname] = val
                alg.setParameter(pname, val)
        if inputs:
            alg.setInputs(inputs)
        return alg
      
    # .............................................................................
    def _getAlgorithms(self, config, sectionPrefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX):
        """
        @note: Returns configured algorithms, uses default algorithms only 
               if no others exist
        """
        algs = {}
        defaultAlgs = {}
        # Get algorithms for SDM modeling or SDM mask
        sections = config.getsections(sectionPrefix)
        for algHeading in sections:
            alg = self._getAlgorithm(config, algHeading)
           
            if algHeading.endswith(SERVER_DEFAULT_HEADING_POSTFIX):
                defaultAlgs[algHeading] = alg
            else:
                algs[algHeading] = alg
        if len(algs) == 0:
            algs = defaultAlgs
        return algs

#     # ...............................................
#     def _findScenPkgMeta(self, scenpkgName):
#         scenpkg_meta_file = os.path.join(ENV_DATA_PATH, scenpkgName + '.py')
#         if not os.path.exists(scenpkg_meta_file):
#             raise LMError(currargs='Climate metadata {} does not exist'
#                          .format(scenpkg_meta_file))
#         # TODO: change to importlib on python 2.7 --> 3.3+  
#         try:
#             import imp
#             SPMETA = imp.load_source('currentmetadata', scenpkg_meta_file)
#         except Exception, e:
#             raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
#                           .format(scenpkg_meta_file, e)) 
#         pkgMeta = SPMETA.CLIMATE_PACKAGES[scenpkgName]
#         mask_lyrname = SPMETA.SDM_MASK_META['name']
#         return pkgMeta, mask_lyrname

    # ...............................................
    def _find_scenpkg_base_and_mask(self, scenpkgName):
#         pkgMeta, mask_lyrname = self._findScenPkgMeta(scenpkgName)
        public_scenpkg_meta_file = os.path.join(ENV_DATA_PATH, scenpkgName + '.py')
        user_scenpkg_meta_file = os.path.join(self.userIdPath, scenpkgName + '.py')
        if os.path.exists(public_scenpkg_meta_file):
            scenpkg_meta_file = public_scenpkg_meta_file
        elif os.path.exists(user_scenpkg_meta_file):
            scenpkg_meta_file = user_scenpkg_meta_file
        else:
            raise LMError(currargs='Climate metadata does not exist in {} or {}'
                         .format(public_scenpkg_meta_file, user_scenpkg_meta_file))
        # TODO: change to importlib on python 2.7 --> 3.3+  
        try:
            import imp
            SPMETA = imp.load_source('currentmetadata', scenpkg_meta_file)
        except Exception, e:
            raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                          .format(scenpkg_meta_file, e)) 
        pkgMeta = SPMETA.CLIMATE_PACKAGES[scenpkgName]
        # Mask is optional
        try:
            mask_lyrname = SPMETA.SDM_MASK_META['name']
        except:
            mask_lyrname = None
        baseCode = pkgMeta['baseline']
        return baseCode, mask_lyrname

    # ...............................................
    def readParamVals(self):
        if self.inParamFname is None or not os.path.exists(self.inParamFname):
            raise Exception('Missing config file {}'.format(self.inParamFname))
        
        paramFname = self.inParamFname
        config = Config(siteFn=paramFname)
        
        # ..........................
        usr = self._getBoomParam(config, BoomKeys.ARCHIVE_USER, defaultValue=PUBLIC_USER)
        earl = EarlJr()
        usrPath = earl.createDataPath(usr, LMFileType.BOOM_CONFIG)
        usrEmail = self._getBoomParam(config, BoomKeys.ARCHIVE_USER_EMAIL, 
                                          defaultValue='{}{}'.format(usr, 
                                                        DEFAULT_EMAIL_POSTFIX))
        
        archiveName = self._getBoomParam(config, BoomKeys.ARCHIVE_NAME)
        if archiveName is None:
            raise Exception('Failed to configure ARCHIVE_NAME')
        
        if usr == PUBLIC_USER:
            def_priority = Priority.NORMAL
        else:
            def_priority = Priority.REQUESTED
        priority = self._getBoomParam(config, BoomKeys.ARCHIVE_PRIORITY, 
                                          defaultValue=def_priority)
            
        # ..........................
        # Species data source and input
        occFname = occSep = user_taxonomy_basefilename = occIdFname = None
        taxon_id_filename = taxon_name_filename = None
        dataSource = self._getBoomParam(config, BoomKeys.DATA_SOURCE)
        if dataSource is None:
            raise Exception('Failed to configure DATA_SOURCE')
        else:
            dataSource = dataSource.upper()
        if dataSource not in (SpeciesDatasource.GBIF, 
                              SpeciesDatasource.USER, 
                              SpeciesDatasource.EXISTING, 
                              SpeciesDatasource.TAXON_IDS, 
                              SpeciesDatasource.TAXON_NAMES):
            raise Exception('Failed to configure supported DATA_SOURCE')
        elif dataSource in (SpeciesDatasource.GBIF, SpeciesDatasource.USER):        
            occFname = self._getBoomParam(config, BoomKeys.OCC_DATA_NAME)
            occSep = self._getBoomParam(config, BoomKeys.OCC_DATA_DELIMITER)
            # Taxonomy is optional, 
            if dataSource == SpeciesDatasource.USER:
                user_taxonomy_basefilename = self._getBoomParam(config, BoomKeys.USER_TAXONOMY_FILENAME)
            if occSep is None:
                occSep = GBIF.DATA_DUMP_DELIMITER
            if occFname is None:
                raise Exception('Failed to configure OCC_DATA_NAME for DATA_SOURCE=GBIF or USER')
        elif dataSource == SpeciesDatasource.EXISTING:        
            occIdFname = self._getBoomParam(config, BoomKeys.OCC_ID_FILENAME)
            if occIdFname is None:
                raise Exception('Failed to configure OCC_ID_FILENAME for DATA_SOURCE=EXISTING')
        elif dataSource == SpeciesDatasource.TAXON_IDS:        
            taxon_id_filename = self._getBoomParam(config, BoomKeys.TAXON_ID_FILENAME)
            if taxon_id_filename is None:
                raise Exception('Failed to configure TAXON_ID_FILENAME for DATA_SOURCE=TAXON_IDS')
        elif dataSource == SpeciesDatasource.TAXON_NAMES:        
            taxon_name_filename = self._getBoomParam(config, BoomKeys.TAXON_NAME_FILENAME)
            if taxon_name_filename is None:
                raise Exception('Failed to configure TAXON_NAME_FILENAME for DATA_SOURCE=TAXON_NAMES')
        # ..........................    
        minpoints = self._getBoomParam(config, BoomKeys.POINT_COUNT_MIN)
        today = mx.DateTime.gmt()
        expyr = self._getBoomParam(config, BoomKeys.OCC_EXP_YEAR, defaultValue=today.year)
        expmo = self._getBoomParam(config, BoomKeys.OCC_EXP_MONTH, defaultValue=today.month)
        expdy = self._getBoomParam(config, BoomKeys.OCC_EXP_DAY, defaultValue=today.day)
        
        # ..........................
        algs = self._getAlgorithms(config, sectionPrefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX)
        # ..........................
        # One optional Mask for pre-processing
        maskAlg = None
        maskAlgList = self._getAlgorithms(config, sectionPrefix=SERVER_SDM_MASK_HEADING_PREFIX)
        if maskAlgList:
            if len(maskAlgList) == 1:
                maskAlg = maskAlgList.values()[0]
            else:
                raise Exception('Only one PREPROCESSING SDM_MASK supported')
        # ..........................
        # optional MCPA inputs, data values indicate processing steps
        treeFname = self._getBoomParam(config, BoomKeys.TREE)
        biogeoName = self._getBoomParam(config, BoomKeys.BIOGEO_HYPOTHESES_LAYERS)
        bghypFnames = self._getBioGeoHypothesesLayerFilenames(biogeoName, usrPath)
        # ..........................
        # optional layer inputs 
        other_lyr_names = self._getBoomParam(config, BoomKeys.OTHER_LAYERS, 
                                                 defaultValue=[], isList=True)
        # ..........................
        # RAD/PAM params, defaults to "Do not intersect"
        intersectParams = None
        compute_pam_stats = None
        compute_mcpa = None
        num_permutations = None
        
        do_assemble_pams = self._getBoomParam(config, BoomKeys.ASSEMBLE_PAMS, 
                                                  isBool=True, defaultValue=False)
        gridbbox = self._getBoomParam(config, BoomKeys.GRID_BBOX, isList=True)
        cellsides = self._getBoomParam(config, BoomKeys.GRID_NUM_SIDES)
        cellsize = self._getBoomParam(config, BoomKeys.GRID_CELL_SIZE)
        gridname = '{}-Grid-{}'.format(archiveName, cellsize)
        # TODO: enable filter string
        gridFilter = self._getBoomParam(config, BoomKeys.INTERSECT_FILTER_STRING)
        gridIntVal = self._getBoomParam(config, BoomKeys.INTERSECT_VAL_NAME)
        gridMinPct = self._getBoomParam(config, BoomKeys.INTERSECT_MIN_PERCENT)
        gridMinPres = self._getBoomParam(config, BoomKeys.INTERSECT_MIN_PRESENCE)
        gridMaxPres = self._getBoomParam(config, BoomKeys.INTERSECT_MAX_PRESENCE)
        if do_assemble_pams:
            for var in (gridbbox, cellsides, cellsize, gridIntVal, gridMinPct,
                        gridMinPres, gridMaxPres):
                if not var:
                    raise Exception("""Failed to configure one or more GRID 
                    parameters: GRID_BBOX, GRID_NUM_SIDES, GRID_CELL_SIZE,
                    INTERSECT_VAL_NAME, INTERSECT_MIN_PERCENT, 
                    INTERSECT_MIN_PRESENCE, INTERSECT_MAX_PERCENT""")
            intersectParams = {MatrixColumn.INTERSECT_PARAM_FILTER_STRING: gridFilter,
                               MatrixColumn.INTERSECT_PARAM_VAL_NAME: gridIntVal,
                               MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: gridMinPres,
                               MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: gridMaxPres,
                               MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: gridMinPct}
            # More computations, only if 
            compute_pam_stats = self._getBoomParam(config, BoomKeys.COMPUTE_PAM_STATS, 
                                                       isBool=True, defaultValue=False)
            compute_mcpa = self._getBoomParam(config, BoomKeys.COMPUTE_MCPA, 
                                                  isBool=True, defaultValue=False)
            num_permutations = self._getBoomParam(config, BoomKeys.NUM_PERMUTATIONS,
                                                      defaultValue=DEFAULT_NUM_PERMUTATIONS)
        # ..........................
        scenPackageName = self._getBoomParam(config, BoomKeys.SCENARIO_PACKAGE)
        if scenPackageName is None:
            raise LMError('Failed to configure SCENARIO_PACKAGE')
        mdl_scencode = self._getBoomParam(config, BoomKeys.SCENARIO_PACKAGE_MODEL_SCENARIO)
        if mdl_scencode is None:
            self.log.info('Retrieve `baseline` scenario from SCENARIO_PACKAGE metadata')
        prj_scencodes = self._getBoomParam(config, 
                    BoomKeys.SCENARIO_PACKAGE_PROJECTION_SCENARIOS, isList=True)
        if not prj_scencodes:
            self.log.info('Retrieve all scenarios from SCENARIO_PACKAGE metadata')
        
        return (usr, usrPath, usrEmail, user_taxonomy_basefilename, 
                archiveName, priority, scenPackageName, 
                mdl_scencode, prj_scencodes, dataSource, 
                occIdFname, taxon_name_filename, taxon_id_filename, 
                occFname, occSep, minpoints, (expyr, expmo, expdy), algs, 
                do_assemble_pams, gridbbox, cellsides, cellsize, gridname, 
                intersectParams, maskAlg, treeFname, bghypFnames, 
                compute_pam_stats, compute_mcpa, num_permutations, other_lyr_names)
      
    # ...............................................
    def writeConfigFile(self, tree=None, biogeoLayers=[]):
        config = ConfigParser.SafeConfigParser()
        config.add_section(SERVER_BOOM_HEADING)
        # .........................................      
        # SDM Algorithms with all parameters   
        for heading, alg in self.algorithms.iteritems():
            config.add_section(heading)
            config.set(heading, BoomKeys.ALG_CODE, alg.code)
            for name, val in alg.parameters.iteritems():
                config.set(heading, name, str(val))
        # SDM Mask input for optional pre-processing
        if self.maskAlg is not None:
            config.add_section(SERVER_SDM_MASK_HEADING_PREFIX)
            config.set(SERVER_SDM_MASK_HEADING_PREFIX, BoomKeys.ALG_CODE, 
                       self.maskAlg.code)
            for name, val in self.maskAlg.parameters.iteritems():
                config.set(SERVER_SDM_MASK_HEADING_PREFIX, name, str(val))
        # .........................................      
        email = self.userEmail
        if email is None:
            email = ''
        # General config
        config.set(SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_USER, self.userId)
        config.set(SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_NAME, self.archiveName)
        config.set(SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_PRIORITY, str(self.priority))
        config.set(SERVER_BOOM_HEADING, BoomKeys.TROUBLESHOOTERS, email)
        # .........................................      
        # SDM input environmental data, pulled from SCENARIO_PACKAGE metadata
        pcodes = ','.join(self.prj_scencodes)
        config.set(SERVER_BOOM_HEADING, BoomKeys.SCENARIO_PACKAGE_PROJECTION_SCENARIOS, 
                   pcodes)
        config.set(SERVER_BOOM_HEADING, BoomKeys.SCENARIO_PACKAGE_MODEL_SCENARIO, 
                   self.mdl_scencode)
        config.set(SERVER_BOOM_HEADING, BoomKeys.MAPUNITS, self.scenPkg.mapUnits)
        config.set(SERVER_BOOM_HEADING, BoomKeys.EPSG, str(self.scenPkg.epsgcode))
        config.set(SERVER_BOOM_HEADING, BoomKeys.SCENARIO_PACKAGE, self.scenPkg.name)
        # SDM input species source data
        config.set(SERVER_BOOM_HEADING, BoomKeys.DATA_SOURCE, self.dataSource)
        # Use/copy public data
        if self.dataSource == SpeciesDatasource.EXISTING:
            config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_ID_FILENAME, 
                      self.occIdFname)
        # Use GBIF taxon ids to pull iDigBio data
        elif self.dataSource == SpeciesDatasource.TAXON_IDS:
            config.set(SERVER_BOOM_HEADING, BoomKeys.TAXON_ID_FILENAME, self.taxon_id_filename)
        # Use GBIF data dump, with supporting provider and taxonomy files 
        elif self.dataSource == SpeciesDatasource.GBIF:
            config.set(SERVER_BOOM_HEADING, BoomKeys.GBIF_PROVIDER_FILENAME, 
                       GBIF_PROVIDER_FILENAME)
            config.set(SERVER_BOOM_HEADING, BoomKeys.GBIF_TAXONOMY_FILENAME, 
                       GBIF_TAXONOMY_FILENAME)
            config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_NAME, self.occFname)
            config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_DELIMITER, self.occSep)
        # User data
        else:            
            config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_NAME, self.occFname)
            config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_DELIMITER, self.occSep)        
            # optional user-provided taxonomy
            if self.user_taxonomy_basefilename is not None:
                config.set(SERVER_BOOM_HEADING, BoomKeys.USER_TAXONOMY_FILENAME, 
                           self.userTaxonomyBasename)
        # .........................................      
        # Expiration date triggering re-query and computation
        config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_EXP_YEAR, str(self.expdate[0]))
        config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_EXP_MONTH, str(self.expdate[1]))
        config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_EXP_DAY, str(self.expdate[2]))
        config.set(SERVER_BOOM_HEADING, BoomKeys.POINT_COUNT_MIN, str(self.minpoints))
        # TODO: Use this in boomer
        config.set(SERVER_BOOM_HEADING, BoomKeys.OCC_EXP_MJD, str(self.woof_time_mjd))
        # .........................................      
        # Multi-species flags
        config.set(SERVER_BOOM_HEADING, BoomKeys.ASSEMBLE_PAMS, str(self.do_assemble_pams))
        config.set(SERVER_BOOM_HEADING, BoomKeys.COMPUTE_PAM_STATS, str(self.compute_pam_stats))
        config.set(SERVER_BOOM_HEADING, BoomKeys.COMPUTE_MCPA, str(self.compute_mcpa)) 
        # Grid and Intersect params
        config.set(SERVER_BOOM_HEADING, BoomKeys.GRID_NUM_SIDES, str(self.cellsides))
        config.set(SERVER_BOOM_HEADING, BoomKeys.GRID_CELL_SIZE, str(self.cellsize))
        config.set(SERVER_BOOM_HEADING, BoomKeys.GRID_BBOX, 
                   ','.join(str(v) for v in self.gridbbox))
        config.set(SERVER_BOOM_HEADING, BoomKeys.GRID_NAME, self.gridname)
        # Intersection params
        for k, v in self.intersectParams.iteritems():
            # refer to BoomKeys.INTERSECT_*
            config.set(SERVER_BOOM_HEADING, 'INTERSECT_{}'.format(k.upper()), str(v))
        # Multi-species randomization 
        config.set(SERVER_BOOM_HEADING, BoomKeys.NUM_PERMUTATIONS, 
                   str(self.num_permutations))
        # .........................................      
        # MCPA Biogeographic Hypotheses
        if len(biogeoLayers) > 0:
            bioGeoLayerNames = ','.join(biogeoLayers)
            config.set(SERVER_BOOM_HEADING, BoomKeys.BIOGEO_HYPOTHESES_LAYERS, bioGeoLayerNames)
        # Phylogenetic data for PD
        if tree is not None:
            config.set(SERVER_BOOM_HEADING, BoomKeys.TREE, tree.name)
              
        readyFilename(self.outConfigFilename, overwrite=True)
        with open(self.outConfigFilename, 'wb') as configfile:
            config.write(configfile)
        self._fixPermissions(files=[self.outConfigFilename])
        self.scribe.log.info('******')
        self.scribe.log.info('--config_file={}'.format(self.outConfigFilename))   
        self.scribe.log.info('******')

   
    # ...............................................
    def _getVarValue(self, var):
        # Remove spaces and empty strings
        if var is not None and not isinstance(var, bool):
            var = var.strip()
            if var == '':
                var = None
        # Convert to number if needed
        try:
            var = int(var)
        except:
            try:
                var = float(var)
            except:
                pass
        return var
   
    # ...............................................
    def _getBoomParam(self, config, varname, defaultValue=None, 
                      isList=False, isBool=False):
        var = None
        # Get value from BOOM or default config file
        if isBool:
            try:
                var = config.getboolean(SERVER_BOOM_HEADING, varname)
            except:
                if isinstance(defaultValue, bool):
                    var = defaultValue
                else:
                    raise LMError('Var {} must be set to True or False'.format(varname))
        else:
            var = config.get(SERVER_BOOM_HEADING, varname)
            # Interpret value
            if var is not None:
                if isList:
                    try:
                        tmplist = [v.strip() for v in var.split(',')]
                        var = []
                    except:
                        raise LMError('Failed to split variables on \',\'')
                    for v in tmplist:
                        v = self._getVarValue(v)
                        var.append(v)
                else:
                    var = self._getVarValue(var)
            # or take default if present
            else:
                if defaultValue is not None:
                    if isList and isinstance(defaultValue, list):
                        var = defaultValue
                    elif isBool and isinstance(defaultValue, bool):
                        var = defaultValue
                    elif not isList and not isBool:
                        var = defaultValue
        return var
   

    # ...............................................
    def addUser(self):
        """
        @summary Adds provided userid to the database
        """
        user = LMUser(self.userId, self.userEmail, self.userEmail, 
                      modTime=mx.DateTime.gmt().mjd)
        self.scribe.log.info('  Find or insert user {} ...'.format(self.userId))
        updatedUser = self.scribe.findOrInsertUser(user)
        # If exists, found by unique Id or Email, update values
        self.userId = updatedUser.userid
        self.userEmail = updatedUser.email
   
    # ...............................................
    def _checkOccurrenceSets(self, limit=10):
        legalUsers = [PUBLIC_USER, self.userId]
        missingCount = 0
        wrongUserCount = 0
        nonIntCount = 0
        if not os.path.exists(self.occIdFname):
            raise LMError('Missing OCCURRENCE_ID_FILENAME {}'.format(self.occIdFname))
        else:
            count = 0
            for line in open(self.occIdFname, 'r'):
                count += 1
                try:
                    tmp = line.strip()
                except Exception, e:
                    self.scribe.log.info('Error reading line {} ({}), stopping'
                                         .format(count, str(e)))
                    break
                try:
                    occid = int(tmp)
                except Exception, e:
                    self.scribe.log.info('Unable to get Id from data {} on line {}'
                                         .format(tmp, count))
                    nonIntCount += 1
                else:
                    occ = self.scribe.getOccurrenceSet(occId=occid)
                    if occ is None:
                        missingCount += 1
                    elif occ.getUserId() not in legalUsers:
                        self.scribe.log.info('Unauthorized user {} for ID {}'
                                             .format(occ.getUserId(), occid))
                        wrongUserCount += 1
                if count >= limit:
                    break
        self.scribe.log.info('Errors out of {} read OccurrenceSets (limit {}):'.format(count, limit))
        self.scribe.log.info('  Missing: {} '.format(missingCount))
        self.scribe.log.info('  Unauthorized data: {} '.format(wrongUserCount))
        self.scribe.log.info('  Bad ID: {} '.format(nonIntCount))
    
    # ...............................................
    def _addIntersectGrid(self):
        shp = ShapeGrid(self.gridname, self.userId, self.scenPkg.epsgcode, self.cellsides, 
                        self.cellsize, self.scenPkg.mapUnits, self.gridbbox,
                        status=JobStatus.INITIALIZE, 
                        statusModTime=mx.DateTime.gmt().mjd)
        newshp = self.scribe.findOrInsertShapeGrid(shp)
        validData = False
        if newshp: 
            # check existence
            validData, _ = ShapeGrid.testVector(newshp.getDLocation())
            if not validData:
                try:
                    # Write new shapegrid
                    dloc = newshp.getDLocation()
                    newshp.buildShape(overwrite=True)
                    validData, _ = ShapeGrid.testVector(dloc)
                    self._fixPermissions(files=newshp.getShapefiles())
                except Exception, e:
                    self.scribe.log.warning('Unable to build Shapegrid ({})'.format(str(e)))
                if not validData:
                    raise LMError(currargs='Failed to write Shapegrid {}'.format(dloc))
            if validData and newshp.status != JobStatus.COMPLETE:
                newshp.updateStatus(JobStatus.COMPLETE)
                success = self.scribe.updateObject(newshp)
                if success is False:
                    self.scribe.log.warning('Failed to update Shapegrid record')
        else:
            raise LMError(currargs='Failed to find or insert Shapegrid')
        return newshp
      
    # ...............................................
    def _findOrAddPAM(self, gridset, alg, scen):
        # Create Global PAM for this archive, scenario
        # Pam layers are added upon boom processing
        pamType = MatrixType.PAM
        if not self.compute_pam_stats:
            pamType = MatrixType.ROLLING_PAM
            
        kws = [GPAM_KEYWORD]
        for kw in (scen.code, scen.gcmCode, scen.altpredCode, scen.dateCode):
            if kw is not None:
                kws.append(kw)
                
        desc = '{} for Scenario {}'.format(GPAM_KEYWORD, scen.code)
        pamMeta = {ServiceObject.META_DESCRIPTION: desc,
                   ServiceObject.META_KEYWORDS: kws}
        
        tmpGpam = LMMatrix(None, matrixType=pamType,
                           # TODO: replace 3 codes with scenarioId 
                           scenarioid=scen.getId(),
                           gcmCode=scen.gcmCode, altpredCode=scen.altpredCode, 
                           dateCode=scen.dateCode, 
                           algCode=alg.code, 
                           metadata=pamMeta, userId=self.userId, 
                           gridset=gridset, 
                           status=JobStatus.GENERAL, 
                           statusModTime=mx.DateTime.gmt().mjd)
        gpam = self.scribe.findOrInsertMatrix(tmpGpam)
        return gpam

    # ...............................................
    def _findOrAddGRIM(self, gridset, scen):
        # Create Scenario-GRIM for this archive, scenario
        # GRIM layers are added now
        kws = [GGRIM_KEYWORD]
        for kw in (scen.code, scen.gcmCode, scen.altpredCode, scen.dateCode):
            if kw is not None:
                kws.append(kw)
        
        desc = '{} for Scenario {}'.format(GGRIM_KEYWORD, scen.code)
        grimMeta = {ServiceObject.META_DESCRIPTION: desc,
                    ServiceObject.META_KEYWORDS: kws}
        
        tmpGrim = LMMatrix(None, matrixType=MatrixType.GRIM, 
                           # TODO: replace 3 codes with scenarioId 
                           scenarioid=scen.getId(),
                           gcmCode=scen.gcmCode, altpredCode=scen.altpredCode, 
                           dateCode=scen.dateCode, 
                           metadata=grimMeta, userId=self.userId, 
                           gridset=gridset, 
                           status=JobStatus.GENERAL, 
                           statusModTime=mx.DateTime.gmt().mjd)
        grim = self.scribe.findOrInsertMatrix(tmpGrim)
        for lyr in scen.layers:
            # Add to GRIM Makeflow ScenarioLayer and MatrixColumn
            mtxcol = self._initGRIMIntersect(lyr, grim)
        return grim

    # ...............................................
    def addShapeGridGPAMGridset(self):
        """
        @summary: Create a Gridset, Shapegrid, PAMs, GRIMs for this archive, and
                  update attributes with new or existing values from DB
        """
        scenGrims = {}
        self.scribe.log.info('  Find or insert, build shapegrid {} ...'.format(self.gridname))
        shp = self._addIntersectGrid()
        self.scribe.log.info('  Found or inserted shapegrid')
        self.shapegrid = shp
        # "BOOM" Archive
        # TODO: change 'parameters' to ServiceObject.META_PARAMS
        meta = {ServiceObject.META_DESCRIPTION: ARCHIVE_KEYWORD,
                ServiceObject.META_KEYWORDS: [ARCHIVE_KEYWORD],
                'parameters': self.inParamFname}
        grdset = Gridset(name=self.archiveName, metadata=meta, shapeGrid=shp, 
                         epsgcode=self.scenPkg.epsgcode, 
                         userId=self.userId, modTime=self.woof_time_mjd)
        updatedGrdset = self.scribe.findOrInsertGridset(grdset)
        if updatedGrdset.modTime < self.woof_time_mjd:
            updatedGrdset.modTime = self.woof_time_mjd
            self.scribe.updateObject(updatedGrdset)
            
            # TODO: Decide: do we want to delete old makeflows for this gridset?
            fnames = self.scribe.deleteMFChainsReturnFilenames(updatedGrdset.getId())
            for fn in fnames:
                os.remove(fn)
                
            self.scribe.log.info('  Found and updated modtime for gridset {}'
                                 .format(updatedGrdset.getId()))
        else:
            self.scribe.log.info('  Inserted new gridset {}'
                                 .format(updatedGrdset.getId()))
            
        # TODO: Reset expiration date to Woof-date in MJD
        
        for code, scen in self.scenPkg.scenarios.iteritems():
            # "Global" PAM (one per scenario/algorithm)
            if code in self.prj_scencodes:
                # TODO: Allow alg to be specified for each species, all in same PAM
                for alg in self.algorithms.values():
                    gPam = self._findOrAddPAM(updatedGrdset, alg, scen)
                    
                # "Global" GRIM (one per scenario) 
                if not(self.userId == DEFAULT_POST_USER) or self.compute_mcpa:
                    scenGrim = self._findOrAddGRIM(updatedGrdset, scen)
                    scenGrims[code] = scenGrim
                
        return scenGrims, updatedGrdset
   
    # ...............................................
    def _initGRIMIntersect(self, lyr, mtx):
        """
        @summary: Initialize model, projections for inputs/algorithm.
        """
        mtxcol = None
        intersectParams = {MatrixColumn.INTERSECT_PARAM_WEIGHTED_MEAN: True}
        
        if lyr is not None:
            # TODO: Save processType into the DB??
            if LMFormat.isGDAL(driver=lyr.dataFormat):
                ptype = ProcessType.INTERSECT_RASTER_GRIM
            else:
                self.scribe.log.debug('Vector intersect not yet implemented for GRIM column {}'
                                      .format(mtxcol.getId()))
            
            # TODO: Change ident to lyr.ident when that is populated
            tmpCol = MatrixColumn(None, mtx.getId(), self.userId, 
                   layer=lyr, shapegrid=self.shapegrid, 
                   intersectParams=intersectParams, 
                   squid=lyr.squid, ident=lyr.name, processType=ptype, 
                   status=JobStatus.GENERAL, statusModTime=self.woof_time_mjd,
                   postToSolr=False)
            mtxcol = self.scribe.findOrInsertMatrixColumn(tmpCol)
            
            # DB does not populate with shapegrid on insert
            mtxcol.shapegrid = self.shapegrid
            
            # TODO: This is a hack, post to solr needs to be retrieved from DB
            mtxcol.postToSolr = False
            if mtxcol is not None:
                self.scribe.log.debug('Found/inserted MatrixColumn {}'.format(mtxcol.getId()))
                # Reset processType (not in db)
                mtxcol.processType = ptype            
        return mtxcol

    # ...............................................
    def _getMCProcessType(self, mtxColumn, mtxType):
        """
        @summary Initialize configured and stored inputs for ArchiveFiller class.
        """
        if LMFormat.isOGR(driver=mtxColumn.layer.dataFormat):
            if mtxType == MatrixType.PAM:
                ptype = ProcessType.INTERSECT_VECTOR
            elif mtxType == MatrixType.GRIM:
                raise LMError('Vector GRIM intersection is not implemented')
        else:
            if mtxType == MatrixType.PAM:
                ptype = ProcessType.INTERSECT_RASTER
            elif mtxType == MatrixType.GRIM:
                ptype = ProcessType.INTERSECT_RASTER_GRIM
        return ptype
   
      
    # ...............................................
    def addTree(self, gridset, encoded_tree=None):
        tree = None
        # Provided tree filename takes precedence
        if self.treeFname is not None:
            name, _ = os.path.splitext(self.treeFname)
            treeFilename = os.path.join(self.userIdPath, self.treeFname) 
            if os.path.exists(treeFilename):
                baretree = Tree(name, dlocation=treeFilename, userId=self.userId, 
                                gridsetId=gridset.getId(), modTime=self.woof_time_mjd)
                baretree.read()
                tree = self.scribe.findOrInsertTree(baretree)
            else:
                self.scribe.log.warning('No tree at {}'.format(treeFilename))
        elif encoded_tree is not None:
            tree = self.scribe.findOrInsertTree(encoded_tree)
            
        if tree is not None:
            # Update tree properties and write file
            tree.clearDLocation()
            tree.setDLocation()
            tree.writeTree()
            tree.updateModtime(mx.DateTime.gmt().mjd)
            # Update database
            success = self.scribe.updateObject(tree)        
            self._fixPermissions(files=[tree.getDLocation()])
            
            # Save tree link to gridset
            print "Add tree to grid set"
            gridset.addTree(tree)
            gridset.updateModtime(self.woof_time_mjd)
            
            self.scribe.updateObject(gridset)
        return tree


    # ...............................................
    def _getBGMeta(self, bgFname):
        # defaults for no metadata file
        # lower-case dict keys
        bgkeyword = 'biogeographic hypothesis'
        lyrMeta = {MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): None,
                   ServiceObject.META_DESCRIPTION.lower(): 
        'Biogeographic hypothesis based on layer {}'.format(bgFname)}
        fpthbasename, _ = os.path.splitext(bgFname)
        metaFname = fpthbasename + LMFormat.JSON.ext
        if os.path.exists(metaFname):
            with open(metaFname) as f:
                meta = json.load(f)
                if type(meta) is dict:
                    for k, v in meta.iteritems():
                        lyrMeta[k.lower()] = v
                    # Add keyword to metadata
                    try:
                        kwdStr = meta[ServiceObject.META_KEYWORDS]
                        keywords = kwdStr.split(',')
                        if bgkeyword not in keywords:
                            keywords.append(bgkeyword)
                    except:
                        meta[ServiceObject.META_KEYWORDS] = bgkeyword
                else:
                    raise LMError('Metadata must be a dictionary or a JSON-encoded dictionary')
        return lyrMeta
   
    # ...............................................
    def _getOtherLayerFilenames(self):
        layers = []
        for lyrname in self.other_lyr_names:
            lyrpth = os.path.join(self.userIdPath, lyrname) 
            # accept vector shapefiles
            if os.path.exists(lyrpth + LMFormat.SHAPE.ext):
                layers.append((lyrname, lyrpth + LMFormat.SHAPE.ext))
            # accept raster geotiffs
            elif os.path.exists(lyrpth + LMFormat.GTIFF.ext):
                layers.append((lyrname, lyrpth + LMFormat.GTIFF.ext))
            # accept shapefiles or geotiffs in a 
            else:
                self.scribe.log.warning('No layers at {}'.format(lyrpth))
        return layers
    
    # ...............................................
    def addOtherLayers(self):
        """
        @note: assumes same EPSG as scenario provided
        """
        otherLayerNames = []
        layers = self._getOtherLayerFilenames()
        for (lyrname, fname) in layers:
            lyr = None
            if fname.endswith(LMFormat.SHAPE.ext):
                lyr = Vector(lyrname, self.userId, self.scenPkg.epsgcode, 
                             dlocation=fname,  
                             dataFormat=LMFormat.SHAPE.driver, 
                             modTime=self.woof_time_mjd)
                updatedLyr = self.scribe.findOrInsertLayer(lyr)
                otherLayerNames.append(updatedLyr.name)
            elif fname.endswith(LMFormat.GTIFF.ext):
                lyr = Raster(lyrname, self.userId, self.scenPkg.epsgcode, 
                             dlocation=fname, 
                             dataFormat=LMFormat.getDefaultGDAL().driver, 
                             modTime=self.woof_time_mjd)
            if lyr is not None:
                updatedLyr = self.scribe.findOrInsertLayer(lyr)
                otherLayerNames.append(updatedLyr.name)        
        self.scribe.log.info('  Added other layers {} for user'.format(otherLayerNames))
        return otherLayerNames
   

    # ...............................................
    def _getBioGeoHypothesesLayerFilenames(self, biogeoName, usrPath):
        bghypFnames = []
        if biogeoName is not None:
            bgpth = os.path.join(usrPath, biogeoName) 
            if os.path.exists(bgpth + LMFormat.SHAPE.ext):
                bghypFnames = [bgpth + LMFormat.SHAPE.ext]
            elif os.path.isdir(bgpth):
                import glob
                pattern = os.path.join(bgpth, '*' + LMFormat.SHAPE.ext)
                bghypFnames = glob.glob(pattern)
            else:
                self.scribe.log.warning('No biogeo shapefiles at {}'.format(bgpth))
        return bghypFnames
    
    # ...............................................
    def addBioGeoHypothesesMatrixAndLayers(self, gridset):
        biogeoLayerNames = []
        bgMtx = None
           
        if len(self.bghypFnames) > 0:
            mtxKeywords = ['biogeographic hypotheses']
            for bgFname in self.bghypFnames:
                if os.path.exists(bgFname):
                    lyrMeta = self._getBGMeta(bgFname)
                    valAttr = lyrMeta[MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
                    try:
                        name = lyrMeta['name']
                    except:
                        name, _ = os.path.splitext(os.path.basename(bgFname))
                    mtxKeywords.append('Layer {}'.format(name))
                    lyr = Vector(name, self.userId, self.scenPkg.epsgcode, 
                                 dlocation=bgFname, metadata=lyrMeta, 
                                 dataFormat=LMFormat.SHAPE.driver, 
                                 valAttribute=valAttr, modTime=self.woof_time_mjd)
                    updatedLyr = self.scribe.findOrInsertLayer(lyr)
                    biogeoLayerNames.append(updatedLyr.name)
                    self.scribe.log.info('  Added {} layers for biogeo hypotheses matrix'
                                         .format(len(biogeoLayerNames)))
            # Add the matrix to contain biogeo hypotheses layer intersections
            meta={ServiceObject.META_DESCRIPTION.lower(): 
                  'Biogeographic Hypotheses for archive {}'.format(self.archiveName),
                  ServiceObject.META_KEYWORDS.lower(): mtxKeywords}
            tmpMtx = LMMatrix(None, matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                              processType=ProcessType.ENCODE_HYPOTHESES,
                              userId=self.userId, gridset=gridset, metadata=meta,
                              status=JobStatus.INITIALIZE, statusModTime=self.woof_time_mjd)
            bgMtx = self.scribe.findOrInsertMatrix(tmpMtx)
            if bgMtx is None:
                self.scribe.log.info('  Failed to add biogeo hypotheses matrix')
        return bgMtx, biogeoLayerNames
   
    # .............................
    def addGrimMFs(self, defaultGrims, target_dir):
        rules = []
        
        # Get shapegrid rules / files
        shapegrid_filename = self.shapegrid.getDLocation()
        
        for code, grim in defaultGrims.iteritems():
            mtxcols = self.scribe.getColumnsForMatrix(grim.getId())
            self.scribe.log.info('  Adding {} grim columns for scencode {}'
                          .format(len(mtxcols), code))
            
            colFilenames = []
            for mtxcol in mtxcols:
                mtxcol.postToSolr = False
                mtxcol.processType = self._getMCProcessType(
                    mtxcol, grim.matrixType)
                mtxcol.shapegrid = self.shapegrid
            
                relDir, _ = os.path.splitext(
                    mtxcol.layer.getRelativeDLocation())
                col_filename = os.path.join(
                    target_dir, relDir, mtxcol.getTargetFilename())
                try:
                    min_percent = mtxcol.intersectParams[
                        mtxcol.INTERSECT_PARAM_MIN_PERCENT]
                except:
                    min_percent = None
                intersect_cmd = GrimRasterCommand(
                    shapegrid_filename, mtxcol.layer.getDLocation(),
                    col_filename, minPercent=min_percent, ident=mtxcol.ident)
                rules.append(intersect_cmd.getMakeflowRule())
               
                # Keep track of intersection filenames for matrix concatenation
                colFilenames.append(col_filename)
            
            # Add concatenate command
            rules.extend(self._get_matrix_assembly_and_stockpile_rules(
                grim.getId(), ProcessType.CONCATENATE_MATRICES, colFilenames,
                work_dir=target_dir))
            
        return rules

    # .............................
    def _get_matrix_assembly_and_stockpile_rules(self, matrix_id, process_type,
                                                 col_filenames, work_dir=None):
        """Get assembly and stockpile rules for a matrix

        Args:
            matrix_id : The matrix database id
                process_type : The ProcessType constant for the process used to
            create this matrix
            col_filenames : A list of file names for each column in the matrix
            work_dir : A relative directory where work should be performed
        """
        rules = []
        if work_dir is None:
            work_dir = ''
        
        # Add concatenate command
        mtx_out_filename = os.path.join(
            work_dir, 'mtx_{}{}'.format(matrix_id, LMFormat.MATRIX.ext))
        concat_cmd = ConcatenateMatricesCommand(
            col_filenames, '1', mtx_out_filename)
        rules.append(concat_cmd.getMakeflowRule())
        
        # Stockpile matrix
        mtx_success_filename = os.path.join(
            work_dir, 'mtx_{}.success'.format(matrix_id))
        stockpile_cmd = StockpileCommand(
            process_type, matrix_id, mtx_success_filename, mtx_out_filename,
            status=JobStatus.COMPLETE)
        rules.append(stockpile_cmd.getMakeflowRule(local=True))
        return rules


    # .............................
    def _getIdigQueryCmd(self, ws_dir):
        if not os.path.exists(self.taxon_id_filename):
            raise LMError('Taxon ID file {} is missing'.format(self.taxon_id_filename))
        
        # Note: These paths must exist longer than the workflow because they
        #    will be used by a different workflow
        base_fname = os.path.basename(os.path.splitext(self.taxon_id_filename)[0])
        earl = EarlJr()
        tmp_pth = earl.createDataPath(self.userId, LMFileType.TEMP_USER_DATA)
        self._fixPermissions(dirs=[tmp_pth])
        point_output_file = os.path.join(tmp_pth, base_fname + LMFormat.CSV.ext)
        meta_output_file = os.path.join(tmp_pth, base_fname + LMFormat.JSON.ext)
        
        # Success file should be in workspace, it will be sent to boomer
        success_file = os.path.join(ws_dir, base_fname + '.success')
        
        idigCmd = IdigbioQueryCommand(
            self.taxon_id_filename, point_output_file, meta_output_file,
            success_file, missing_id_file=None)
        return idigCmd, point_output_file

    # ...............................................
    def _getTaxonomyCommand(self, target_dir):
        """
        @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                  and configFile written by BOOMFiller.initBoom.
        @todo: Define format and enable ingest user taxonomy, commented out below
        """
        cattaxCmd = taxSuccessFname = taxSuccessLocalFname = taxDataFname = None
        config = Config(siteFn=self.inParamFname)
        if self.dataSource == SpeciesDatasource.GBIF:
            taxDataBasename = self._getBoomParam(config, 
                        BoomKeys.GBIF_TAXONOMY_FILENAME, GBIF_TAXONOMY_FILENAME)
            taxDataFname = os.path.join(SPECIES_DATA_PATH, taxDataBasename)
            taxSourceName = TAXONOMIC_SOURCE['GBIF']['name']
            taxSourceUrl = TAXONOMIC_SOURCE['GBIF']['url']
        
        # If there is taxonomy ...
        if taxDataFname and os.path.exists(taxDataFname):
            taxDataBase, _ = os.path.splitext(taxDataFname)
            taxSuccessFname = os.path.join(taxDataBase + '.success')
            if os.path.exists(taxSuccessFname):
                self.scribe.log.info('Taxonomy {} has already been cataloged'
                                     .format(taxDataFname))
            else:         
                # logfile, walkedTaxFname added to outputs in command construction
                taxSuccessLocalFname = os.path.join(
                    target_dir, 'catalog_taxonomy.success')
                # Write taxonomy success to workspace and pass that along, also
                #    copy local taxonomy success file to absolute location
                cattaxCmd = ChainCommand(
                    [CatalogTaxonomyCommand(
                        taxSourceName, taxDataFname, taxSuccessLocalFname,
                        source_url=taxSourceUrl, delimiter='\t'),
                    SystemCommand(
                        'cp', '{} {}'.format(
                            taxSuccessLocalFname, taxSuccessFname),
                        inputs=taxSuccessLocalFname)])
        return cattaxCmd, taxSuccessLocalFname
    
    # ...............................................
    def _write_update_MF(self, mfchain):
        mfchain.write()
        # Give lmwriter rw access (this script may be run as root)
        self._fixPermissions(files=[mfchain.getDLocation()])
        # Set as ready to go
        mfchain.updateStatus(JobStatus.INITIALIZE)
        self.scribe.updateObject(mfchain)
        try:
            self.scribe.log.info('  Wrote Makeflow {} for {} for gridset {}'
                .format(mfchain.objId, 
                        mfchain.mfMetadata[MFChain.META_DESCRIPTION], 
                        mfchain.mfMetadata[MFChain.META_GRIDSET]))
        except:
            self.scribe.log.info('  Wrote Makeflow {}'.format(mfchain.objId))
        return mfchain

    # ...............................................
    def addBoomRules(self, tree, target_dir):
        """
        @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                  and configFile written by BOOMFiller.initBoom.
        """
        rules = []
        base_config_fname = os.path.basename(self.outConfigFilename)
        # ChristopherWalken writes when finished walking through 
        # species data (initiated by this Makeflow).  
        boom_success_fname = os.path.join(target_dir, base_config_fname + '.success')
        boomCmd = BoomerCommand(self.outConfigFilename, boom_success_fname)
                  
        # Add iDigBio MF before Boom, if specified as occurrence input
        if self.dataSource == SpeciesDatasource.TAXON_IDS:
            idigCmd, point_output_file = self._getIdigQueryCmd(target_dir)
            # Update config to User (CSV) datasource and point_output_file
            self.dataSource = SpeciesDatasource.USER
            self.occFname = point_output_file
            self.occSep = IdigbioAPI.DELIMITER
            # Add command to this Makeflow
            rules.append(idigCmd.getMakeflowRule(local=True))
            # Boom requires iDigBio data
            boomCmd.inputs.extend(idigCmd.outputs)
            
        # Add taxonomy before Boom, if taxonomy is specified
        cattaxCmd, taxSuccessFname = self._getTaxonomyCommand(target_dir)
        if cattaxCmd:
            # Add catalog taxonomy command to this Makeflow
            rules.append(cattaxCmd.getMakeflowRule(local=True))
            # Boom requires catalog taxonomy completion
            boomCmd.inputs.append(taxSuccessFname)
        
        # Add boom command to this Makeflow
        rules.append(boomCmd.getMakeflowRule(local=True))
        return rules

    # .............................................................................
    def _fixDirectoryPermissions(self, boomGridset):
        lyrdir = os.path.dirname(boomGridset.getShapegrid().getDLocation())
        self._fixPermissions(dirs=[lyrdir])
        earl = EarlJr()
        mfdir = earl.createDataPath(self.userId, LMFileType.MF_DOCUMENT)
        self._fixPermissions(dirs=[mfdir])
    
    # ...............................................
    def _getPartnerTreeData(self, pquery, gbifids, basefilename):
        treename = os.path.basename(basefilename)
        otree, gbif_to_ott, ott_unmatched_gbif_ids = pquery.assembleOTOLData(
                                                            gbifids, treename)
        encoded_tree = pquery.encodeOTTTreeToGBIF(otree, gbif_to_ott, 
                                                  scribe=self.scribe)
        return encoded_tree

    # ...............................................
    def _getPartnerIds(self, pquery, names, basefilename):
        gbif_results_filename = basefilename + '.gids'
        unmatched_names, name_to_gbif_ids = pquery.assembleGBIFTaxonIds(names, 
                                                        gbif_results_filename)
        return unmatched_names, name_to_gbif_ids, gbif_results_filename
        
    # ...............................................
    def _getUserInput(self, filename):
        items = []
        if os.path.exists(filename):
            try:
                for line in open(filename):
                    items.append(line.strip())
            except:
                raise LMError('Failed to read file {}'.format(filename))
        else:
            raise LMError('File {} does not exist'.format(filename))
        return items

    # ...............................................
    def initBoom(self):
        try:
            # Also adds user
            self.initializeInputs()
                
            # Add or get ShapeGrid, Global PAM, Gridset for this archive
            # This updates the gridset, shapegrid, default PAMs (rolling, with no 
            #     matrixColumns, default GRIMs with matrixColumns
            scenGrims, boomGridset = self.addShapeGridGPAMGridset()
            # Insert other layers that may be used for SDM_MASK or other processing
            otherLayerNames = self.addOtherLayers()
            
            # Create makeflow for computations and start rule list
            # TODO: Init makeflow
            script_name = os.path.splitext(os.path.basename(__file__))[0]
            meta = {
                MFChain.META_CREATED_BY: script_name,
                MFChain.META_GRIDSET : boomGridset.getId(),
                MFChain.META_DESCRIPTION : 'Makeflow for gridset {}'.format(
                    boomGridset.getId())
                }
            new_mfc = MFChain(
                self.userId, priority=Priority.HIGH, metadata=meta,
                status=JobStatus.GENERAL, statusModTime=mx.DateTime.gmt().mjd)
            gridset_mf = self.scribe.insertMFChain(
                new_mfc, boomGridset.getId())
            target_dir = gridset_mf.getRelativeDirectory()
            rules = []
            
            # Add GRIM rules
            rules.extend(self.addGrimMFs(scenGrims, target_dir))
            
            # Check for a file OccurrenceLayer Ids for existing or PUBLIC user
            if self.occIdFname:
                self._checkOccurrenceSets()

            # Fix user makeflow and layer directory permissions
            self._fixDirectoryPermissions(boomGridset)
                        
            # If there is a tree, add db object
            tree = self.addTree(boomGridset, encoded_tree=None)
            
            # If there are biogeographic hypotheses, add layers and matrix 
            biogeoMtx, biogeoLayerNames = self.addBioGeoHypothesesMatrixAndLayers(boomGridset)            
                  
            # init Makeflow
            if biogeoMtx and len(biogeoLayerNames) > 0:
                # TODO: Create a separate module to create BG Hypotheses 
                #       encoding Makeflow, independent of Boom completion
                #       so this may be added later or called from this script 
                # Add BG hypotheses
                bgh_success_fname = os.path.join(target_dir, 'bg.success')
                bg_cmd = EncodeBioGeoHypothesesCommand(
                    self.userId, boomGridset.name, bgh_success_fname)
                rules.append(bg_cmd.getMakeflowRule(local=True))

            # This also adds commands for iDigBio occurrence data retrieval 
            #   and taxonomy insertion before Boom
            #   and tree encoding after Boom 
            rules.extend(self.addBoomRules(tree, target_dir))

            # Write config file for archive, update permissions
            self.writeConfigFile(tree=tree, biogeoLayers=biogeoLayerNames)
            
            # Write rules
            gridset_mf.addCommands(rules)
            self._write_update_MF(gridset_mf)
            
        finally:
            self.close()
           
        # BOOM POST from web requires gridset object to be returned
        return boomGridset
   
# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
             description=(""""Populate a Lifemapper archive with metadata for 
                              single- or multi-species computations specific to 
                              the configured input data or the data package 
                              named."""))
    parser.add_argument('param_file', default=None,
             help=('Parameter file for the workflow with inputs and outputs ' +
                   'to be created from these data.'))
    parser.add_argument('--logname', type=str, default=None,
             help=('Basename of the logfile, without extension'))
    args = parser.parse_args()
    paramFname = args.param_file
    logname = args.logname
          
    if paramFname is not None and not os.path.exists(paramFname):
        print ('Missing configuration file {}'.format(paramFname))
        exit(-1)
       
    if logname is None:
        import time
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(scriptname, timestamp)
    
    print('Running initWorkflow with paramFname = {}'
          .format(paramFname))
    
    filler = BOOMFiller(paramFname, logname=logname)
    gs = filler.initBoom()
    print('Completed initWorkflow creating gridset: {}'.format(gs.getId()))

    
"""
import ConfigParser
import json
import mx.DateTime
import os
import stat
import types

from LmBackend.command.boom import BoomerCommand
from LmBackend.command.common import (IdigbioQueryCommand ,
    ConcatenateMatricesCommand, SystemCommand, ChainCommand)
from LmBackend.command.server import (
    CatalogTaxonomyCommand, EncodeBioGeoHypothesesCommand, StockpileCommand)
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.apiquery import IdigbioAPI
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, LMFormat, MatrixType, 
      ProcessType, DEFAULT_POST_USER, GBIF, BoomKeys,
      SERVER_BOOM_HEADING, SERVER_SDM_MASK_HEADING_PREFIX, 
      SERVER_SDM_ALGORITHM_HEADING_PREFIX,
      SERVER_DEFAULT_HEADING_POSTFIX, SERVER_PIPELINE_HEADING)
from LmCommon.common.readyfile import readyFilename

from LmDbServer.common.lmconstants import (SpeciesDatasource, TAXONOMIC_SOURCE)
from LmDbServer.common.localconstants import (GBIF_PROVIDER_FILENAME, 
                                              GBIF_TAXONOMY_FILENAME)
from LmDbServer.tools.catalogScenPkg import SPFiller
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (ARCHIVE_KEYWORD, GGRIM_KEYWORD,
                           GPAM_KEYWORD, LMFileType, Priority, ENV_DATA_PATH,
                           PUBLIC_ARCHIVE_NAME, DEFAULT_EMAIL_POSTFIX,
                           SPECIES_DATA_PATH, DEFAULT_NUM_PERMUTATIONS)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isLMUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree
from LmBackend.command.single import GrimRasterCommand

from LmDbServer.boom.initWorkflow import *


# Public archive
config_file = '/opt/lifemapper/config/boom.public.params'

import time
secs = time.time()
timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
logname = 'initWorkflow.debug.{}'.format(timestamp)

self = BOOMFiller(config_file, logname=logname)
self.initializeInputs()

###################################################################
###################################################################

"""
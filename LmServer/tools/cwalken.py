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

@note: Removed sel.sdmMaskInputLayer attribute and removed calls to pass it to
          projection objects

"""
# .............................................................................
import glob
import mx.DateTime as dt
from osgeo.ogr import wkbPoint
import os
from types import IntType, FloatType

from LmBackend.common.lmconstants import RegistryKey, MaskMethod
from LmBackend.common.lmobj import LMError, LMObject
from LmBackend.common.parameter_sweep_config import ParameterSweepConfiguration
from LmBackend.command.server import (MultiIndexPAVCommand, 
                                      MultiStockpileCommand)
from LmBackend.command.single import SpeciesParameterSweepCommand

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat, GBIF,
          SERVER_BOOM_HEADING, SERVER_PIPELINE_HEADING, BoomKeys,
          SERVER_SDM_ALGORITHM_HEADING_PREFIX,
          SERVER_SDM_MASK_HEADING_PREFIX, SERVER_DEFAULT_HEADING_POSTFIX, 
          MatrixType) 
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LMFileType, SPECIES_DATA_PATH,
            Priority, BUFFER_KEY, CODE_KEY, ECOREGION_MASK_METHOD, MASK_KEY, 
            MASK_LAYER_KEY, PRE_PROCESS_KEY, PROCESSING_KEY, 
            MASK_LAYER_NAME_KEY,SCALE_PROJECTION_MINIMUM, 
            SCALE_PROJECTION_MAXIMUM, DEFAULT_NUM_PERMUTATIONS)
from LmServer.common.localconstants import (PUBLIC_USER, DEFAULT_EPSG, 
                                            POINT_COUNT_MAX)
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.sdmproj import SDMProjection
from LmServer.tools.occwoc import (UserWoC, ExistingWoC)

# .............................................................................
class ChristopherWalken(LMObject):
    """
    Class to ChristopherWalken with a species iterator through a sequence of 
    species data creating a Spud for each species.  Creates and catalogs objects 
    (OccurrenceSets, SMDModels, SDMProjections, and MatrixColumns and MFChains 
     for their calculation) in the database .
    """
    # .............................................................................
    # Constructor
    # .............................................................................
    def __init__(self, configFname, jsonFname=None, scribe=None):
        """
        @summary Constructor for ChristopherWalken class which creates a Spud 
                 (Single-species Makeflow chain) for a species.
        """
        super(ChristopherWalken, self).__init__()

        self.configFname = configFname
        baseAbsFilename, _ = os.path.splitext(configFname)
        basename = os.path.basename(baseAbsFilename)
        # Chris writes this file when completed walking through species data
        self.walkedArchiveFname = baseAbsFilename + LMFormat.LOG.ext
        self.name = '{}_{}'.format(self.__class__.__name__.lower(), basename)       
        # Config
        if configFname is not None and os.path.exists(configFname):
            self.cfg = Config(siteFn=configFname)
        else:
            raise LMError(currargs='Missing config file {}'.format(configFname))
        
        # JSON or ini based configuration
        if jsonFname is not None:
            raise LMError('JSON Walken is not yet implemented')
        
        # Optionally use parent process Database connection
        if scribe is not None:
            self.log = scribe.log
            self._scribe = scribe
        else:
            self.log = ScriptLogger(self.name)
            try:
                self._scribe = BorgScribe(self.log)
                success = self._scribe.openConnections()
            except Exception, e:
                raise LMError(currargs='Exception opening database', prevargs=e.args)
            else:
                if not success:
                    raise LMError(currargs='Failed to open database')
                else:
                    self.log.info('{} opened databases'.format(self.name))
        
        # Global PAM Matrix for each scenario
        self.globalPAMs = {}

    # .............................
    def initializeMe(self):
        """
        @summary: Sets objects and parameters for workflow on this object
        """
        self.moreDataToProcess = False
        (self.userId, 
         self.archiveName, 
         self.priority, 
         self.boompath, 
         self.weaponOfChoice,
         self._obsoleteTime, 
         self.epsg, 
         self.minPoints, 
         self.algs, 
         self.mdlScen, 
         self.prjScens, 
         self.model_mask_base,
         self.boomGridset, 
         self.intersectParams, 
         self.assemblePams, 
         self.compute_pam_stats, 
         self.compute_mcpa, 
         self.num_permutations) = self._getConfiguredObjects()
        
        self.columnMeta = None
        try:
            self.columnMeta = self.weaponOfChoice.occParser.columnMeta
        except:
            pass
        # One Global PAM for each scenario
        # TODO: Allow assemblePams on RollingPAM?
        if self.assemblePams:
            for alg in self.algs:
                for prjscen in self.prjScens:
                    pamcode = '{}_{}'.format(prjscen.code, alg.code)
                    self.globalPAMs[pamcode] = self.boomGridset.getPAMForCodes(
                        prjscen.gcmCode, prjscen.altpredCode, prjscen.dateCode,
                        alg.code)

    # ...............................................
    def moveToStart(self):
        self.weaponOfChoice.moveToStart()
      
    # ...............................................
    def saveNextStart(self, fail=False):
        self.weaponOfChoice.saveNextStart(fail=fail)
   
    # ...............................................
    @property
    def currRecnum(self):
        return self.weaponOfChoice.currRecnum

    # ...............................................
    @property
    def nextStart(self):
        return self.weaponOfChoice.nextStart
    
    # ...............................................
    @property
    def complete(self):
        return self.weaponOfChoice.complete

    # ...............................................
    def _getVarValue(self, var):
        try:
            var = int(var)
        except:
            try:
                var = float(var)
            except:
                pass
        return var

    # ...............................................
    def _getBoomOrDefault(self, varname, defaultValue=None, isList=False, isBool=False):
        var = None
        # Get value from BOOM or default config file
        if isBool:
            try:
                var = self.cfg.getboolean(SERVER_BOOM_HEADING, varname)
            except:
                try:
                    var = self.cfg.getboolean(SERVER_PIPELINE_HEADING, varname)
                except:
                    pass
        else:
            try:
                var = self.cfg.get(SERVER_BOOM_HEADING, varname)
            except:
                try:
                    var = self.cfg.get(SERVER_PIPELINE_HEADING, varname)
                except:
                    pass
        # Take default if present
        if var is None:
            if defaultValue is not None:
                var = defaultValue
        # or interpret value
        elif not isBool:
            if not isList:
                var = self._getVarValue(var)
            else:
                try:
                    tmplist = [v.strip() for v in var.split(',')]
                    var = []
                except:
                    raise LMError('Failed to split variables on \',\'')
                for v in tmplist:
                    v = self._getVarValue(v)
                    var.append(v)
        return var

    # .............................................................................
    def _getOccWeaponOfChoice(self, userId, archiveName, epsg, boompath):
        # Get datasource and optional taxonomy source
        datasource = self._getBoomOrDefault(BoomKeys.DATA_SOURCE)
        try:
            taxonSourceName = TAXONOMIC_SOURCE[datasource]['name']
        except:
            taxonSourceName = None
                       
        # Expiration date for retrieved species data 
        expDate = dt.DateTime(self._getBoomOrDefault(BoomKeys.OCC_EXP_YEAR), 
                              self._getBoomOrDefault(BoomKeys.OCC_EXP_MONTH), 
                              self._getBoomOrDefault(BoomKeys.OCC_EXP_DAY)).mjd

        occname = self._getBoomOrDefault(BoomKeys.OCC_DATA_NAME)
        occdir = self._getBoomOrDefault(BoomKeys.OCC_DATA_DIR)
        occ_delimiter = self._getBoomOrDefault(BoomKeys.OCC_DATA_DELIMITER) 
        if occ_delimiter != ',':
            occ_delimiter = GBIF.DATA_DUMP_DELIMITER
        occ_csv_fname, occ_meta_fname, self.moreDataToProcess = self._findData(
            occname, occdir, boompath)
                
        # Copy public data to user space
        # TODO: Handle taxonomy, useGBIFTaxonomy=??
        if datasource == SpeciesDatasource.EXISTING:
            occIdFname = self._getBoomOrDefault(BoomKeys.OCC_ID_FILENAME)
            weaponOfChoice = ExistingWoC(self._scribe, userId, archiveName, epsg,
                                         expDate, occIdFname, logger=self.log)
           
        else:
            # Handle GBIF data, taxon and provider lookup data
            useGBIFTaxonIds = False
            gbifProvFile = None
            if datasource == SpeciesDatasource.GBIF:
                useGBIFTaxonIds = True
                gbifProv = self._getBoomOrDefault(BoomKeys.GBIF_PROVIDER_FILENAME)
                gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
            weaponOfChoice = UserWoC(self._scribe, userId, archiveName, epsg,
                                     expDate, occ_csv_fname, occ_meta_fname, 
                                     occ_delimiter, logger=self.log, 
                                     processType=ProcessType.USER_TAXA_OCCURRENCE,
                                     providerFname=gbifProvFile,
                                     useGBIFTaxonomy=useGBIFTaxonIds,
                                     taxonSourceName=taxonSourceName)
           
        weaponOfChoice.initializeMe()
           
        return weaponOfChoice, expDate

    # .............................................................................
    def _findData(self, occname, occdir, boompath):
        moreDataToProcess = False
        occ_csv_fname = occ_meta_fname = None

        if occname is not None:        
            # Complete base filename
            if not occname.endswith(LMFormat.CSV.ext):
                occ_csv = occname + LMFormat.CSV.ext 
                occ_meta = occname + LMFormat.JSON.ext
            else:
                occ_csv = occname
                occ_meta = os.path.splitext(occ_csv)[0] + LMFormat.JSON.ext
    
            
            installed_csv = os.path.join(SPECIES_DATA_PATH, occ_csv)
            user_csv = os.path.join(boompath, occ_csv)
            
            # Look for data in relative path, installed path, user path
            if os.path.exists(occ_csv):
                occ_csv_fname = occ_csv
                occ_meta_fname = occ_meta
            elif os.path.exists(installed_csv):
                occ_csv_fname = installed_csv
                occ_meta_fname = os.path.join(SPECIES_DATA_PATH, occ_meta)
            elif os.path.exists(user_csv):
                occ_csv_fname = user_csv
                occ_meta_fname = os.path.join(boompath, occ_meta)
            else:
                raise LMError("""
                 Species file {} does not exist as relative path, or in public data 
                 directory {} or in user directory {}"""
                 .format(occname, SPECIES_DATA_PATH, boompath))
                        
        # TODO: Add 'OCC_DATA_DIR' as parameter for individual CSV files
        #       in a directory, one per species 
        #       (for LmServer.tools.occwoc.TinyBubblesWoC)
        elif occdir is not None:
            installed_dir = os.path.join(SPECIES_DATA_PATH, occdir)
            user_dir = os.path.join(boompath, occdir)
            # Check for directory location - either absolute or relative path
            if os.path.isdir(occdir):
                pass
            #   or in User top data directory
            elif os.path.isdir(installed_dir):
                occdir = installed_dir
            #   or in installation data directory
            elif os.path.isdir(user_dir):
                occdir = user_dir
            else:
                raise LMError("""Failed to find file {} in relative location 
                                 or in user dir {} or installation dir {}"""
                              .format(occname, boompath, SPECIES_DATA_PATH))
            occ_meta_fname = occdir + LMFormat.JSON.ext
            
            fnames = glob.glob(os.path.join(occdir, '*' + LMFormat.CSV.ext))
            if len(fnames) > 0:
                occ_csv_fname = fnames[0]
                if len(fnames) > 1:
                    moreDataToProcess = True
            else:
                raise LMError("""Failed to find csv file in relative directory {}
                                 or in user path {} or installation path {}"""
                              .format(occdir, boompath, SPECIES_DATA_PATH))
                
        if not os.path.exists(occ_meta_fname):
            raise LMError('Missing metadata file {}'.format(occ_meta_fname))
                         
        return occ_csv_fname, occ_meta_fname, moreDataToProcess


    # .............................................................................
    def _getAlgorithm(self, algHeading):
        """
        @note: Returns configured algorithm
        """
        acode =  self.cfg.get(algHeading, BoomKeys.ALG_CODE)
        alg = Algorithm(acode)
        alg.fillWithDefaults()
        inputs = {}
        # override defaults with any option specified
        algoptions = self.cfg.getoptions(algHeading)
        for name in algoptions:
            pname, ptype = alg.findParamNameType(name)
            if pname is not None:
                if ptype == IntType:
                    val = self.cfg.getint(algHeading, pname)
                elif ptype == FloatType:
                    val = self.cfg.getfloat(algHeading, pname)
                else:
                    val = self.cfg.get(algHeading, pname)
                    # Some algorithms(mask) may have a parameter indicating a layer,
                    # if so, add name to parameters and object to inputs
                    if acode == ECOREGION_MASK_METHOD and pname == 'region':
                        inputs[pname] = val
                alg.setParameter(pname, val)
        if inputs:
            alg.setInputs(inputs)
        return alg

    # .............................................................................
    def _getAlgorithms(self, sectionPrefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX):
        algs = []
        defaultAlgs = []
        # Get algorithms for SDM modeling
        sections = self.cfg.getsections(sectionPrefix)
        for algHeading in sections:
            alg = self._getAlgorithm(algHeading)
            
            if algHeading.endswith(SERVER_DEFAULT_HEADING_POSTFIX):
                defaultAlgs.append(alg)
            else:
                algs.append(alg)
        if len(algs) == 0:
            algs = defaultAlgs
        return algs

    # .............................................................................
    def _getProjParams(self, userId, epsg):
        prjScens = []      
        mdlScen = None
        model_mask_base = None
        
        # Get environmental data model and projection scenarios
        mdlScenCode = self._getBoomOrDefault(BoomKeys.SCENARIO_PACKAGE_MODEL_SCENARIO)
        prjScenCodes = self._getBoomOrDefault(
            BoomKeys.SCENARIO_PACKAGE_PROJECTION_SCENARIOS, isList=True)
        scenPkgs = self._scribe.getScenPackagesForUserCodes(userId, prjScenCodes, 
                                                            fillLayers=True)
        if not scenPkgs:
            scenPkgs = self._scribe.getScenPackagesForUserCodes(PUBLIC_USER, 
                                                                prjScenCodes, 
                                                                fillLayers=True)
        if scenPkgs:
            scenPkg = scenPkgs[0]
            mdlScen = scenPkg.getScenario(code=mdlScenCode)
            for pcode in prjScenCodes:
                prjScens.append(scenPkg.getScenario(code=pcode))
        else:
            raise LMError('Failed to retrieve ScenPackage for scenarios {}'
                          .format(prjScenCodes))
           
        # Put params into SDMProject metadata
        maskAlgList = self._getAlgorithms(sectionPrefix=SERVER_SDM_MASK_HEADING_PREFIX)
        if len(maskAlgList) > 1:
            raise LMError(currargs='Unable to handle > 1 SDM pre-process')
        elif len(maskAlgList) == 1:
            sdmMaskAlg = maskAlgList[0]
           
            # TODO: Handle if there is more than one input layer?
            maskData = sdmMaskAlg.getInputs()
            if maskData and len(maskData) > 1:
                raise LMError(currargs='Unable to process > 1 input SDM mask layer')
            
            lyrname = maskData.values()[0]
            
            # Get processing parameters for masking
            proc_params = {
                  PRE_PROCESS_KEY : {
                     MASK_KEY : {
                        MASK_LAYER_KEY : lyrname,
                        MASK_LAYER_NAME_KEY : sdmMaskAlg.getParameterValue('region'),
                        CODE_KEY : ECOREGION_MASK_METHOD,
                        BUFFER_KEY : sdmMaskAlg.getParameterValue(BUFFER_KEY)
                     }}}
                            
            mask_layer_name = proc_params[PRE_PROCESS_KEY][MASK_KEY][MASK_LAYER_KEY]
            mask_layer = self._scribe.getLayer(userId=userId, 
                                    lyrName=mask_layer_name, epsg=epsg)
            if mask_layer is None:
                raise LMError('Failed to retrieve layer {} for user {}'
                              .format(mask_layer_name, userId))
            model_mask_base = {
                RegistryKey.REGION_LAYER_PATH : mask_layer.getDLocation(),
                RegistryKey.BUFFER : proc_params[PRE_PROCESS_KEY][MASK_KEY][
                    BUFFER_KEY],
                RegistryKey.METHOD : MaskMethod.HULL_REGION_INTERSECT}
           
        return (mdlScen, prjScens, model_mask_base)  

    # .............................................................................
    def _getGlobalPamObjects(self, userId, archiveName, epsg):
        # Get existing intersect grid, gridset and parameters for Global PAM
        gridname = self._getBoomOrDefault(BoomKeys.GRID_NAME)
        if gridname:
            intersectGrid = self._scribe.getShapeGrid(userId=userId, lyrName=gridname, 
                                                     epsg=epsg)
            if intersectGrid is None:
                raise LMError('Failed to retrieve Shapegrid for intersection {}'
                              .format(gridname))
        
        # Global PAM and Scenario GRIM for each scenario
        boomGridset = self._scribe.getGridset(name=archiveName, userId=userId, 
                                              fillMatrices=True)
        if boomGridset is None:
            raise LMError('Failed to retrieve Gridset for shapegrid {}, user {}'
                          .format(gridname, userId))
        boomGridset.setMatrixProcessType(ProcessType.CONCATENATE_MATRICES, 
                                         matrixTypes=[MatrixType.PAM, 
                                                      MatrixType.ROLLING_PAM, 
                                                      MatrixType.GRIM])
        intersectParams = {
           MatrixColumn.INTERSECT_PARAM_FILTER_STRING: 
              self._getBoomOrDefault(BoomKeys.INTERSECT_FILTER_STRING),
           MatrixColumn.INTERSECT_PARAM_VAL_NAME: 
              self._getBoomOrDefault(BoomKeys.INTERSECT_VAL_NAME),
           MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: 
              self._getBoomOrDefault(BoomKeys.INTERSECT_MIN_PRESENCE),
           MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: 
              self._getBoomOrDefault(BoomKeys.INTERSECT_MAX_PRESENCE),
           MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: 
              self._getBoomOrDefault(BoomKeys.INTERSECT_MIN_PERCENT)}

        # TODO: remove this from gridset and add to each SDMProject
        maskAlgList = self._getAlgorithms(sectionPrefix=SERVER_SDM_MASK_HEADING_PREFIX)
        if len(maskAlgList) == 1:
            sdmMaskAlg = maskAlgList[0]
            # TODO: Handle if there is more than one input layer
            if len(sdmMaskAlg.getInputs()) > 1:
                raise LMError(currargs='Unable to process > 1 input SDM mask layer')
            for inputKey, lyrname in sdmMaskAlg.getInputs().iteritems():
                sdmMaskInputLayer = self._scribe.getLayer(userId=userId, 
                                                          lyrName=lyrname, epsg=epsg)
                sdmMaskAlg.setInput(inputKey, sdmMaskInputLayer)
            
            procParams = {
               PROCESSING_KEY : {
                  PRE_PROCESS_KEY : {
                     MASK_KEY : {
                        MASK_LAYER_KEY : sdmMaskInputLayer,
                        MASK_LAYER_NAME_KEY : sdmMaskAlg.getParameterValue('region'),
                        CODE_KEY : ECOREGION_MASK_METHOD,
                        BUFFER_KEY : sdmMaskAlg.getParameterValue(BUFFER_KEY)
                     }
                  }
               }
            }
            # TODO: AMS - If this will be saved, may need to remove the mask layer object
            boomGridset.addGrdMetadata(procParams)
      
        return (boomGridset, intersectParams)  

    # .............................................................................
    @classmethod
    def getConfig(cls, configFname):
        """
        @summary: Get user, archive, path, and configuration object 
        """
        if configFname is None or not os.path.exists(configFname):
            raise LMError(currargs='Missing config file {}'.format(configFname))
        config = Config(siteFn=configFname)
        return config
         
    # .............................................................................
    def _getConfiguredObjects(self):
        """
        @summary: Get configured string values and any corresponding db objects 
        @TODO: Make all archive/default config keys consistent
        """
        userId = self._getBoomOrDefault(BoomKeys.ARCHIVE_USER, defaultValue=PUBLIC_USER)
        archiveName = self._getBoomOrDefault(BoomKeys.ARCHIVE_NAME)
        archivePriority = self._getBoomOrDefault(BoomKeys.ARCHIVE_PRIORITY, 
                                                 defaultValue=Priority.NORMAL)
        # Get user-archive configuration file
        if userId is None or archiveName is None:
            raise LMError(currargs='Missing ARCHIVE_USER or ARCHIVE_NAME in {}'
                          .format(self.cfg.configFiles))
        earl = EarlJr()
        boompath = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
        epsg = self._getBoomOrDefault(BoomKeys.EPSG, defaultValue=DEFAULT_EPSG)
        
        # Species parser/puller
        weaponOfChoice, expDate = self._getOccWeaponOfChoice(userId, archiveName, 
                                                             epsg, boompath)
        # SDM inputs
        minPoints = self._getBoomOrDefault(BoomKeys.POINT_COUNT_MIN)
        algorithms = self._getAlgorithms(sectionPrefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX)
        
        (mdlScen, prjScens, model_mask_base) = self._getProjParams(userId, epsg)
        # Global PAM inputs
        (boomGridset, intersectParams) = self._getGlobalPamObjects(userId, 
                                                              archiveName, epsg)
        assemblePams = self._getBoomOrDefault(BoomKeys.ASSEMBLE_PAMS, isBool=True)
        compute_pam_stats = self._getBoomOrDefault(BoomKeys.COMPUTE_PAM_STATS, 
                                                   isBool=True)
        compute_mcpa = self._getBoomOrDefault(BoomKeys.COMPUTE_MCPA, 
                                              isBool=True)
        num_permutations = self._getBoomOrDefault(BoomKeys.NUM_PERMUTATIONS,
                                                  defaultValue=DEFAULT_NUM_PERMUTATIONS)
        
        return (userId, archiveName, archivePriority, boompath, weaponOfChoice, expDate,
                epsg, minPoints, algorithms, mdlScen, prjScens, model_mask_base, 
                boomGridset, intersectParams, assemblePams, compute_pam_stats, 
                compute_mcpa, num_permutations)  

    # ...............................
    def _getJSONObjects(self):
        """
        @summary: Get provided values from JSON and any corresponding db objects 
        {
        OccDriver: datasource,
        Occdata: ( filename of taxonids, csv of datablocks, etc each handled differently)
        Algs: [algs/params]
        ScenPkg: mdlscen
        [prjscens]
        """
        pass
    
    # ...............................
    def startWalken(self, workdir):
        """
        @summary: Walks a list of Lifemapper objects for computation
        @return: Single-species MFChain (spud), dictionary of 
                 scenarioCode: PAV filename for input into multi-species
                 MFChains (potatoInputs)
        """
        squid = None
        spudRules = []
        index_pavs_document_filename = None
        gsid = 0
        currtime = dt.gmt().mjd
        
        try:
            gsid = self.boomGridset.getId()
        except:
            self.log.warning('Missing self.boomGridset id!!')
            
        # WeaponOfChoice.getOne returns the next occurrenceset for species 
        # input data. If it is new, failed, or outdated, write the raw  
        # data and update the rawDlocation.
        occ = self.weaponOfChoice.getOne()
        if self.weaponOfChoice.finishedInput:
            self._writeDoneWalkenFile()
        if occ:
            squid = occ.squid
            
            occ_work_dir = os.path.join(workdir, 'occ_{}'.format(occ.getId()))
            sweep_config = ParameterSweepConfiguration(work_dir=occ_work_dir)
            
            # If we have enough points to model
            if occ.queryCount >= self.minPoints:
                self.log.info('   Will compute for Grid {}:'.format(gsid))
                for alg in self.algs:
                    prjs = []
                    mtxcols = []
                    for prj_scen in self.prjScens:
                        pamcode = '{}_{}'.format(prj_scen.code, alg.code) 
                        prj = self._findOrInsertSDMProject(
                            occ, alg, prj_scen, dt.gmt().mjd)
                        if prj is not None:
                            prjs.append(prj)
                            if self.assemblePams:
                                mtx = self.globalPAMs[pamcode]
                                mtxcol = self._findOrInsertIntersect(
                                    prj, mtx, currtime)
                                if mtxcol is not None:
                                    mtxcols.append(mtxcol)
                    doSDM = self._doComputeSDM(occ, prjs, mtxcols)
            
                    if doSDM:
                        # Add SDM commands for the algorithm
                        self._fill_sweep_config(
                            sweep_config, workdir, alg, occ, prjs, mtxcols)
                
            else:
                doSDM = self._doComputeSDM(occ, [], [])
                if doSDM:
                    # Only add the occurrence set to the sweep config.  Empty
                    #    lists for projections and PAVs will omit those objects
                    self._fill_sweep_config(
                        sweep_config, workdir, None, occ, [], [])

            # Only add rules if we have something to compute
            num_comps = sum([
                len(sweep_config.occurrence_sets),
                len(sweep_config.models),
                len(sweep_config.projections),
                len(sweep_config.pavs)
                ])
            if num_comps > 0:
                # Write the sweep config file
                species_config_filename = os.path.join(
                    os.path.dirname(occ.getDLocation()),
                    'species_config_{}{}'.format(
                        occ.getId(), LMFormat.JSON.ext))
                sweep_config.save_config(species_config_filename)
    
                # Add sweep rule
                param_sweep_cmd = SpeciesParameterSweepCommand(
                    species_config_filename, sweep_config.get_input_files(),
                    sweep_config.get_output_files())
                spudRules.append(param_sweep_cmd.getMakeflowRule())
    
                # Add stockpile rule
                stockpile_success_filename = os.path.join(
                    occ_work_dir, 'occ_{}stockpile.success'.format(
                        occ.getId()))
                stockpile_cmd = MultiStockpileCommand(
                    sweep_config.stockpile_filename,
                    stockpile_success_filename)
                spudRules.append(
                    stockpile_cmd.getMakeflowRule(local=True))
    
                # Add multi-index rule if we added PAVs
                if len(sweep_config.pavs) > 0:
                    index_pavs_document_filename = os.path.join(
                        occ_work_dir, 'solr_pavs_post{}'.format(
                            LMFormat.XML.ext))
                    index_cmd = MultiIndexPAVCommand(
                        sweep_config.pavs_filename,
                        index_pavs_document_filename)
                    spudRules.append(index_cmd.getMakeflowRule(local=True))

            # TODO: Add metrics / snippets processing
        return squid, spudRules, index_pavs_document_filename
    
    # ...............................
    def _doComputeSDM(self, occ, prjs, mtxcols):
        doSDM = self._doResetOcc(occ.status, occ.statusModTime, 
                                 occ.getDLocation(), occ.getRawDLocation())
        for o in prjs:
            if doSDM:
                break
            else:
                doSDM = self._doReset(o.status, o.statusModTime)
        for o in mtxcols:
            if doSDM:
                break
            else:
                doSDM = self._doReset(o.status, o.statusModTime)
        return doSDM
      
    # ...............................
    def _fill_sweep_config(self, sweep_config, work_dir, alg, occ, prjs, mtxcols):
    #def _getSweepConfig(self, workdir, alg, occ, prjs, mtxcols):
        #occ_work_dir = os.path.join(workdir, 'occ_{}'.format(occ.getId()))
        #sweep_config = ParameterSweepConfiguration(work_dir=occ_work_dir)
        
        # Add occurrence set if there is a process to perform
        if occ.processType is not None:
            rawmeta_dloc = occ.getRawDLocation() + LMFormat.JSON.ext
            # TODO: replace metadata filename with metadata dict in self.columnMeta?
            sweep_config.add_occurrence_set(
                occ.processType, occ.getId(), occ.getRawDLocation(),
                occ.getDLocation(), occ.getDLocation(largeFile=True),
                POINT_COUNT_MAX, metadata=rawmeta_dloc)
#                 POINT_COUNT_MAX, metadata=occ.rawMetaDLocation)
            
        for prj in prjs:
            if self.model_mask_base is not None:
                model_mask = self.model_mask_base.copy()
                model_mask[
                    RegistryKey.OCCURRENCE_SET_PATH
                    ] = prj.occurrenceSet.getDLocation()
                projection_mask = {
                    RegistryKey.METHOD : MaskMethod.BLANK_MASK,
                    RegistryKey.TEMPLATE_LAYER_PATH : prj.projScenario.layers[
                        0].getDLocation()
                }
            else:
                model_mask = None
                projection_mask = None
            
            scale_parameters = multiplier = None
            if prj.isATT():
                scale_parameters = (SCALE_PROJECTION_MINIMUM,
                                    SCALE_PROJECTION_MAXIMUM)
                #TODO: This should be in config somewhere
                multiplier = None

            sweep_config.add_projection(
                prj.processType, prj.getId(), prj.getOccurrenceSetId(),
                prj.occurrenceSet.getDLocation(),
                alg, prj.modelScenario, prj.projScenario,
                prj.getDLocation(), prj.getProjPackageFilename(),
                model_mask=model_mask,
                projection_mask=projection_mask,
                scale_parameters=scale_parameters,
                multiplier=multiplier)

        for mtxcol in mtxcols:
            pav_filename = os.path.join(
                work_dir, 'pavs', 'pav_{}{}'.format(
                    mtxcol.getId(), LMFormat.MATRIX.ext))
            sweep_config.add_pav_intersect(
                 mtxcol.shapegrid.getDLocation(),
                 mtxcol.getId(), prj.getId(), pav_filename,
                 prj.squid,
                 mtxcol.intersectParams[
                     mtxcol.INTERSECT_PARAM_MIN_PRESENCE],
                 mtxcol.intersectParams[
                     mtxcol.INTERSECT_PARAM_MAX_PRESENCE],
                 mtxcol.intersectParams[
                     mtxcol.INTERSECT_PARAM_MIN_PERCENT])
        return sweep_config

    # ...............................
    def stopWalken(self):
        """
        @summary: Walks a list of Lifemapper objects for computation
        """
        if not self.weaponOfChoice.complete:
            self.log.info('Christopher, stop walken')
            self.log.info('Saving next start {} ...'.format(self.nextStart))
            self.saveNextStart()
            self.weaponOfChoice.close()
        else:
            self.log.info('Christopher is done walken')
      
    # ...............................................
    def _findOrInsertIntersect(self, prj, mtx, currtime):
        """
        @summary: Initialize model, projections for inputs/algorithm.
        """
        mtxcol = None
        if prj is not None:
            # TODO: Save processType into the DB??
            if LMFormat.isGDAL(driver=prj.dataFormat):
                ptype = ProcessType.INTERSECT_RASTER
            else:
                ptype = ProcessType.INTERSECT_VECTOR
            
            tmpCol = MatrixColumn(None, mtx.getId(), self.userId, 
                           layer=prj, shapegrid=self.boomGridset.getShapegrid(), 
                           intersectParams=self.intersectParams, 
                           squid=prj.squid, ident=prj.ident,
                           processType=ptype, metadata={}, matrixColumnId=None, 
                           postToSolr=self.assemblePams,
                           status=JobStatus.GENERAL, statusModTime=currtime)
            mtxcol = self._scribe.findOrInsertMatrixColumn(tmpCol)
            if mtxcol is not None:
                self.log.debug('Found/inserted MatrixColumn {}'.format(mtxcol.getId()))
                
                # Reset processType, shapegrid obj (not in db)
                mtxcol.processType = ptype
                mtxcol.shapegrid = self.boomGridset.getShapegrid()
                
        return mtxcol

    # ...............................................
    def _doReset(self, status, statusModTime):
        willCompute = False
        if (JobStatus.incomplete(status) or
            JobStatus.failed(status) or 
            (status == JobStatus.COMPLETE and 
             statusModTime < self.weaponOfChoice.expirationDate)):
            willCompute = True
        return willCompute

# ...............................................
    def _doResetOcc(self, status, statusModTime, dlocation, rawDataLocation):
        willCompute = False
        noRawData = rawDataLocation is None or not os.path.exists(rawDataLocation)
        noCompleteData = dlocation is None or not os.path.exists(dlocation)
        obsoleteData = statusModTime > 0 and statusModTime < self._obsoleteTime
        if (JobStatus.incomplete(status) or
             JobStatus.failed(status) or
              # waiting with missing data
             (JobStatus.waiting(status) and noRawData) or 
              # out-of-date
             (status == JobStatus.COMPLETE and noCompleteData or obsoleteData)):
            willCompute = True
        return willCompute

    # ...............................................
    def _findOrInsertSDMProject(self, occ, alg, prjscen, currtime):
        """
        @summary: Iterates through all input combinations to create or reset
                  SDMProjections for the given occurrenceset.
        @param occ: OccurrenceSet for which to initialize or rollback all 
                    dependent projections
        """
        prj = None
        if occ is not None:
            tmpPrj = SDMProjection(occ, alg, self.mdlScen, prjscen, 
                           dataFormat=LMFormat.GTIFF.driver,
                           status=JobStatus.GENERAL, statusModTime=currtime)
            prj = self._scribe.findOrInsertSDMProject(tmpPrj)
            if prj is not None:
                self.log.debug('Found/inserted SDMProject {}'.format(prj.getId()))
                # Fill in projection with input scenario layers, masks
                prj._modelScenario = self.mdlScen
                prj._projScenario = prjscen
        return prj


    # ...............................................
    def _writeDoneWalkenFile(self):
        try:
            f = open(self.walkedArchiveFname, 'w')
            f.write('# Completed walking species input {}\n'
                    .format(self.weaponOfChoice.inputFilename))
            f.write('# From config file {}\n'
                    .format(self.configFname))
            f.write('# Full logs in {}\n'
                    .format(self.log.baseFilename))            
            f.close()
        except:
            self.log.error('Failed to write doneWalken file {} for config {}'
                           .format(self.walkedArchiveFname, self.configFname))


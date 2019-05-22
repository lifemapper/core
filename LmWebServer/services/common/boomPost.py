"""This module processes BOOM style HTTP POST requests

Todo:
    * Process grids
    * Constants instead of strings
    * Testing
    * Documentation
"""
import cherrypy
from ConfigParser import ConfigParser
import json
from mx.DateTime import gmt
import os
import random

from LmCommon.common.lmconstants import (BoomKeys,
    HTTPStatus,LMFormat, SERVER_BOOM_HEADING, SERVER_SDM_MASK_HEADING_PREFIX,
    SERVER_SDM_ALGORITHM_HEADING_PREFIX)
#from LmDbServer.boom.boominput import ArchiveFiller
#from LmDbServer.boom.initboom import initBoom
from LmDbServer.boom.initWorkflow import BOOMFiller
from LmDbServer.common.lmconstants import SpeciesDatasource
from LmDbServer.tools.catalogScenPkg import SPFiller
from LmServer.common.lmconstants import TEMP_PATH, Priority, ENV_DATA_PATH,\
    ARCHIVE_PATH
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.base.lmobj import LmHTTPError
from LmWebServer.common.lmconstants import APIPostKeys
from LmCommon.common.apiquery import IdigbioAPI

# .............................................................................
class BoomPoster(object):
    """This class processes BOOM-style POST requests

    This class processes BOOM-stype POST requests and produces a BOOM input
    config file to be used for creating a BOOM.

    Todo:
        * Make case-insensitive
        * We need BOOM config constants
    """
    # ................................
    def __init__(self, userId, userEmail, reqJson, scribe):
        """
        @todo: Make this more generic
        """
        self.has_tree = False
        self.scribe = scribe
        self.userId = userId
        self.config = ConfigParser()
        self.config.add_section(SERVER_BOOM_HEADING)
        self.config.set(SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_USER, userId)
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_USER_EMAIL, userEmail)
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_PRIORITY, Priority.REQUESTED)
        
        # Look for an archive name
        if APIPostKeys.ARCHIVE_NAME in reqJson.keys():
            archiveName = reqJson[APIPostKeys.ARCHIVE_NAME].replace(' ', '_')
        else:
            archiveName = '{}_{}'.format(userId, gmt().mjd)
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_NAME, archiveName)
        
        # NOTE: For this version, we will follow what is available from the 
        #             .ini file and not let it get too fancy / complicated.  We
        #             can add functionality later to connect different parts as 
        #             needed but for now they will either be present or not
        
        # Look for occurrence set specification at top level
        occSec = self._get_json_section(reqJson, APIPostKeys.OCCURRENCE)
        if occSec:
            self._process_occurrence_sets(occSec)
        
        # Look for scenario package information at top level
        scnSec = self._get_json_section(reqJson, APIPostKeys.SCENARIO_PACKAGE)
        if scnSec:
            self._process_scenario_package(scnSec)
            
        # Look for global pam information
        globalPamSec = self._get_json_section(reqJson, APIPostKeys.GLOBAL_PAM)
        if globalPamSec:
            self._process_global_pam(globalPamSec)

        # Look for tree information
        treeSec = self._get_json_section(reqJson, APIPostKeys.TREE)
        if treeSec:
            self._process_tree(treeSec)
            
        # Look for SDM options (masks / scaling / etc)
        sdmSec = self._get_json_section(reqJson, APIPostKeys.SDM)
        if sdmSec:
            self._process_sdm(sdmSec)
        
        # PAM stats
        pamStatsSec = self._get_json_section(reqJson, APIPostKeys.PAM_STATS)
        if pamStatsSec:
            self._process_pam_stats(pamStatsSec)
            
        # MCPA
        mcpaSec = self._get_json_section(reqJson, APIPostKeys.MCPA)
        if mcpaSec:
            self._process_mcpa(mcpaSec)
        
        
        # TODO: Masks
        # TODO: Pre / post processing (scaling)
        # TODO: Randomizations

        
    
    # ................................
    def _get_json_section(self, json_doc, section_key):
        """Attempts to retrieve a section for a JSON document.

        Attempts to retrieve a section from the JSON document in a
            case-insensitive way.

        Args:
            json_doc : The JSON document to search
            section_key : The section to return

        Returns:
            The JSON section in the document or None if not found
        """
        search_key = section_key.replace(' ', '').replace('_', '').lower()
        for key in json_doc.keys():
            if key.lower().replace(' ', '').replace('_', '') == search_key:
                return json_doc[key]
        return None

    # ................................
    def _get_temp_filename(self, ext, prefix='file_'):
        """Returns a name for a temporary file.

        Args:
            ext : The file extension to use for this temporary file.
        """
        return os.path.join(
            TEMP_PATH, '{}{}{}'.format(prefix, random.randint(0, 100000), ext))
    
    # ................................
    def _process_global_pam(self, global_pam_json):
        """Process global pam information from request.
        
        Process Global PAM information from request including shapegrid and
        intersect parameters

        Args:
            global_pam_json : JSON chunk of Global PAM information.

        Note:
            * This version is somewhat limited.  Must provide shapegrid
                parameters and only one set of intersect parameters.

        Todo:
            * Shapegrid EPSG?
            * Shapegrid map units?
            * Expand to other intersect methods
        """
        # Process shapegrid
        shapegrid_json = global_pam_json[APIPostKeys.SHAPEGRID]
        shapegrid_bbox = [
            shapegrid_json[APIPostKeys.MIN_X],
            shapegrid_json[APIPostKeys.MIN_Y],
            shapegrid_json[APIPostKeys.MAX_X],
            shapegrid_json[APIPostKeys.MAX_Y]]
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.GRID_BBOX, shapegrid_bbox)
        shapegrid_name = shapegrid_json[APIPostKeys.NAME]
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.GRID_NAME, shapegrid_name)
        #sgEpsg = sg['epsg']
        shapegrid_cell_sides = shapegrid_json[APIPostKeys.CELL_SIDES]
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.GRID_NUM_SIDES, shapegrid_cell_sides)
        #sgMapUnits = sg['map_units']
        shapegrid_resolution = shapegrid_json[APIPostKeys.RESOLUTION]
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.GRID_CELL_SIZE, shapegrid_resolution)
        
        # Process intersect parameters
        intersect_parameters = global_pam_json[APIPostKeys.INTERSECT_PARAMETERS]
        min_presence = intersect_parameters[APIPostKeys.MIN_PRESENCE]
        max_presence = intersect_parameters[APIPostKeys.MAX_PRESENCE]
        value_name = intersect_parameters[APIPostKeys.VALUE_NAME]
        min_percent = intersect_parameters[APIPostKeys.MIN_PERCENT]
        
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.INTERSECT_FILTER_STRING, None)
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.INTERSECT_VAL_NAME, value_name)
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.INTERSECT_MIN_PERCENT, min_percent)
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.INTERSECT_MIN_PRESENCE, min_presence)
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.INTERSECT_MAX_PRESENCE, max_presence)
    
    # ................................
    def _process_mcpa(self, mcpa_json):
        """Process MCPA information from the request.

        Args:
            mcpa_json : JSON chunk of MCPA information

        Todo:
            * Fill in iterations
        """
        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.BIOGEO_HYPOTHESES_LAYERS,
            mcpa_json['hypotheses_package_name'])
        should_compute = int(self.has_tree)

        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.COMPUTE_MCPA, should_compute)

    # ................................
    def _process_occurrence_sets(self, occ_json):
        """Processes occurrence sets in POST request.

        Args:
            occ_json : JSON chunk of occurrence information
        """
        if APIPostKeys.OCCURRENCE_IDS in occ_json.keys():
            occ_filename = self._get_temp_filename(
                LMFormat.TXT.ext, prefix='user_existing_occ_')
            with open(occ_filename, 'w') as out_f:
                for occ_id in occ_json[APIPostKeys.OCCURRENCE_IDS]:
                    out_f.write('{}\n'.format(occ_id))
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.OCC_ID_FILENAME,
                occ_filename)
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.DATA_SOURCE,
                SpeciesDatasource.EXISTING)
        elif APIPostKeys.TAXON_IDS in occ_json.keys():
            tax_id_filename = self._get_temp_filename(
                LMFormat.TXT.ext, prefix='user_taxon_ids_')
            with open(tax_id_filename, 'w') as out_f:
                for tax_id in occ_json[APIPostKeys.TAXON_IDS]:
                    out_f.write('{}\n'.format(tax_id))
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.TAXON_ID_FILENAME,
                tax_id_filename)
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.DATA_SOURCE,
                SpeciesDatasource.TAXON_IDS)
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_DELIMITER,
                IdigbioAPI.DELIMITER)
        elif APIPostKeys.TAXON_NAMES in occ_json.keys():
            tax_names_filename = self._get_temp_filename(
                LMFormat.TXT.ext, prefix='user_taxon_names_')
            with open(tax_names_filename, 'w') as out_f:
                for tax_name in occ_json[APIPostKeys.TAXON_NAMES]:
                    out_f.write('{}\n'.format(tax_name))
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.TAXON_NAME_FILENAME,
                tax_names_filename)
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.DATA_SOURCE,
                SpeciesDatasource.TAXON_NAMES)
        else:
            points_filename = occ_json[APIPostKeys.POINTS_FILENAME]
            #TODO: Full file path?
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.DATA_SOURCE,
                SpeciesDatasource.USER)
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_NAME,
                points_filename)
            
            try:
                meta_filename = os.path.join(
                        ARCHIVE_PATH, self.userId, '{}{}'.format(
                            points_filename.replace(
                            LMFormat.CSV.ext, ''), LMFormat.JSON.ext))
                self.scribe.log.debug(
                    'Meta filename?: {}'.format(meta_filename))
                if os.path.exists(meta_filename):
                    with open(meta_filename) as in_file:
                        point_meta = json.load(in_file)
                    if 'delimiter' in point_meta.keys():
                        # TODO: Remove this pop hack when we can process delimiter in JSON
                        delim = point_meta['delimiter']
                        del point_meta['delimiter']
                        self.config.set(
                            SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_DELIMITER,
                            delim)
                        self.scribe.log.debug(json.dumps(point_meta))
                        with open(meta_filename, 'w') as out_f:
                            json.dump(point_meta, out_f)
                        
            except Exception as e:
                self.scribe.log.debug(
                    'Failed to get delimiter from occ upload')
                self.scribe.log.debug(str(e))
                try:
                    delimiter = occ_json[APIPostKeys.DELIMITER]
                    self.config.set(
                        SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_DELIMITER,
                        delimiter)
                except KeyError:  # Not provided, skip and it will default to tab
                    pass
            if APIPostKeys.MIN_POINTS in occ_json.keys():
                min_points = occ_json[APIPostKeys.MIN_POINTS]
            else:
                min_points = 5
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.POINT_COUNT_MIN, min_points)
                
    
    # ................................
    def _process_pam_stats(self, pam_stats_json):
        """Processes PAM stats information

        Args:
            pam_stats_json : JSON chunk indicating how to process PAM stats

        Note:
            This version just computes them or doesn't

        Todo:
            Add additional processing such as iterations and randomization
                method
        """
        try:
            should_compute = int(pam_stats_json[APIPostKeys.DO_PAM_STATS])
        except:
            should_compute = 0

        self.config.set(
            SERVER_BOOM_HEADING, BoomKeys.COMPUTE_PAM_STATS, should_compute)
    
    # ................................
    def _process_scenario_package(self, scenario_json):
        """Processes scenario information from the request.

        Args:
            scenario_json : JSON chunk of scenario information
        """
        if APIPostKeys.PACKAGE_FILENAME in scenario_json.keys():
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.SCENARIO_PACKAGE, 
                scenario_json[APIPostKeys.PACKAGE_FILENAME])
        else:
            model_scenario_code = scenario_json[
                APIPostKeys.MODEL_SCENARIO][APIPostKeys.SCENARIO_CODE]
            self.config.set(
                SERVER_BOOM_HEADING, BoomKeys.SCENARIO_PACKAGE_MODEL_SCENARIO,
                model_scenario_code)
            proj_scenario_codes = []
            for scn in scenario_json[APIPostKeys.PROJECTION_SCENARIO]:
                proj_scenario_codes.append(scn[APIPostKeys.SCENARIO_CODE])
            self.config.set(
                SERVER_BOOM_HEADING,
                BoomKeys.SCENARIO_PACKAGE_PROJECTION_SCENARIOS, 
                ','.join(proj_scenario_codes))
            
            all_scenario_codes = set(proj_scenario_codes)
            all_scenario_codes.add(model_scenario_code)

            if APIPostKeys.PACKAGE_NAME in scenario_json.keys():
                scenario_package_name = scenario_json[APIPostKeys.PACKAGE_NAME]
            else:
                possible_packages = self.scribe.getScenPackagesForScenario(
                    userId=self.userId, scenCode=model_scenario_code)
                possible_packages.extend(
                    self.scribe.getScenPackagesForScenario(
                        userId=PUBLIC_USER, scenCode=model_scenario_code))
                
                scenario_package = None
                # Find first scenario package that has scenarios matching the 
                #     specified codes
                for sp in possible_packages:
                    # Verify that all scenarios are in this package
                    if all([
                        i in sp.scenarios.keys() for i in all_scenario_codes]):

                        scenario_package = sp
                        break
            
                # If public user, copy into user space
                if scenario_package.getUserId() == PUBLIC_USER:
                    # Need to copy the scenario package
                    scen_package_meta = os.path.join(
                        ENV_DATA_PATH, '{}{}'.format(
                            scenario_package.name, LMFormat.PYTHON.ext))
                    if not os.path.exists(scen_package_meta):
                        raise LmHTTPError(
                            HTTPStatus.BAD_REQUEST, 
                            'Scenario package metadata could not be found')
                    user = self.scribe.findUser(userId=self.userId)
                    user_email = user.email
                    
                    filler = SPFiller(
                        scen_package_meta, self.userId, email=user_email)
                    filler.initializeMe()        
                    filler.catalogScenPackages()
                
                if scenario_package is None:
                    raise LmHTTPError(
                        HTTPStatus.BAD_REQUEST, 
                        ('No acceptable scenario package could be found '
                         'matching scenario inputs'))
                scenario_package_name = scenario_package.name
            
            self.config.set(
                SERVER_BOOM_HEADING, 'scenario_package', scenario_package_name)
    
    # ................................
    def _process_sdm(self, sdm_json):
        """Processes SDM information in the request.

        Args:
            sdm_json : JSON chunk of SDM configuration options

        Note:
            This version only handles algorithms and masks

        Todo:
            * Add scaling here
        """
        # Algorithms
        i = 0
        for algo in sdm_json[APIPostKeys.ALGORITHM]:
            algo_section = '{} - {}'.format(SERVER_SDM_ALGORITHM_HEADING_PREFIX, i)
            self.config.add_section(algo_section)

            self.config.set(
                algo_section, BoomKeys.ALG_CODE, algo[APIPostKeys.ALGORITHM_CODE])
            for param in algo[APIPostKeys.ALGORITHM_PARAMETERS].keys():
                self.config.set(
                    algo_section, param.lower(),
                    algo[APIPostKeys.ALGORITHM_PARAMETERS][param])
            i += 1

        # Masks
        if APIPostKeys.HULL_REGION in sdm_json.keys() and \
                sdm_json[APIPostKeys.HULL_REGION] is not None:
            try:
                buffer_val = sdm_json[
                    APIPostKeys.HULL_REGION][APIPostKeys.BUFFER]
                region = sdm_json[APIPostKeys.HULL_REGION][APIPostKeys.REGION]
                
                self.config.add_section(SERVER_SDM_MASK_HEADING_PREFIX)
                self.config.set(
                    SERVER_SDM_MASK_HEADING_PREFIX, BoomKeys.ALG_CODE,
                    'hull_region_intersect')
                self.config.set(
                    SERVER_SDM_MASK_HEADING_PREFIX, BoomKeys.BUFFER,
                    buffer_val)
                self.config.set(
                    SERVER_SDM_MASK_HEADING_PREFIX, BoomKeys.REGION, region)
                # Set the model and scenario mask options
                #TODO: Take this out later
                self.config.set(
                    SERVER_BOOM_HEADING, BoomKeys.MODEL_MASK_NAME, region)
                self.config.set(
                    SERVER_BOOM_HEADING, BoomKeys.PROJECTION_MASK_NAME, region)
            except KeyError as ke:
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST, 'Missing key: {}'.format(str(ke)))

    # ................................
    def _process_tree(self, tree_json):
        """Processes the tree information from the request

        Args:
            tree_json : JSON chunk with tree information

        Note:
            This version only allows a tree to be specified by file name
        """
        tree_name_1 = tree_json[APIPostKeys.TREE_FILENAME]
        # Make sure we include extension
        tree_base, _ = os.path.splitext(tree_name_1)
        tree_name = '{}{}'.format(tree_base, LMFormat.NEXUS.ext)
        self.config.set(SERVER_BOOM_HEADING, BoomKeys.TREE, tree_name)
        self.has_tree = True
    
    # ................................
    def init_boom(self):
        """Initializes the BOOM by writing the file and calling BOOMFiller
        """
        filename = self._get_temp_filename(
            LMFormat.PARAMS.ext, prefix='boom_config_')
            
        with open(filename, 'w') as configOutF:
            self.config.write(configOutF)
            
        import time
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(
            time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(scriptname, timestamp)
        
        filler = BOOMFiller(filename, logname=logname)
        gridset = filler.initBoom(initMakeflow=True)
        
        return gridset

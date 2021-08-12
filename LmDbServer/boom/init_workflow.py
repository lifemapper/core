"""Initialize a BOOM workflow."""
import argparse
import configparser
import glob
import imp
import json
import logging
import os
import stat
import sys
import time

from LmBackend.command.boom import BoomerCommand
from LmBackend.command.common import (
    ChainCommand, ConcatenateMatricesCommand, IdigbioQueryCommand,
    SystemCommand)
from LmBackend.command.server import (
    CatalogTaxonomyCommand, EncodeBioGeoHypothesesCommand, StockpileCommand)
from LmBackend.command.single import GrimRasterCommand
from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.api_query import IdigbioAPI
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (
    BoomKeys, DEFAULT_POST_USER, GBIF, JobStatus, LMFormat, MatrixType,
    ProcessType, SERVER_BOOM_HEADING, SERVER_DEFAULT_HEADING_POSTFIX, ENCODING,
    SERVER_SDM_ALGORITHM_HEADING_PREFIX, SERVER_SDM_MASK_HEADING_PREFIX)
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt
from LmDbServer.common.lmconstants import (SpeciesDatasource, TAXONOMIC_SOURCE)
from LmDbServer.common.localconstants import (
    GBIF_PROVIDER_FILENAME, GBIF_TAXONOMY_FILENAME)
from LmDbServer.tools.catalog_scen_package import SPFiller
from LmServer.base.layer import Vector, Raster
from LmServer.base.service_object import ServiceObject
from LmServer.base.utilities import is_lm_user
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import (
    ARCHIVE_KEYWORD, DEFAULT_EMAIL_POSTFIX, DEFAULT_NUM_PERMUTATIONS,
    ENV_DATA_PATH, GGRIM_KEYWORD, GPAM_KEYWORD, LMFileType, Priority,
    SPECIES_DATA_PATH)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.process_chain import MFChain
from LmServer.legion.shapegrid import Shapegrid
from LmServer.legion.tree import Tree


# .............................................................................
class BOOMFiller(LMObject):
    """Populate database with BOOM archive inputs.

    Class to:
        1) populate a Lifemapper database with inputs for a BOOM archive
            including: user, scenario package, shapegrid, Tree,
                Biogeographic Hypotheses, gridset
        2) If named scenario package does not exist for the user, add it.
        3) create default matrices for each scenario, PAMs for SDM projections
            and GRIMs for Scenario layers
        4) Write a configuration file for computations (BOOM daemon) on the
            inputs
        5) Write a Makeflow to begin the BOOM daemon
    """

    # ................................
    def __init__(self, param_fname, logname=None):
        """Constructor.

        Args:
            param_fname: Absolute path to file containing parameters for
                initiating a Lifemapper workflow
            logname: name for logfile
        """
        super(BOOMFiller, self).__init__()

        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        self.name = scriptname
        if logname is None:
            secs = time.time()
            timestamp = "{}".format(
                time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}'.format(scriptname, timestamp)
        self.log_name = logname

        self.in_param_fname = param_fname
        # Get database
        self.scribe = self._get_db(self.log_name)
        self.open()

        # Initialize variables
        self.user_id = self.user_id_path = self.user_email = None
        self.user_taxonomy_base_filename = self.archive_name = None
        self.priority = self.scen_package_name = self.data_source = None
        self.occ_id_fname = self.taxon_name_filename = None
        self.taxon_id_filename = self.occ_fname = self.occ_sep = None
        self.min_points = self.exp_date = self.algorithms = None
        self.do_assemble_pams = self.grid_bbox = self.cell_sides = None
        self.cell_size = self.grid_name = self.intersect_params = None
        self.mask_alg = self.tree_fname = self.bg_hyp_fnames = None
        self.compute_pam_stats = self.compute_mcpa = None
        self.num_permutations = self.scen_pkg = self.shapegrid = None
        self.other_lyr_names = self.woof_time_mjd = None
        self.out_config_filename = self.mdl_scencode = None
        self.prj_scencodes = None

    # ................................
    @property
    def log(self):
        """Gets the logger off of the scribe attribute."""
        return self.scribe.log

    # ................................
    def initialize_inputs(self):
        """Initialize configured inputs for workflow."""
        (self.user_id, self.user_id_path,
         self.user_email,
         self.user_taxonomy_base_filename,
         self.archive_name,
         self.priority,
         self.scen_package_name,
         mdl_scencode,
         prj_scencodes,
         self.data_source,
         self.occ_id_fname,
         self.taxon_name_filename,
         self.taxon_id_filename,
         self.occ_fname,
         self.occ_sep,
         self.min_points,
         self.exp_date,
         self.algorithms,
         self.do_assemble_pams,
         self.grid_bbox,
         self.cell_sides,
         self.cell_size,
         self.grid_name,
         self.intersect_params,
         self.mask_alg,
         self.tree_fname,
         self.bg_hyp_fnames,
         self.compute_pam_stats,
         self.compute_mcpa,
         self.num_permutations,
         self.other_lyr_names) = self.read_param_vals()
        self.woof_time_mjd = gmt().mjd
        earl = EarlJr()
        self.out_config_filename = earl.create_filename(
            LMFileType.BOOM_CONFIG, obj_code=self.archive_name,
            usr=self.user_id)

        start_file = earl.create_start_walken_filename(
            self.user_id, self.archive_name)
        if os.path.exists(start_file):
            os.remove(start_file)

        # Add/find user for this Boom process (should exist)
        self.add_user()

        # Find existing scenarios or create from user or public ScenPackage
        #    metadata
        self.scen_pkg = self.find_or_add_scenario_package()
        (self.mdl_scencode, self.prj_scencodes,
         mask_lyr_name_scen) = self.find_mdl_proj_scenarios(
             mdl_scencode, prj_scencodes)

        if self.mask_alg:
            if self.mask_alg.code == 'hull_region_intersect':
                mask_lyr_name = self.mask_alg.get_parameter_value('region')
                if mask_lyr_name is None:
                    self.mask_alg.setParameter('region', mask_lyr_name_scen)

        # Fill grid bbox with scenario package (intersection of all bboxes) if
        #    it is absent
        if self.grid_bbox is None:
            self.grid_bbox = self.scen_pkg.bbox

        # Created by addArchive
        self.shapegrid = None

    # ................................
    def find_or_add_scenario_package(self):
        """Find Scenarios from codes.

        Note:
            - Boom parameters must include SCENARIO_PACKAGE, and optionally,
                SCENARIO_PACKAGE_MODEL_SCENARIO,
                SCENARIO_PACKAGE_PROJECTION_SCENARIOS
            - If SCENARIO_PACKAGE_PROJECTION_SCENARIOS is not present, SDMs
                will be projected onto all scenarios
            - This code can only parse scenario metadata marked as version 2.x
        """
        # Make sure Scenario Package exists for this user
        scen_package = self.scribe.get_scen_package(
            user_id=self.user_id, scen_package_name=self.scen_package_name,
            fill_layers=True)
        if scen_package is None:
            # See if metadata exists in user or public environmental directory
            sp_meta_fname = None
            for pth in (self.user_id_path, ENV_DATA_PATH):
                this_fname = os.path.join(pth, self.scen_package_name + '.py')
                if os.path.exists(this_fname):
                    sp_meta_fname = this_fname
                    break
                #  sp_meta_fname = os.path.join(ENV_DATA_PATH,
                #    self.scen_package_name + '.py')
            if sp_meta_fname is None:
                raise LMError(
                    ('ScenPackage {} must be authorized for user {} or all '
                     'users (with public metadata file {})').format(
                         self.scen_package_name, self.user_id, sp_meta_fname))

            sp_filler = SPFiller(
                sp_meta_fname, self.user_id, scribe=self.scribe)
            scen_package = sp_filler.catalog_scen_packages()

        return scen_package

    # ................................
    def find_mdl_proj_scenarios(self, mdl_scencode, prj_scencodes):
        """Find Scenario for modeling, which for projecting.

        Args:
            mdl_scencode: scenario code to use for SDM modeling
            prj_scencodes: list of scenario codes to use for SDM projecting

        Note:
            If either of these codes is None, use the scenario designated as
                "base" in the scenario package metadata for modeling, and use
                all scenarios in the package for projecting.
        """
        valid_scencodes = list(self.scen_pkg.scenarios.keys())
        if len(valid_scencodes) == 0 or None in valid_scencodes:
            raise LMError(
                ('ScenPackage {} metadata is incorrect, scenario '
                 'codes = {}').format(self.scen_package_name, valid_scencodes))

        base_scencode, mask_lyr_name = self._find_scenpkg_base_and_mask(
            self.scen_package_name)
        if base_scencode not in valid_scencodes:
            raise LMError(
                ('ScenPackage {} metadata is incorrect, {} not in scenarios'
                 ).format(self.scen_package_name, base_scencode))

        # If model Scenarios are not listed, use scenPackage default baseline
        if mdl_scencode is None:
            mdl_scencode = base_scencode
        # If model scenarios does not match scenPackage, error params file
        elif mdl_scencode not in valid_scencodes:
            raise LMError(
                ('Params file {} metadata is incorrect, {} not in scenarios '
                 '{} for package {}').format(
                     self.in_param_fname, mdl_scencode, valid_scencodes,
                     self.scen_package_name))
        # If projection Scenarios are not listed, use all scenarios in
        #    scenPackage
        if not prj_scencodes:
            prj_scencodes = valid_scencodes
        # If any prj scenario does not match scenPackage, error params file
        else:
            for pcode in prj_scencodes:
                if pcode not in valid_scencodes:
                    raise LMError(
                        ('Params file {} metadata is incorrect, {} not in '
                         'scenarios {} for package {}').format(
                             self.in_param_fname, pcode, valid_scencodes,
                             self.scen_package_name))

        return mdl_scencode, prj_scencodes, mask_lyr_name

    # ................................
    def open(self):
        """Open database connection."""
        success = self.scribe.open_connections()
        if not success:
            raise LMError('Failed to open database')

    # ................................
    def close(self):
        """Close database connection."""
        self.scribe.close_connections()

    # ................................
    @property
    def log_filename(self):
        """Return the absolute log filename."""
        try:
            fname = self.scribe.log.base_filename
        except Exception:
            fname = None
        return fname

    # ................................
    def _fix_permissions(self, files=None, dirs=None):
        if is_lm_user():
            print('Permissions created correctly by LMUser')
        else:
            dirname = os.path.dirname(self.out_config_filename)
            stats = os.stat(dirname)
            # item 5 is group id; get for lmwriter
            gid = stats[5]
            if files is not None:
                if not isinstance(files, (list, tuple)):
                    files = [files]
                    for file_descriptor in files:
                        try:
                            os.chown(file_descriptor, -1, gid)
                            os.chmod(file_descriptor, 0o664)
                        except Exception as e:
                            print((
                                'Failed to fix permissions on {} ({})'.format(
                                    file_descriptor, e)))
            if dirs is not None:
                if not isinstance(dirs, (list, tuple)):
                    dirs = [dirs]
                    for my_dir in dirs:
                        currperms = oct(os.stat(my_dir)[stat.ST_MODE])[-3:]
                        if currperms != '775':
                            try:
                                os.chown(my_dir, -1, gid)
                                os.chmod(my_dir, 0o775)
                            except Exception as e:
                                print((
                                    'Failed to fix permissions on {}'.format(
                                        my_dir)))

    # ................................
    @staticmethod
    def _get_db(log_name):
        logger = ScriptLogger(log_name, level=logging.INFO)
        # DB connection
        return BorgScribe(logger)

    # ................................
    @staticmethod
    def _get_algorithm(config, alg_heading):
        """Get a configured algorithm.

        Returns:
            Algorithm - Configured with the parameters in config
        """
        acode = config.get(alg_heading, BoomKeys.ALG_CODE)
        alg = Algorithm(acode)
        alg.fill_with_defaults()
        inputs = {}
        # override defaults with any option specified
        alg_options = config.getoptions(alg_heading)
        for name in alg_options:
            pname, ptype = alg.find_param_name_type(name)
            if pname is not None:
                if ptype == int:
                    val = config.getint(alg_heading, pname)
                elif ptype == float:
                    val = config.getfloat(alg_heading, pname)
                else:
                    val = config.get(alg_heading, pname)
                    # Some algorithms(mask) may have a parameter indicating a
                    # layer if so, add name to parameters and object to inputs
                    if acode == 'hull_region_intersect' and pname == 'region':
                        pass
                alg.set_parameter(pname, val)
        if inputs:
            alg.set_inputs(inputs)
        return alg

    # ................................
    def _get_algorithms(self, config,
                        section_prefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX):
        """Get configured algorithms.

        Note:
            Uses default algorithms only if no others exist

        Returns:
            List of Algorithms
        """
        algs = {}
        default_algs = {}
        # Get algorithms for SDM modeling or SDM mask
        sections = config.getsections(section_prefix)
        for alg_heading in sections:
            alg = self._get_algorithm(config, alg_heading)

            if alg_heading.endswith(SERVER_DEFAULT_HEADING_POSTFIX):
                default_algs[alg_heading] = alg
            else:
                algs[alg_heading] = alg
        if len(algs) == 0:
            algs = default_algs
        return algs

    # ................................
    def _find_scenpkg_base_and_mask(self, scen_package_name):
        # pkg_meta, mask_lyr_name = self._findScenPkgMeta(scen_package_name)
        public_scenpkg_meta_file = os.path.join(
            ENV_DATA_PATH, scen_package_name + '.py')
        user_scenpkg_meta_file = os.path.join(
            self.user_id_path, scen_package_name + '.py')
        if os.path.exists(public_scenpkg_meta_file):
            scenpkg_meta_file = public_scenpkg_meta_file
        elif os.path.exists(user_scenpkg_meta_file):
            scenpkg_meta_file = user_scenpkg_meta_file
        else:
            raise LMError(
                'Climate metadata does not exist in {} or {}'.format(
                    public_scenpkg_meta_file, user_scenpkg_meta_file))
        # TODO: change to importlib for python 3.3+???
        try:
            sp_meta = imp.load_source('currentmetadata', scenpkg_meta_file)
        except Exception as e:
            raise LMError(
                'Climate metadata {} cannot be imported; ({})'.format(
                    scenpkg_meta_file, e))
        pkg_meta = sp_meta.CLIMATE_PACKAGES[scen_package_name]
        # Mask is optional
        try:
            mask_lyr_name = sp_meta.SDM_MASK_META['name']
        except Exception:
            mask_lyr_name = None
        base_code = pkg_meta['baseline']
        return base_code, mask_lyr_name

    # ................................
    def read_param_vals(self):
        """Return parameters for workflow from the configuration file."""
        if self.in_param_fname is None or not os.path.exists(
                self.in_param_fname):
            raise LMError(
                'Missing config file {}'.format(self.in_param_fname))

        param_fname = self.in_param_fname
        config = Config(site_fn=param_fname)

        # ..........................
        usr = self._get_boom_param(
            config, BoomKeys.ARCHIVE_USER, default_value=PUBLIC_USER)
        earl = EarlJr()
        user_path = earl.create_data_path(usr, LMFileType.BOOM_CONFIG)
        user_email = self._get_boom_param(
            config, BoomKeys.ARCHIVE_USER_EMAIL,
            default_value='{}{}'.format(usr, DEFAULT_EMAIL_POSTFIX))

        archive_name = self._get_boom_param(config, BoomKeys.ARCHIVE_NAME)
        if archive_name is None:
            raise Exception('Failed to configure ARCHIVE_NAME')

        if usr == PUBLIC_USER:
            def_priority = Priority.NORMAL
        else:
            def_priority = Priority.REQUESTED
        priority = self._get_boom_param(
            config, BoomKeys.ARCHIVE_PRIORITY, default_value=def_priority)

        # ..........................
        # Species data source and input
        occ_fname = occ_sep = user_taxonomy_base_filename = occ_id_fname = None
        taxon_id_filename = taxon_name_filename = None
        data_source = self._get_boom_param(config, BoomKeys.DATA_SOURCE)
        if data_source is None:
            raise Exception('Failed to configure DATA_SOURCE')

        data_source = data_source.upper()
        if data_source not in (
                SpeciesDatasource.GBIF, SpeciesDatasource.USER,
                SpeciesDatasource.EXISTING, SpeciesDatasource.TAXON_IDS,
                SpeciesDatasource.TAXON_NAMES):
            raise LMError('Failed to configure supported DATA_SOURCE')
        if data_source in (SpeciesDatasource.GBIF, SpeciesDatasource.USER):
            occ_fname = self._get_boom_param(config, BoomKeys.OCC_DATA_NAME)
            occ_sep = self._get_boom_param(config, BoomKeys.OCC_DATA_DELIMITER)
            # Taxonomy is optional,
            if data_source == SpeciesDatasource.USER:
                user_taxonomy_base_filename = self._get_boom_param(
                    config, BoomKeys.USER_TAXONOMY_FILENAME)
            if occ_sep is None:
                occ_sep = GBIF.DATA_DUMP_DELIMITER
            if occ_fname is None:
                raise LMError(
                    ('Failed to configure OCC_DATA_NAME for DATA_SOURCE=GBIF '
                     'or USER'))
        elif data_source == SpeciesDatasource.EXISTING:
            occ_id_fname = self._get_boom_param(
                config, BoomKeys.OCC_ID_FILENAME)
            if occ_id_fname is None:
                raise LMError(
                    ('Failed to configure OCC_ID_FILENAME for '
                     'DATA_SOURCE=EXISTING'))
        elif data_source == SpeciesDatasource.TAXON_IDS:
            taxon_id_filename = self._get_boom_param(
                config, BoomKeys.TAXON_ID_FILENAME)
            if taxon_id_filename is None:
                raise LMError(
                    ('Failed to configure TAXON_ID_FILENAME for '
                     'DATA_SOURCE=TAXON_IDS'))
        elif data_source == SpeciesDatasource.TAXON_NAMES:
            taxon_name_filename = self._get_boom_param(
                config, BoomKeys.TAXON_NAME_FILENAME)
            if taxon_name_filename is None:
                raise LMError(
                    ('Failed to configure TAXON_NAME_FILENAME for '
                     'DATA_SOURCE=TAXON_NAMES'))

        # ..........................
        min_points = self._get_boom_param(config, BoomKeys.POINT_COUNT_MIN)
        today = gmt()
        exp_year = self._get_boom_param(
            config, BoomKeys.OCC_EXP_YEAR, default_value=today.year)
        exp_month = self._get_boom_param(
            config, BoomKeys.OCC_EXP_MONTH, default_value=today.month)
        exp_day = self._get_boom_param(
            config, BoomKeys.OCC_EXP_DAY, default_value=today.day)

        # ..........................
        algs = self._get_algorithms(
            config, section_prefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX)
        # ..........................
        # One optional Mask for pre-processing
        mask_alg = None
        mask_alg_list = self._get_algorithms(
            config, section_prefix=SERVER_SDM_MASK_HEADING_PREFIX)
        if mask_alg_list:
            if len(mask_alg_list) == 1:
                mask_alg = list(mask_alg_list.values())[0]
            else:
                raise LMError('Only one PREPROCESSING SDM_MASK supported')
        # ..........................
        # optional MCPA inputs, data values indicate processing steps
        tree_fname = self._get_boom_param(config, BoomKeys.TREE)
        biogeo_name = self._get_boom_param(
            config, BoomKeys.BIOGEO_HYPOTHESES_LAYERS)
        biogeo_hyp_names = self._get_biogeo_hypotheses_layer_filenames(
            biogeo_name, user_path)
        # ..........................
        # optional layer inputs
        other_lyr_names = self._get_boom_param(
            config, BoomKeys.OTHER_LAYERS, default_value=[], is_list=True)
        # ..........................
        # RAD/PAM params, defaults to "Do not intersect"
        intersect_params = None
        compute_pam_stats = None
        compute_mcpa = None
        num_permutations = None

        do_assemble_pams = self._get_boom_param(
            config, BoomKeys.ASSEMBLE_PAMS, is_bool=True, default_value=False)
        grid_bbox = self._get_boom_param(
            config, BoomKeys.GRID_BBOX, is_list=True)
        cell_sides = self._get_boom_param(config, BoomKeys.GRID_NUM_SIDES)
        cell_size = self._get_boom_param(config, BoomKeys.GRID_CELL_SIZE)
        grid_name = '{}-Grid-{}'.format(archive_name, cell_size)
        grid_filter = self._get_boom_param(
            config, BoomKeys.INTERSECT_FILTER_STRING)
        grid_int_val = self._get_boom_param(
            config, BoomKeys.INTERSECT_VAL_NAME)
        grid_min_pct = self._get_boom_param(
            config, BoomKeys.INTERSECT_MIN_PERCENT)
        grid_min_pres = self._get_boom_param(
            config, BoomKeys.INTERSECT_MIN_PRESENCE)
        grid_max_pres = self._get_boom_param(
            config, BoomKeys.INTERSECT_MAX_PRESENCE)
        if do_assemble_pams:
            for var in (
                    grid_bbox, cell_sides, cell_size, grid_int_val,
                    grid_min_pct, grid_min_pres, grid_max_pres):
                if not var:
                    raise LMError(
                        ('Failed to configure one or more GRID parameters: '
                         'GRID_BBOX, GRID_NUM_SIDES, GRID_CELL_SIZE, '
                         'INTERSECT_VAL_NAME, INTERSECT_MIN_PERCENT, '
                         'INTERSECT_MIN_PRESENCE, INTERSECT_MAX_PRESENCE'))
            intersect_params = {
                MatrixColumn.INTERSECT_PARAM_FILTER_STRING: grid_filter,
                MatrixColumn.INTERSECT_PARAM_VAL_NAME: grid_int_val,
                MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: grid_min_pres,
                MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: grid_max_pres,
                MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: grid_min_pct
                }
            # More computations, only if
            compute_pam_stats = self._get_boom_param(
                config, BoomKeys.COMPUTE_PAM_STATS, is_bool=True,
                default_value=False)
            compute_mcpa = self._get_boom_param(
                config, BoomKeys.COMPUTE_MCPA, is_bool=True,
                default_value=False)
            num_permutations = self._get_boom_param(
                config, BoomKeys.NUM_PERMUTATIONS,
                default_value=DEFAULT_NUM_PERMUTATIONS)
        # ..........................
        scen_package_name = self._get_boom_param(
            config, BoomKeys.SCENARIO_PACKAGE)
        if scen_package_name is None:
            raise LMError('Failed to configure SCENARIO_PACKAGE')
        mdl_scencode = self._get_boom_param(
            config, BoomKeys.SCENARIO_PACKAGE_MODEL_SCENARIO)
        if mdl_scencode is None:
            self.log.info(
                'Retrieve `baseline` scenario from SCENARIO_PACKAGE metadata')
        prj_scencodes = self._get_boom_param(
            config, BoomKeys.SCENARIO_PACKAGE_PROJECTION_SCENARIOS,
            is_list=True)
        if not prj_scencodes:
            self.log.info(
                'Retrieve all scenarios from SCENARIO_PACKAGE metadata')

        return (usr, user_path, user_email, user_taxonomy_base_filename,
                archive_name, priority, scen_package_name,
                mdl_scencode, prj_scencodes, data_source,
                occ_id_fname, taxon_name_filename, taxon_id_filename,
                occ_fname, occ_sep, min_points, (exp_year, exp_month, exp_day),
                algs, do_assemble_pams, grid_bbox, cell_sides, cell_size,
                grid_name, intersect_params, mask_alg, tree_fname,
                biogeo_hyp_names, compute_pam_stats, compute_mcpa,
                num_permutations, other_lyr_names)

    # ................................
    def write_config_file(self, tree=None, biogeo_layers=None):
        """Write configuration file.

        Args:
            tree: tree object for a multi-species workflow.
            biogeo_layers: list of names of layers to be used as biogeographic
                hypotheses in a multi-species workflow.
        """
        config = configparser.SafeConfigParser()
        config.add_section(SERVER_BOOM_HEADING)

        # SDM Algorithms with all parameters
        for heading, alg in self.algorithms.items():
            config.add_section(heading)
            config.set(heading, BoomKeys.ALG_CODE, alg.code)
            for name, val in alg.parameters.items():
                config.set(heading, name, str(val))
        # SDM Mask input for optional pre-processing
        if self.mask_alg is not None:
            config.add_section(SERVER_SDM_MASK_HEADING_PREFIX)
            config.set(SERVER_SDM_MASK_HEADING_PREFIX, BoomKeys.ALG_CODE,
                       self.mask_alg.code)
            for name, val in self.mask_alg.parameters.items():
                config.set(SERVER_SDM_MASK_HEADING_PREFIX, name, str(val))

        email = self.user_email
        if email is None:
            email = ''
        # General config
        config.set(SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_USER, self.user_id)
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_NAME, self.archive_name)
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.ARCHIVE_PRIORITY, str(self.priority))
        config.set(SERVER_BOOM_HEADING, BoomKeys.TROUBLESHOOTERS, email)

        # SDM input environmental data, pulled from SCENARIO_PACKAGE metadata
        pcodes = ','.join(self.prj_scencodes)
        config.set(
            SERVER_BOOM_HEADING,
            BoomKeys.SCENARIO_PACKAGE_PROJECTION_SCENARIOS, pcodes)
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.SCENARIO_PACKAGE_MODEL_SCENARIO,
            self.mdl_scencode)
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.MAPUNITS, self.scen_pkg.map_units)
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.EPSG, str(self.scen_pkg.epsg_code))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.SCENARIO_PACKAGE, self.scen_pkg.name)
        # SDM input species source data
        config.set(SERVER_BOOM_HEADING, BoomKeys.DATA_SOURCE, self.data_source)
        # Use/copy public data
        if self.data_source == SpeciesDatasource.EXISTING:
            config.set(
                SERVER_BOOM_HEADING, BoomKeys.OCC_ID_FILENAME,
                self.occ_id_fname)
        # Use GBIF taxon ids to pull iDigBio data
        elif self.data_source == SpeciesDatasource.TAXON_IDS:
            config.set(
                SERVER_BOOM_HEADING, BoomKeys.TAXON_ID_FILENAME,
                self.taxon_id_filename)
        # Use GBIF data dump, with supporting provider and taxonomy files
        elif self.data_source == SpeciesDatasource.GBIF:
            config.set(SERVER_BOOM_HEADING, BoomKeys.GBIF_PROVIDER_FILENAME,
                       GBIF_PROVIDER_FILENAME)
            config.set(SERVER_BOOM_HEADING, BoomKeys.GBIF_TAXONOMY_FILENAME,
                       GBIF_TAXONOMY_FILENAME)
            config.set(
                SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_NAME, self.occ_fname)
            config.set(
                SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_DELIMITER, self.occ_sep)
        # User data
        else:
            config.set(
                SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_NAME, self.occ_fname)
            config.set(
                SERVER_BOOM_HEADING, BoomKeys.OCC_DATA_DELIMITER, self.occ_sep)
            # optional user-provided taxonomy
            if self.user_taxonomy_base_filename is not None:
                config.set(
                    SERVER_BOOM_HEADING, BoomKeys.USER_TAXONOMY_FILENAME,
                    self.user_taxonomy_base_filename)

        # Expiration date triggering re-query and computation
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.OCC_EXP_YEAR, str(self.exp_date[0]))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.OCC_EXP_MONTH, str(self.exp_date[1]))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.OCC_EXP_DAY, str(self.exp_date[2]))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.POINT_COUNT_MIN,
            str(self.min_points))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.OCC_EXP_MJD, str(self.woof_time_mjd))

        # Multi-species flags
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.ASSEMBLE_PAMS,
            str(self.do_assemble_pams))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.COMPUTE_PAM_STATS,
            str(self.compute_pam_stats))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.COMPUTE_MCPA, str(self.compute_mcpa))
        # Grid and Intersect params
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.GRID_NUM_SIDES, str(self.cell_sides))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.GRID_CELL_SIZE, str(self.cell_size))
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.GRID_BBOX,
            ','.join(str(v) for v in self.grid_bbox))
        config.set(SERVER_BOOM_HEADING, BoomKeys.GRID_NAME, self.grid_name)
        # Intersection params
        for k, val in self.intersect_params.items():
            # refer to BoomKeys.INTERSECT_*
            config.set(
                SERVER_BOOM_HEADING,
                'INTERSECT_{}'.format(k.upper()), str(val))
        # Multi-species randomization
        config.set(
            SERVER_BOOM_HEADING, BoomKeys.NUM_PERMUTATIONS,
            str(self.num_permutations))

        # MCPA Biogeographic Hypotheses
        if biogeo_layers and len(biogeo_layers) > 0:
            biogeo_layer_names = ','.join(biogeo_layers)
            config.set(
                SERVER_BOOM_HEADING, BoomKeys.BIOGEO_HYPOTHESES_LAYERS,
                biogeo_layer_names)
        # Phylogenetic data for PD
        if tree is not None:
            config.set(SERVER_BOOM_HEADING, BoomKeys.TREE, tree.name)

        ready_filename(self.out_config_filename, overwrite=True)
        with open(self.out_config_filename, 'w', encoding=ENCODING) as config_file:
            config.write(config_file)
        self._fix_permissions(files=[self.out_config_filename])
        self.scribe.log.info('******')
        self.scribe.log.info(
            '--config_file={}'.format(self.out_config_filename))
        self.scribe.log.info('******')

    # ................................
    @staticmethod
    def _get_var_value(var):
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

    # ................................
    def _get_boom_param(self, config, var_name, default_value=None,
                        is_list=False, is_bool=False):
        var = None
        # Get value from BOOM or default config file
        if is_bool:
            try:
                var = config.getboolean(SERVER_BOOM_HEADING, var_name)
            except Exception:
                if isinstance(default_value, bool):
                    var = default_value
                else:
                    raise LMError(
                        'Var {} must be set to True or False'.format(var_name))
        else:
            try:
                var = config.get(SERVER_BOOM_HEADING, var_name)
            except Exception:
                pass
            # Interpret value
            if var is not None:
                if is_list:
                    try:
                        tmp_list = [v.strip() for v in var.split(',')]
                        var = []
                    except Exception:
                        raise LMError('Failed to split variables on \',\'')
                    for val in tmp_list:
                        var.append(self._get_var_value(val))
                else:
                    var = self._get_var_value(var)
            # or take default if present
            else:
                if default_value is not None:
                    if is_list and isinstance(default_value, list):
                        var = default_value
                    elif is_bool and isinstance(default_value, bool):
                        var = default_value
                    elif not is_list and not is_bool:
                        var = default_value
        return var

    # ................................
    def add_user(self):
        """Add provided user_id to the database."""
        user = LMUser(
            self.user_id, self.user_email, self.user_email, mod_time=gmt().mjd)
        self.scribe.log.info(
            '  Find or insert user {} ...'.format(self.user_id))
        updated_user = self.scribe.find_or_insert_user(user)
        # If exists, found by unique Id or Email, update values
        self.user_id = updated_user.user_id
        self.user_email = updated_user.email

    # ................................
    def _check_occurrence_sets(self, limit=10):
        legal_users = [PUBLIC_USER, self.user_id]
        missing_count = 0
        wrong_user_count = 0
        non_int_count = 0
        if not os.path.exists(self.occ_id_fname):
            raise LMError('Missing OCCURRENCE_ID_FILENAME {}'.format(
                self.occ_id_fname))

        count = 0
        for line in open(self.occ_id_fname, 'r', encoding=ENCODING):
            count += 1
            try:
                tmp = line.strip()
            except Exception as e:
                self.scribe.log.info(
                    'Error reading line {} ({}), stopping'.format(
                        count, str(e)))
                break
            try:
                occ_id = int(tmp)
            except Exception as e:
                self.scribe.log.info(
                    'Unable to get Id from data {} on line {}'.format(
                        tmp, count))
                non_int_count += 1
            else:
                occ = self.scribe.get_occurrence_set(occ_id=occ_id)
                if occ is None:
                    missing_count += 1
                elif occ.get_user_id() not in legal_users:
                    self.scribe.log.info(
                        'Unauthorized user {} for ID {}'.format(
                            occ.get_user_id(), occ_id))
                    wrong_user_count += 1
            if count >= limit:
                break
        self.scribe.log.info(
            'Errors out of {} read OccurrenceSets (limit {}):'.format(
                count, limit))
        self.scribe.log.info('  Missing: {} '.format(missing_count))
        self.scribe.log.info(
            '  Unauthorized data: {} '.format(wrong_user_count))
        self.scribe.log.info('  Bad ID: {} '.format(non_int_count))

    # ................................
    def _add_intersect_grid(self):
        shp = Shapegrid(
            self.grid_name, self.user_id, self.scen_pkg.epsg_code,
            self.cell_sides, self.cell_size, self.scen_pkg.map_units,
            self.grid_bbox, status=JobStatus.INITIALIZE,
            status_mod_time=gmt().mjd)
        new_shp = self.scribe.find_or_insert_shapegrid(shp)
        valid_data = False
        if new_shp:
            # check existence
            valid_data, _ = Shapegrid.test_vector(new_shp.get_dlocation())
            if not valid_data:
                try:
                    # Write new shapegrid
                    dloc = new_shp.get_dlocation()
                    new_shp.build_shape(overwrite=True)
                    valid_data, _ = Shapegrid.test_vector(dloc)
                    self._fix_permissions(files=new_shp.get_shapefiles())
                except Exception as e:
                    self.scribe.log.warning(
                        'Unable to build Shapegrid ({})'.format(str(e)))
                if not valid_data:
                    raise LMError('Failed to write Shapegrid {}'.format(dloc))
            if valid_data and new_shp.status != JobStatus.COMPLETE:
                new_shp.update_status(JobStatus.COMPLETE)
                success = self.scribe.update_object(new_shp)
                if success is False:
                    self.scribe.log.warning(
                        'Failed to update Shapegrid record')
        else:
            raise LMError('Failed to find or insert Shapegrid')
        return new_shp

    # ................................
    def _find_or_add_pam(self, gridset, alg, scen):
        # Create Global PAM for this archive, scenario
        # Pam layers are added upon boom processing
        pam_type = MatrixType.PAM
        if not self.compute_pam_stats:
            pam_type = MatrixType.ROLLING_PAM

        keywords = [GPAM_KEYWORD]
        for keyword in (
                scen.code, scen.gcm_code, scen.alt_pred_code, scen.date_code):
            if keyword is not None:
                keywords.append(keyword)

        desc = '{} for Scenario {}'.format(GPAM_KEYWORD, scen.code)
        pam_meta = {
            ServiceObject.META_DESCRIPTION: desc,
            ServiceObject.META_KEYWORDS: keywords
            }

        tmp_global_pam = LMMatrix(
            None, matrix_type=pam_type,
            scenario_id=scen.get_id(), gcm_code=scen.gcm_code,
            alt_pred_code=scen.alt_pred_code, date_code=scen.date_code,
            alg_code=alg.code, metadata=pam_meta, user_id=self.user_id,
            gridset=gridset, status=JobStatus.GENERAL,
            status_mod_time=gmt().mjd)
        return self.scribe.find_or_insert_matrix(tmp_global_pam)

    # ................................
    def _find_or_add_grim(self, gridset, scen):
        # Create Scenario-GRIM for this archive, scenario
        # GRIM layers are added now
        keywords = [GGRIM_KEYWORD]
        for keyword in (
                scen.code, scen.gcm_code, scen.alt_pred_code, scen.date_code):
            if keyword is not None:
                keywords.append(keyword)

        desc = '{} for Scenario {}'.format(GGRIM_KEYWORD, scen.code)
        grim_meta = {
            ServiceObject.META_DESCRIPTION: desc,
            ServiceObject.META_KEYWORDS: keywords
            }

        tmp_grim = LMMatrix(
            None, matrix_type=MatrixType.GRIM,
            scenario_id=scen.get_id(), gcm_code=scen.gcm_code,
            alt_pred_code=scen.alt_pred_code, date_code=scen.date_code,
            metadata=grim_meta, user_id=self.user_id, gridset=gridset,
            status=JobStatus.GENERAL, status_mod_time=gmt().mjd)
        grim = self.scribe.find_or_insert_matrix(tmp_grim)
        for lyr in scen.layers:
            # Add to GRIM Makeflow ScenarioLayer and MatrixColumn
            _ = self._init_grim_intersect(lyr, grim)
        return grim

    # ................................
    def add_shapegrid_gpam_gridset(self):
        """Add shapegrid and create gridset.

        Create a Gridset, Shapegrid, PAMs, GRIMs for this archive, and update
        attributes with new or existing values from DB
        """
        scen_grims = {}
        self.scribe.log.info(
            '  Find or insert, build shapegrid {} ...'.format(self.grid_name))
        shp = self._add_intersect_grid()
        self.scribe.log.info('  Found or inserted shapegrid')
        self.shapegrid = shp
        # "BOOM" Archive
        meta = {
            ServiceObject.META_DESCRIPTION: ARCHIVE_KEYWORD,
            ServiceObject.META_KEYWORDS: [ARCHIVE_KEYWORD],
            'parameters': self.in_param_fname}
        grdset = Gridset(
            name=self.archive_name, metadata=meta, shapegrid=shp,
            epsg_code=self.scen_pkg.epsg_code, user_id=self.user_id,
            mod_time=self.woof_time_mjd)
        updated_gridset = self.scribe.find_or_insert_gridset(grdset)
        if updated_gridset.mod_time < self.woof_time_mjd:
            updated_gridset.mod_time = self.woof_time_mjd
            self.scribe.update_object(updated_gridset)

            fnames = self.scribe.delete_mf_chains_return_filenames(
                updated_gridset.get_id())
            for fname in fnames:
                os.remove(fname)

            self.scribe.log.info(
                '  Found and updated mod_time for gridset {}'.format(
                    updated_gridset.get_id()))
        else:
            self.scribe.log.info(
                '  Inserted new gridset {}'.format(updated_gridset.get_id()))

        for code, scen in self.scen_pkg.scenarios.items():
            # "Global" PAM (one per scenario/algorithm)
            if code in self.prj_scencodes:
                for alg in list(self.algorithms.values()):
                    _ = self._find_or_add_pam(updated_gridset, alg, scen)

                # "Global" GRIM (one per scenario)
                if not(self.user_id == DEFAULT_POST_USER) or self.compute_mcpa:
                    scen_grims[code] = self._find_or_add_grim(
                        updated_gridset, scen)

        return scen_grims, updated_gridset

    # ................................
    def _init_grim_intersect(self, lyr, mtx):
        """Initialize model, projections for inputs/algorithm."""
        mtx_col = None
        intersect_params = {MatrixColumn.INTERSECT_PARAM_WEIGHTED_MEAN: True}

        if lyr is not None:
            if LMFormat.is_gdal(driver=lyr.data_format):
                ptype = ProcessType.INTERSECT_RASTER_GRIM
            else:
                self.scribe.log.debug(
                    ('Vector intersect not yet implemented for GRIM '
                     'column {}').format(mtx_col.get_id()))

            tmp_col = MatrixColumn(
                None, mtx.get_id(), self.user_id, layer=lyr,
                shapegrid=self.shapegrid, intersect_params=intersect_params,
                squid=lyr.squid, ident=lyr.name, process_type=ptype,
                status=JobStatus.GENERAL, status_mod_time=self.woof_time_mjd,
                post_to_solr=False)
            mtx_col = self.scribe.find_or_insert_matrix_column(tmp_col)

            # DB does not populate with shapegrid on insert
            mtx_col.shapegrid = self.shapegrid

            # TODO: This is a hack, post to solr needs to be retrieved from DB
            mtx_col.post_to_solr = False
            if mtx_col is not None:
                self.scribe.log.debug(
                    'Found/inserted MatrixColumn {}'.format(mtx_col.get_id()))
                # Reset process_type (not in db)
                mtx_col.process_type = ptype
        return mtx_col

    # ................................
    @staticmethod
    def _get_mc_process_type(mtx_column, mtx_type):
        """Initialize configured and stored inputs for ArchiveFiller class."""
        if LMFormat.is_ogr(driver=mtx_column.layer.data_format):
            if mtx_type == MatrixType.PAM:
                ptype = ProcessType.INTERSECT_VECTOR
            elif mtx_type == MatrixType.GRIM:
                raise LMError('Vector GRIM intersection is not implemented')
        else:
            if mtx_type == MatrixType.PAM:
                ptype = ProcessType.INTERSECT_RASTER
            elif mtx_type == MatrixType.GRIM:
                ptype = ProcessType.INTERSECT_RASTER_GRIM
        return ptype

    # ................................
    def add_tree(self, gridset, encoded_tree=None):
        """Find or insert a tree from an encoded tree or configured tree name.

        Args:
            gridset: Gridset object for this workflow
            encoded_tree: tree object

        Return:
            new or existing tree object
        """
        tree = None
        # Provided tree filename takes precedence
        if self.tree_fname is not None:
            name, _ = os.path.splitext(self.tree_fname)
            tree_filename = os.path.join(self.user_id_path, self.tree_fname)
            if os.path.exists(tree_filename):
                baretree = Tree(
                    name, dlocation=tree_filename, user_id=self.user_id,
                    gridset_id=gridset.get_id(), mod_time=self.woof_time_mjd)
                baretree.read()
                tree = self.scribe.find_or_insert_tree(baretree)
            else:
                self.scribe.log.warning('No tree at {}'.format(tree_filename))
        elif encoded_tree is not None:
            tree = self.scribe.find_or_insert_tree(encoded_tree)

        if tree is not None:
            # Update tree properties and write file
            tree.clear_dlocation()
            tree.set_dlocation()
            tree.write_tree()
            tree.update_mod_time(gmt().mjd)
            # Update database
            _success = self.scribe.update_object(tree)
            self._fix_permissions(files=[tree.get_dlocation()])

            # Save tree link to gridset
            print("Add tree to grid set")
            gridset.add_tree(tree)
            gridset.update_mod_time(self.woof_time_mjd)

            self.scribe.update_object(gridset)
        return tree

    # ................................
    @staticmethod
    def _get_bg_meta(bg_fname):
        # defaults for no metadata file
        # lower-case dict keys
        bg_keyword = 'biogeographic hypothesis'
        lyr_meta = {
            MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): None,
            ServiceObject.META_DESCRIPTION.lower():
                'Biogeographic hypothesis based on layer {}'.format(bg_fname)
                }
        fpthbasename, _ = os.path.splitext(bg_fname)
        meta_fname = fpthbasename + LMFormat.JSON.ext
        if os.path.exists(meta_fname):
            with open(meta_fname, 'r', encoding=ENCODING) as in_file:
                meta = json.load(in_file)
                if isinstance(meta, dict):
                    for k, val in meta.items():
                        lyr_meta[k.lower()] = val
                    # Add keyword to metadata
                    try:
                        kwd_str = meta[ServiceObject.META_KEYWORDS]
                        keywords = kwd_str.split(',')
                        if bg_keyword not in keywords:
                            keywords.append(bg_keyword)
                    except Exception:
                        meta[ServiceObject.META_KEYWORDS] = bg_keyword
                else:
                    raise LMError(
                        'Metadata must be a dictionary or JSON')
        return lyr_meta

    # ................................
    def _get_other_layer_filenames(self):
        layers = []
        for lyr_name in self.other_lyr_names:
            lyr_path = os.path.join(self.user_id_path, lyr_name)
            # accept vector shapefiles
            if os.path.exists(lyr_path + LMFormat.SHAPE.ext):
                layers.append((lyr_name, lyr_path + LMFormat.SHAPE.ext))
            # accept raster geotiffs
            elif os.path.exists(lyr_path + LMFormat.GTIFF.ext):
                layers.append((lyr_name, lyr_path + LMFormat.GTIFF.ext))
            # accept shapefiles or geotiffs in a
            else:
                self.scribe.log.warning('No layers at {}'.format(lyr_path))
        return layers

    # ................................
    def add_other_layers(self):
        """Add other layers in the configuration file for workflow."""
        other_layer_names = []
        layers = self._get_other_layer_filenames()
        for (lyr_name, fname) in layers:
            lyr = None
            if fname.endswith(LMFormat.SHAPE.ext):
                lyr = Vector(lyr_name, self.user_id, self.scen_pkg.epsg_code,
                             dlocation=fname,
                             data_format=LMFormat.SHAPE.driver,
                             mod_time=self.woof_time_mjd)
                updated_layer = self.scribe.find_or_insert_layer(lyr)
                other_layer_names.append(updated_layer.name)
            elif fname.endswith(LMFormat.GTIFF.ext):
                lyr = Raster(lyr_name, self.user_id, self.scen_pkg.epsg_code,
                             dlocation=fname,
                             data_format=LMFormat.GTIFF.driver,
                             mod_time=self.woof_time_mjd)
            if lyr is not None:
                updated_layer = self.scribe.find_or_insert_layer(lyr)
                other_layer_names.append(updated_layer.name)
        self.scribe.log.info(
            '  Added other layers {} for user'.format(other_layer_names))
        return other_layer_names

    # ................................
    def _get_biogeo_hypotheses_layer_filenames(self, biogeo_name, user_path):
        biogeo_hyp_names = []
        if biogeo_name is not None:
            bgpth = os.path.join(user_path, biogeo_name)
            if os.path.exists(bgpth + LMFormat.SHAPE.ext):
                biogeo_hyp_names = [bgpth + LMFormat.SHAPE.ext]
            elif os.path.isdir(bgpth):
                pattern = os.path.join(bgpth, '*' + LMFormat.SHAPE.ext)
                biogeo_hyp_names = glob.glob(pattern)
            else:
                self.scribe.log.warning(
                    'No biogeo shapefiles at {}'.format(bgpth))
        return biogeo_hyp_names

    # ................................
    def add_biogeo_hypotheses_matrix_and_layers(self, gridset):
        """Add hypotheses matrix and layers to the workflow.

        Args:
            gridset: gridset object for this workflow.
        """
        biogeo_layer_names = []
        bg_mtx = None

        if len(self.bg_hyp_fnames) > 0:
            mtx_keywords = ['biogeographic hypotheses']
            for bg_fname in self.bg_hyp_fnames:
                if os.path.exists(bg_fname):
                    lyr_meta = self._get_bg_meta(bg_fname)
                    val_attr = lyr_meta[
                        MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
                    try:
                        name = lyr_meta['name']
                    except KeyError:
                        name, _ = os.path.splitext(os.path.basename(bg_fname))
                    mtx_keywords.append('Layer {}'.format(name))
                    lyr = Vector(
                        name, self.user_id, self.scen_pkg.epsg_code,
                        dlocation=bg_fname, metadata=lyr_meta,
                        data_format=LMFormat.SHAPE.driver,
                        val_attribute=val_attr, mod_time=self.woof_time_mjd)
                    updated_layer = self.scribe.find_or_insert_layer(lyr)
                    biogeo_layer_names.append(updated_layer.name)
                    self.scribe.log.info(
                        ' Added {} layers for biogeo hypotheses matrix'.format(
                            len(biogeo_layer_names)))
            # Add the matrix to contain biogeo hypotheses layer intersections
            meta = {
                ServiceObject.META_DESCRIPTION.lower():
                    'Biogeographic Hypotheses for archive {}'.format(
                        self.archive_name),
                ServiceObject.META_KEYWORDS.lower(): mtx_keywords
                }
            tmp_mtx = LMMatrix(
                None, matrix_type=MatrixType.BIOGEO_HYPOTHESES,
                process_type=ProcessType.ENCODE_HYPOTHESES,
                user_id=self.user_id, gridset=gridset, metadata=meta,
                status=JobStatus.INITIALIZE,
                status_mod_time=self.woof_time_mjd)
            bg_mtx = self.scribe.find_or_insert_matrix(tmp_mtx)
            if bg_mtx is None:
                self.scribe.log.info(
                    '  Failed to add biogeo hypotheses matrix')
        return bg_mtx, biogeo_layer_names

    # ................................
    def add_grim_mfs(self, default_grims, target_dir):
        """Add makeflows to compute scenario GRIMs.

        Args:
            default_grims: matrices for intersection of scenarios with
                shapegrid.
            target_dir: absolute path to directory for makeflow files.
        """
        rules = []

        # Get shapegrid rules / files
        shapegrid_filename = self.shapegrid.get_dlocation()

        for code, grim in default_grims.items():
            mtx_cols = self.scribe.get_columns_for_matrix(grim.get_id())
            self.scribe.log.info(
                '  Adding {} grim columns for scen_code {}'.format(
                    len(mtx_cols), code))

            col_filenames = []
            for mtx_col in mtx_cols:
                mtx_col.post_to_solr = False
                mtx_col.process_type = self._get_mc_process_type(
                    mtx_col, grim.matrix_type)
                mtx_col.shapegrid = self.shapegrid

                rel_dir, _ = os.path.splitext(
                    mtx_col.layer.get_relative_dlocation())
                col_filename = os.path.join(
                    target_dir, rel_dir, mtx_col.get_target_filename())
                try:
                    min_percent = mtx_col.intersect_params[
                        mtx_col.INTERSECT_PARAM_MIN_PERCENT]
                except KeyError:
                    min_percent = None
                intersect_cmd = GrimRasterCommand(
                    shapegrid_filename, mtx_col.layer.get_dlocation(),
                    col_filename, minPercent=min_percent, ident=mtx_col.ident)
                rules.append(intersect_cmd.get_makeflow_rule())

                # Keep track of intersection filenames for matrix concatenation
                col_filenames.append(col_filename)

            # Add concatenate command
            rules.extend(
                self._get_matrix_assembly_and_stockpile_rules(
                    grim.get_id(), ProcessType.CONCATENATE_MATRICES,
                    col_filenames, work_dir=target_dir))

        return rules

    # ................................
    @staticmethod
    def _get_matrix_assembly_and_stockpile_rules(matrix_id, process_type,
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
        rules.append(concat_cmd.get_makeflow_rule())

        # Stockpile matrix
        mtx_success_filename = os.path.join(
            work_dir, 'mtx_{}.success'.format(matrix_id))
        stockpile_cmd = StockpileCommand(
            process_type, matrix_id, mtx_success_filename, mtx_out_filename,
            status=JobStatus.COMPLETE)
        rules.append(stockpile_cmd.get_makeflow_rule(local=True))
        return rules

    # ................................
    def _get_idig_query_cmd(self, ws_dir):
        if not os.path.exists(self.taxon_id_filename):
            raise LMError(
                'Taxon ID file {} is missing'.format(self.taxon_id_filename))

        # Note: These paths must exist longer than the workflow because they
        #    will be used by a different workflow
        base_fname = os.path.basename(
            os.path.splitext(self.taxon_id_filename)[0])
        earl = EarlJr()
        tmp_pth = earl.create_data_path(
            self.user_id, LMFileType.TEMP_USER_DATA)
        self._fix_permissions(dirs=[tmp_pth])
        point_output_file = os.path.join(
            tmp_pth, base_fname + LMFormat.CSV.ext)
        meta_output_file = os.path.join(
            tmp_pth, base_fname + LMFormat.JSON.ext)

        # Success file should be in workspace, it will be sent to boomer
        success_file = os.path.join(ws_dir, base_fname + '.success')

        idig_cmd = IdigbioQueryCommand(
            self.taxon_id_filename, point_output_file, meta_output_file,
            success_file, missing_id_file=None)
        return idig_cmd, point_output_file

    # ................................
    def _get_taxonomy_command(self, target_dir):
        """Get a command to insert taxonomic information into the database.
        """
        cat_tax_cmd = tax_success_fname = tax_success_local_fname = None
        tax_data_fname = None
        config = Config(site_fn=self.in_param_fname)
        if self.data_source == SpeciesDatasource.GBIF:
            tax_data_base_name = self._get_boom_param(
                config, BoomKeys.GBIF_TAXONOMY_FILENAME,
                GBIF_TAXONOMY_FILENAME)
            tax_data_fname = os.path.join(
                SPECIES_DATA_PATH, tax_data_base_name)
            tax_source_name = TAXONOMIC_SOURCE['GBIF']['name']
            tax_source_url = TAXONOMIC_SOURCE['GBIF']['url']

        # If there is taxonomy ...
        if tax_data_fname and os.path.exists(tax_data_fname):
            tax_data_base, _ = os.path.splitext(tax_data_fname)
            tax_success_fname = os.path.join(tax_data_base + '.success')
            if os.path.exists(tax_success_fname):
                self.scribe.log.info(
                    'Taxonomy {} has already been cataloged'.format(
                        tax_data_fname))
            else:
                # logfile, walkedTaxFname added to outputs in command
                #    construction
                tax_success_local_fname = os.path.join(
                    target_dir, 'catalog_taxonomy.success')
                # Write taxonomy success to workspace and pass that along, also
                #    copy local taxonomy success file to absolute location
                cat_tax_cmd = ChainCommand(
                    [CatalogTaxonomyCommand(
                        tax_source_name, tax_data_fname,
                        tax_success_local_fname, source_url=tax_source_url,
                        delimiter='\t'),
                     SystemCommand(
                         'cp', '{} {}'.format(
                             tax_success_local_fname, tax_success_fname),
                         inputs=tax_success_local_fname)])
        return cat_tax_cmd, tax_success_local_fname

    # ................................
    def _write_update_mf(self, mf_chain):
        mf_chain.write()
        # Give lmwriter rw access (this script may be run as root)
        self._fix_permissions(files=[mf_chain.get_dlocation()])
        # Set as ready to go
        mf_chain.update_status(JobStatus.INITIALIZE)
        self.scribe.update_object(mf_chain)
        try:
            self.scribe.log.info(
                '  Wrote Makeflow {} for {} for gridset {}'.format(
                    mf_chain.obj_id,
                    mf_chain.makeflow_metadata[MFChain.META_DESCRIPTION],
                    mf_chain.makeflow_metadata[MFChain.META_GRIDSET]))
        except Exception:
            self.scribe.log.info('  Wrote Makeflow {}'.format(mf_chain.obj_id))
        return mf_chain

    # ................................
    def add_boom_rules(self, tree, target_dir):
        """Create a Makeflow to initiate Boomer with inputs

        Args:
            tree (TreeWrapper): A tree to add rules for.
            target_dir: The directory to store workspace files.
        """
        rules = []
        base_config_fname = os.path.basename(self.out_config_filename)
        # ChristopherWalken writes when finished walking through
        # species data (initiated by this Makeflow).
        boom_success_fname = os.path.join(
            target_dir, base_config_fname + '.success')
        boom_cmd = BoomerCommand(self.out_config_filename, boom_success_fname)

        # Add iDigBio MF before Boom, if specified as occurrence input
        if self.data_source == SpeciesDatasource.TAXON_IDS:
            idig_cmd, point_output_file = self._get_idig_query_cmd(target_dir)
            # Update config to User (CSV) datasource and point_output_file
            self.data_source = SpeciesDatasource.USER
            self.occ_fname = point_output_file
            self.occ_sep = IdigbioAPI.DELIMITER
            # Add command to this Makeflow
            rules.append(idig_cmd.get_makeflow_rule(local=True))
            # Boom requires iDigBio data
            boom_cmd.inputs.extend(idig_cmd.outputs)

        # Add taxonomy before Boom, if taxonomy is specified
        cat_tax_cmd, tax_success_fname = self._get_taxonomy_command(target_dir)
        if cat_tax_cmd:
            # Add catalog taxonomy command to this Makeflow
            rules.append(cat_tax_cmd.get_makeflow_rule(local=True))
            # Boom requires catalog taxonomy completion
            boom_cmd.inputs.append(tax_success_fname)

        # Add boom command to this Makeflow
        rules.append(boom_cmd.get_makeflow_rule(local=True))
        return rules

    # ................................
    def _fix_directory_permissions(self, boom_gridset):
        lyrdir = os.path.dirname(boom_gridset.get_shapegrid().get_dlocation())
        self._fix_permissions(dirs=[lyrdir])
        earl = EarlJr()
        mfdir = earl.create_data_path(self.user_id, LMFileType.MF_DOCUMENT)
        self._fix_permissions(dirs=[mfdir])

    # ................................
    def _get_partner_tree_data(self, pquery, gbifids, base_filename):
        treename = os.path.basename(base_filename)
        (otree, gbif_to_ott, _ott_unmatched_gbif_ids
         ) = pquery.assemble_otol_data(gbifids, treename)
        encoded_tree = pquery.encode_ott_tree_to_gbif(
            otree, gbif_to_ott, scribe=self.scribe)
        return encoded_tree

    # ................................
    @staticmethod
    def _get_partner_ids(pquery, names, base_filename):
        gbif_results_filename = base_filename + '.gids'
        (unmatched_names, name_to_gbif_ids
         ) = pquery.assemble_gbif_taxon_ids(names, gbif_results_filename)
        return unmatched_names, name_to_gbif_ids, gbif_results_filename

    # ................................
    @staticmethod
    def _get_user_input(filename):
        items = []
        if os.path.exists(filename):
            try:
                for line in open(filename, 'r', encoding=ENCODING):
                    items.append(line.strip())
            except Exception:
                raise LMError('Failed to read file {}'.format(filename))
        else:
            raise LMError('File {} does not exist'.format(filename))
        return items

    # ................................
    def init_boom(self):
        """Initialize a workflow from configuration file."""
        try:
            # Also adds user
            self.initialize_inputs()

            # Add or get Shapegrid, Global PAM, Gridset for this archive
            # This updates the gridset, shapegrid, default PAMs (rolling, with
            #     no matrixColumns, default GRIMs with matrixColumns
            scen_grims, boom_gridset = self.add_shapegrid_gpam_gridset()
            # Insert other layers that may be used for SDM_MASK or other
            #    processing
            _other_layer_names = self.add_other_layers()

            # Create makeflow for computations and start rule list
            script_name = os.path.splitext(os.path.basename(__file__))[0]
            meta = {
                MFChain.META_CREATED_BY: script_name,
                MFChain.META_GRIDSET: boom_gridset.get_id(),
                MFChain.META_DESCRIPTION: 'Makeflow for gridset {}'.format(
                    boom_gridset.get_id())
                }
            new_mfc = MFChain(
                self.user_id, priority=Priority.HIGH, metadata=meta,
                status=JobStatus.GENERAL, status_mod_time=gmt().mjd)
            gridset_mf = self.scribe.insert_mf_chain(
                new_mfc, boom_gridset.get_id())
            target_dir = gridset_mf.get_relative_directory()
            rules = []

            # Add GRIM rules
            rules.extend(self.add_grim_mfs(scen_grims, target_dir))

            # Check for a file OccurrenceLayer Ids for existing or PUBLIC user
            if self.occ_id_fname:
                self._check_occurrence_sets()

            # Fix user makeflow and layer directory permissions
            self._fix_directory_permissions(boom_gridset)

            # If there is a tree, add db object
            tree = self.add_tree(boom_gridset, encoded_tree=None)

            # If there are biogeographic hypotheses, add layers and matrix
            (biogeo_mtx, biogeo_layer_names
             ) = self.add_biogeo_hypotheses_matrix_and_layers(boom_gridset)

            # init Makeflow
            if biogeo_mtx and len(biogeo_layer_names) > 0:
                # Add BG hypotheses
                bgh_success_fname = os.path.join(target_dir, 'bg.success')
                bg_cmd = EncodeBioGeoHypothesesCommand(
                    self.user_id, boom_gridset.name, bgh_success_fname)
                rules.append(bg_cmd.get_makeflow_rule(local=True))

            # This also adds commands for iDigBio occurrence data retrieval
            #   and taxonomy insertion before Boom
            #   and tree encoding after Boom
            rules.extend(self.add_boom_rules(tree, target_dir))

            # Write config file for archive, update permissions
            self.write_config_file(tree=tree, biogeo_layers=biogeo_layer_names)

            # Write rules
            gridset_mf.add_commands(rules)
            self._write_update_mf(gridset_mf)

        finally:
            self.close()

        # BOOM POST from web requires gridset object to be returned
        return boom_gridset


# .............................................................................
def main():
    """Main method for script"""
    parser = argparse.ArgumentParser(
        description=(
            'Populate a Lifemapper archive with metadata for single- or '
            'multi-species computations specific to the configured input data '
            'or the data package named'))
    parser.add_argument(
        'param_file', default=None,
        help=(
            'Parameter file for the workflow with inputs and outputs '
            'to be created from these data.'))
    parser.add_argument(
        '--logname', type=str, default=None,
        help=('Basename of the logfile, without extension'))
    args = parser.parse_args()
    param_fname = args.param_file
    logname = args.logname

    if param_fname is not None and not os.path.exists(param_fname):
        print(('Missing configuration file {}'.format(param_fname)))
        sys.exit(-1)

    if logname is None:
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(
            time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(scriptname, timestamp)

    print(('Running initWorkflow with param_fname = {}'.format(param_fname)))

    filler = BOOMFiller(param_fname, logname=logname)
    gridset = filler.init_boom()
    print(('Completed initWorkflow creating gridset: {}'.format(
        gridset.get_id())))


# .............................................................................
if __name__ == '__main__':
    main()

"""
import configparser
import glob
import imp
import json
import logging
import os
import stat
import sys
import time

from LmBackend.command.boom import BoomerCommand
from LmBackend.command.common import (
    ChainCommand, ConcatenateMatricesCommand, IdigbioQueryCommand,
    SystemCommand)
from LmBackend.command.server import (
    CatalogTaxonomyCommand, EncodeBioGeoHypothesesCommand, StockpileCommand)
from LmBackend.command.single import GrimRasterCommand
from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.api_query import IdigbioAPI
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (
    BoomKeys, DEFAULT_POST_USER, GBIF, JobStatus, LMFormat, MatrixType,
    ProcessType, SERVER_BOOM_HEADING, SERVER_DEFAULT_HEADING_POSTFIX, ENCODING,
    SERVER_SDM_ALGORITHM_HEADING_PREFIX, SERVER_SDM_MASK_HEADING_PREFIX)
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt
from LmDbServer.common.lmconstants import (SpeciesDatasource, TAXONOMIC_SOURCE)
from LmDbServer.common.localconstants import (
    GBIF_PROVIDER_FILENAME, GBIF_TAXONOMY_FILENAME)
from LmDbServer.tools.catalog_scen_package import SPFiller
from LmServer.base.layer import Vector, Raster
from LmServer.base.service_object import ServiceObject
from LmServer.base.utilities import is_lm_user
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import (
    ARCHIVE_KEYWORD, DEFAULT_EMAIL_POSTFIX, DEFAULT_NUM_PERMUTATIONS,
    ENV_DATA_PATH, GGRIM_KEYWORD, GPAM_KEYWORD, LMFileType, Priority,
    SPECIES_DATA_PATH)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.process_chain import MFChain
from LmServer.legion.shapegrid import Shapegrid
from LmServer.legion.tree import Tree

from LmDbServer.boom.init_workflow import *

param_fname = '/share/lm/data/archive/testams/heuchera_boom_na_10min.params'
filler = BOOMFiller(param_fname, logname='tst_2021-08-10')
self = filler


config = Config(site_fn=param_fname)

# ..........................
usr = self._get_boom_param(
    config, BoomKeys.ARCHIVE_USER, default_value=PUBLIC_USER)
earl = EarlJr()
user_path = earl.create_data_path(usr, LMFileType.BOOM_CONFIG)
user_email = self._get_boom_param(
    config, BoomKeys.ARCHIVE_USER_EMAIL,
    default_value='{}{}'.format(usr, DEFAULT_EMAIL_POSTFIX))

archive_name = self._get_boom_param(config, BoomKeys.ARCHIVE_NAME)

# self.initialize_inputs()

# gridset = filler.init_boom()


"""

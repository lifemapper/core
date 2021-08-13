"""Christopher Walken initialize script
"""
import datetime
import glob
import os

from LmBackend.command.server import (
    MultiIndexPAVCommand, MultiStockpileCommand)
from LmBackend.command.single import SpeciesParameterSweepCommand
from LmBackend.common.lmconstants import RegistryKey, MaskMethod
from LmBackend.common.lmobj import LMError, LMObject
from LmBackend.common.parameter_sweep_config import ParameterSweepConfiguration
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (
    BoomKeys, GBIF, JobStatus, LMFormat, MatrixType, ProcessType,
    SERVER_BOOM_HEADING, SERVER_PIPELINE_HEADING,
    SERVER_SDM_ALGORITHM_HEADING_PREFIX, SERVER_DEFAULT_HEADING_POSTFIX,
    SERVER_SDM_MASK_HEADING_PREFIX, ENCODING)
from LmCommon.common.time import gmt, LmTime
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import (
    BUFFER_KEY, CODE_KEY, DEFAULT_NUM_PERMUTATIONS, ECOREGION_MASK_METHOD,
    LMFileType, MASK_KEY, MASK_LAYER_KEY, MASK_LAYER_NAME_KEY, PRE_PROCESS_KEY,
    Priority, PROCESSING_KEY, SCALE_PROJECTION_MAXIMUM,
    SCALE_PROJECTION_MINIMUM, SPECIES_DATA_PATH)
from LmServer.common.localconstants import (
    DEFAULT_EPSG, POINT_COUNT_MAX, PUBLIC_USER)
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.sdm_proj import SDMProjection
from LmServer.tools.occ_woc import (UserWoC, ExistingWoC)


# .............................................................................
class ChristopherWalken(LMObject):
    """Species process iterator

    Class to ChristopherWalken with a species iterator through a sequence of
    species data creating a Spud (set of computations) for each species.
    Creates and catalogs objects (OccurrenceSets, SMDModels, SDMProjections,
    and MatrixColumns and MFChains for their calculation) in the database.
    """
    # ....................................
    def __init__(self, config_fname, json_fname=None, scribe=None):
        """Constructor."""
        super(ChristopherWalken, self).__init__()

        self.config_fname = config_fname
        base_abs_filename, _ = os.path.splitext(config_fname)
        basename = os.path.basename(base_abs_filename)
        # Chris writes this file when completed walking through species data
        self.walked_archive_fname = base_abs_filename + LMFormat.LOG.ext
        self.name = '{}_{}'.format(self.__class__.__name__.lower(), basename)
        # Config
        if config_fname is not None and os.path.exists(config_fname):
            self.cfg = Config(site_fn=config_fname)
        else:
            raise LMError('Missing config file {}'.format(config_fname))

        # JSON or ini based configuration
        if json_fname is not None:
            raise LMError('JSON Walken is not yet implemented')

        # Optionally use parent process Database connection
        if scribe is not None:
            self.log = scribe.log
            self._scribe = scribe
        else:
            self.log = ScriptLogger(self.name)
            try:
                self._scribe = BorgScribe(self.log)
                success = self._scribe.open_connections()
            except Exception as e:
                raise LMError('Exception opening database', e)
            else:
                if not success:
                    raise LMError('Failed to open database')

                self.log.info('{} opened databases'.format(self.name))

        # Global PAM Matrix for each scenario
        self.global_pams = {}

    # ....................................
    def initialize_me(self):
        """Set objects and parameters for workflow on this object."""
        self.more_data_to_process = False

        self.user_id = self._get_boom_or_default(BoomKeys.ARCHIVE_USER, default_value=PUBLIC_USER)
        self.archive_name = self._get_boom_or_default(BoomKeys.ARCHIVE_NAME)
        self.archive_priority = self._get_boom_or_default(BoomKeys.ARCHIVE_PRIORITY, default_value=Priority.NORMAL)
        self.epsg = self._get_boom_or_default(BoomKeys.EPSG, default_value=DEFAULT_EPSG)

        if self.user_id is None or self.archive_name is None:
            raise LMError('Missing ARCHIVE_USER or ARCHIVE_NAME in {}'
                          .format(self.cfg.config_files))
        earl = EarlJr()
        self.boom_path = earl.create_data_path(self.user_id, LMFileType.BOOM_CONFIG)
        # Species parser/puller
        self.weapon_of_choice = self._get_occ_weapon_of_choice()
        
        # SDM inputs
        self.min_points = self._get_boom_or_default(BoomKeys.POINT_COUNT_MIN)
        self.algorithms = self._get_algorithms(section_prefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX)
        (self.mdl_scen, self.prj_scens, self.model_mask_base) = self._get_proj_params()
        
        # Global PAM inputs
        (self.boom_gridset, self.intersect_params) = self._get_global_pam_objects()
        self._obsolete_time = self.boom_gridset.mod_time
        self.weapon_of_choice.reset_expiration_date(self._obsolete_time)
        self.num_permutations = self._get_boom_or_default(
            BoomKeys.NUM_PERMUTATIONS, default_value=DEFAULT_NUM_PERMUTATIONS)
        self.compute_pam_stats = self._get_boom_or_default(BoomKeys.COMPUTE_PAM_STATS, is_bool=True)
        self.compute_mcpa = self._get_boom_or_default(BoomKeys.COMPUTE_MCPA, is_bool=True)

        self.column_meta = None
        try:
            self.column_meta = self.weapon_of_choice.occParser.column_meta
        except Exception:
            pass
        # One Global PAM for each scenario
        for alg in self.algs:
            for prjscen in self.prj_scens:
                pamcode = '{}_{}'.format(prjscen.code, alg.code)
                self.global_pams[
                    pamcode] = self.boom_gridset.get_pam_for_codes(
                        prjscen.gcm_code, prjscen.alt_pred_code,
                        prjscen.date_code, alg.code)

    # ....................................
    def move_to_start(self):
        """Move to starting location of WoC."""
        self.weapon_of_choice.move_to_start()

    # ....................................
    def save_next_start(self, fail=False):
        """Save the next starting location."""
        self.weapon_of_choice.save_next_start(fail=fail)

    # ....................................
    @property
    def curr_rec_num(self):
        """Return the current record number."""
        return self.weapon_of_choice.curr_rec_num

    # ....................................
    @property
    def next_start(self):
        """Return the next starting location."""
        return self.weapon_of_choice.next_start

    # ....................................
    @property
    def complete(self):
        """Return boolean indicating if complete."""
        return self.weapon_of_choice.complete

    # ....................................
    @property
    def occ_delimiter(self):
        """Return the occurrence delimiter."""
        return self.weapon_of_choice.occ_delimiter

    # ....................................
    @staticmethod
    def _get_var_value(var):
        try:
            var = int(var)
        except (TypeError, ValueError):
            try:
                var = float(var)
            except (TypeError, ValueError):
                pass
        return var

    # ....................................
    def _get_boom_or_default(self, var_name, default_value=None, is_list=False,
                             is_bool=False):
        var = None
        # Get value from BOOM or default config file
        if is_bool:
            try:
                var = self.cfg.getboolean(SERVER_BOOM_HEADING, var_name)
            except Exception:
                try:
                    var = self.cfg.getboolean(
                        SERVER_PIPELINE_HEADING, var_name)
                except Exception:
                    pass
        else:
            try:
                var = self.cfg.get(SERVER_BOOM_HEADING, var_name)
            except Exception:
                try:
                    var = self.cfg.get(SERVER_PIPELINE_HEADING, var_name)
                except Exception:
                    pass
        # Take default if present
        if var is None:
            if default_value is not None:
                var = default_value
        # or interpret value
        elif not is_bool:
            if not is_list:
                var = self._get_var_value(var)
            else:
                try:
                    tmp_list = [v.strip() for v in var.split(',')]
                    var = []
                except Exception:
                    raise LMError('Failed to split variables on \',\'')
                for temp_v in tmp_list:
                    temp_v = self._get_var_value(temp_v)
                    var.append(temp_v)
        return var

    # ....................................
    def _get_occ_weapon_of_choice(self):
        # Get data_source and optional taxonomy source
        data_source = self._get_boom_or_default(BoomKeys.DATA_SOURCE)
        try:
            taxon_source_name = TAXONOMIC_SOURCE[data_source]['name']
        except KeyError:
            taxon_source_name = None

        # Expiration date for retrieved species data
        exp_date = LmTime(dtime=datetime.datetime(
            self._get_boom_or_default(BoomKeys.OCC_EXP_YEAR),
            self._get_boom_or_default(BoomKeys.OCC_EXP_MONTH),
            self._get_boom_or_default(BoomKeys.OCC_EXP_DAY),
            tzinfo=datetime.timezone.utc)).mjd

        # Copy public data to user space
        # TODO: Handle taxonomy, use_gbif_taxonomy=??
        if data_source == SpeciesDatasource.EXISTING:
            occ_id_fname = self._get_boom_or_default(BoomKeys.OCC_ID_FILENAME)
            weapon_of_choice = ExistingWoC(
                self._scribe, self.user_id, self.archive_name, self.epsg, exp_date,
                occ_id_fname, logger=self.log)

        else:
            occ_name = self._get_boom_or_default(BoomKeys.OCC_DATA_NAME)
            occ_dir = None
            occ_delimiter = str(
                self._get_boom_or_default(BoomKeys.OCC_DATA_DELIMITER))
            if occ_delimiter != ',':
                occ_delimiter = GBIF.DATA_DUMP_DELIMITER
            (occ_csv_fname, occ_meta_fname, self.more_data_to_process
             ) = self._find_data(occ_name, occ_dir, self.boom_path)

            # Handle GBIF data, saving taxononomy data with GBIF acceptedTaxonKey
            use_gbif_taxon_ids = False
            if data_source == SpeciesDatasource.GBIF:
                use_gbif_taxon_ids = True
            weapon_of_choice = UserWoC(
                self._scribe, self.user_id, self.archive_name, self.epsg, exp_date, 
                occ_csv_fname, occ_meta_fname, occ_delimiter, logger=self.log,
                use_gbif_taxonomy=use_gbif_taxon_ids, taxon_source_name=taxon_source_name)

        weapon_of_choice.initialize_me()

        return weapon_of_choice, exp_date

    # ....................................
    @staticmethod
    def _find_data(occ_name, occ_dir, boom_path):
        more_data_to_process = False
        occ_csv_fname = occ_meta_fname = None

        if occ_name is not None:
            # Complete base filename
            if not occ_name.endswith(LMFormat.CSV.ext):
                occ_csv = occ_name + LMFormat.CSV.ext
                occ_meta = occ_name + LMFormat.JSON.ext
            else:
                occ_csv = occ_name
                occ_meta = os.path.splitext(occ_csv)[0] + LMFormat.JSON.ext

            installed_csv = os.path.join(SPECIES_DATA_PATH, occ_csv)
            user_csv = os.path.join(boom_path, occ_csv)

            # Look for data in relative path, installed path, user path
            if os.path.exists(occ_csv):
                occ_csv_fname = occ_csv
                occ_meta_fname = occ_meta
            elif os.path.exists(installed_csv):
                occ_csv_fname = installed_csv
                occ_meta_fname = os.path.join(SPECIES_DATA_PATH, occ_meta)
            elif os.path.exists(user_csv):
                occ_csv_fname = user_csv
                occ_meta_fname = os.path.join(boom_path, occ_meta)
            else:
                raise LMError(
                    ('Species file {} does not exist as relative path, or in'
                     'public data directory {} or in user dir {}').format(
                         occ_name, SPECIES_DATA_PATH, boom_path))

        # TODO: Add 'OCC_DATA_DIR' as parameter for individual CSV files
        #       in a directory, one per species
        #       (for LmServer.tools.occwoc.TinyBubblesWoC)
        elif occ_dir is not None:
            installed_dir = os.path.join(SPECIES_DATA_PATH, occ_dir)
            user_dir = os.path.join(boom_path, occ_dir)
            # Check for directory location - either absolute or relative path
            if os.path.isdir(occ_dir):
                pass
            #   or in User top data directory
            elif os.path.isdir(installed_dir):
                occ_dir = installed_dir
            #   or in installation data directory
            elif os.path.isdir(user_dir):
                occ_dir = user_dir
            else:
                raise LMError(
                    ('Failed to find file {} in relative location or in '
                     'user dir {} or installation dir {}').format(
                         occ_name, boom_path, SPECIES_DATA_PATH))
            occ_meta_fname = occ_dir + LMFormat.JSON.ext

            fnames = glob.glob(os.path.join(occ_dir, '*' + LMFormat.CSV.ext))
            if len(fnames) > 0:
                occ_csv_fname = fnames[0]
                if len(fnames) > 1:
                    more_data_to_process = True
            else:
                raise LMError(
                    ('Failed to find csv file in dir {} or user '
                     'path {} or installation path {}').format(
                         occ_dir, boom_path, SPECIES_DATA_PATH))
        if not os.path.exists(occ_meta_fname):
            raise LMError('Missing metadata file {}'.format(occ_meta_fname))
        return occ_csv_fname, occ_meta_fname, more_data_to_process

    # ....................................
    def _get_algorithm(self, alg_heading):
        acode = self.cfg.get(alg_heading, BoomKeys.ALG_CODE)
        alg = Algorithm(acode)
        alg.fill_with_defaults()
        inputs = {}
        # override defaults with any option specified
        algoptions = self.cfg.getoptions(alg_heading)
        for name in algoptions:
            pname, ptype = alg.find_param_name_type(name)
            if pname is not None:
                if ptype == int:
                    val = self.cfg.getint(alg_heading, pname)
                elif ptype == float:
                    val = self.cfg.getfloat(alg_heading, pname)
                else:
                    val = self.cfg.get(alg_heading, pname)
                    # Some algorithms(mask) may have a parameter indicating a
                    # layer, if so, add name to parameters and object to inputs
                    if acode == ECOREGION_MASK_METHOD and pname == 'region':
                        inputs[pname] = val
                alg.set_parameter(pname, val)
        if inputs:
            alg.set_inputs(inputs)
        return alg

    # ....................................
    def _get_algorithms(self,
                        section_prefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX):
        algs = []
        default_algs = []
        # Get algorithms for SDM modeling
        sections = self.cfg.getsections(section_prefix)
        for alg_heading in sections:
            alg = self._get_algorithm(alg_heading)

            if alg_heading.endswith(SERVER_DEFAULT_HEADING_POSTFIX):
                default_algs.append(alg)
            else:
                algs.append(alg)
        if len(algs) == 0:
            algs = default_algs
        return algs

    # ....................................
    def _get_proj_params(self):
        prj_scens = []
        mdl_scen = None
        model_mask_base = None

        # Get environmental data model and projection scenarios
        mdl_scen_code = self._get_boom_or_default(BoomKeys.SCENARIO_PACKAGE_MODEL_SCENARIO)
        prj_scen_codes = self._get_boom_or_default(BoomKeys.SCENARIO_PACKAGE_PROJECTION_SCENARIOS, is_list=True)
        scen_pkgs = self._scribe.get_scen_packages_for_user_codes(
            self.user_id, prj_scen_codes, fill_layers=True)
        if not scen_pkgs:
            scen_pkgs = self._scribe.get_scen_packages_for_user_codes(
                PUBLIC_USER, prj_scen_codes, fill_layers=True)
        if scen_pkgs:
            scen_pkg = scen_pkgs[0]
            mdl_scen = scen_pkg.get_scenario(code=mdl_scen_code)
            for pcode in prj_scen_codes:
                prj_scens.append(scen_pkg.get_scenario(code=pcode))
        else:
            raise LMError('Failed to retrieve ScenPackage for scenarios {}'.format(prj_scen_codes))

        # Put params into SDMProject metadata
        mask_alg_list = self._get_algorithms(section_prefix=SERVER_SDM_MASK_HEADING_PREFIX)
        if len(mask_alg_list) > 1:
            raise LMError('Unable to handle > 1 SDM pre-process')
        if len(mask_alg_list) == 1:
            sdm_mask_alg = mask_alg_list[0]

            # TODO: Handle if there is more than one input layer?
            mask_data = sdm_mask_alg.get_inputs()
            if mask_data and len(mask_data) > 1:
                raise LMError('Unable to process > 1 input SDM mask layer')

            lyr_name = list(mask_data.values())[0]

            # Get processing parameters for masking
            proc_params = {
                PRE_PROCESS_KEY: {
                    MASK_KEY: {
                        MASK_LAYER_KEY: lyr_name,
                        MASK_LAYER_NAME_KEY: sdm_mask_alg.get_parameter_value(
                            'region'),
                        CODE_KEY: ECOREGION_MASK_METHOD,
                        BUFFER_KEY: sdm_mask_alg.get_parameter_value(
                            BUFFER_KEY)
                        }
                    }
                }

            mask_layer_name = proc_params[PRE_PROCESS_KEY][MASK_KEY][MASK_LAYER_KEY]
            mask_layer = self._scribe.get_layer(
                user_id=self.user_id, lyr_name=mask_layer_name, epsg=self.epsg)
            if mask_layer is None:
                raise LMError(
                    'Failed to retrieve layer {}'.format(mask_layer_name))
            model_mask_base = {
                RegistryKey.REGION_LAYER_PATH: mask_layer.get_dlocation(),
                RegistryKey.BUFFER: proc_params[PRE_PROCESS_KEY][MASK_KEY][BUFFER_KEY],
                RegistryKey.METHOD: MaskMethod.HULL_REGION_INTERSECT
                }

        return (mdl_scen, prj_scens, model_mask_base)

    # ....................................
    def _get_global_pam_objects(self):
        # Get existing intersect grid, gridset and parameters for Global PAM
        grid_name = self._get_boom_or_default(BoomKeys.GRID_NAME)
        if grid_name:
            intersect_grid = self._scribe.get_shapegrid(
                user_id=self.user_id, lyr_name=grid_name, epsg=self.epsg)
            if intersect_grid is None:
                raise LMError(
                    'Failed to retrieve Shapegrid for intersection {}'.format(
                        grid_name))

        # Global PAM and Scenario GRIM for each scenario
        boom_gridset = self._scribe.get_gridset(
            name=self.archive_name, user_id=self.user_id, fill_matrices=True)
        if boom_gridset is None:
            raise LMError(
                'Failed to retrieve Gridset for shapegrid {}, user {}'.format(
                    grid_name, self.user_id))
        boom_gridset.set_matrix_process_type(
            ProcessType.CONCATENATE_MATRICES,
            matrix_types=[
                MatrixType.PAM, MatrixType.ROLLING_PAM, MatrixType.GRIM])
        intersect_params = {
            MatrixColumn.INTERSECT_PARAM_FILTER_STRING:
                self._get_boom_or_default(BoomKeys.INTERSECT_FILTER_STRING),
            MatrixColumn.INTERSECT_PARAM_VAL_NAME:
                self._get_boom_or_default(BoomKeys.INTERSECT_VAL_NAME),
            MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE:
                self._get_boom_or_default(BoomKeys.INTERSECT_MIN_PRESENCE),
            MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE:
                self._get_boom_or_default(BoomKeys.INTERSECT_MAX_PRESENCE),
            MatrixColumn.INTERSECT_PARAM_MIN_PERCENT:
                self._get_boom_or_default(BoomKeys.INTERSECT_MIN_PERCENT)}

        # TODO: remove this from gridset and add to each SDMProject
        mask_alg_list = self._get_algorithms(
            section_prefix=SERVER_SDM_MASK_HEADING_PREFIX)
        if len(mask_alg_list) == 1:
            sdm_mask_alg = mask_alg_list[0]
            # TODO: Handle if there is more than one input layer
            if len(sdm_mask_alg.get_inputs()) > 1:
                raise LMError('Unable to process > 1 input SDM mask layer')
            for input_key, lyr_name in sdm_mask_alg.get_inputs().items():
                sdm_mask_input_layer = self._scribe.get_layer(
                    user_id=self.user_id, lyr_name=lyr_name, epsg=self.epsg)
                sdm_mask_alg.set_input(input_key, sdm_mask_input_layer)

            proc_params = {
                PROCESSING_KEY: {
                    PRE_PROCESS_KEY: {
                        MASK_KEY: {
                            MASK_LAYER_KEY: sdm_mask_input_layer,
                            MASK_LAYER_NAME_KEY:
                                sdm_mask_alg.get_parameter_value('region'),
                            CODE_KEY: ECOREGION_MASK_METHOD,
                            BUFFER_KEY: sdm_mask_alg.get_parameter_value(
                                BUFFER_KEY)
                            }
                        }
                    }
                }
            # TODO: AMS - If this will be saved, may need to remove the mask
            #    layer object
            boom_gridset.add_grid_metadata(proc_params)

        return (boom_gridset, intersect_params)

    # ....................................
    @classmethod
    def get_config(cls, config_fname):
        """Return user, archive, path, and configuration object."""
        if config_fname is None or not os.path.exists(config_fname):
            raise LMError('Missing config file {}'.format(config_fname))
        config = Config(site_fn=config_fname)
        return config

    # # ....................................
    # def _get_configured_objects(self):
    #     """Return configured string values and any corresponding db objects.
    #
    #     Todo:
    #         Make all archive/default config keys consistent
    #     """
    #     user_id = self._get_boom_or_default(
    #         BoomKeys.ARCHIVE_USER, default_value=PUBLIC_USER)
    #     archive_name = self._get_boom_or_default(BoomKeys.ARCHIVE_NAME)
    #     archive_priority = self._get_boom_or_default(
    #         BoomKeys.ARCHIVE_PRIORITY, default_value=Priority.NORMAL)
    #     # Get user-archive configuration file
    #     if user_id is None or archive_name is None:
    #         raise LMError('Missing ARCHIVE_USER or ARCHIVE_NAME in {}'
    #                       .format(self.cfg.config_files))
    #     earl = EarlJr()
    #     boom_path = earl.create_data_path(user_id, LMFileType.BOOM_CONFIG)
    #     epsg = self._get_boom_or_default(
    #         BoomKeys.EPSG, default_value=DEFAULT_EPSG)
    #
    #     # Species parser/puller
    #     weapon_of_choice, _ = self._get_occ_weapon_of_choice(
    #         user_id, archive_name, epsg, boom_path)
    #     # SDM inputs
    #     min_points = self._get_boom_or_default(BoomKeys.POINT_COUNT_MIN)
    #     algorithms = self._get_algorithms(
    #         section_prefix=SERVER_SDM_ALGORITHM_HEADING_PREFIX)
    #
    #     (mdl_scen, prj_scens, model_mask_base) = self._get_proj_params(
    #         user_id, epsg)
    #     # Global PAM inputs
    #     (boom_gridset, intersect_params) = self._get_global_pam_objects(
    #         user_id, archive_name, epsg)
    #     new_date_mjd = boom_gridset.mod_time
    #     exp_date = new_date_mjd
    #     weapon_of_choice.reset_expiration_date(new_date_mjd)
    #
    #     num_permutations = self._get_boom_or_default(
    #         BoomKeys.NUM_PERMUTATIONS, default_value=DEFAULT_NUM_PERMUTATIONS)
    #
    #     return (
    #         user_id, archive_name, archive_priority, boom_path,
    #         weapon_of_choice, exp_date, epsg, min_points, algorithms, mdl_scen,
    #         prj_scens, model_mask_base, boom_gridset, intersect_params,
    #         num_permutations)

    # ....................................
    def _get_json_objects(self):
        """Return provided values from JSON and any corresponding db objects.
        {
        OccDriver: data_source,
        Occdata: ( filename of taxonids, csv of datablocks, etc
            each handled differently)
        Algs: [algs/params]
        ScenPkg: mdlscen
        [prjscens]
        """

    # ....................................
    def start_walken(self, work_dir):
        """Walks the configured data parameters for computation.

        Returns:
            Single-species MFChain (spud), dictionary of
                {scenarioCode: PAV filename for input into multi-species
                    MFChains (potatoInputs)}
        """
        squid = None
        spud_rules = []
        index_pavs_document_filename = None
        gs_id = 0
        curr_time = gmt().mjd

        try:
            gs_id = self.boom_gridset.get_id()
        except Exception:
            self.log.warning('Missing self.boom_gridset id!!')

        # WeaponOfChoice.get_one returns the next occurrenceset for species
        # input data. If it is new, failed, or outdated, write the raw
        # data and update the rawDlocation.
        occ = self.weapon_of_choice.get_one()
        if self.weapon_of_choice.finished_input:
            self._write_done_walken_file()
        if occ:
            squid = occ.squid

            occ_work_dir = 'occ_{}'.format(occ.get_id())
            sweep_config = ParameterSweepConfiguration(work_dir=occ_work_dir)

            # If we have enough points to model
            # TODO: why is boomer creating projections for occsets with < min points??
            if occ.query_count >= self.min_points:
                for alg in self.algs:
                    prjs = []
                    mtx_cols = []
                    for prj_scen in self.prj_scens:
                        pamcode = '{}_{}'.format(prj_scen.code, alg.code)
                        prj = self._find_or_insert_sdm_project(
                            occ, alg, prj_scen, gmt().mjd)
                        if prj is not None:
                            prjs.append(prj)
                            mtx = self.global_pams[pamcode]
                            mtx_col = self._find_or_insert_intersect(
                                prj, mtx, curr_time)
                            if mtx_col is not None:
                                mtx_cols.append(mtx_col)
                    do_sdm = self._do_compute_sdm(occ, prjs, mtx_cols)
                    self.log.info(
                        ('    Will compute for Grid {} alg {}: {} projs, {}'
                         'intersects').format(
                             gs_id, alg.code, len(prjs), len(mtx_cols)))

                    if do_sdm:
                        # Add SDM commands for the algorithm
                        self._fill_sweep_config(
                            sweep_config, alg, occ, prjs, mtx_cols)
            else:
                do_sdm = self._do_compute_sdm(occ, [], [])
                if do_sdm:
                    # Only add the occurrence set to the sweep config.  Empty
                    #    lists for projections and PAVs will omit those objects
                    self._fill_sweep_config(
                        sweep_config, None, occ, [], [])

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
                    os.path.dirname(occ.get_dlocation()),
                    'species_config_{}{}'.format(
                        occ.get_id(), LMFormat.JSON.ext))
                sweep_config.save_config(species_config_filename)

                # Add sweep rule
                param_sweep_cmd = SpeciesParameterSweepCommand(
                    species_config_filename, sweep_config.get_input_files(),
                    sweep_config.get_output_files(work_dir), work_dir)
                spud_rules.append(param_sweep_cmd.get_makeflow_rule())

                # Add stockpile rule
                stockpile_success_filename = os.path.join(
                    work_dir, occ_work_dir, 'occ_{}stockpile.success'.format(
                        occ.get_id()))
                stockpile_cmd = MultiStockpileCommand(
                    os.path.join(work_dir, sweep_config.stockpile_filename),
                    stockpile_success_filename,
                    pav_filename=os.path.join(
                        work_dir, sweep_config.pavs_filename))
                spud_rules.append(
                    stockpile_cmd.get_makeflow_rule(local=True))

                # Add multi-index rule if we added PAVs
                if len(sweep_config.pavs) > 0:
                    index_pavs_document_filename = os.path.join(
                        work_dir, occ_work_dir, 'solr_pavs_post{}'.format(
                            LMFormat.XML.ext))
                    index_cmd = MultiIndexPAVCommand(
                        os.path.join(work_dir, sweep_config.pavs_filename),
                        index_pavs_document_filename)
                    spud_rules.append(index_cmd.get_makeflow_rule(local=True))

            # TODO: Add metrics / snippets processing
        return squid, spud_rules, index_pavs_document_filename

    # ....................................
    def _do_compute_sdm(self, occ, prjs, mtx_cols):
        # occ is initalized by occwoc if ready for reset, check only status
        #    here
        do_sdm = (occ.status == JobStatus.INITIALIZE)
        if not do_sdm:
            for prj in prjs:
                if do_sdm:
                    break

                do_sdm = self._do_reset(prj.status, prj.status_mod_time)
            for mtx_col in mtx_cols:
                if do_sdm:
                    break

                do_sdm = self._do_reset(
                    mtx_col.status, mtx_col.status_mod_time)
        return do_sdm

    # ....................................
    def _fill_sweep_config(self, sweep_config, alg, occ, prjs, mtx_cols):
        # Add occurrence set if status = 1 and there is a process to perform
        if occ.status == JobStatus.INITIALIZE and occ.process_type is not None:
            raw_meta_dloc = occ.get_raw_dlocation() + LMFormat.JSON.ext
            # TODO: replace metadata filename with metadata dict in self.column
            #    meta?
            sweep_config.add_occurrence_set(
                occ.process_type, occ.get_id(), occ.get_raw_dlocation(),
                occ.get_dlocation(), occ.get_dlocation(large_file=True),
                POINT_COUNT_MAX, metadata=raw_meta_dloc,
                delimiter=self.occ_delimiter)
#                 POINT_COUNT_MAX, metadata=occ.raw_meta_dlocation)

        for prj in prjs:
            if self.model_mask_base is not None:
                model_mask = self.model_mask_base.copy()
                model_mask[
                    RegistryKey.OCCURRENCE_SET_PATH
                    ] = prj.occ_layer.get_dlocation()
                projection_mask = {
                    RegistryKey.METHOD: MaskMethod.BLANK_MASK,
                    RegistryKey.TEMPLATE_LAYER_PATH: prj.proj_scenario.layers[
                        0].get_dlocation()
                }
            else:
                model_mask = None
                projection_mask = None

            scale_parameters = multiplier = None
            if prj.is_att():
                scale_parameters = (
                    SCALE_PROJECTION_MINIMUM, SCALE_PROJECTION_MAXIMUM)
                # TODO: This should be in config somewhere
                multiplier = None

            sweep_config.add_projection(
                prj.process_type, prj.get_id(), prj.get_occ_layer_id(),
                prj.occ_layer.get_dlocation(),
                alg, prj.model_scenario, prj.proj_scenario,
                prj.get_dlocation(), prj.get_proj_package_filename(),
                model_mask=model_mask, projection_mask=projection_mask,
                scale_parameters=scale_parameters, multiplier=multiplier)

        for mtx_col in mtx_cols:
            pav_filename = os.path.join(
                'pavs', 'pav_{}{}'.format(
                    mtx_col.get_id(), LMFormat.MATRIX.ext))
            # TODO: Make sure this works as expected, was refrencing 'prj'
            #    form previous loop, changed to .layer property of mtx_col
            sweep_config.add_pav_intersect(
                mtx_col.shapegrid.get_dlocation(), mtx_col.get_id(),
                mtx_col.layer.get_id(), pav_filename, mtx_col.layer.squid,
                mtx_col.intersect_params[
                    mtx_col.INTERSECT_PARAM_MIN_PRESENCE],
                mtx_col.intersect_params[
                    mtx_col.INTERSECT_PARAM_MAX_PRESENCE],
                mtx_col.intersect_params[
                    mtx_col.INTERSECT_PARAM_MIN_PERCENT])
        return sweep_config

    # ....................................
    def stop_walken(self):
        """Stop walking configured data."""
        if not self.weapon_of_choice.complete:
            self.log.info('Christopher, stop walken')
            self.log.info('Saving next start {} ...'.format(self.next_start))
            self.save_next_start()
            self.weapon_of_choice.close()
        else:
            self.log.info('Christopher is done walken')

    # ....................................
    def _find_or_insert_intersect(self, prj, mtx, curr_time):
        """Initialize model, projections for inputs/algorithm."""
        mtx_col = None
        if prj is not None:
            # TODO: Save process_type into the DB??
            if LMFormat.is_gdal(driver=prj.data_format):
                ptype = ProcessType.INTERSECT_RASTER
            else:
                ptype = ProcessType.INTERSECT_VECTOR

            tmp_col = MatrixColumn(
                None, mtx.get_id(), self.user_id, layer=prj,
                shapegrid=self.boom_gridset.get_shapegrid(),
                intersect_params=self.intersect_params, squid=prj.squid,
                ident=prj.ident, process_type=ptype, metadata={},
                matrix_column_id=None, post_to_solr=True,
                status=JobStatus.GENERAL, status_mod_time=curr_time)
            mtx_col = self._scribe.find_or_insert_matrix_column(tmp_col)
            if mtx_col is not None:
                self.log.debug(
                    'Found/inserted MatrixColumn {}'.format(mtx_col.get_id()))

                # Reset process_type, shapegrid obj (not in db)
                mtx_col.process_type = ptype
                mtx_col.shapegrid = self.boom_gridset.get_shapegrid()

        return mtx_col

    # ....................................
    def _do_reset(self, status, status_mod_time):
        return any(
            [JobStatus.incomplete(status), JobStatus.failed(status),
             (
                 status == JobStatus.COMPLETE and
                 status_mod_time < self.weapon_of_choice.expiration_date
                 )])

    # ....................................
    def _find_or_insert_sdm_project(self, occ, alg, prj_scen, curr_time):
        prj = None
        if occ is not None:
            tmp_prj = SDMProjection(
                occ, alg, self.mdl_scen, prj_scen,
                data_format=LMFormat.GTIFF.driver,
                status=JobStatus.GENERAL, status_mod_time=curr_time)
            prj = self._scribe.find_or_insert_sdm_project(tmp_prj)
            if prj is not None:
                self.log.debug(
                    'Found/inserted SDMProject {}'.format(prj.get_id()))
                # Fill in projection with input scenario layers, masks
                prj._model_scenario = self.mdl_scen
                prj._proj_scenario = prj_scen
        return prj

    # ....................................
    def _write_done_walken_file(self):
        try:
            with open(self.walked_archive_fname, 'w', 
                      encoding=ENCODING) as out_file:
                out_file.write(
                    '# Completed walking species input {}\n'.format(
                        self.weapon_of_choice.input_filename))
                out_file.write('# From config file {}\n'.format(
                    self.config_fname))
                out_file.write('# Full logs in {}\n'.format(
                    self.log.base_fname))
        except Exception:
            self.log.error(
                'Failed to write doneWalken file {} for config {}'.format(
                    self.walked_archive_fname, self.config_fname))

"""This module contains a class for processing parameter sweep configurations
"""
from hashlib import md5
import json
import os

from LmBackend.common.lmconstants import MaskMethod, RegistryKey
from LmCommon.common.lmconstants import LMFormat, ProcessType, ENCODING
from LmCommon.common.ready_file import ready_filename

# Local dictionary keys for determining the type of masks to create
DO_ASCII = 'do_ascii'
DO_TIFF = 'do_tiff'

# Local filename constants.  The final path is accessible through an attribute.
LOG_FILENAME = 'species.log'
METRICS_FILENAME = 'metrics.txt'
PAVS_FILENAME = 'pavs.json'
SNIPPETS_FILENAME = 'snippets.xml'
STOCKPILE_FILENAME = 'stockpile.json'


# .............................................................................
class ParameterSweepConfiguration:
    """This class handles configuration of a parameter sweep
    """

    # ........................................
    def __init__(self, work_dir=''):
        """Constructor
        """
        self.work_dir = work_dir
        self.masks = {}
        self.models = {}
        self.occurrence_sets = []
        self.pavs = []
        self.projections = []
        self.log_filename = os.path.join(self.work_dir, LOG_FILENAME)
        self.metrics_filename = os.path.join(self.work_dir, METRICS_FILENAME)
        self.snippets_filename = os.path.join(self.work_dir, SNIPPETS_FILENAME)
        self.stockpile_filename = os.path.join(
            self.work_dir, STOCKPILE_FILENAME)
        self.pavs_filename = os.path.join(self.work_dir, PAVS_FILENAME)

    # ........................................
    @classmethod
    def load(cls, fn_or_flo):
        """Loads a file or file like object

        Args:
            fn_or_flo : A file name or a file-like object containing
                configuration information in JSON format.
        """
        try:
            config = json.load(fn_or_flo)
        except AttributeError:
            # Try to load as if argument is a file name
            with open(fn_or_flo, 'r', encoding=ENCODING) as in_file:
                config = json.load(in_file)
        my_obj = cls(work_dir=config[RegistryKey.WORK_DIR])
        my_obj.masks = config[RegistryKey.MASK]
        my_obj.models = config[RegistryKey.MODEL]
        my_obj.occurrence_sets = config[RegistryKey.OCCURRENCE]
        my_obj.pavs = config[RegistryKey.PAV]
        my_obj.projections = config[RegistryKey.PROJECTION]
        # my_obj.work_dir = config[RegistryKey.WORK_DIR]
        # my_obj.log_filename = os.path.join(my_obj.work_dir, LOG_FILENAME)
        # my_obj.metrics_filename = os.path.join(
        #    my_obj.work_dir, METRICS_FILENAME)
        # my_obj.snippets_filename = os.path.join(
        #    my_obj.work_dir, SNIPPETS_FILENAME)
        # my_obj.stockpile_filename = os.path.join(
        #    my_obj.work_dir, STOCKPILE_FILENAME)
        # my_obj.pavs_filename = os.path.join(my_obj.work_dir, PAVS_FILENAME)
        return my_obj

    # ........................................
    @staticmethod
    def _process_algorithm(algorithm):
        """Generates an identifier and JSON for an algorithm

        Args:
            algorithm : An algorithm object
        """
        # This is a list of algorithm information that will be used for hashing
        #    to create an identifier
        algorithm_info = [algorithm.code]
        # This is the object that will be returned
        algorithm_object = {
            RegistryKey.ALGORITHM_CODE: algorithm.code,
            RegistryKey.PARAMETER: []
        }

        for param in algorithm.parameters.keys():
            algorithm_object[RegistryKey.PARAMETER].append(
                {
                    RegistryKey.NAME: param,
                    RegistryKey.VALUE: algorithm.parameters[param]
                })
            algorithm_info.append((param, str(algorithm.parameters[param])))

        identifier = md5(str(set(algorithm_info))).hexdigest()[:16]

        return identifier, algorithm_object

    # ........................................
    def _process_mask(self, mask_info, ext):
        """Gererates masking configuration and returns an identifier

        Args:
            mask_info : A dictionary of mask parameters
            ext : A file extension for the mask, used to determine if we should
                create an ASCII or TIFF mask.
        """
        # Determine mask id
        mask_id = None
        if mask_info is not None:
            mask_id = md5(
                str(set((k, val) for k, val in mask_info.items()))
                ).hexdigest()[:16]
            # See if the mask has been defined, if not, add it
            if mask_id not in self.masks.keys():
                self.masks[mask_id] = mask_info
                self.masks[mask_id][
                    RegistryKey.PATH] = os.path.join(
                        self.work_dir, 'masks', mask_id)
            if ext == LMFormat.MXE.ext:
                self.masks[mask_id][DO_ASCII] = True
            elif ext == LMFormat.GTIFF.ext:
                self.masks[mask_id][DO_TIFF] = True

        return mask_id

    # ........................................
    @staticmethod
    def _process_scenario(scenario, ext):
        """Generates an identifier and JSON for scenario layers

        Args:
            scenario : A Scenario object populated with layers
            ext : The raster extension to use for layers (.tif or .asc)
        """
        identifier = 'scenario_{}_{}'.format(scenario.get_id(), ext)
        layer_obj = {RegistryKey.LAYER: []}

        for lyr in scenario.layers:
            layer_obj[RegistryKey.LAYER].append(
                {
                    RegistryKey.IDENTIFIER: 'layer-{}'.format(lyr.get_id()),
                    RegistryKey.PATH: '{}{}'.format(os.path.splitext(
                        lyr.get_dlocation())[0], ext)})
        return identifier, layer_obj

    # ........................................
    def add_occurrence_set(self, process_type, occ_set_id, url_fn_or_key,
                           out_filename, big_out_filename, max_points,
                           metadata=None, delimiter=None):
        """Adds an occurrence set configuration to the parameter sweep

        Args:
            process_type : The ProcessType of the occurrence set to create.
            occ_set_id : The database ID of the occurrence set to create.
            url_fn_or_key : Either a URL to pull points from, a CSV file with
                point data, or a taxon key to use in an API query.  The one to
                provide is dependent on the ProcessType.
            out_filename : A file location to store the resulting processed
                occurrence data.
            big_out_filename : A file location to store the complete processed
                occurrence data if there are more than max_points points.
            max_points : The maximum number of points to store in the regular
                occurrence set output file.
            metadata : Optional.  This should be provided with user occurrence
                sets to describe the fields in the CSV.
            delimiter : Optional.  Use this as the delimiter for the csv data.
        """
        add_me = True  # Should we add this occurrence set config
        # Check if occurrence set is already in the config
        for occ_info in self.occurrence_sets:
            if occ_set_id == occ_info[1]:
                add_me = False
        if add_me:
            self.occurrence_sets.append(
                (process_type, occ_set_id, url_fn_or_key, out_filename,
                 big_out_filename, max_points, metadata, delimiter))

    # ........................................
    def add_pav_intersect(self, shapegrid_filename, pav_id, projection_id,
                          pav_filename, squid, min_presence, max_presence,
                          min_coverage):
        """Adds a presence absence vector configuration to the parameter sweep

        Args:
            shapegrid_filename : The file location of the shapegrid to use for
                intersecting the projection.
            pav_id : The identifier of this PAV or matrix column.
            projection_id : The identifier of the projection to be intersected
                for this PAV.
            pav_filename : The file path to store this PAV.
            squid : A species identifier for this PAV that will be used as a
                column header in the resulting vector (single column matrix).
            min_presence : The minimum value in the projection that should be
                considered "present".
            max_presence : The maximum value in the projection that should be
                considered "present".
            min_coverage : The minimum proportion of each shapegrid cell that
                must be classified as "present" to determine that the cell is
                present.
        """
        self.pavs.append(
            [shapegrid_filename, pav_id, projection_id, pav_filename, squid,
             min_presence, max_presence, min_coverage])

    # ........................................
    def add_projection(self, process_type, projection_id, occ_set_id,
                       occ_shp_filename,
                       algorithm, model_scenario, projection_scenario,
                       projection_path, package_path, model_mask=None,
                       projection_mask=None, scale_parameters=None,
                       multiplier=None):
        """Adds a projection (and dependencies) through the configuration

        Args:
            process_type : A ProcessType value for the projection
            projection_id : An identifier for this projection
            occ_set_id : An identifier for the occurrence set to use when
                generating the model for this projection.
            algorithm_parameters : A Algorithm object used to create the model
                and projection.
            model_scenario : A Scenario object, with populated layers, used to
                generate the model to use for this projection.
            projection_scenario : A Scenario object, with populated layers, to
                use for projecting the model.
            projection_path : This is the final file location to store the
                generated projection.
            package_path : This is the final file location to store the output
                package from the projection computation.
            model_mask : Optional. A dictionary of model mask parameters.
            projection_mask : Optional. A dictionary of projection mask
                parameters.
            scale_parameters: Optional. If provided, should be a minimum and a
                maximum value for the projection to be scaled to.
            multiplier : Optional. If provided, raw projection values will be
                multiplied by this value before being converted.  Useful for
                reducing data size by converting a float layer into integer.
        """
        # Process algorithm
        algorithm_identifier, algo = self._process_algorithm(algorithm)
        if algo[RegistryKey.ALGORITHM_CODE].lower() == 'att_maxent':
            ext = LMFormat.MXE.ext
        else:
            ext = LMFormat.GTIFF.ext

        # Process model and projection scenarios
        mdl_scn_id, mdl_scn = self._process_scenario(model_scenario, ext)
        prj_scn_id, prj_scn = self._process_scenario(projection_scenario, ext)

        model_id = '{}-{}-{}'.format(
            occ_set_id, algorithm_identifier, mdl_scn_id)
        # Process masks
        mdl_mask_id = self._process_mask(model_mask, ext)

        prj_mask_id = self._process_mask(projection_mask, ext)

        # Check if model has been defined and define if necessary
        if model_id not in self.models.keys():
            if int(process_type) == ProcessType.ATT_PROJECT:
                mdl_process_type = ProcessType.ATT_MODEL
                mdl_ruleset_path = os.path.join(
                    self.work_dir, model_id, '{}_ruleset{}'.format(
                        model_id, LMFormat.TXT.ext))
            elif int(process_type) == ProcessType.OM_PROJECT:
                mdl_process_type = ProcessType.OM_MODEL
                mdl_ruleset_path = os.path.join(
                    self.work_dir, model_id, '{}_ruleset{}'.format(
                        model_id, LMFormat.XML.ext))
            else:
                raise Exception(
                    'Cannot process process type: {}, {}'.format(
                        process_type, type(process_type)))
            self.models[model_id] = {
                RegistryKey.PROCESS_TYPE: mdl_process_type,
                RegistryKey.OCCURRENCE_SET_ID: occ_set_id,
                RegistryKey.OCCURRENCE_SET_PATH: occ_shp_filename,
                RegistryKey.ALGORITHM: algo,
                RegistryKey.SCENARIO: mdl_scn,
                RegistryKey.MASK_ID: mdl_mask_id,
                RegistryKey.RULESET_PATH: mdl_ruleset_path
            }

        # Check if model and projection scenarios match, if so, update model
        if mdl_scn_id == prj_scn_id:
            self.models[model_id].update({
                RegistryKey.PROJECTION_ID: projection_id,
                RegistryKey.SCALE_PARAMETERS: scale_parameters,
                RegistryKey.MULTIPLIER: multiplier,
                RegistryKey.PROJECTION_PATH: projection_path,
                RegistryKey.PACKAGE_PATH: package_path
            })
        else:
            self.projections.append([
                process_type, projection_id, model_id, algo, prj_scn,
                projection_path, package_path, prj_mask_id, scale_parameters,
                multiplier])

    # ........................................
    def get_input_files(self):
        """Returns a list of input files required for for this configuration.

        Returns a list of input files required for this configuration.  These
        should be input files that would need to be added to a workspace for
        remote computations and not intermediate products.
        """
        input_files = set([])

        # Mask layers
        for mask_id in self.masks.keys():
            mask_config = self.masks[mask_id]
            if mask_config[RegistryKey.METHOD
                           ] == MaskMethod.HULL_REGION_INTERSECT:
                input_files.add(mask_config[RegistryKey.REGION_LAYER_PATH])
            elif mask_config[RegistryKey.METHOD] == MaskMethod.BLANK_MASK:
                input_files.add(mask_config[RegistryKey.TEMPLATE_LAYER_PATH])

        # Occurrence set inputs
        for occ_config in self.occurrence_sets:
            # occ_config = (process_type, occ_set_id, url_fn_or_key, out_file,
            #                big_out_file, max_points, metadata)
            (process_type, _, url_fn_or_key, _, _, _, _, _) = occ_config
            # If GBIF or user, add the input csv file
            if process_type in [ProcessType.GBIF_TAXA_OCCURRENCE,
                                ProcessType.USER_TAXA_OCCURRENCE]:
                input_files.add(url_fn_or_key)

        # Shapegrids
        for pav_config in self.pavs:
            # pav_config = (shapegrid filename, pav id, projection id,
            #                pav filename, squid, min presence, max presence
            #                min coverage)
            input_files.add(pav_config[0])

        # Only return relative paths.  Makeflow doesn't want absolute paths.
        relative_input_files = [
            fn for fn in list(input_files
                              ) if fn is not None and not os.path.isabs(fn)]
        return relative_input_files

    # ........................................
    def get_mask_config(self):
        """Returns a generator for mask creation
        """
        for mask_id in self.masks.keys():
            mask_config = self.masks[mask_id]
            method = mask_config[RegistryKey.METHOD]
            mask_basename = mask_config[RegistryKey.PATH]

            if DO_ASCII in mask_config.keys():
                do_asc = mask_config[DO_ASCII]
            else:
                do_asc = False

            if DO_TIFF in mask_config.keys():
                do_tif = mask_config[DO_TIFF]
            else:
                do_tif = False

            # Always return: method, mask id, out basename, do ascii, do tiff
            config_list = [method, mask_id, mask_basename, do_asc, do_tif]

            # Individual masking methods
            if method == MaskMethod.HULL_REGION_INTERSECT:
                # Add region layer, buffer, and occurrence set id
                config_list.append(mask_config[RegistryKey.REGION_LAYER_PATH])
                config_list.append(mask_config[RegistryKey.BUFFER])
                config_list.append(
                    mask_config[RegistryKey.OCCURRENCE_SET_PATH])
            elif method == MaskMethod.BLANK_MASK:
                # Add template layer path for blank mask creation
                config_list.append(
                    mask_config[RegistryKey.TEMPLATE_LAYER_PATH])

            yield config_list

    # ........................................
    def get_model_config(self):
        """Returns a generator for model creation
        """
        for model_id in self.models.keys():
            # Always return: process type, model id, occurrence set id,
            #    algorithm, model scenario, and mask id
            # Always: process_type, model_id, occ_set_id, algorithm,
            #    model_scenario, mask_id
            model_config = [
                self.models[model_id][RegistryKey.PROCESS_TYPE],
                model_id,
                self.models[model_id][RegistryKey.OCCURRENCE_SET_ID],
                self.models[model_id][RegistryKey.OCCURRENCE_SET_PATH],
                self.models[model_id][RegistryKey.ALGORITHM],
                self.models[model_id][RegistryKey.SCENARIO],
                self.models[model_id][RegistryKey.MASK_ID],
                self.models[model_id][RegistryKey.RULESET_PATH]
            ]

            # If we are projecting onto the same scenario, these keys will be
            #    present
            try:
                # Projection id, scaling parameters, multiplier
                model_config.append(
                    self.models[model_id][RegistryKey.PROJECTION_ID])
                model_config.append(
                    self.models[model_id][RegistryKey.PROJECTION_PATH])
                model_config.append(
                    self.models[model_id][RegistryKey.PACKAGE_PATH])
                model_config.append(
                    self.models[model_id][RegistryKey.SCALE_PARAMETERS])
                model_config.append(
                    self.models[model_id][RegistryKey.MULTIPLIER])
            except KeyError:
                # Should not project onto this scenario
                pass

            yield model_config

    # ........................................
    def get_occurrence_set_config(self):
        """Returns a generator for occurrence set configurations

        Note:
            * Returns a generator for consistency with other methods.
        """
        for occ_config in self.occurrence_sets:
            # occ_config = (process_type, occ_set_id, url_fn_or_key, out_file,
            #                big_out_file, max_points, metadata)
            yield occ_config

    # ........................................
    def get_output_files(self, work_dir=None):
        """Returns a list of files that will be generated by this configuration

        Returns a list of all of the files that should be generated by a
        parameter sweep that is given this configuration.

        Note:
            * These are only the files that would not be written to their final
                location.
            * This includes metrics, snippets, stockpile information, and PAVs
        """
        output_files = [
            self.metrics_filename,
            self.snippets_filename,
            self.stockpile_filename,
            self.pavs_filename]

        if work_dir is None:
            return output_files

        new_output_files = []
        for file_name in output_files:
            new_output_files.append(os.path.join(work_dir, file_name))
        return new_output_files

    # ........................................
    def get_pav_config(self):
        """Returns a generator for presence absence vector creation
        """
        for pav_config in self.pavs:
            # pav_config = (shapegrid filename, pav id, projection id,
            #                pav filename, squid, min presence, max presence
            #                min coverage)
            yield pav_config

    # ........................................
    def get_projection_config(self):
        """Returns a generator for projection creation

        Note:
            * Returns a generator for consistency with other methods.
        """
        for prj_config in self.projections:
            # prj_config = (process type, projection id, model id, algorithm,
            #                projection scenario, mask id, scale parameters,
            #                multiplier)
            yield prj_config

    # ........................................
    def save_config(self, fn_or_flo):
        """Saves the configuration to a file path or file like object

        Args:
            fn_or_flo : A file name or a file-like object where the
                configuration should be saved in JSON format.
        """
        config = {
            RegistryKey.MASK: self.masks,
            RegistryKey.MODEL: self.models,
            RegistryKey.OCCURRENCE: self.occurrence_sets,
            RegistryKey.PAV: self.pavs,
            RegistryKey.PROJECTION: self.projections,
            RegistryKey.WORK_DIR: self.work_dir
        }

        if not hasattr(fn_or_flo, 'write'):
            ready_filename(fn_or_flo, overwrite=True)
            with open(fn_or_flo, 'w', encoding=ENCODING) as out_f:
                json.dump(config, out_f)
        else:
            json.dump(config, fn_or_flo)

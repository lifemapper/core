"""This module contains methods for performing a single species parameter sweep

Note:
    * Debug instructions:
        Run a parameter sweep for a species by calling this script with the
            species configuration JSON filename as the argument.  This script
            performs all of the computations and prepares files that are used
            for stockpiling all of the outputs at once.  Be aware that running
            this script will put the generated output files in the locations
            specified by the configuration and that will probably be their
            final location in a shared data space.

Todo:
    * CJ - Add a debugging or "dry run" mode that does not move files to their
        final location
"""
import json
import os
from time import sleep

from osgeo import ogr

import LmBackend.common.layer_tools as layer_tools
from LmBackend.common.lmconstants import MaskMethod, RegistryKey
from LmCommon.common.lmconstants import (
    ProcessType, JobStatus, LMFormat, ENCODING)
from LmCommon.common.ready_file import ready_filename
from LmCommon.compression.binary_list import compress
from LmCommon.encoding.layer_encoder import LayerEncoder
from LmCompute.common.log import LmComputeLogger
import LmCompute.plugins.single.mask.create_mask as create_mask
from LmCompute.plugins.single.modeling.maxent import MaxentWrapper
from LmCompute.plugins.single.modeling.openModeller import OpenModellerWrapper
from LmCompute.plugins.single.occurrences.csv_occ import (
    create_shapefile_from_csv)

WAIT_THRESHOLD = 60


# .............................................................................
class ParameterSweep:
    """This class performs a parameter sweep for a single species
    """

    # ........................................
    def __init__(self, sweep_config, base_work_dir='.', pavs_only=False,
                 pedantic_mode=False):
        """Constructor

        Args:
            sweep_config : A parameter sweep configuration object used to
                determine what should be done.
            base_work_dir (str): A base work directory for all computations to
                be run within.  The sweep config work directory would then be
                a sub directory of this one.
            pavs_only (:obj:`bool`): If true, only compute PAVs and skip the
                other computations.
            pedantic_mode (:obj: `bool`) : If true, raise every exception to
                calling method
        """
        self.sweep_config = sweep_config
        self.base_work_dir = base_work_dir
        self.work_dir = os.path.join(base_work_dir, self.sweep_config.work_dir)
        log_filename = os.path.join(
            base_work_dir, self.sweep_config.log_filename)
        ready_filename(log_filename)

        log_name = os.path.basename(self.work_dir)
        self.log = LmComputeLogger(
            log_name, add_console=True, add_file=True,
            log_filename=log_filename)

        # Note: The registry is a place for registering outputs
        self.registry = {}
        self.pavs = []
        self.pedantic = pedantic_mode
        self.pavs_only = pavs_only

    # ........................................
    def _create_masks(self):
        """Creates masks from configuration
        """
        for mask_config in self.sweep_config.get_mask_config():
            mask_method = mask_config[0]
            asc_filename = tif_filename = None

            (mask_method, mask_id, out_mask_base_filename, do_ascii, do_tiff
             ) = mask_config[:5]
            out_mask_base_filename = os.path.join(
                self.base_work_dir, out_mask_base_filename)
            if do_ascii:
                asc_filename = '{}{}'.format(
                    out_mask_base_filename, LMFormat.ASCII.ext)
            if do_tiff:
                tif_filename = '{}{}'.format(
                    out_mask_base_filename, LMFormat.GTIFF.ext)

            try:
                if mask_method == MaskMethod.HULL_REGION_INTERSECT:
                    (region_layer_filename, buffer_distance, occ_shp_filename
                     ) = mask_config[5:]
                    # Prepend base directory
                    region_layer_filename = os.path.join(
                        self.base_work_dir, region_layer_filename)
                    # Get occurrence set shapefile
                    if not os.path.exists(occ_shp_filename):
                        occ_status = JobStatus.NOT_FOUND
                    else:
                        occ_status = JobStatus.COMPUTED
                    # occ_shp_filename, occ_status = self._get_registry_output(
                    #    RegistryKey.OCCURRENCE, occ_set_id)
                    if occ_status < JobStatus.GENERAL_ERROR and \
                            occ_shp_filename:
                        create_mask.create_convex_hull_region_intersect_mask(
                            occ_shp_filename, out_mask_base_filename,
                            region_layer_filename, buffer_distance,
                            ascii_filename=asc_filename,
                            tiff_filename=tif_filename)
                elif mask_method == MaskMethod.BLANK_MASK:
                    template_layer_filename = mask_config[5]
                    create_mask.create_blank_mask_from_layer(
                        template_layer_filename, ascii_filename=asc_filename,
                        tiff_filename=tif_filename)
                else:
                    self.log.error(
                        'Could not create mask for method: {}'.format(
                            mask_method))
                status = JobStatus.COMPUTED
                if asc_filename is not None:
                    if not os.path.exists(asc_filename):
                        status = JobStatus.GENERAL_ERROR
                if tif_filename is not None:
                    if not os.path.exists(tif_filename):
                        status = JobStatus.GENERAL_ERROR
            except Exception as err:
                self._process_error(
                    err, msg='Could not create mask {} : {}'.format(
                        mask_id, str(err)))
                status = JobStatus.MASK_ERROR

            # TODO: Consider adding rasters as secondary outputs
            # TODO: Add metrics and snippets
            self._register_output_object(
                RegistryKey.MASK, mask_id, status, out_mask_base_filename)

    # ........................................
    def _create_models(self):
        """Create models from configuration

        Todo:
            * Get species name from somewhere.
            * Get CRS_WKT from somewhere.
        """
        for mdl_config in self.sweep_config.get_model_config():
            if len(mdl_config) == 13:
                (process_type, model_id, _occ_set_id, occ_shp_filename,
                 algorithm, model_scenario, mask_id, mdl_ruleset_path,
                 projection_id, projection_path, package_path, scale_params,
                 multiplier) = mdl_config
            else:
                (process_type, model_id, _occ_set_id, occ_shp_filename,
                 algorithm, model_scenario, mask_id, mdl_ruleset_path
                 ) = mdl_config
                projection_id = projection_path = package_path = None
                scale_params = multiplier = None

            mdl_ruleset_path = os.path.join(
                self.base_work_dir, mdl_ruleset_path)
            occ_cont = True
            mask_cont = True
            mdl_metrics = None
            mdl_snippets = None

            # Get occurrence set shapefile, sleep 3 seconds to allow
            #    file to exist on front end as it may take a bit to
            #    sync
            sleep(3)
            if not os.path.exists(occ_shp_filename):
                occ_status = JobStatus.NOT_FOUND
            else:
                occ_status = JobStatus.COMPUTED
            # occ_shp_filename, occ_status = self._get_registry_output(
            #    RegistryKey.OCCURRENCE, occ_set_id)
            # We can only compute if occurrence set was created successfully
            if occ_status >= JobStatus.GENERAL_ERROR:
                occ_cont = False

            if mask_id is not None:
                mask_filename_base, mask_status = self._get_registry_output(
                    RegistryKey.MASK, mask_id)
                # We can only compute if (needed) mask was created successfully
                if mask_status >= JobStatus.GENERAL_ERROR:
                    mask_cont = False
            else:
                mask_filename_base = None

            # We can only continue if occurrence set and mask (if needed) were
            #    created successfully
            if occ_cont and mask_cont:
                # Get points
                points = self._get_model_points(occ_shp_filename)
                work_dir = os.path.join(self.work_dir, model_id)
                ready_filename(work_dir)

                # TODO(CJ): Get species name and CRS WKT from somewhere
                species_name = 'species'
                crs_wkt = None

                # If Maxent
                if process_type in [
                        ProcessType.ATT_MODEL, ProcessType.ATT_PROJECT]:

                    mask_filename = None
                    if mask_filename_base is not None:
                        mask_filename = '{}{}'.format(
                            mask_filename_base, LMFormat.ASCII.ext)
                        print(('Mask file name: {}'.format(mask_filename)))
                    wrapper = MaxentWrapper(
                        work_dir, species_name, logger=self.log)
                    wrapper.create_model(
                        points, model_scenario, algorithm,
                        mask_filename=mask_filename, crs_wkt=crs_wkt)

                    # Get outputs
                    status = wrapper.get_status()
                    if status < JobStatus.GENERAL_ERROR:
                        wrapper.copy_ruleset(mdl_ruleset_path, overwrite=True)

                        # Append log
                        if os.path.exists(wrapper.get_log_filename()):
                            with open(wrapper.get_log_filename(), 
                                      encoding=ENCODING) as log_f:
                                self.log.debug('-----------------------------')
                                self.log.debug(wrapper.get_log_filename())
                                self.log.debug('-----------------------------')
                                self.log.debug(log_f.read())

                        mdl_metrics = wrapper.get_metrics()

                    # Get / process projection
                    if projection_id is not None:
                        # Only convert if success, else we'll register failure
                        if status < JobStatus.GENERAL_ERROR:
                            try:
                                raw_prj_filename = \
                                    wrapper.get_projection_filename()
                                # Convert layer and scale layer
                                layer_tools.convert_and_modify_ascii_to_tiff(
                                    raw_prj_filename, projection_path,
                                    scale=scale_params, multiplier=multiplier)
                                wrapper.get_output_package(
                                    package_path, overwrite=True)
                            except ZeroDivisionError as zde:
                                msg = ('Could not get projection for model. '
                                       'Projection was blank')
                                self._process_error(zde, msg=msg)
                                status = JobStatus.BLANK_PROJECTION_ERROR
                        # Use same secondary outputs as model and register
                        self._register_output_object(
                            RegistryKey.PROJECTION, projection_id, status,
                            projection_path,
                            process_type=ProcessType.ATT_PROJECT,
                            metrics=mdl_metrics, snippets=mdl_snippets)

                # If openModeller
                elif process_type in [ProcessType.OM_MODEL,
                                      ProcessType.OM_PROJECT]:
                    mask_filename = None
                    if mask_filename_base is not None:
                        mask_filename = '{}{}'.format(
                            mask_filename_base, LMFormat.GTIFF.ext)
                    wrapper = OpenModellerWrapper(
                        work_dir, species_name, logger=self.log)
                    wrapper.create_model(
                        points, model_scenario, algorithm,
                        mask_filename=mask_filename, crs_wkt=crs_wkt)

                    # Get outputs
                    status = wrapper.get_status()
                    if status < JobStatus.GENERAL_ERROR:
                        wrapper.copy_ruleset(mdl_ruleset_path, overwrite=True)

                        # Append log
                        with open(wrapper.get_log_filename(), 
                                  encoding=ENCODING) as log_f:
                            self.log.debug('---------------------------------')
                            self.log.debug(wrapper.get_log_filename())
                            self.log.debug('---------------------------------')
                            self.log.debug(log_f.read())

                        # Generate projection for openModeller model
                        wrapper.create_projection(
                            wrapper.get_ruleset_filename(), model_scenario,
                            algorithm, mask_filename)

                        mdl_metrics = wrapper.get_metrics()

                        if projection_id is not None:

                            status = wrapper.get_status()
                            if status < JobStatus.GENERAL_ERROR:
                                # Append log
                                with open(wrapper.get_log_filename(), 
                                          encoding=ENCODING) as log_f:
                                    self.log.debug(
                                        '-----------------------------')
                                    self.log.debug('Projection log')
                                    self.log.debug(wrapper.get_log_filename())
                                    self.log.debug(
                                        '-----------------------------')
                                    self.log.debug(log_f.read())
                                # Move raster
                                wrapper.copy_projection(
                                    projection_path, overwrite=True)
                                # Package
                                wrapper.get_output_package(
                                    package_path, overwrite=True)
                                prj_metrics = wrapper.get_metrics()
                                prj_snippets = None
                                # Use same secondary outputs as model and
                                #    register
                                self._register_output_object(
                                    RegistryKey.PROJECTION, projection_id,
                                    status, projection_path,
                                    process_type=ProcessType.OM_PROJECT,
                                    metrics=prj_metrics, snippets=prj_snippets)

                # If other
                else:
                    status = JobStatus.UNKNOWN_ERROR
                    self.log.error(
                        'Unknown process type: {} for model {}'.format(
                            process_type, model_id))
            else:
                status = max([occ_status, mask_status])

            # Register model output
            self._register_output_object(
                RegistryKey.MODEL, model_id, status, mdl_ruleset_path,
                metrics=mdl_metrics, snippets=mdl_snippets)

    # ........................................
    def _create_occurrence_sets(self):
        """Creates occurrence sets from configuration

        Todo:
            * Generate metrics
            * Generate snippets
            * Do we need a work directory?
        """
        for occ_config in self.sweep_config.get_occurrence_set_config():
            (process_type, occ_set_id, url_fn_or_key, out_file, big_out_file,
             max_points, metadata, delimiter) = occ_config

            occ_metrics = None
            occ_snippets = None

            if delimiter is None:
                delimiter = '\t'
            is_gbif = False
            if process_type in (ProcessType.USER_TAXA_OCCURRENCE,
                                ProcessType.GBIF_TAXA_OCCURRENCE):
                if process_type == ProcessType.GBIF_TAXA_OCCURRENCE:
                    is_gbif = True
                status = create_shapefile_from_csv(
                    url_fn_or_key, metadata, out_file, big_out_file,
                    max_points, is_gbif=is_gbif, log=self.log,
                    delimiter=str(delimiter))
                if status == JobStatus.COMPUTED:
                    waited = 0
                    while waited < WAIT_THRESHOLD and not os.path.exists(
                            out_file):
                        waited += 1
                        sleep(1)

                    if not os.path.exists(out_file):
                        self.log.error(
                            'Occurrence set not present: {}'.format(out_file))

            else:
                self.log.error(
                    'Unknown process type: {} for occurrence set: {}'.format(
                        process_type, occ_set_id))
                status = JobStatus.UNKNOWN_ERROR
            self._register_output_object(
                RegistryKey.OCCURRENCE, occ_set_id, status, out_file,
                secondary_outputs=[big_out_file], process_type=process_type,
                metrics=occ_metrics, snippets=occ_snippets)

    # ........................................
    def _create_pavs(self):
        """Create presence absence vectors from configuration

        Todo:
            * Add metrics
            * Add snippets
        """
        for pav_config in self.sweep_config.get_pav_config():
            (shapegrid_filename, pav_id, projection_id, pav_filename, squid,
             min_presence, max_presence, min_coverage) = pav_config

            # Prepend a work directory if provided
            pav_filename = os.path.join(self.work_dir, pav_filename)
            ready_filename(pav_filename, overwrite=True)
            # TODO(CJ) : Consider if we can reuse the encoder
            encoder = LayerEncoder(shapegrid_filename)

            # Initialize status, only set to success if successful
            status = JobStatus.GENERAL_ERROR
            prj_filename, prj_status = self._get_registry_output(
                RegistryKey.PROJECTION, projection_id)
            if prj_filename is not None and \
                    prj_status < JobStatus.GENERAL_ERROR:
                try:
                    encoder.encode_presence_absence(
                        prj_filename, squid, min_presence, max_presence,
                        min_coverage)
                    pav = encoder.get_encoded_matrix()
                    if pav is not None:
                        status = JobStatus.COMPUTED
                        pav.write(pav_filename)
                except Exception as err:
                    self._process_error(
                        err, msg='Failed to encode PAV: {}'.format(str(err)))
                    status = JobStatus.ENCODING_ERROR
            else:
                if prj_status >= JobStatus.GENERAL_ERROR:
                    status = prj_status
            # Register output
            self._register_output_object(
                RegistryKey.PAV, pav_id, status, pav_filename,
                process_type=ProcessType.INTERSECT_RASTER)
            # If successful, add to index pavs list
            if status < JobStatus.GENERAL_ERROR:
                self.pavs.append(
                    {
                        RegistryKey.COMPRESSED_PAV_DATA: compress(pav),
                        RegistryKey.IDENTIFIER: pav_id,
                        RegistryKey.PROJECTION_ID: projection_id
                    })
            else:
                # Only do this on error so we catch failures that look like
                # successes
                # Touch file if it doesn't exist
                if not os.path.exists(pav_filename):
                    with open(pav_filename, 'a', encoding=ENCODING):
                        os.utime(pav_filename, None)

    # ........................................
    def _create_projections(self):
        """Create projections from configuration

        Todo:
            * Get species name from somewhere
            * Use projection path and package path
        """
        for prj_config in self.sweep_config.get_projection_config():

            (process_type, projection_id, model_id, algorithm,
             prj_scenario, projection_path, package_path, mask_id,
             scale_params, multiplier) = prj_config

            mask_cont = True
            prj_metrics = None
            prj_snippets = None

            # If this projection was already completed, skip
            prj_raster_filename, _ = self._get_registry_output(
                RegistryKey.PROJECTION, projection_id)
            if prj_raster_filename is None:

                if mask_id is not None:
                    (mask_filename_base, mask_status
                     ) = self._get_registry_output(RegistryKey.MASK, mask_id)
                    # mask_filename_base = os.path.join(
                    #    self.base_work_dir, mask_filename_base)
                    # We can only compute if (needed) mask was created
                    #    successfully
                    if mask_status >= JobStatus.GENERAL_ERROR:
                        mask_cont = False
                else:
                    mask_filename_base = None

                # We can only continue if mask (if needed) was created
                #    successfully
                if mask_cont:
                    # Get points
                    work_dir = os.path.join(self.work_dir, 'prj_{}'.format(
                        projection_id))
                    ready_filename(work_dir)

                    # TODO(CJ): Get species name from somewhere
                    species_name = 'species'

                    ruleset_filename, status = self._get_registry_output(
                        RegistryKey.MODEL, model_id)

                    # TODO(CJ): Check this
                    if status < JobStatus.GENERAL_ERROR:
                        # If Maxent
                        if process_type == ProcessType.ATT_PROJECT:

                            if mask_filename_base is not None:
                                mask_filename = '{}{}'.format(
                                    mask_filename_base, LMFormat.ASCII.ext)
                            else:
                                mask_filename = None
                            wrapper = MaxentWrapper(
                                work_dir, species_name, logger=self.log)
                            wrapper.create_projection(
                                ruleset_filename, prj_scenario, algorithm,
                                mask_filename)

                            # Get outputs
                            status = wrapper.get_status()
                            if status < JobStatus.GENERAL_ERROR:
                                try:
                                    raw_prj_filename = \
                                        wrapper.get_projection_filename()
                                    # Convert layer and scale layer
                                    layer_tools\
                                        .convert_and_modify_ascii_to_tiff(
                                            raw_prj_filename, projection_path,
                                            scale=scale_params,
                                            multiplier=multiplier)

                                    # Append log
                                    if os.path.exists(
                                            wrapper.get_log_filename()):
                                        with open(wrapper.get_log_filename(), 
                                                  encoding=ENCODING) as log_f:
                                            self.log.debug('-----------------')
                                            self.log.debug(
                                                wrapper.get_log_filename())
                                            self.log.debug('-----------------')
                                            self.log.debug(log_f.read())
                                    wrapper.get_output_package(
                                        package_path, overwrite=True)
                                    prj_metrics = wrapper.get_metrics()
                                except ZeroDivisionError as zde:
                                    msg = ('Could not convert to Tiff.  '
                                           'Divide by zero : empty projection')
                                    self._process_error(zde, msg=msg)
                                    status = JobStatus.BLANK_PROJECTION_ERROR

                        # If openModeller
                        elif process_type in [
                                ProcessType.OM_MODEL, ProcessType.OM_PROJECT]:
                            if mask_filename_base is not None:
                                mask_filename = '{}{}'.format(
                                    mask_filename_base, LMFormat.GTIFF.ext)
                            else:
                                mask_filename = None
                            wrapper = OpenModellerWrapper(
                                work_dir, species_name, logger=self.log)
                            wrapper.create_projection(
                                ruleset_filename, prj_scenario, algorithm,
                                mask_filename)

                            # Get outputs
                            status = wrapper.get_status()
                            if status < JobStatus.GENERAL_ERROR:
                                wrapper.copy_projection(
                                    projection_path, overwrite=True)

                                # Append log
                                if os.path.exists(wrapper.get_log_filename()):
                                    with open(wrapper.get_log_filename(), 
                                              encoding=ENCODING) as log_f:
                                        self.log.debug('---------------------')
                                        self.log.debug(
                                            wrapper.get_log_filename())
                                        self.log.debug('---------------------')
                                        self.log.debug(log_f.read())
                                wrapper.get_output_package(
                                    package_path, overwrite=True)
                                prj_metrics = wrapper.get_metrics()

                        # If other
                        else:
                            status = JobStatus.UNKNOWN_ERROR
                            self.log.error(
                                'Unknown process type: {} for proj {}'.format(
                                    process_type, projection_id))

                # Register model output
                self._register_output_object(
                    RegistryKey.PROJECTION, projection_id, status,
                    projection_path, process_type=process_type,
                    metrics=prj_metrics, snippets=prj_snippets)

    # ........................................
    @staticmethod
    def _get_model_points(occ_shp_filename):
        """Get minimal point csv to be used for modeling.

        Args:
            occ_shp_filename : The file location of a point shapefile.

        Note:
            Removes duplicate point locations
        """
        points = set([])
        drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
        dataset = drv.Open(occ_shp_filename, 0)
        lyr = dataset.GetLayer()

        for feature in lyr:
            geom = feature.GetGeometryRef()
            pt_geom = geom.GetPoint()
            points.add((pt_geom[0], pt_geom[1]))

        # Add identifiers and create list
        i = 0
        ret_points = []
        for x_coord, y_coord in points:
            ret_points.append((i, x_coord, y_coord))
            i += 1

        return ret_points

    # ........................................
    def _get_registry_output(self, object_type, object_id):
        """Gets the primary output filename and status for an object

        Args:
            object_type : The type of object to retrieve
            object_id : The id of the object to retrieve
        """
        ret_file = None
        status = JobStatus.GENERAL_ERROR
        try:
            obj = self.registry[object_type][object_id]
            status = obj[RegistryKey.STATUS]
            if status == JobStatus.COMPUTED:
                ret_file = obj[RegistryKey.PRIMARY_OUTPUT]
        except Exception as err:
            msg = 'Failed to get registry output: {}'.format(str(err))
            self._process_error(err, msg=msg)
            status = JobStatus.NOT_FOUND
        return ret_file, status

    # ........................................
    def _prepare_only_pavs(self):
        """Prepares the metadata objects to perform PAV computations.
        """
        # Loop through projections and register outputs
        for prj_config in self.sweep_config.get_projection_config():

            (process_type, projection_id, _, _, _, projection_path, _, _, _, _
             ) = prj_config

            self._register_output_object(
                RegistryKey.PROJECTION, projection_id, JobStatus.COMPUTED,
                projection_path, process_type=process_type)

    # ........................................
    def _process_error(self, err, msg=None, raise_err=False):
        """Processes an exception and stops if configured to do so

        Args:
            err (:obj: `Exception`): An exception (or subclass) thrown by one
                of the compute processes
            msg (:obj: `str`): An optional message to include with this error
                processing that will be sent to the logger
            raise_err (:obj: `bool`): If set to true, will raise error even if
                pedantic mode is off

        Note:
            If pedantic is set to true, we will raise the exception so that the
                calling method receives it, rather than processing it and
                moving on
        """
        if msg is not None:
            self.log.error(msg)
        else:
            self.log.error(str(err))
        if self.pedantic or raise_err:
            raise err

    # ........................................
    def _register_output_object(self, object_type, object_id, status,
                                primary_output, secondary_outputs=None,
                                process_type=None, metrics=None,
                                snippets=None):
        """Adds an output object to the registry

        Args:
            object_type : The type of object being added (should be one of the
                RegistryKey type values).
            object_id : An identifier for this object
            status : The status of this object (JobStatus value).
            primary_output : The primary output of the object. This file may be
                used for follow up computations.
            secondary_outputs : Optional. These are other output files
                generated by the computation.
            process_type : Optional. The process type of the operation used to
                generate this object.
            metrics : Optional. Metrics generated by the computation.
            snippets : Optional. Snippets generated by the computation.
        """
        if object_type not in list(self.registry.keys()):
            self.registry[object_type] = {}
        if metrics is not None:
            metrics = metrics.get_metrics_dictionary()
        self.registry[object_type][object_id] = {
            RegistryKey.IDENTIFIER: object_id,
            RegistryKey.METRICS: metrics,
            RegistryKey.PRIMARY_OUTPUT: primary_output,
            RegistryKey.PROCESS_TYPE: process_type,
            RegistryKey.SECONDARY_OUTPUTS: secondary_outputs,
            RegistryKey.SNIPPETS: snippets,
            RegistryKey.STATUS: status
        }

    # ........................................
    def get_metrics(self):
        """Generates a report of the metrics generated in this run
        """
        metrics = []
        for group_key in RegistryKey.group_keys():
            if group_key in list(self.registry.keys()):
                for object_id in list(self.registry[group_key].keys()):
                    obj_metrics = self.registry[
                        group_key][object_id][RegistryKey.METRICS]
                    if obj_metrics is not None:
                        metrics.append(obj_metrics)
        return metrics

    # ........................................
    def get_pav_info(self):
        """Generates a report of successfully generated PAV objects for index

        Returns:
            * A list of dictionaries for PAVs to index
        """
        return self.pavs

    # ........................................
    def get_snippets(self):
        """Generates a report of the snippets generated in this run
        """
        snippets = []
        for group_key in RegistryKey.group_keys():
            if group_key in list(self.registry.keys()):
                for object_id in list(self.registry[group_key].keys()):
                    obj_snippets = self.registry[
                        group_key][object_id][RegistryKey.SNIPPETS]
                    if obj_snippets is not None:
                        snippets.extend(obj_snippets)
        return snippets

    # ........................................
    def get_stockpile_info(self):
        """Generates a report that can be used by stockpile script

        Returns:
            * A list of dictionaries that have a process type
        """
        stockpile = []
        for group_key in RegistryKey.group_keys():
            if group_key in list(self.registry.keys()):
                for object_id in list(self.registry[group_key].keys()):
                    obj = self.registry[group_key][object_id]
                    if obj[RegistryKey.PROCESS_TYPE] is not None:
                        stockpile.append(obj)
        return stockpile

    # ........................................
    def run(self):
        """Runs the parameter sweep
        """
        if not self.pavs_only:
            self._create_occurrence_sets()
            self._create_masks()
            self._create_models()
            self._create_projections()
        else:
            self._prepare_only_pavs()
        self._create_pavs()

        # Write metrics
        with open(
            os.path.join(
                self.base_work_dir, self.sweep_config.metrics_filename), 'w', 
                encoding=ENCODING) as out_metrics:
            json.dump(self.get_metrics(), out_metrics)

        # Write snippets
        with open(
            os.path.join(
                self.base_work_dir,
                self.sweep_config.snippets_filename), 'w',
                encoding=ENCODING) as out_snippets:
            json.dump(self.get_snippets(), out_snippets)

        # Write stockpile information
        with open(
            os.path.join(
                self.base_work_dir,
                self.sweep_config.stockpile_filename), 'w', 
                encoding=ENCODING) as out_stockpile:
            json.dump(self.get_stockpile_info(), out_stockpile)

        # Write PAV information
        with open(
                os.path.join(
                    self.base_work_dir,
                    self.sweep_config.pavs_filename), 'w', 
                    encoding=ENCODING) as out_pavs:
            json.dump(self.get_pav_info(), out_pavs)

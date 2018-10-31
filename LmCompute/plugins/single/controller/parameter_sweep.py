"""This module contains methods for performing a single species parameter sweep
"""
import json
import os
from osgeo import ogr

import LmBackend.common.layerTools as layer_tools

from LmCommon.common.lmconstants import ProcessType, JobStatus, LMFormat
from LmCommon.common.readyfile import readyFilename

import LmCompute.plugins.single.occurrences.csvOcc as csv_occ
import LmCompute.plugins.single.mask.create_mask as create_mask
from LmCompute.plugins.single.modeling.maxent import MaxentWrapper
from LmCompute.plugins.single.modeling.openModeller import OpenModellerWrapper

# process config file
# Do each portion
# Output
# TODO: Generate stockpile information
# TODO: Method for retrieving metrics

# TODO: Move to constants
class MaskMethod(object):
    HULL_REGION_INTERSECT = 'hull_region_intersect'
    BLANK_MASK = 'blank_mask'

# .............................................................................
# TODO: Move to constants
class REGISTRY_KEY(object):
    ID = 'id'
    METRICS = 'metrics'
    PRIMARY_OUTPUT = 'primary'
    SECONDARY_OUTPUTS = 'secondary'
    SNIPPETS = 'snippets'
    STATUS = 'status'
    # ..................
    # Types
    # ..................
    OCCURRENCE = 'occurrence'
    MASK = 'mask'
    MODEL = 'model'
    PROJECTION = 'projection'
    PAV = 'pav'

# .............................................................................
class ParameterSweep(object):
    """This class performs a parameter sweep for a single species
    """
    # ........................................
    def __init__(self, sweep_config, work_dir):
        """Constructor

        Args:
            sweep_config : A parameter sweep configuration object used to
                determine what should be done.
            work_dir : A top directory where work should be performed.
        """
        # TODO: Use a logger
        self.log = None
        self.sweep_config = sweep_config
        self.work_dir = work_dir
        # Note: The registry is a place for registering outputs
        # TODO: How should this work?
        self.registry = {}

    # ........................................
    def run(self):
        """Runs the parameter sweep
        """
        self._create_occurrence_sets()
        self._create_masks()
        self._create_models()
        self._create_projections()
        self._create_pavs()

    # ........................................
    def _create_masks(self):
        """Creates masks from configuration
        """
        for mask_config in self.sweep_config.get_mask_config():
            mask_method = mask_config[0]
            asc_filename = tif_filename = None

            (mask_method, mask_id, out_mask_base_filename, do_ascii, do_tiff
             ) = mask_config[:5]
            if do_ascii:
                asc_filename = '{}{}'.format(
                    out_mask_base_filename, LMFormat.ASCII.ext)
            if do_tiff:
                tif_filename = '{}{}'.format(
                    out_mask_base_filename, LMFormat.GTIFF.ext)

            if mask_method == MaskMethod.HULL_REGION_INTERSECT:
                (region_layer_filename, buffer_distance, occ_set_id
                 ) = mask_config[5:]
                # Get occurrence set shapefile
                occ_shp_filename, occ_status = self._get_registry_output(
                    REGISTRY_KEY.OCCURRENCE, occ_set_id)
                if occ_status < JobStatus.GENERAL_ERROR and occ_shp_filename:
                    create_mask.create_convex_hull_region_intersect_mask(
                        occ_shp_filename, region_layer_filename, 
                        buffer_distance, ascii_filename=asc_filename,
                        tiff_filename=tif_filename)
            elif mask_method == MaskMethod.BLANK_MASK:
                template_layer_filename = mask_config[5]
                create_mask.create_blank_mask_from_layer(
                    template_layer_filename, ascii_filename=asc_filename,
                    tiff_filename=tif_filename)
            else:
                self.log.error('Could not create mask for method: {}'.format(
                    mask_method))
            status = JobStatus.COMPUTED
            if asc_filename is not None:
                if not os.path.exists(asc_filename):
                    status = JobStatus.GENERAL_ERROR
            if tif_filename is not None:
                if not os.path.exists(tif_filename):
                    status = JobStatus.GENERAL_ERROR
            # TODO: Consider adding rasters as secondary outputs
            # TODO: Add metrics and snippets
            self._register_output_object(
                REGISTRY_KEY.MASK, mask_id, status, out_mask_base_filename)

    # ........................................
    def _create_models(self):
        """Create models from configuration

        Todo:
            * Get species name from somewhere.
            * Get CRS_WKT from somewhere.
        """
        for mdl_config in self.sweep_config.get_model_config():
            
            (process_type, model_id, occ_set_id, algorithm_filename, 
             model_scenario_filename, mask_id) = mdl_config[:5]
            
            ruleset_filename = None
            occ_cont = True
            mask_cont = True
            mdl_metrics = None
            mdl_snippets = None
            
            occ_shp_filename, occ_status = self._get_registry_output(
                REGISTRY_KEY.OCCURRENCE, occ_set_id)
            # We can only compute if occurrence set was created successfully
            if occ_status >= JobStatus.GENERAL_ERROR:
                occ_cont = False

            if mask_id is not None:
                mask_filename_base, mask_status = self._get_registry_output(
                    REGISTRY_KEY.MASK, mask_id)
                # We can only compute if (needed) mask was created successfully
                if mask_status >= JobStatus.GENERAL_ERROR:
                    mask_cont = False
                
            # We can only continue if occurrence set and mask (if needed) were
            #    created successfully
            if occ_cont and mask_cont:
                # Get points
                points = self._get_model_points(occ_shp_filename)
                work_dir = os.path.join(self.work_dir, model_id)
                readyFilename(work_dir)
                # Load algorithm parameters
                with open(algorithm_filename) as algo_f:
                    parameters_json = json.load(algo_f)
                # Load model scenario layer json
                with open(model_scenario_filename) as mdl_scn_f:
                    layer_json = json.load(mdl_scn_f)

                # TODO(CJ): Get species name and CRS WKT from somewhere
                species_name = 'species'
                crs_wkt = None
                
                # If Maxent
                if process_type in [ProcessType.ATT_MODEL,
                                    ProcessType.ATT_PROJECT]:
                    
                    projection_id, scale_params, multiplier = mdl_config[5:]
                    
                    mask_filename = '{}{}'.format(
                        mask_filename_base, LMFormat.ASCII.ext)
                    wrapper = MaxentWrapper(
                        work_dir, species_name, logger=self.log)
                    wrapper.create_model(
                        points, layer_json, parameters_json,
                        mask_filename=mask_filename, crs_wkt=crs_wkt)
                    
                    # Get outputs
                    status = wrapper.get_status()
                    if status < JobStatus.GENERAL_ERROR:
                        ruleset_filename = wrapper.get_ruleset_filename()
                        log_filename = wrapper.get_log_filename()
                        package_filename = os.path.join(
                            work_dir, 'package.zip')
                        wrapper.get_output_package(package_filename,
                                                   overwrite=True)
                        mdl_secondary_outputs = [log_filename, 
                                                 package_filename]
                        mdl_metrics = wrapper.get_metrics()

                    # Get / process projection
                    if projection_id is not None:
                        out_prj_filename = None
                        # Only convert if success, else we'll register failure
                        if status < JobStatus.GENERAL_ERROR:
                            
                            raw_prj_filename = wrapper.get_projection_filename()
                            out_prj_filename = '{}{}'.format(
                                os.path.splitext(raw_prj_filename),
                                LMFormat.GTIFF.ext)
                            # Convert layer and scale layer
                            layer_tools.convertAndModifyAsciiToTiff(
                                raw_prj_filename, out_prj_filename,
                                scale=scale_params, multiplier=multiplier)
                        # Use same secondary outputs as model and register
                        self._register_output_object(
                            REGISTRY_KEY.PROJECTION, projection_id, status,
                            out_prj_filename,
                            secondary_outputs=mdl_secondary_outputs,
                            metrics=mdl_metrics, snippets=mdl_snippets)
                    
                # If openModeller
                elif process_type in [ProcessType.OM_MODEL,
                                      ProcessType.OM_PROJECT]:
                    mask_filename = '{}{}'.format(
                        mask_filename_base, LMFormat.GTIFF.ext)
                    wrapper = OpenModellerWrapper(
                        work_dir, species_name, logger=self.log)
                    wrapper.create_model(
                        points, layer_json, parameters_json,
                        mask_filename=mask_filename, crs_wkt=crs_wkt)
                    
                    # Get outputs
                    status = wrapper.get_status()
                    if status < JobStatus.GENERAL_ERROR:
                        ruleset_filename = wrapper.get_ruleset_filename()
                        log_filename = wrapper.get_log_filename()
                        package_filename = os.path.join(
                            work_dir, 'package.zip')
                        wrapper.get_output_package(package_filename,
                                                   overwrite=True)
                        mdl_secondary_outputs = [log_filename, 
                                                 package_filename]
                        mdl_metrics = wrapper.get_metrics()
                
                # If other
                else:
                    status = JobStatus.UNKNOWN_ERROR
                    mdl_secondary_outputs = []
                    self.log.error(
                        'Unknown process type: {} for model {}'.format(
                            process_type, model_id))

            # Register model output
            self._register_output_object(
                REGISTRY_KEY.MODEL, model_id, status, ruleset_filename, 
                secondary_outputs=mdl_secondary_outputs, metrics=mdl_metrics,
                snippets=mdl_snippets)

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
             max_points, metadata) = occ_config

            occ_metrics = None
            occ_snippets = None

            if process_type == ProcessType.BISON_TAXA_OCCURRENCE:
                status = csv_occ.createBisonShapefile(
                    url_fn_or_key, out_file, big_out_file, max_points,
                    log=self.log)
            elif process_type == ProcessType.GBIF_TAXA_OCCURRENCE:
                status = csv_occ.createGBIFShapefile(
                    url_fn_or_key, out_file, big_out_file, max_points,
                    log=self.log)
            elif process_type == ProcessType.IDIGBIO_TAXA_OCCURRENCE:
                status = csv_occ.createIdigBioShapefile(
                    url_fn_or_key, out_file, big_out_file, max_points,
                    log=self.log)
            elif process_type == ProcessType.USER_TAXA_OCCURRENCE:
                status = csv_occ.createUserShapefile(
                    url_fn_or_key, metadata, out_file, big_out_file,
                    max_points, log=self.log)
            else:
                self.log.error(
                    'Unknown process type: {} for occurrence set: {}'.format(
                        occ_set_id, process_type))
                status = JobStatus.UNKNOWN_ERROR
            self._register_output_object(
                REGISTRY_KEY.OCCURRENCE, occ_set_id, status, out_file, 
                secondary_outputs=[big_out_file], metrics=occ_metrics,
                snippets=occ_snippets)

    # ........................................
    def _create_pavs(self):
        pass

    # ........................................
    def _create_projections(self):
        """Create projections from configuration

        Todo:
            * Get species name from somewhere
        """
        for prj_config in self.sweep_config.get_projection_config():
            
            (process_type, projection_id, model_id, algorithm_filename,
             prj_scenario_filename, mask_id, scale_params, multiplier
             ) = prj_config
            
            mask_cont = True
            prj_metrics = None
            prj_snippets = None
            
            # If this projection was already completed, skip
            prj_raster_filename, _ = self._get_registry_output(
                REGISTRY_KEY.PROJECTION, projection_id)
            if prj_raster_filename is None:

                if mask_id is not None:
                    mask_filename_base, mask_status = self._get_registry_output(
                        REGISTRY_KEY.MASK, mask_id)
                    # We can only compute if (needed) mask was created
                    #    successfully
                    if mask_status >= JobStatus.GENERAL_ERROR:
                        mask_cont = False
                
                # We can only continue if mask (if needed) was created successfully
                if mask_cont:
                    # Get points
                    work_dir = os.path.join(self.work_dir, 'prj_{}'.format(
                        projection_id))
                    readyFilename(work_dir)
                    # Load algorithm parameters
                    with open(algorithm_filename) as algo_f:
                        parameters_json = json.load(algo_f)
                    # Load model scenario layer json
                    with open(prj_scenario_filename) as prj_scn_f:
                        layer_json = json.load(prj_scn_f)
    
                    # TODO(CJ): Get species name from somewhere
                    species_name = 'species'
                    
                    ruleset_filename = self._get_registry_output(
                        REGISTRY_KEY.MODEL, model_id)
                    
                    # If Maxent
                    if process_type == ProcessType.ATT_PROJECT:
                        
                        mask_filename = '{}{}'.format(
                            mask_filename_base, LMFormat.ASCII.ext)
                        wrapper = MaxentWrapper(
                            work_dir, species_name, logger=self.log)
                        
                        wrapper.create_projection(
                            ruleset_filename, layer_json, parameters_json,
                            mask_filename)
                        
                        # Get outputs
                        out_prj_filename = None
                        status = wrapper.get_status()
                        if status < JobStatus.GENERAL_ERROR:
                            
                            raw_prj_filename = wrapper.get_projection_filename()
                            out_prj_filename = '{}{}'.format(
                                os.path.splitext(raw_prj_filename),
                                LMFormat.GTIFF.ext)
                            # Convert layer and scale layer
                            layer_tools.convertAndModifyAsciiToTiff(
                                raw_prj_filename, out_prj_filename,
                                scale=scale_params, multiplier=multiplier)
                            
                            log_filename = wrapper.get_log_filename()
                            package_filename = os.path.join(
                                work_dir, 'package.zip')
                            wrapper.get_output_package(package_filename,
                                                       overwrite=True)
                            prj_secondary_outputs = [log_filename, 
                                                     package_filename]
                            prj_metrics = wrapper.get_metrics()
    
                    # If openModeller
                    elif process_type in [ProcessType.OM_MODEL,
                                          ProcessType.OM_PROJECT]:
                        mask_filename = '{}{}'.format(
                            mask_filename_base, LMFormat.GTIFF.ext)
                        wrapper = OpenModellerWrapper(
                            work_dir, species_name, logger=self.log)
                        wrapper.create_projection(
                            ruleset_filename, layer_json, parameters_json,
                            mask_filename)
                        
                        # Get outputs
                        status = wrapper.get_status()
                        if status < JobStatus.GENERAL_ERROR:
                            
                            out_prj_filename = wrapper.get_projection_filename()
                            
                            log_filename = wrapper.get_log_filename()
                            package_filename = os.path.join(
                                work_dir, 'package.zip')
                            wrapper.get_output_package(package_filename,
                                                       overwrite=True)
                            prj_secondary_outputs = [log_filename, 
                                                     package_filename]
                            prj_metrics = wrapper.get_metrics()
    
                    # If other
                    else:
                        status = JobStatus.UNKNOWN_ERROR
                        prj_secondary_outputs = []
                        self.log.error(
                            'Unknown process type: {} for proj {}'.format(
                                process_type, projection_id))
    
                # Register model output
                self._register_output_object(
                    REGISTRY_KEY.PROJECTION, projection_id, status,
                    out_prj_filename, secondary_outputs=prj_secondary_outputs,
                    metrics=prj_metrics, snippets=prj_snippets)
    
    # ........................................
    def _get_model_points(self, occ_shp_filename):
        """Get minimal point csv to be used for modeling.

        Args:
            occ_shp_filename : The file location of a point shapefile.
        
        Note:
            * Removes duplicate point locations
        """
        points = set([])
        drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
        ds = drv.Open(occ_shp_filename, 0)
        lyr = ds.GetLayer()

        for feature in lyr:
            geom = feature.GetGeometryRef()
            pt_geom = geom.GetPoint()
            points.add((pt_geom[0], pt_geom[1]))
            
        # Add identifiers and create list
        i = 0
        ret_points = []
        for x, y in points:
            ret_points.append((i, x, y))
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
            status = obj[REGISTRY_KEY.STATUS]
            if status == JobStatus.COMPUTED:
                ret_file = obj[REGISTRY_KEY.PRIMARY_OUTPUT]
        except:
            status = JobStatus.NOT_FOUND
            pass
        return ret_file, status
        
    # ........................................
    def _register_output_object(self, object_type, object_id, status,
                                primary_output, secondary_outputs=None, 
                                metrics=None, snippets=None):
        """Adds an output object to the registry

        Args:
            object_type : The type of object being added (should be one of the
                REGISTRY_KEY type values).
            object_id : An identifier for this object
            status : The status of this object (JobStatus value).
            primary_output : The primary output of the object. This file may be
                used for follow up computations.
            secondary_outputs : Optional. These are other output files
                generated by the computation.
            metrics : Optional. Metrics generated by the computation.
            snippets : Optional. Snippets generated by the computation.
        """
        if object_type not in self.registry.keys():
            self.registry[object_type] = []
        self.registry[object_type][object_id] = {
            REGISTRY_KEY.ID : object_id,
            REGISTRY_KEY.METRICS : metrics,
            REGISTRY_KEY.PRIMARY_OUTPUT : primary_output,
            REGISTRY_KEY.SECONDARY_OUTPUTS : secondary_outputs,
            REGISTRY_KEY.SNIPPETS : snippets,
            REGISTRY_KEY.STATUS : status
        }

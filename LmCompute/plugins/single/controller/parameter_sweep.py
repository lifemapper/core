"""This module contains methods for performing a single species parameter sweep
"""
import os


from LmCommon.common.lmconstants import ProcessType, JobStatus, LMFormat

import LmCompute.plugins.single.occurrences.csvOcc as csv_occ
import LmCompute.plugins.single.mask.create_mask as create_mask

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
    def __init__(self, sweep_config):
        """Constructor

        Args:
            sweep_config: A parameter sweep configuration object used to
                determine what should be done.
        """
        # TODO: Use a logger
        self.log = None
        self.sweep_config = sweep_config
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
        """
        for mdl_config in self.sweep_config.get_model_config():
            process_type, occ_set_id, algorithm_file, model_scenario, mask_id, projection_id, processing_options
            # Create model
            # Conditionally create / get projection
            # Conditionally process projection
        pass

    # ........................................
    def _create_occurrence_sets(self):
        """Creates occurrence sets from configuration

        Todo:
            * Generate metrics
            * Generate snippets
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
        pass
    
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

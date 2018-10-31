"""This module contains a class for processing parameter sweep configurations

Todo:
    Read config file
    Write config file
    Add sections
    Add individuals
    Get individuals
"""
import json
from LmServer.common.localconstants import POINT_COUNT_MAX

# .............................................................................
class ParameterSweepConfiguration(object):
    """
    """
    def __init__(self):
        self.occurrence_sets = []

    def read_config_json(self, json_flo):
        pass
    
    def process_config_json(self):
        pass
    
    def save_config_json(self):
        pass
    
    def add_occurrence_set(self, process_type, occ_set_id, url_fn_or_key,
                           out_filename, big_out_filename, 
                           max_points=POINT_COUNT_MAX, metadata=None):
        """Adds an occurrence set configuration to the parameter sweep
        """
        self.occurrence_sets.append(
            (process_type, occ_set_id, url_fn_or_key, out_filename, 
             big_out_filename, max_points, metadata))

    def add_projection(self, model_id, scenario_file, mask=None, parameters=None):
        pass
    
    def add_full_projection(self):
        pass
    
    def add_pav_intersect(self):
        pass
    
    def get_mask_config(self):
        """Generator for mask configurations
        """
        # Return mask_method, mask_id, mask_base_filename, do_ascii, do_tiff + specific options
        pass

    def get_occurrence_set_config(self):
        """Generator for occurrence set configurations

        Todo:
            * Consider if this is too explicit.  We could easily just return
                list tuples instead of breaking it out and then returning new
                tuple.
        """
        for occ_vals in self.occurrence_sets:
            (process_type, occ_set_id, url_fn_or_key, out_file, big_out_file, 
             max_points, metadata) = occ_vals
            yield (process_type, occ_set_id, url_fn_or_key, out_file,
                   big_out_file, max_points, metadata)
            
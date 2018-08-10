"""
@summary: This tool will perform a single species parameter sweep workflow to
             generate all SDM related products.  
@note: This is useful to run all of the computations for a single species in 
          the same place to limit data transfer.  This is especially useful 
          when we are running many species through the same process and it 
          doesn't matter if one is a little slow.
@author: CJ Grady
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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

@note: Iteration 1: Use existing code without optimizing too much
@todo: Iteration 2: Start optimizations to reduce memory use and disk IO

@todo: Use LmMetrics


"""

# TODO: Organize
import argparse
import json
import ogr
import os
# TODO: Use a different testing method probably
from LmCommon.common.lmconstants import LMFormat, ProcessType, JobStatus
from LmCompute.plugins.single.occurrences.csvOcc import (createBisonShapefile, 
            createGBIFShapefile, createIdigBioShapefile, createUserShapefile)
from LmCompute.plugins.single.mask.create_mask import create_blank_mask_from_layer, create_convex_hull_region_intersect_mask
from LmServer.common.localconstants import POINT_COUNT_MAX
from LmCompute.common.lmObj import LmException

# .............................................................................
# TODO: Move to constants
class MaskMethod(object):
   HULL_REGION_INTERSECT = 'hull_region_intersect'
   BLANK_MASK = 'blank_mask'

# .............................................................................
class SingleSpeciesParameterSweep(object):
   """
   @summary: Class controlling single species parameter sweep
   @todo: Inherit from LMObject maybe?
   """
   def __init__(self):
      # Process config options?
      pass
   
   # ................................
   def _process_points(self, occ_config):
      """
      @todo: Consider moving this to csvOcc
      @todo: Snippets
      
      THis function should:
         * Collect metrics about running time
         * Return convex hull?
       
      occ schema
      
        process_type : Integer corresponding to ProcessType class
        
        Bison:
         process_type : ProcessType.BISON_TAXA_OCCURRENCE
         url : {url}
         shp_filename : {string}
         big_filename : {string}
         occ_set_id : {int}
       
        GBIF:
         process_type : ProcessType.GBIF_TAXA_OCCURRENCE
         csv_filename : {string}
         shp_filename : {string}
         big_filename : {string}
         occ_set_id : {int}
       
        IdigBio:
         process_type : ProcessType.IDIGBIO_TAXA_OCCURRENCE
         taxon_key : {string}
         csv_filename : {string}
         shp_filename : {string}
         big_filename : {string}
         occ_set_id : {int}
       
        User points:
         process_type : ProcessType.USER_TAXA_OCCURRENCE
         csv_filename : {string}
         shp_filename : {string}
         big_filename : {string}
         occ_set_id : {int}
         occ_meta : {}
       
      """
      # TODO: Collect metrics
      occ_metrics = {}
      
      # TODO: Use constants
      # TODO: Formally define schema
      process_type = occ_config['process_type']
      shp_filename = occ_config['shp_filename']
      big_filename = occ_config['big_filename']
      occ_set_id = occ_config['occ_set_id']
      
      # Create the shapefile(s) using the appropriate method
      # Note: Generated shapefiles are tested when they are created
      if process_type == ProcessType.BISON_TAXA_OCCURRENCE:
         status = createBisonShapefile(occ_config['url'], shp_filename, 
                                 big_filename, POINT_COUNT_MAX, log=self.log)
      elif process_type == ProcessType.GBIF_TAXA_OCCURRENCE:
         status = createGBIFShapefile(occ_config['csv_filename'], shp_filename,
                                 big_filename, POINT_COUNT_MAX, log=self.log)
      elif process_type == ProcessType.IDIGBIO_TAXA_OCCURRENCE:
         status = createIdigBioShapefile(occ_config['taxon_key'], shp_filename,
                                 big_filename, POINT_COUNT_MAX, log=self.log)
      elif process_type == ProcessType.USER_TAXA_OCCURRENCE:
         status = createUserShapefile(occ_config['csv_filename'], 
                                 occ_config['occ_meta'], shp_filename, 
                                 big_filename, POINT_COUNT_MAX, log=self.log)
      else:
         status = JobStatus.UNKNOWN_ERROR
         self.log.error(
            'Could not create occurrence set, do not know how to handle process type: {}'.format(
               process_type))
      
      # Return occurrence set metrics and status tuple
      return occ_metrics, (process_type, occ_set_id, status), occ_snippets
      
   def _create_mask(self, maskConfig):
      """
      This function should:
         * Create a mask layer matching the provided scenario
         * Could use convex hull / region intersect method
         * Could be blank
         * Could be other
         
      Need method
      Decide what to do from there
      @todo: Constants
      """
      method = maskConfig['method']
      out_ascii = out_tiff = None
      if maskConfig.has_key('out_ascii_filename'):
         out_ascii = maskConfig['out_ascii_filename']
      if maskConfig.has_key('out_tiff_filename'):
         out_tiff = maskConfig['out_tiff_filename']
      
      if method == MaskMethod.HULL_REGION_INTERSECT:
         # TODO: Optional nodata parameter
         region_layer_filename = maskConfig['template_layer']
         buff = maskConfig['buffer']
         occ_shp_filename = maskConfig['occ_shp_filename']
         create_convex_hull_region_intersect_mask(occ_shp_filename, 
                                                region_layer_filename, buff, 
                                                ascii_filename=out_ascii, 
                                                tiff_filename=out_tiff)
      elif method == MaskMethod.BLANK_MASK:
         # TODO: Optional nodata parameter
         template_layer_filename = maskConfig['template_layer']
         create_blank_mask_from_layer(template_layer_filename,
                                      ascii_filename=out_ascii, 
                                      tiff_filename=out_tiff)
      else:
         # TODO: Mask error status
         raise LmException(JobStatus.GENERAL_ERROR,
                           'Unknown masking method: {}'.format(method))
   
   # ..........................................................................
   def _create_model(self, mdl_config, model_points):
      """
      @summary: This function calls the appropriate modeling code based on the
                   configuration passed to it
      """
      # Determine if it is maxent or openModeller
      if mdl_config['algorithm']['code'].lower() == 'att_maxent':
         # Maxent
         mdl_filename, mdl_metrics, mdl_snippets, projection_info = \
               self._create_maxent_model(model_points, mdl_config['algorithm'],
                                   mdl_config['model_scenario'], 
                                   mask_filename=mdl_config['mask_filename_ascii'])
      else:
         # openModeller
         mdl_filename, mdl_metrics, mdl_snippets, projection_info = \
               self._create_openModeller_model(model_points, 
                                       mdl_config['algorithm'],
                                       mdl_config['model_scenario'], 
                                       mask_filename=mdl_config['mask_filename_tiff'])
      
      return mdl_filename, mdl_metrics, mdl_snippets, projection_info
   
   # ..........................................................................
   def _create_maxent_model(self, model_points, algorithm_config, 
                                          scenario_json, mask_filename=None):
      """
      This function should:
         * Create a maxent model
         * Probably create a projection for that model
         * Return status information for the created projection
         * Metrics
         * Logging
         * Check outputs
         * Transform outputs
         
         write projectoin
         write package
         metrics
         status
         
         
      """
      pass
   
   def _create_maxent_projection(self):
      """
      * Create a projection
      * Check outputs
      * Metrics
      * Logging
      * Status info
      * Transform outputs
      * Move outputs as appropriate
      """
      pass
   
   def _create_openModeller_model(self):
      """
      * Create a model
      * Check outputs
      * Metrics
      * Logging
      """
      pass
   
   def _create_openModeller_projection(self):
      """
      * Create a projection
      * Check outputs
      * Metrics
      * Logging
      * Status info
      * Transform outputs
      * Move outputs as appropriate
      """
      pass
   
   def create_models(self):
      """
      * Create models for each algorithm / model scenario combination
      * Projection status as needed
      """
      pass
   
   def create_projections(self):
      """
      * Create projections for each model / proj scenario combination
      * Status and such 
      * Logging
      """
      pass
   
   def _get_model_points(self, occ_shp_filename):
      """
      """
      points = set([])
      
      drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
      ds = drv.Open(occ_shp_filename, 0)
      lyr = ds.GetLayer()
      
      i = 0
      for feature in lyr:
         geom = feature.GetGeometryRef()
         ptGeom = geom.GetPoint()
         points.add((i, ptGeom[0], ptGeom[1]))
         i += 1
      
      return list(points)
   
   def _get_scenario_template_layer(self, scenario_json_filename):
      with open(scenario_json_filename) as inF:
         scn = json.load(inF)
      # Return first layer
      return scn['layers'][0]['path']
      
         

   def _intersect_projection(self):
      """
      * Intersect a projection
      * Check outputs
      * Return solr post section
      * Status info
      * Logging
      * Metrics
      """
      pass
   
   def intersect_projections(self):
      """
      * Intersect all projections
      * Build big solr post document
      * Post to solr
      """
      pass
   
   def catlog_outputs(self, catalog_entries):
      """
      * Loop through list of outputs and update db
      """
      pass
   
   # ........................................
   def run(self):
      """
      @note: Stores completed files in lookups so that it doesn't recreate them
      """
      # TODO: Process config if necessary
      
      # TODO: Initialize things
      
      # TODO: Constants for keys
      # Metrics dictionary use for reporting
      metrics = {
         'occurrence_sets' : [],
         'masks' : [],
         'models' : [],
         'projections' : [],
         'intersections' : []
      }
      
      algorithms = {}
      
      statuses = [] # Used for reporting all output statuses
      snippets = [] # Used to report all snippet information collected
      solr_docs = [] # A list of solr docs for intersect posts for global pam
      
      # TODO: Determine if we have maxent and openmodeller
      do_maxent = False
      do_openModeller = False
      for alg in config['algorithms']:
         algorithms[alg['ref_id']] = alg
         if alg['code'].lower() == 'att_maxent':
            do_maxent = True
         else:
            do_openModeller = True

      # Note: Possible to specify multiple occurrence sets to turn this into
      #          multiple single-species computations
      for occ_config in config['occurrence_sets']:
         
         masks = {} # Mask lookup for completed masks
         models = {} # Model lookup for completed models
         
         work_dir = 'occ_{}_work'.format(occ_config['occ_set_id'])
         
         occ_metrics, occ_status, occ_snippets = self._process_points(
                                                                    occ_config)
         # Add metrics and status
         metrics['occurrence_sets'].append(occ_metrics)
         statuses.append(occ_status)
         snippets.append(occ_snippets)
         occ_shp_filename = occ_config['shp_filename']
         
         model_points = self._get_model_points(occ_shp_filename)
      
         for prj in config['projections']:
            
            prj_tiff_filename = None
            
            # If we should use masks, create or retrieve the masks
            if config.has_key('mask'):
               # Model mask
               mdl_scn_name = os.path.splitext(
                                    os.path.basename(prj['model_scenario']))[0]
               
               if masks.has_key(prj['model_scenario']):
                  # Retrieve model mask filenames
                  mdl_mask_ascii, mdl_mask_tiff = masks[prj['model_scenario']]
               else:
                  # Create mask
                  if do_maxent:
                     mdl_mask_ascii = os.path.join(work_dir, 
                                                '{}_mask{}'.format(
                                                         mdl_scn_name, 
                                                         LMFormat.ASCII.ext))
                  else:
                     mdl_mask_ascii = None
                  if do_openModeller:
                     mdl_mask_tiff = os.path.join(work_dir, '{}_mask{}'.format(
                                             mdl_scn_name, LMFormat.GTIFF.ext))
                  else:
                     mdl_mask_tiff = None
                  
                  mask_config = config['mask']
                  mask_config['out_ascii_filename'] = mdl_mask_ascii
                  mask_config['out_tiff_filename'] = mdl_mask_tiff
                  mask_config['template_layer'] = \
                                          self._get_scenario_template_layer(
                                                         prj['model_scenario'])
                  mask_config['occ_shp_filename'] = occ_shp_filename
                  self._create_mask(mask_config)
               
                  # Add mask to dictionary
                  masks[prj['model_scenario']] = (mdl_mask_ascii, mdl_mask_tiff)
               
               # Projection mask
               prj_scn_name = os.path.splitext(
                                       os.path.basename(
                                                prj['projection_scenario']))[0]
               if masks.has_key(prj['projection_scenario']):
                  # Retrieve projection mask filenames
                  prj_mask_ascii, prj_mask_tiff = masks[prj['projection_scenario']]
               else:
                  # Create mask
                  if do_maxent:
                     prj_mask_ascii = os.path.join(work_dir, 
                                                '{}_mask{}'.format(
                                                         prj_scn_name, 
                                                         LMFormat.ASCII.ext))
                  else:
                     prj_mask_ascii = None
                  if do_openModeller:
                     prj_mask_tiff = os.path.join(work_dir, '{}_mask{}'.format(
                                             prj_scn_name, LMFormat.GTIFF.ext))
                  else:
                     prj_mask_tiff = None
                  
                  mask_config = config['mask']
                  mask_config = {
                     'method' : MaskMethod.BLANK_MASK,
                     'out_ascii_filename' : prj_mask_ascii,
                     'out_tiff_filename' : prj_mask_tiff,
                     'template_layer' : self._get_scenario_template_layer(
                                                      prj['model_scenario'])
                  }
                  self._create_mask(mask_config)
                  masks[prj['projection_scenario']] = (prj_mask_ascii, prj_mask_tiff)
            else:
               mdl_mask_ascii, mdl_mask_tiff = None, None
               prj_mask_ascii, prj_mask_tiff = None, None
            
            # Create or retrieve model
            # -----------------------------------------
            mdl_key = (prj['model_scenario'], prj['algorithm_ref'])
            if models.has_key(mdl_key):
               mdl_filename = models[mdl_key]
            else:
               # Create model
               raise Exception, 'implement'
               # TODO: Add any post processing we want
               mdl_config = {
                  'algorithm' : algorithms[prj['algorithm_ref']],
                  'model_scenario' : prj['model_scenario'],
                  'mask_filename_ascii' : mdl_mask_ascii,
                  'mask_filename_tiff' : mdl_mask_tiff
               }
               (mdl_filename, mdl_metrics, mdl_snippets, 
                                       projection_info) = self._create_model(
                                                      mdl_config, model_points)
               models[mdl_key] = mdl_filename
               metrics['models'].append(mdl_metrics)
               snippets.append(mdl_snippets)
            
               # What if we just save these and process it later?
               
               if projection_info is not None:
                  (prj_tiff_filename, prj_metrics, prj_status, 
                                             prj_snippets) = projection_info
         
            
            # Create projection if not already completed
            if prj_tiff_filename is None:
               #TODO: Compute the projection
               raise Exception, 'implement'
               prj_tiff_filename, prj_metrics, prj_status, prj_snippets = self._create_projection(prj_config, ruleset)
      
            # Process projection info
            metrics['projections'].append(prj_metrics)
            statuses.append(prj_status)
            snippets.append(prj_snippets)
      
            # Perform intersection
            (solr_doc, int_metrics, int_status, 
                              int_snippets) = self._intersect_projection(
                                          intersect_config, prj_tiff_filename)
            
            metrics['intersections'].append(int_metrics)
            snippets.append(int_snippets)
            statuses.append(int_status)
            solr_docs.append(solr_doc)
      
      
      
         
         # TODO: Write documents probably if we cannot do from nodes
            
         # Catalog
         self._post_snippets(snippets)
         self._post_intersects(solr_docs)
         self._catalog_outputs(statuses)
      
      # TODO: Report     
   


# Use existing scripts probably
# Make sure everything cleans up
# What are the outputs?
# How should it fail?
# Keep track of metrics / stats?  - a report file maybe?


# .............................................................................
if __name__ == '__main__':

   sample_config = """\
{
   "algorithms" : [
      {
         "ref_id" : "",
         "code" : "",
         "parameters" : {
         }
      }
   ],
   "occurrence_sets" : [
      {
         "occ_id"
         "dlocation"
         "big_dlocation"
      },
   ],
   "mask" : {
      "method" : "hull_region_intersect",
      "buffer" : 30
   },
   "projections" : [
      {
         "projection_id" : 1
         "algorithm_ref" : "",
         "model_scenario" : "",
         "projection_scenario" : "",
         "dlocation" : ""
      }
   ],
   "shapegrid" : {},
   "intersect_parameters" : {}
}
"""
   pass

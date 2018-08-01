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

inputs
 csv
 algo files
 scn files
 shapegrid

 1. process points
  a. write csv for maxent
  b. write xml for om
  c. write shapefile
  d. check outputs
  e. write object type, id, status to status file
 2. create masks
  a. for model
  b. for each projection
   i. option to create blank masks
 3. create models
  a. for maxent, create observed projection too
  b. om models
 4. create projections
  a. maxent
   i. convert output to tiff
     1. write to final location
   ii. zip package output
     1. write to final location
   iii. verify outputs
     1. verify
     2. write object type, id, status to status file
 5. intersects
  a. for each projection
    i. intersect with shapegrid
    ii. build a solr post section
    iii. verify outputs
    iv. object type, id, status to status dump
  b. post all intersects to solr
 6. catalog outputs
  a. for each status set
   i. update db
"""

# TODO: Organize
import argparse
import os
# TODO: Use a different testing method probably
from LmServer.base.layer2 import Vector
from LmCommon.common.lmconstants import LMFormat, ProcessType, JobStatus
from LmCompute.plugins.single.occurrences.csvOcc import (createBisonShapefile, 
            createGBIFShapefile, createIdigBioShapefile, createUserShapefile)
from LmServer.common.localconstants import POINT_COUNT_MAX

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
   def _processPoints(self, occ_config, occ_set_id):
      """
      @todo: Consider moving this to csvOcc
      
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
      return occ_metrics, (process_type, occ_set_id, status)
      
   def _createMask(self):
      """
      This function should:
         * Create a mask layer matching the provided scenario
         * Could use convex hull / region intersect method
         * Could be blank
         * Could be other
      """
      pass
   
   def _createMaxentModel(self):
      """
      This function should:
         * Create a maxent model
         * Probably create a projection for that model
         * Return status information for the created projection
         * Metrics
         * Logging
         * Check outputs
         * Transform outputs
      """
      pass
   
   def _createMaxentProjection(self):
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
   
   def _createOpenModellerModel(self):
      """
      * Create a model
      * Check outputs
      * Metrics
      * Logging
      """
      pass
   
   def _createOpenModellerProjection(self):
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
   
   def createModels(self):
      """
      * Create models for each algorithm / model scenario combination
      * Projection status as needed
      """
      pass
   
   def createProjections(self):
      """
      * Create projections for each model / proj scenario combination
      * Status and such 
      * Logging
      """
      pass
   
   def _intersectProjection(self):
      """
      * Intersect a projection
      * Check outputs
      * Return solr post section
      * Status info
      * Logging
      * Metrics
      """
      pass
   
   def intersectProjections(self):
      """
      * Intersect all projections
      * Build big solr post document
      * Post to solr
      """
      pass
   
   def catlogOutputs(self):
      """
      * Loop through list of outputs and update db
      """
      pass
   
   def run(self):
      # Process config if not done already
      # Perform each chunk
      pass
   


# Use existing scripts probably
# Make sure everything cleans up
# What are the outputs?
# How should it fail?
# Keep track of metrics / stats?  - a report file maybe?


# .............................................................................
if __name__ == '__main__':
   pass

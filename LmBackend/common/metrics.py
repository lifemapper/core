"""
@summary: Module containing tools for Lifemapper metrics collection
@author: CJ Grady
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
@todo: Consider adding constants for collected metric names
"""
import json
import os

from LmBackend.common.lmobj import LMObject

# .............................................................................
class LmMetricNames(object):
   """
   @summary: Class containing standard metric names
   @note: Other metric names can be used, but these are common metrics
   """
   ALGORITHM_CODE = 'algorithm_code'
   NUMBER_OF_FEATURES = 'num_features'
   OUTPUT_SIZE = 'output_size'
   PROCESS_TYPE = 'process_type'
   RUNNING_TIME = 'running_time'
   STATUS = 'status'

# .............................................................................
class LmMetrics(LMObject):
   """
   @summary: This class is used to track performance metrics
   """
   # ................................
   def __init__(self, out_file):
      """
      @summary: Metrics constructor
      @param out_file: Write metrics to this location, if None, don't write
      @note: This allows you to always collect metrics and only conditionally
                write them
      """
      self.out_file = out_file
      self.metrics = {}
   
   # ................................
   def add_metric(self, name, value):
      """
      @summary: Add a metric to track
      """
      self.metrics[name] = value
   
   # ................................
   def get_dir_size(self, dir_name):
      """
      @summary: Get the size of a directory, presumably for determining output 
                   footprint
      @param dir_name: The directory to determine the size of
      """
      total_size = os.path.getsize(dir_name)
      for item in os.listdir(dir_name):
         itempath = os.path.join(dir_name, item)
         if os.path.isfile(itempath):
            total_size += os.path.getsize(itempath)
         elif os.path.isdir(itempath):
            total_size += self.get_dir_size(itempath)
      return total_size
   
   # ................................
   def write_metrics(self):
      """
      @summary: Write the metrics to the specified file
      """
      if self.out_file is not None:
         self.readyFilename(self.out_file, overwrite=True)
         with open(self.out_file, 'w') as outF:
            json.dump(self.metrics, outF)
            
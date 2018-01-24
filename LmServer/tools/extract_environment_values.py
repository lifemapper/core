'''
Created on Jan 23, 2018

@author: cjgrady


Define input parameters

function to extract data for a layer

'''
"""
@summary: This script extracts metrics for a set of points and a set of 
             environmental variables.  These data are then formatted as a
             matrix
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
"""

import argparse
import json
import numpy as np
from osgeo import gdal,ogr

from LmCommon.common.matrix import Matrix

# .............................................................................
def get_layer_info(layer_json_file):
   """
   @summary: Get layer information to use for metrics from the JSON file
   """
   with open(layer_json_file) as inF:
      raw_layers = json.load(inF)
   
   layers = []
   for lyr in raw_layers:
      layers.append((lyr['identifier'], lyr['dlocation']))
   return layers

# .............................................................................
def get_metrics(points_filename, layer_info, identifier, 
                removeDuplicateLocations=True):
   """
   @summary: Get metrics for the data
   """
   metrics = [('minimum', np.min),
              ('maximum', np.max),
              ('mean', np.mean)]
   metricFunctions = [f for _, f in metrics]
   
   # Add an extra dimension so we can attach a header for this set and then
   #    stack this matrix with others
   metricsData = np.zeros((len(metrics), len(layer_info), 1), dtype=np.float)

   # Get the points
   points = get_point_xys(points_filename, 
                          removeDuplicateLocations=removeDuplicateLocations)
   
   # Get matrics for layers
   for i in xrange(len(layer_info)):
      # Get metrics for a layer
      lyrMetrics = get_metrics_for_layer(points, layer_info[i][1], 
                                         metricFunctions)
      
      # Set values in matrix
      for j in xrange(len(lyrMetrics)):
         metricsData[j,i,0] = lyrMetrics[j]
   
   metricsMatrix = Matrix(metricsData, headers={
      '0' : [h for h, _ in metrics],
      '1' : [lyr[0] for lyr in layer_info],
      '2' : [identifier]
      })
   return metricsMatrix

# .............................................................................
def get_metrics_for_layer(points, layer_filename, metricFunctions):
   """
   @summary: Get layer values for each point and then generate metrics
   """
   ds = gdal.Open(layer_filename)
   band = ds.GetRasterBand(1)
   data = np.array(band.ReadAsArray())
   gt = ds.GetGeoTransform()
   nodataVal = band.GetNoDataValue()
   
   values = []
   
   for x, y in points:
      px = int((x - gt[0]) / gt[1])
      py = int((y - gt[3]) / gt[5])
      val = data[py, px]
      # TODO: Needs to be safer.  This will only work for negative no data values
      if val > nodataVal:
         values.append(data[py, px])
      else:
         print 'Could not append value at ({}, {}): {}'.format(px, py, val)
   
   arr = np.array(values)
   
   mVals = []
   for func in metricFunctions:
      mVals.append(func(arr))
   return mVals
    
# .............................................................................
def get_point_xys(points_filename, removeDuplicateLocations=True):
   """
   @summary: Get x,y pairs for each point in a shapefile
   """
   points = []
   
   ds = ogr.Open(points_filename)
   lyr = ds.GetLayer()
   
   for feat in lyr:
      geom = feat.GetGeometryRef()
      points.append((geom.GetX(), geom.GetY()))
   
   if removeDuplicateLocations:
      points = list(set(points))
      
   return points

# .............................................................................
if __name__ == '__main__':
   
   parser = argparse.ArgumentParser(
      description='This script extracts environmental data metrics for points')
   
   parser.add_argument('points_file', type=str, 
                       help='The file location of the points shapefile')
   parser.add_argument('points_name', type=str, 
         help='A name (such as the squid) to be associated with these points')
   parser.add_argument('layer_json_file', type=str, 
                       help='JSON file containing layer information')
   parser.add_argument('output_file', type=str, 
                       help='File location to write the output matrix')
   # TODO: Add parameters for metrics to collect
   args = parser.parse_args()
   
   layer_info = get_layer_info(args.layer_json_file)
   
   metrics = get_metrics(args.points_file, layer_info, args.points_name)
   
   # Write outputs
   with open(args.output_file, 'w') as outF:
      metrics.save(outF)


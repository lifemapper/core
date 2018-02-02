"""
@summary: Create a mask 
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
"""
import argparse
import numpy as np
from osgeo import gdal, ogr
import gdalconst

from LmCommon.common.lmconstants import DEFAULT_NODATA

def getXYsForShapefile(filename):
   def getXY(wkt):
      startidx = wkt.find('(')
      if wkt[:startidx].strip().lower() == 'point':
         tmp = wkt[startidx+1:]
         endidx = tmp.find(')')
         tmp = tmp[:endidx]
         vals = tmp.split()
         if len(vals)  == 2:
            try:
               x = float(vals[0])
               y = float(vals[1])
               return x, y
            except:
               return None
         else:
            return None
      else:
         return None

   drv = ogr.GetDriverByName('ESRI Shapefile')
   ds = drv.Open(filename)
   lyr = ds.GetLayer()
   
   pts = []
   for feat in lyr:
      geom = feat.GetGeometryRef()
      r = getXY(geom.Centroid().ExportToWkt())
      if r is not None:
         pts.append(r)
   lyr = ds = None
   
   return pts


def createMaskRaster(inRasterFn, pointsFn, outRasterFn):
   pts = getXYsForShapefile(pointsFn)
   ds = gdal.Open(inRasterFn)
   band = ds.GetRasterBand(1)
   
   cols = ds.RasterXSize
   rows = ds.RasterYSize
   
   transform = ds.GetGeoTransform()
   xOrigin = transform[0]
   yOrigin = transform[3]
   pixelWidth = transform[1]
   pixelHeight = -transform[5]
   
   data = band.ReadAsArray(0, 0, cols, rows)
   
   vals = set([])
   for x,y in pts:
      col = int((x - xOrigin) / pixelWidth)
      row = int((yOrigin - y) / pixelHeight)
      try:
         vals.add(data[row][col])
      except:
         pass
   
   listVals = list(vals)
   if len(listVals) == 0:
      raise Exception, 'No intersection between points and raster'
   newData = DEFAULT_NODATA * np.ones(data.shape, dtype=int)
   
   for i in range(data.shape[0]):
      for j in range(data.shape[1]):
         if data[i,j] in listVals:
            newData[i,j] = 1
   
   drv = ds.GetDriver()
   outDs = drv.Create(outRasterFn, cols, rows, 1, gdalconst.GDT_Int32)
   outBand = outDs.GetRasterBand(1)
   
   # Write the data
   outBand.WriteArray(newData, 0, 0)
   outBand.FlushCache()
   outBand.SetNoDataValue(DEFAULT_NODATA)
   
   # Georeference the image and set the projection
   outDs.SetGeoTransform(ds.GetGeoTransform())
   outDs.SetProjection(ds.GetProjection())
   del newData

    
# ................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
      description='This script creates a mask from a point layer and an ecoregions layer')
   
   parser.add_argument('inRasterFilename', type=str, 
                       help='The input ecoregions raster')
   parser.add_argument('pointsFilename', type=str, 
                       help='The file location for the points shapefile')
   parser.add_argument('outRasterFilename', type=str, 
                       help='The file location to write the output raster')
   
   args = parser.parse_args()

   createMaskRaster(args.inRasterFilename, args.pointsFilename, 
                    args.outRasterFilename)


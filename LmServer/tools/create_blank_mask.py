"""
@summary: Create a blank mask layer
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
@todo: Does this need to be more robust?
"""
import argparse
import numpy as np
from osgeo import gdal, gdalconst

from LmCommon.common.lmconstants import DEFAULT_NODATA

# .............................................................................
def createMaskRaster(inRasterFn, outRasterFn):
   """
   @summary: Generate a mask layer based on the input raster layer
   """
   ds = gdal.Open(inRasterFn)
   band = ds.GetRasterBand(1)
   
   cols = ds.RasterXSize
   rows = ds.RasterYSize
   
   data = band.ReadAsArray(0, 0, cols, rows)
   
   newData = np.ones(data.shape, dtype=int)
   
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
         description='This script creates a blank mask layer for projections')
   
   parser.add_argument('inRasterFilename', type=str, 
                       help='The input raster to base the mask on')
   parser.add_argument('outRasterFilename', type=str, 
                       help='The file location to write the output raster')
   
   args = parser.parse_args()

   createMaskRaster(args.inRasterFilename, args.outRasterFilename)


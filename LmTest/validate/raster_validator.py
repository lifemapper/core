"""
@summary: This module contains functions for validating raster files
@author: CJ Grady
@version: 1.0
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
@todo: Determine if file or file-like object, then validate
@todo: Generalize
"""
import os
from osgeo import gdal

from LmCommon.common.lmconstants import LMFormat

# .............................................................................
def validate_raster_file(raster_filename, raster_format=None):
   """
   @summary: Validates a raster file by seeing if it can be loaded by GDAL
   """
   msg = 'Valid'
   valid = False
   if os.path.exists(raster_filename):
      # If a raster format was not provided, try to get it from the file 
      #    extension
      if raster_format is None:
         _, ext = os.path.splitext(raster_filename)
         if ext == LMFormat.ASCII.ext:
            raster_format = LMFormat.ASCII
         elif ext == LMFormat.GTIFF.ext:
            raster_format = LMFormat.GTIFF
         else:
            msg = 'Extension {} did not map to a known raster format'.format(
                                                                           ext)
         
      if raster_format is not None:
         try:
            drv = gdal.GetDriverByName(raster_format.driver)
            ds = drv.Open(raster_filename)
            if ds is None:
               msg = 'Could not open {}'.format(raster_filename)
            else:
               lyr = ds.GetLayer()
               valid = True
         except Exception, e:
            msg = str(e)
   else:
      msg = 'File does not exist'
   
   return valid, msg
   
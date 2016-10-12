"""
@summary: Module containing GBIF functions
@author: Aimee Stewart
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import os
from osgeo import ogr, osr
import StringIO

from LmCommon.common.createshape import ShapeShifter
from LmCommon.common.lmconstants import OutputFormat, ProcessType, JobStatus
from LmCompute.common.lmObj import LmException

# .............................................................................
# PUBLIC
# .............................................................................


# .............................................................................
def parseGBIFData(count, csvInputBlob, basePath, maxPoints, outName):
   """
   @summary: Parses a CSV-format GBIF data set and saves it to a shapefile in the 
                specified location
   @param csvInputBlob: A string of CSV data
   @param basePath: A directory where the shapefile should be stored
   @param maxPoints: The maximum number of points to include if subsetting
   @return: The name of the file where the data is stored (.shp extension)
   @rtype: String
   """
   if csvInputBlob is None or len(csvInputBlob.strip()) == 0:
      raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                        "The CSV provided was empty")
   outfilename = os.path.join(basePath, "{baseName}{ext}".format(
                                    baseName=outName, ext=OutputFormat.SHAPE))
   subsetOutfilename = None
   subsetIndices = None
   
   if count > maxPoints:
      subsetOutfilename = os.path.join(basePath, 
                                 "{baseName}_subset{ext}".format(
                                     baseName=outName, ext=OutputFormat.SHAPE))
   
   shaper = ShapeShifter(ProcessType.GBIF_TAXA_OCCURRENCE, csvInputBlob, count)
   shaper.writeOccurrences(outfilename, maxPoints=maxPoints, 
                           subsetfname=subsetOutfilename)

   return outfilename, subsetOutfilename

"""
@summary: This module does the work of pulling down information from an URL 
             and parsing it
@author: Aimee Stewart and CJ Grady
@version: 4.0.0
@status: alpha

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

from LmCommon.common.apiquery import BisonAPI
from LmCommon.common.createshape import ShapeShifter
from LmCommon.common.lmconstants import OutputFormat, ProcessType

# .............................................................................
def createBisonShapefileFromUrl(url, basePath, maxPoints, outName):
   """
   @summary: Receives a BISON url, pulls in the data, and returns a shapefile
   @param url: The url to pull data from
   @param basePath: A directory where the shapefile should be stored
   @param maxPoints: The maximum number of points to include if subsetting
   @param outName: The name to use for the shapefiles
   @return: The name of the file(s) where the data is stored (.shp extension)
   @rtype: String and String/None
   """
   outfilename = os.path.join(basePath, "{baseName}{ext}".format(
                                    baseName=outName, ext=OutputFormat.SHAPE))
   subsetOutfilename = None
   
   occAPI = BisonAPI.initFromUrl(url)
   occList = occAPI.getTSNOccurrences()
   
   count = len(occList)
   if count > maxPoints:
      subsetOutfilename = os.path.join(basePath, 
                                 "{baseName}_subset{ext}".format(
                                     baseName=outName, ext=OutputFormat.SHAPE))

   shaper = ShapeShifter(ProcessType.BISON_TAXA_OCCURRENCE, occList, count)
   shaper.writeOccurrences(outfilename, maxPoints=maxPoints, 
                           subsetfname=subsetOutfilename)
   
   return outfilename, subsetOutfilename

# ..............................................................................
# MAIN
# ..............................................................................
if __name__ == '__main__':
   
   from LmCommon.common.lmconstants import BISON
   tsn = 179680
   tsn = 31787
   occAPI = BisonAPI(qFilters={BISON.HIERARCHY_KEY: '*-%d-*' % tsn}, 
                     otherFilters={'rows': 400})
   createBisonShapefileFromUrl(occAPI.url, '/tmp', 100, tsn)
   
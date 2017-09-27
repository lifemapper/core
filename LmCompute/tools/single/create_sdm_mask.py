"""
@summary: The goal of this script is to intersect occurrence set points and an 
             ecoregions layer then intersect the results with the occurrence 
             set convex hull to create a modeling mask for SDM
@author: CJ Grady
@status: alpha
@version: 1.0.0
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
from osgeo import ogr

from LmCommon.common.lmconstants import LMFormat
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
def getMaskShapefile(occId, ecoregionsFilename, maskFilename):
   """
   @summary: Main method for this script to generate a mask shapefile
   """
   # TODO: Logger
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   occ = scribe.getOccurrenceSet(occId=int(occId))
   occ.readShapefile()
   
   mergedPointGeom = getMergedPointsGeometry(occ.getDLocation())
   mergedEcoGeom = getMergedIntersectingEcoregions(mergedPointGeom, ecoregionsFilename)
   
   convexHullGeom = ogr.CreateGeometryFromWkt(occ.getConvexHullWkt())
   # Get the mask geometry
   maskGeom = convexHullGeom.Intersection(mergedEcoGeom)

   drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
   ds = drv.CreateDataSource(maskFilename)
   
   oLyr = ds.CreateLayer(maskFilename, geom_type=ogr.wkbPolygon)
   featDefn = oLyr.GetLayerDefn()

   maskFeat = ogr.Feature(featDefn)
   maskFeat.SetGeometry(maskGeom)
   oLyr.CreateFeature(maskFeat)
   maskFeat = None
   ds = None
 
# .............................................................................
def getMergedIntersectingEcoregions(mergedPtGeom, ecoregionFilename):
   """
   @summary: Find the features that intersect the merged points geometry and 
                merge them together
   """
   drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
   ds = drv.Open(ecoregionFilename)
   lyr = ds.getLayer(0)
   
   regGeom = None
   for feat in lyr:
      geom = feat.GetGeometryRef()
      if geom.Intersection(mergedPtGeom) is not None:
         if regGeom is None:
            regGeom = geom.Clone()
         else:
            regGeom = regGeom.Union(geom)
   
   lyr = None
   ds = None
   return regGeom

# .............................................................................
def getMergedPointsGeometry(pointsFilename):
   """
   @summary: Merge all point geometries into one
   """
   drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
   ds = drv.Open(pointsFilename)
   ptLayer = ds.GetLayer(0)
   
   ptGeom = None
   for feature in ptLayer:
      geom = feature.GetGeometryRef()
      if ptGeom is None:
         ptGeom = geom.Clone()
      else:
         ptGeom = ptGeom.Union(geom)
   
   ptLayer = None
   ds = None
   
   return ptGeom
 
# .............................................................................
if __name__ == '__main__':
   
   parser = argparse.ArgumentParser(
      description='Create a convex hull based mask for modeling an occurrence set')
   parser.add_argument('occId', type=int, 
                       help='The id of the occurrence set to use')
   parser.add_argument('ecoFilename', type=str, 
                       help='The file location of the ecoregions shapefile')
   parser.add_argument('outFilename', type=str, 
                       help='The location to write the output mask shapefile')
   args = parser.parse_args()
   
   getMaskShapefile(args.occId, args.ecoFilename, args.outFilename)
   
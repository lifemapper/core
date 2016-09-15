"""
@summary: Module containing methods to build a shapegrid
@author: Jeff Cavner ; modified by CJ Grady
@license: gpl2
@version: 4.0.0
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
@note: This was created from Jeff's code that ran on the web server.  
"""
import math
import numpy as np
from osgeo import ogr, osr

from LmCommon.common.lmconstants import (JobStatus, OutputFormat, 
                                         DEFAULT_OGR_FORMAT)
from LmCompute.common.lmObj import LmException

# Calculate this once and store as a constant instead of for every cell
SQRT_3 = math.sqrt(3)

# .............................................................................
def buildShapegrid(workDir, minX, minY, maxX, maxY, cellSize, epsgCode, cellSides, 
                   siteId='siteid', siteX='centerX', siteY='siteY', 
                   cutoutWKT=None):
   """
   @summary: This function builds a shapegrid from the provided parameters
   @param workDir: A directory to use for performing work
   @param env: Environment methods that can be used by this function
   @param minX: The minimum value for X
   @param minY: The maximum value for Y
   @param maxX: The maximum value for X
   @param maxY: The maximum value for Y
   @param cellSize: The size of each cell (in units indicated by EPSG code)
   @param epsgCode: The EPSG code for the new shapegrid
   @param cellSides: The number of sides for each cell of the shapegrid 
                        (4 - squares, 6 - hexagons)
   @param siteId: The name of the site id field for the shapefile
   @param siteX: The name of the X field for the shapefile
   @param siteY: The name of the Y field for the shapefile
   @param cutout: (optional) WKT for an area of the shapegrid to be cutout
   """
   shapePath = os.path.join(workDir, "shape{ext}".format(ext=OutputFormat.SHAPE))
   
   # Just in case we decide that these can vary at some point
   xRes = cellSize
   yRes = cellSize
   
   # Initialize shapefile
   tSrs = osr.SpatialReference()
   tSrs.ImportFromEPSG(epsgCode)
   
   drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
   ds = drv.CreateDataSource(shapePath)
   
   layer = ds.CreateLayer(ds.GetName(), geom_type=ogr.wkbPolygon, srs=tSrs)
   layer.CreateField(ogr.FieldDefn(siteId, ogr.OFTInteger))
   layer.CreateField(ogr.FieldDefn(siteX, ogr.OFTReal))
   layer.CreateField(ogr.FieldDefn(siteY, ogr.OFTReal))
   
   if cellSides == 6: # Hexagonal cells
      # Define a lambda function for generating hexagon WKT
      wkt = lambda x, y: \
                  "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f, %f %f, %f %f))" % \
                     (x - (xRes * .25), y + (yRes * .25)*SQRT_3,
                      x + (xRes * .25), y + (yRes * .25)*SQRT_3,
                      x + (xRes * .5),  y,
                      x + (xRes * .25), y - (yRes * .25)*SQRT_3,
                      x - (xRes * .25), y - (yRes * .25)*SQRT_3,
                      x - (xRes * .5),  y,
                      x - (xRes * .25), y + (yRes * .25)*SQRT_3
                     )
                     
      yStep = -1 * yRes * SQRT_3 / 4
      xStep = xRes * 1.5
      
      # ....................................
      def generateXYs():
         """
         @summary: Generates X and Y values for a hexagonal shape grid
         @note: Every other row must be shifted
         """
         y = maxY
         yRow = True
         while y > minY:
            # Every other row needs to be shifted slightly
            if yRow:
               x = minX
            else:
               x = minX + (xRes * .75)
               
            while x < maxX:
               yield x, y
               x += xStep
            yRow = not yRow
            y += yStep
      
   elif cellSides == 4: # Square cells
      # Define a lambda function for generating square WKT
      wkt = lambda x, y: "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" % \
                            (x,        y, 
                             x + xRes, y, 
                             x + xRes, y-yRes, 
                             x,        y-yRes, 
                             x,        y
                            )
      
      # ....................................
      def generateXYs():
         """
         @summary: Generates X and Y values for a rectangular grid
         @note: This is the only place that numpy is used in this module.  
                   Consider removing to reduce imports
         """
         for y in np.arange(maxY, minY, -yRes):
            for x in np.arange(minX, maxX, xRes):
               yield x, y
   else:
      raise LmException(JobStatus.RAD_SHAPEGRID_INVALID_PARAMETERS,
                        "Unknown number of cell sides %s" % cellSides)
         
   shapeId = 0
   # CJG (08/10/2015):
   # I combined both the X and Y loops into one generator function that will
   #   generate the same x and y values.  I did this so that I could define the
   #   looping mechanism for squares and hexagons and then just use the 
   #   appropriate one.  This eliminates the need for duplicating code for
   #   building cells
   for x, y in generateXYs():
      geom = ogr.CreateGeometryFromWkt(wkt(x, y))
      geom.AssignSpatialReference(tSrs)
      c = geom.Centroid()
      xC = c.GetX()
      yC = c.GetY()
      feat = ogr.Feature(feature_def=layer.GetLayerDefn())
      feat.SetGeometryDirectly(geom)
      feat.SetField(siteX, xC)
      feat.SetField(siteY, yC)
      feat.SetField(siteId, shapeId)
      layer.CreateFeature(feat)
      feat.Destroy()
      shapeId += 1
   ds.Destroy()
                  
   if cutoutWKT is not None:
      cutoutPath = os.path.join(workDir, "cutout{ext}".format(ext=OutputFormat.SHAPE))
      status = cutout(shapePath, cutoutPath, cutoutWKT, epsgCode, siteId)
      return cutoutPath, status
   else:
      return shapePath, JobStatus.COMPUTED

# .............................................................................
def cutout(origPath, cutoutPath, cutoutWKT, epsgCode, siteId):
   """
   @summary: Cutout an area of a shapegrid defined by WKT
   @param origPath: The path to the original shapegrid to cut out 
   @param cutoutPath: The path to use for the cutout shapegrid
   @param cutoutWKT: Well-Known Text representing the area to be cut out
   @param epsgCode: The EPSG code for this shapegrid
   @param siteId: The name of the site id field for the shapegrid
   """
   ods = ogr.Open(origPath)
   origLayer = ods.GetLayer(0)
   
   if not origLayer:
      raise LmException(JobStatus.IO_LAYER_READ_ERROR, 
                        "Could not open shapegrid for cut out")
      
   tSrs = osr.SpatialReference()
   tSrs.ImportFromEPSG(epsgCode)
   
   drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
   ds = drv.CreateDataSource(cutoutPath)
   newLayer = ds.CreateLayer(ds.GetName(), geom_type=ogr.wkbPolygon, srs=tSrs)

   # Copy the fields
   origLyrDefn = origLayer.GetLayerDefn()
   origFieldCnt = origLyrDefn.GetFieldCount()
   for fieldIdx in range(0, origFieldCnt):
      origFldDef = origLyrDefn.GetFieldDefn(fieldIdx)
      newLayer.CreateField(origFldDef)

   # Create geometry from cutout WKT
   selectedPoly = ogr.CreateGeometryFromWkt(cutoutWKT)
   minX, maxX, minY, maxY = selectedPoly.GetEnvelope()
   origFeature = origLayer.GetNextFeature()
   siteIdIdx = origFeature.GetFieldIndex(siteId)
   newSiteId = 0

   # Loop through features and clone those that intersect cutout
   while origFeature is not None:
      clone = origFeature.Clone()
      cloneGeomRef = clone.GetGeometryRef()
      if cloneGeomRef.Intersect(selectedPoly):
         newLayer.CreateFeature(clone)
         newSiteId += 1
      origFeature = origLayer.GetNextFeature()
   ds.Destroy()
   
   # No intersecting cells
   if newSiteId == 0:
      raise LmException(JobStatus.RAD_SHAPEGRID_NO_CELLS, 
                        "No cells intersected the cutout")
   
   return JobStatus.COMPUTED

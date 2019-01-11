"""
@summary: Module containing methods to build a shapegrid
@author: Jeff Cavner ; modified by CJ Grady
@license: gpl2
@version: 4.0.0
@copyright: Copyright (C) 2019, University of Kansas Center for Research
 
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

from LmCommon.common.lmconstants import (LMFormat, DEFAULT_GLOBAL_EXTENT, 
                           DEFAULT_EPSG, DEFAULT_CELLSIZE, DEFAULT_CELLSIDES)

# Calculate this once and store as a constant instead of for every cell
SQRT_3 = math.sqrt(3)


# .............................................................................
def generateHexagonWKTs(minX, minY, maxX, maxY, xRes, yRes):
   """
   @summary: Generator producing hexagonal WKT for cells for the shapegrid
   @param minX: The minimum X value
   @param minY: The minimum Y value
   @param maxX: The maximum X value
   @param maxY: The maximum Y value
   @param xRes: The X size of the cell
   @param yRes: The Y size of the cell
   """
   wkt = "POLYGON(({x1} {y1},{x2} {y1},{x3} {y2},{x2} {y3},{x1} {y3}, {x4} {y2}, {x1} {y1}))"
   
   y = maxY
   yRow = True
   yStep = -1 * yRes * SQRT_3 / 4
   xStep = xRes * 1.5

   while y > minY:
      # Every other row needs to be shifted slightly
      if yRow:
         x = minX
      else:
         x = minX + (xRes * .75)
         
      while x < maxX:
         thisWKT = wkt.format(x1=x - (xRes * .25), 
                              x2=x + (xRes * .25), 
                              x3=x + (xRes * .5), 
                              x4=x - (xRes * .5),
                              y1=y + (yRes * .25) * SQRT_3,
                              y2=y,
                              y3=y - (yRes * .25) * SQRT_3)
         yield thisWKT
         x += xStep
      yRow = not yRow
      y += yStep
                     
# .............................................................................
def generateSquareWKTs(minX, minY, maxX, maxY, xRes, yRes):
   """
   @summary: Generator producing square WKT for cells for the shapegrid
   @param minX: The minimum X value
   @param minY: The minimum Y value
   @param maxX: The maximum X value
   @param maxY: The maximum Y value
   @param xRes: The X size of the cell
   @param yRes: The Y size of the cell
   """
   wkt = "POLYGON(({x1} {y1},{x2} {y1},{x2} {y2},{x1} {y2},{x1} {y1}))"

   for y in np.arange(maxY, minY, -yRes):
      for x in np.arange(minX, maxX, xRes):
         thisWKT = wkt.format(x1=x, x2=x + xRes, y1=y, y2=y - yRes)
         yield thisWKT

# .............................................................................
def buildShapegrid(sgFn, minX, minY, maxX, maxY, cellSize, epsgCode, cellSides, 
                   siteId='siteid', siteX='siteX', siteY='siteY', 
                   cutoutWKT=None):
   """
   @summary: Builds a shapegrid with an optional cutout
   @param sgFn: The location to store the resulting shapegrid
   @param minX: The minimum value for X of the shapegrid
   @param minY: The minimum value for Y of the shapegrid
   @param maxX: The maximum value for X of the shapegrid
   @param maxY: The maximum value for Y of the shapegrid
   @param epsgCode: The EPSG code for the new shapegrid
   @param cellSize: The size of each cell (in units indicated by EPSG code)
   @param cellSides: The number of sides for each cell of the shapegrid 
                        (4 - squares, 6 - hexagons)
   @param siteId: The name of the site id field for the shapefile
   @param siteX: The name of the X field for the shapefile
   @param siteY: The name of the Y field for the shapefile
   @param cutout: (optional) WKT for an area of the shapegrid to be cutout
   @return number of features in the new shapegrid
   """
   # We'll always check for intersection to reduce amount of work
   if cutoutWKT is None:
      # Make WKT for entire area
      cutoutWKT = "POLYGON(({x1} {y1},{x2} {y1},{x2} {y2},{x1} {y2},{x1} {y1}))".format(
         x1=minX, x2=maxX, y1=maxY, y2=minY)
   selectedPoly = ogr.CreateGeometryFromWkt(cutoutWKT)
   #minX, maxX, minY, maxY = selectedPoly.GetEnvelope()
   
   # Just in case we decide that these can vary at some point
   xRes = cellSize
   yRes = cellSize
   
   # Initialize shapefile
   tSrs = osr.SpatialReference()
   tSrs.ImportFromEPSG(epsgCode)
   
   drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
   ds = drv.CreateDataSource(sgFn)

   layer = ds.CreateLayer(ds.GetName(), geom_type=ogr.wkbPolygon, srs=tSrs)
   layer.CreateField(ogr.FieldDefn(siteId, ogr.OFTInteger))
   layer.CreateField(ogr.FieldDefn(siteX, ogr.OFTReal))
   layer.CreateField(ogr.FieldDefn(siteY, ogr.OFTReal))

   # Set up generator
   if cellSides == 4:
      wktGenerator = generateSquareWKTs(minX, minY, maxX, maxY, xRes, yRes)
   elif cellSides == 6:
      wktGenerator = generateHexagonWKTs(minX, minY, maxX, maxY, xRes, yRes)
   else:
      raise Exception(
              "Don't know how to generate shapegrid with {0} sides".format(
                 cellSides))
   
   shapeId = 0
   for wkt in wktGenerator:
      geom = ogr.CreateGeometryFromWkt(wkt)
      geom.AssignSpatialReference(tSrs)
      c = geom.Centroid()
      xC = c.GetX()
      yC = c.GetY()
      feat = ogr.Feature(feature_def=layer.GetLayerDefn())
      feat.SetGeometryDirectly(geom)
      feat.SetField(siteX, xC)
      feat.SetField(siteY, yC)
      feat.SetField(siteId, shapeId)
      
      # Check for intersection
      if geom.Intersection(selectedPoly):
         layer.CreateFeature(feat)
         shapeId += 1
      feat.Destroy()
   ds.Destroy()
   return shapeId

# ...............................................
if __name__ == '__main__':
   dlocation = '/tmp/shpgrid_test.shp'
   (minX, minY, maxX, maxY) = DEFAULT_GLOBAL_EXTENT
   cellsize = DEFAULT_CELLSIZE
   epsgcode = DEFAULT_EPSG
   cellsides = DEFAULT_CELLSIDES
   (siteId, siteX, siteY) = ('siteid', 'centerX', 'centerY')
   cutout = None
    
   count = buildShapegrid(dlocation, minX, minY, maxX, maxY, cellsize, epsgcode, 
                          cellsides, siteId=siteId, siteX=siteX, siteY=siteY, 
                          cutoutWKT=cutout) 
   print count
   print('Try ogrinfo on {}'.format(dlocation))
   
   
"""
from LmCommon.shapes.buildShapegrid import buildShapegrid

(dlocation, minX, minY, maxX, maxY, cellsize, epsgcode, cellsides, siteId, 
 siteX, siteY, cutout) = ('/tmp/shpgrid_test.shp', 
 -180.0, -60.0, 180.0, 90.0, 1.0, 4326, 4, 'siteid', 'centerX', 'centerY', None)
 
count = buildShapegrid(dlocation, minX, minY, maxX, maxY, cellsize, epsgcode, 
cellsides, siteId=siteId, siteX=siteX, siteY=siteY, cutoutWKT=cutout) 

"""

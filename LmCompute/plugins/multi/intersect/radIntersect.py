"""
@summary: Module containing methods used to intersect a layer with a shapegrid
@author: CJ Grady (original Jeff Cavner)
@version: 4.0.0
@status: beta
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
import numpy as np
from osgeo import ogr
import rtree

from LmCommon.common.lmconstants import DEFAULT_OGR_FORMAT
from LmCommon.common.matrix import Matrix

from LmCompute.common.agoodle import AGoodle

#................................................              
def _openVectorLayer(dlocation):
   """
   @summary: Open a vector layer and return the dataset
   @param dlocation: The file location of the vector layer
   """
   ogr.RegisterAll()
   drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
   try:
      ds = drv.Open(dlocation)
   except Exception, e:
      raise Exception, 'Invalid datasource, %s: %s' % (dlocation, str(e))
   return ds

#................................................      
def _getShapegridGeomDict(dlocation):
   """
   @summary: Opens a shapegrid shapefile and returns a dictionary of feature id,
                geometry WKT
   @param dlocation: The file location of the shapefile 
   """
   ds = _openVectorLayer(dlocation)
   lyr = ds.GetLayer(0)
   # Read featureId and geometry
   siteGeomDict = {}
   featCount = lyr.GetFeatureCount()
   minx, maxx, miny, maxy = lyr.GetExtent()
   for j in range(featCount):
      currFeat = lyr.GetFeature(j)
      siteidx = currFeat.GetFID()
      # same as Shapegrid.siteIndices
      siteGeomDict[siteidx] = currFeat.geometry().ExportToWkt()
   
   return siteGeomDict
      
#..............................................................................
def _getRasterAreaDictionary(sgFn, rasterFn, resolution):
   """
   @summary: Intersects a Raster dataset by reading as a AGoodle raster 
             object.  Compares the shapegrid cell resolution against the 
             raster resolution.  
             If the shapegrid cell is 5X the resolution of the raster
               * then a regular AGoodle intersection is used, which 
                 treats the raster pixels within the polygon as a numpy matrix 
                 in pixel coords and uses matplotlib to find points, in the 
                 form of an array, that fall within the polygon vertices in 
                 integer pixel coords.  
               * otherwise, each raster pixel is treated as a polygon in real 
                 coords and is intersected with the shapegrid cell polygons.  
             Returns an array of presence (1), and absence(0) for each site
   @todo: Evaluate this, could we do this all better?
   """
   sgSiteGeomDict = _getShapegridGeomDict(sgFn)
   raster =  AGoodle(rasterFn)
          
   areaDict = {}
   for siteIdx, geom in sgSiteGeomDict.iteritems():
      cellgeom = ogr.CreateGeometryFromWkt(geom)
      cellarea = cellgeom.GetArea() 
      if cellarea > (resolution**2) * 25:                  
         summary = raster.summarize_wkt(geom)
      else:        
         summary = raster.raster_as_poly(geom)
      areaDict[siteIdx] = (summary, cellarea)
   return areaDict

# .............................................................................
# .                             Public functions                              .
# .............................................................................
# .............................................................................
def grimRasterIntersect(sgFn, rasterFn, resolution, minPercent=None, 
                        ident=None):
   """
   @summary: Intersects an environment raster layer with a shapegrid to create
                a GRIM vector, returned as a Matrix
   @param sgFn: The file location of the shapegrid
   @param rasterFn: The file location of the input environment raster
   @param resolution: The resolution of the raster
   @param minPercent: If provided, use the largest class method, otherwise use
                         the weighted mean method [0-100]
   @param ident: An identifier to be used as column metadata for the resulting
                    GRIM vector Matrix
   @todo: Do we need resolution?  Couldn't we get that from the raster?
   """
   # Get the area dictionary
   areaDict = _getRasterAreaDictionary(sgFn, rasterFn, resolution)

   layerArray = np.zeros((len(areaDict.keys()), 1), dtype=float)
   
   if minPercent is not None:
      # Largest class method
      minPercent = minPercent / 100.0
      for siteidx, (summary, cellarea) in areaDict.iteritems():
         maxArea = max(summary.values())
         if maxArea / cellarea >= minPercent:
            layerArray[siteidx, 0] = summary.keys()[summary.values().index(maxArea)]
         else:
            layerArray[siteidx, 0] = np.nan
   else:
      # Weighted mean method
      for siteidx, (summary, cellarea) in areaDict.iteritems():
         numerator = 0
         denominator = 0
         for pixelvalue in summary.keys():
            numerator += float(summary[pixelvalue]) * pixelvalue
            denominator += float(summary[pixelvalue])
         try:
            weightedMean = numerator / denominator
         except:
            weightedMean = 0
         layerArray[siteidx, 0] = weightedMean
   
   if ident is not None:
      headers = {1: [ident]}
   else:
      headers = None
      
   grimVector = Matrix(layerArray, headers=headers)
   
   return grimVector

# .............................................................................
def pavRasterIntersect(sgFn, rasterFn, resolution, minPresence, maxPresence, 
                       percentPresence, squid=None):
   """
   @summary: Intersects a raster layer with a shapegrid to create a PAV Matrix
                column
   @param sgFn: The file location of the shapegrid used for intersection
   @param rasterFn: The raster file to use for intersection
   @param resolution: The resolution of the raster
   @param minPresence: The minimum value to be considered presence
   @param maxPresence: The maximum value to be considered presence
   @param percentPresence: The percentage of shapgrid cell coverage required to
                              be considered presence [0,100]
   @param squid: Species identifier used for metadata
   @todo: Evaluate
   @todo: Do we need resolution?  Couldn't we get that from the raster?
   """
   # Get the area dictionary
   areaDict = _getRasterAreaDictionary(sgFn, rasterFn, resolution)
   percentPresenceDec = percentPresence / 100.0
   rowcount = len(areaDict)
   layerArray = np.zeros((rowcount, 1), dtype=bool)
   counter = 0
   for _, (summary, cellarea) in sorted(areaDict.iteritems()):
      mySum = 0
      for pixelvalue in summary.keys():
         if (pixelvalue >= minPresence) and (pixelvalue <= maxPresence):
            mySum += + summary[pixelvalue]
      if mySum > (cellarea * percentPresenceDec):
         layerArray[counter, 0] = True
      counter += 1
   
   if squid is not None:
      headers = {1: [squid]}
   else:
      headers = None
      
   pav = Matrix(layerArray, headers=headers)
   
   return pav

# .............................................................................
def pavVectorIntersect(sgFn, vectFn, presenceAttrib, minPresence, maxPresence, 
                    percentPresence, squid=None):
   """
   @summary: Intersects a vector layer with a shapegrid
   @param sgFn: File location of the shapegrid shapefile
   @param vectFn: The file location of the vector file to intersect with the 
                     shapegrid
   @param presenceAttrib: The attribute in the vector's attribute table that 
                             indicates presence
   @param minPresence: The minimum value of the field to signify presence
   @param maxPresence: The maximum value of the field to signify presence
   @param percentPresence: The percent of the shapegrid polygon that must be
                              considered present for the cell value to be 
                              present
   @param squid: An identifier to use as a header for the PAV Matrix column that
                    connects back to the species
   @return: A Matrix object with one column
   @todo: Evaluate this function to see if it should be rewritten
   """
   # Note: this must be disabled later, 
   # UseExceptions causes failures in AGoodle, possibly elsewhere
   ogr.UseExceptions()

   ds1 = _openVectorLayer(sgFn)
   sgLyr = ds1.GetLayer(0)
   sgMinx, sgMaxx, sgMiny, sgMaxy = sgLyr.GetExtent()
   ds2 = _openVectorLayer(vectFn)
   vLyr = ds2.GetLayer(0)
   vMinx, vMaxx, vMiny, vMaxy = vLyr.GetExtent()
   percentPresenceDec = percentPresence / 100.0

   layerArray = np.zeros((sgLyr.GetFeatureCount(), 1), dtype=bool)     
   
   # make a polygon for broad intersection from the extent of the shapegrid
   points = "{minX} {minY}, \
             {maxX} {minY}, \
             {maxX} {maxY}, \
             {minX} {maxY}, \
             {minX} {minY}".format(minX=str(sgMinx), maxX=str(sgMaxx), 
                                   minY=str(sgMiny), maxY=str(sgMaxy))
   wktstring = 'Polygon (( ' + points + ' ))'
   sgExtentPoly = ogr.CreateGeometryFromWkt(wktstring) 
   fltMinx = max(sgMinx, vMinx)
   fltMiny = max(sgMiny, vMiny)
   fltMaxx = min(sgMaxx, vMaxx)
   fltMaxy = min(sgMaxy, vMaxy)
   # yes, changed order   
   sgLyr.SetSpatialFilterRect(fltMinx, fltMiny, fltMaxx, fltMaxy)
    
   # For ShapeGrid ...
   #   create a dictionary of cells w/in spatial filter, add to rtree index 
   rtreeIndex = rtree.index.Index()
   #   create a dictionary of cells for running total of intersected area 
   areaDict = {}
   cell = sgLyr.GetNextFeature()     
   while cell is not None:
      cellFID = cell.GetFID()
      areaDict[cellFID] = ([],[]) 
      minx, maxx, miny, maxy = cell.GetGeometryRef().GetEnvelope()
      # yes, changed order
      rtreeIndex.insert(cellFID, (minx, miny, maxx, maxy))      
      cell = sgLyr.GetNextFeature()  
       
   # For Vector layer ...
   feat = vLyr.GetNextFeature()
   # Find presence attribute
   presIdx = feat.GetFieldIndex(presenceAttrib)
   while feat is not None:
      pval = feat.GetFieldAsDouble(presIdx)
      if pval >= minPresence and pval <= maxPresence:
         # .....................................................
         polyGeom = feat.GetGeometryRef()
         shpgridcellfids = []
         if polyGeom.Intersect(sgExtentPoly):
            firstintersection = polyGeom.Intersection(sgExtentPoly)              
            minx, maxx, miny, maxy = firstintersection.GetEnvelope()
            shpgridcellfids = list(rtreeIndex.intersection((minx, miny, maxx, 
                                                                        maxy))) 
         else:
            minx, maxx, miny, maxy = polyGeom.GetEnvelope()
                
         # for each feature id in intersection list check and see if corresponding
         # item in areaDict is already coded with area total of cellshape, this keeps
         # cells from being recoded that have an intersection total that is already
         # equal or larger than the area of the cell
         if len(shpgridcellfids) > 0:              
            for cfid in shpgridcellfids:
               shpgrdfeature = sgLyr.GetFeature(cfid) 
               gridgeom =  shpgrdfeature.GetGeometryRef() 
               cellarea = gridgeom.GetArea() 
               
               if sum(areaDict[cfid][0]) < cellarea:   
                  if firstintersection.Contains(gridgeom):                    
                     areaDict[cfid][0].append(cellarea) 
                     if not areaDict[cfid][1]:
                        areaDict[cfid][1].append(cellarea)
                  else:                        
                     if firstintersection.Intersect(gridgeom):
                        intersection = firstintersection.Intersection(gridgeom)
                        area = intersection.GetArea()
                        areaDict[cfid][0].append(area) 
                        if not areaDict[cfid][1]:
                           areaDict[cfid][1].append(cellarea)
                         
               del shpgrdfeature
         # .....................................................
            
      feat = vLyr.GetNextFeature()
                         
   for fid, areas in areaDict.iteritems():
      if len(areas[0]) > 0: 
         if (sum(areas[0]) > (areas[1][0] * percentPresenceDec)):  
            layerArray[fid, 0] = True
   # Disable so doesn't cause AGoodle failures
   ogr.DontUseExceptions()
   
   if squid is not None:
      headers = {1: [squid]}
   else:
      headers = None
   
   pav = Matrix(layerArray, headers=headers)
   
   return pav

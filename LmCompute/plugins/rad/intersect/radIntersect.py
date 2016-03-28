"""
@summary: Module containing methods used to intersect a layer with a shapegrid

@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research
 
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
import os
from osgeo import ogr
import rtree

from LmCommon.common.lmconstants import JobStatus, OutputFormat
from LmCompute.common.agoodle import AGoodle

from LmBackend.common.subprocessManager import SubprocessManager, \
                                             VariableContainer
 
# Relative path (inside plugin directory) to intersect single layer script
INTERSECT_LAYER_SCRIPT = "rad/intersect/radIntersect.py"
 
# .............................................................................
class IntersectLayerObj(object):
   """
   @summary: Intersect Layer object that goes inside of a VariableContainer for 
                serialization and then passed to a subprocess
   """
   def __init__(self, shapegrid, lyrVals, fileName):
      self.shapegrid = shapegrid
      self.layerVals = lyrVals
      self.fileName = fileName

# .............................................................................
def intersect(lyrset, shapegrid, env, outputDir=None):
   """
   @summary: Method used to calculate a RADIntersectJob
   @param lyrset: a dictionary of layer matrix index keys, each with a 
                  dictionary of values including dlocation and intersection 
                  parameters
   @param shapegrid: dlocation of a shapegrid
   @param env: The environment this will run in
   @return: a dictionary of matrixIndex keys with a vector for each 
            representing the layer intersection for all sites in the shapegrid 
   """
   scriptLocation = os.path.join(env.getPluginsPath(), INTERSECT_LAYER_SCRIPT)
   lyrArrays = {}
   try:
      if len(lyrset) == 0:
         status = JobStatus.RAD_INTERSECT_ZERO_LAYERS_ERROR
      else:
         cmds = []
         layerArrayFns = {}
         
         for mtxIdx, lyrvals in lyrset.iteritems():
            fn = env.getTemporaryFilename(OutputFormat.NUMPY, base=outputDir)
            
            vc = VariableContainer(IntersectLayerObj(shapegrid, lyrvals, fn))
            layerArrayFns[mtxIdx] = fn
            
            cmd = "{python} {scriptLocation} \"{args}\"".format(
                     python=env.getPythonCmd(),
                     scriptLocation=scriptLocation,
                     args=str(vc))
            cmds.append(cmd)
         
         spm = SubprocessManager(commandList=cmds)
         spm.runProcesses()
            
         for key in layerArrayFns.keys():
            fn = layerArrayFns[key]
            lyrArrays[key] = fn
            #lyrArrays[key] = np.load(fn)
            #os.remove(fn)
            
         status = JobStatus.COMPLETE
   except Exception, e:
      print str(e)
      status = JobStatus.RAD_INTERSECT_ERROR
   
   return status, lyrArrays
 
 
# ...............................................
def intersectLayer(shapegrid, layerVals):
   """
   @summary: Open a PresenceAbsenceRaster or PresenceAbsenceVector and 
             intersect data with ShapeGrid cells using values defining
             presence, absence, and percent overlap to calculate presence or
             absence for each cell. 
   """
   # Raster intersect
   if layerVals['isRaster']:
      areaDict = _rasterIntersect(shapegrid['dlocation'], 
                                  shapegrid['localIdIdx'], 
                                  layerVals['dlocation'], 
                                  layerVals['resolution'])
      
      if layerVals.has_key('attrPresence'):
         lyrarray = _calcRasterPresenceAbsenceColumn(areaDict, 
                                          layerVals['attrPresence'],
                                          layerVals['minPresence'],
                                          layerVals['maxPresence'],
                                          layerVals['percentPresence'],
                                          layerVals['attrAbsence'],
                                          layerVals['minAbsence'],
                                          layerVals['maxAbsence'],
                                          layerVals['percentAbsence'])
      else:
         if layerVals['weightedMean']:
            lyrarray = _calcRasterWeightedMeanColumn(areaDict)
         elif layerVals['largestClass']:
            lyrarray = _calcRasterLargestClassColumn(areaDict, 
                                                     layerVals['minPercent'])
   # Vector intersect
   elif layerVals.has_key('attrPresence'):
      lyrarray = _vectorIntersect(shapegrid['dlocation'], 
                                  shapegrid['localIdIdx'], 
                                  layerVals['dlocation'], 
                                  layerVals['attrPresence'],
                                  layerVals['minPresence'],
                                  layerVals['maxPresence'],
                                  layerVals['percentPresence'],
                                  layerVals['attrAbsence'],
                                  layerVals['minAbsence'],
                                  layerVals['maxAbsence'],
                                  layerVals['percentAbsence'])
   else:
      raise Exception('Ancillary Vector intersection is not yet supported')

   return lyrarray
          
#................................................      
def _openShapefile(dlocation, localIdIdx):
   """
   """
   ds = _openVectorLayer(dlocation)
   lyr = ds.GetLayer(0)
   # Read featureId and geometry
   siteGeomDict = {}
   featCount = lyr.GetFeatureCount()
   minx, maxx, miny, maxy = lyr.GetExtent()
   for j in range(featCount):
      currFeat = lyr.GetFeature(j)
      if localIdIdx is not None:
         siteidx = currFeat.GetField(localIdIdx)      
      else:
         siteidx = currFeat.GetFID()
      # same as Shapegrid.siteIndices
      siteGeomDict[siteidx] = currFeat.geometry().ExportToWkt()
   
   return lyr, siteGeomDict, (minx, maxx, miny, maxy)
      

#................................................      
def _rasterIntersect(sgDLocation, sgLocalIdIdx, lyrDLocation, lyrResolution):
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
   @param layer: the PresenceAbsenceRaster object
   """     
   sgLyr, sgSiteGeomDict, sgExtent = _openShapefile(sgDLocation, sgLocalIdIdx)
   raster =  AGoodle(lyrDLocation)
          
   areaDict = {}
   for siteIdx, geom in sgSiteGeomDict.iteritems():
      cellgeom = ogr.CreateGeometryFromWkt(geom)
      cellarea = cellgeom.GetArea() 
      if cellarea > (lyrResolution**2) * 25:                  
         summary = raster.summarize_wkt(geom)
      else:        
         summary = raster.raster_as_poly(geom)
      areaDict[siteIdx] = (summary, cellarea)
   return areaDict    

#................................................
def _calcRasterPresenceAbsenceColumn(areaDict, attrPresence, minPresence, 
                                     maxPresence, percentPresence,
                                     attrAbsence, minAbsence, maxAbsence, 
                                     percentAbsence):
   percentPresenceDec = percentPresence/100.0
   rowcount = len(areaDict)
   layerArray = np.zeros(rowcount, dtype=bool)
   counter = 0
   for siteidx, (summary, cellarea) in sorted(areaDict.iteritems()):
      sum = 0
      for pixelvalue in summary.keys():
         if (pixelvalue >= minPresence) and (pixelvalue <= maxPresence):
            sum = sum + summary[pixelvalue]
      if sum > (cellarea * percentPresenceDec):
         # don't use siteidx, in case there are missing site ids
         # array is indexed purely by position
         layerArray[counter] = True 
      counter += 1  
   return layerArray
 
#................................................
def _calcRasterLargestClassColumn(areaDict, minPercent):
   layerArray = np.zeros(len(areaDict), dtype=float)
   minPercent = minPercent / 100.0
   for siteidx, (summary, cellarea) in areaDict.iteritems():
      maxArea = max(summary.values())
      if maxArea / cellarea >= minPercent:
         layerArray[siteidx] = summary.keys()[summary.values().index(maxArea)]
      else:
         layerArray[siteidx] = np.nan
   return layerArray      
       
#................................................  
def _calcRasterWeightedMeanColumn(areaDict):
   """
   @summary: calculates weighted mean for pixels within each cell
   of the shapegrid and returns a column (1-dimensional array) of floating 
   point numbers for the GRIM
   """
   layerArray = np.zeros(len(areaDict), dtype=float)
   for siteidx, (summary, cellarea) in areaDict.iteritems():
      numerator = 0
      denominator = 0
      for pixelvalue in summary.keys():
         numerator += float(summary[pixelvalue]) * pixelvalue
         denominator += float(summary[pixelvalue])
      weightedMean = numerator / denominator
      layerArray[siteidx] = weightedMean
   return layerArray

#................................................                             
def _getSpatialFilterBounds(layer, sgminx, sgmaxx, sgminy, sgmaxy):   
   minx = max(sgminx, layer.minX)
   miny = max(sgminy, layer.minY)
   maxx = min(sgmaxx, layer.maxX)
   maxy = min(sgmaxy, layer.maxY)
   return minx, miny, maxx,  maxy
       
#................................................              
def _openVectorLayer(dlocation):
   ogr.RegisterAll()
   drv = ogr.GetDriverByName('ESRI Shapefile')
   try:
      ds = drv.Open(dlocation)
   except Exception, e:
      raise Exception, 'Invalid datasource, %s: %s' % (dlocation, str(e))
   return ds

#................................................              
def _vectorIntersect(sgDLocation, sgLocalIdIdx, lyrDLocation, attrPresence, 
                     minPresence, maxPresence, percentPresence, attrAbsence, 
                     minAbsence, maxAbsence, percentAbsence):
   """
   @summary: Intersect a vector species layer input with cells in a 
             shapegrid.  Returns an array of presence (1), and absence(0)
             for each site
   """
   # Note: this must be disabled later, 
   # UseExceptions causes failures in AGoodle, possibly elsewhere
   ogr.UseExceptions()

   ds1 = _openVectorLayer(sgDLocation)
   sgLyr = ds1.GetLayer(0)
   sgMinx, sgMaxx, sgMiny, sgMaxy = sgLyr.GetExtent()
   ds2 = _openVectorLayer(lyrDLocation)
   vLyr = ds2.GetLayer(0)
   vMinx, vMaxx, vMiny, vMaxy = vLyr.GetExtent()
   percentPresenceDec = percentPresence/100.0

   layerArray = np.zeros(sgLyr.GetFeatureCount(), dtype=bool)     
   
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
   presIdx = feat.GetFieldIndex(attrPresence)
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
            layerArray[fid] = True
   # Disable so doesn't cause AGoodle failures
   ogr.DontUseExceptions()
   return layerArray

# .............................................................................
def boolStr(val):
   if val.strip().lower() == "false":
      return False
   else:
      return True

# .............................................................................
def getAttributeOrDefault(obj, attribute, default=None, func=str):
   try:
      return func(obj.__getattribute__(attribute).strip())
   except:
      return default

# .............................................................................
if __name__ == "__main__":
   import sys
   from LmCommon.common.lmXml import deserialize, fromstring
   if len(sys.argv) >= 2:
      obj = deserialize(fromstring(sys.argv[1]))
      lvs = obj.IntersectLayerObj.layerVals
      sg = obj.IntersectLayerObj.shapegrid
      fn = obj.IntersectLayerObj.fileName
        
      layerVals = {}
      shapegrid = {}
        
      lvKeys = [
                ('attrAbsence', str),
                ('attrPresence', str), 
                ('dlocation', str),
                ('isRaster', boolStr),
                ('isOrganism', boolStr),
                ('largestClass', str),
                ('maxAbsence', float),
                ('maxPresence', float),
                ('minAbsence', float),
                ('minPercent', float),
                ('minPresence', float),
                ('percentAbsence', float),
                ('percentPresence', int),
                ('resolution', float),
                ('weightedMean', boolStr)
               ]
       
      sgKeys = [
                ('dlocation', str),
                ('localIdIdx', int)
               ]
       
      for key, func in lvKeys:
         layerVals[key] = getAttributeOrDefault(lvs, key, func=func)
        
      for key, func in sgKeys:
         shapegrid[key] = getAttributeOrDefault(sg, key, func=func)
 
      layerArray = intersectLayer(shapegrid, layerVals)
       
      np.save(fn, layerArray)

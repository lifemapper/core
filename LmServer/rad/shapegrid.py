"""
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
from osgeo import ogr, osr
import os
from types import IntType
import math
import numpy as np
import rtree

from LmCommon.common.lmconstants import SHAPEFILE_EXTENSIONS
from LmServer.base.layer import _LayerParameters, Vector
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ProcessObject
from LmServer.common.lmconstants import LMFileType, LMServiceType, LMServiceModule

# .............................................................................
# .............................................................................

# .............................................................................
class ShapeGrid(_LayerParameters, Vector, ProcessObject):
# .............................................................................
   """
   shape grid class inherits from Vector
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, layername, cellsides, cellsize, mapunits, 
                epsgcode, bounds, dlocation=None, 
                ogrType=ogr.wkbPolygon, ogrFormat='ESRI Shapefile', 
                siteId='siteid', siteX='centerX', siteY='centerY', size=None, 
                userId=None, layerId=None, shapegridId=None, verify=None, 
                bucketId=None, status=None, statusModTime=None,
                modTime=None, createTime=None, metadataUrl=None):
      """
      @param layername: Short name for this shapegrid, unique for each user.
      @param dlocation: Data location interpretable by OGR.  If this is a 
                    shapefile, dlocation should be .shp file with the absolute 
                    path including filename with extension. 
      @param cellsides: Number of sides in each cell of a site (i.e. square =4,
                    hexagon = 6).
      @param cellsize: Size in mapunits of each cell.  For cellSides = 6 
                    (hexagon).HEXAGON, this is the measurement between two 
                    vertices.
      @param mapunits: units of map coordinates. These are keywords as used in 
                    mapserver, choice of 
                      [feet|inches|kilometers|meters|miles|nauticalmiles|dd],
                    described in http://mapserver.org/mapfile/map.html)
      @param size: Total number of cells in shapegrid
      @param epsgcode: integer representing the native EPSG code of this layer
      @param bounds : geographic boundary of the layer in one of 3 formats:
                    - a sequence in the format [minX, minY, maxX, maxY]
                    - a string in the format 'minX, minY, maxX, maxY'
      @param siteId: Attribute identifying the site number for each cell 
      @param siteX: Attribute identifying the center X coordinate for each cell 
      @param siteY: Attribute identifying the center Y coordinate for each cell 
      @param xsize: Number of cells in a row
      @param ysize: Number of cells in a column
      @param modtime: time/date last modified
      @param userId: (optional) Id for the owner of these data
      @param shapegridId: (optional) The primary key/id in the ShapeGrid table
                   of the database 
      """
      # siteIndices are a dictionary of {siteIndex: (FID, centerX, centerY)}
      self._siteIndices = None
      # field with unique values identifying each site
      self.siteId = siteId
      # field with longitude of the centroid of a site
      self.siteX = siteX
      # field with latitude of the centroid of a site
      self.siteY = siteY

      _LayerParameters.__init__(self, -1, modTime, userId, shapegridId)
      Vector.__init__(self, name=layername, bbox=bounds, dlocation=dlocation, 
         mapunits=mapunits, resolution=cellsize, epsgcode=epsgcode, ogrType=ogrType, 
         ogrFormat=ogrFormat, fidAttribute=siteId, 
         svcObjId=shapegridId, lyrId=layerId, lyrUserId=userId, verify=verify, 
         createTime=createTime, modTime=modTime, metadataUrl=metadataUrl, 
         serviceType=LMServiceType.LAYERS, moduleType=LMServiceModule.RAD)
      ProcessObject.__init__(self, objId=shapegridId, parentId=bucketId, 
                status=status, statusModTime=statusModTime)
      # Don't necessarily need centroids (requires reading shapegrid), can 
      # explicitely call public method initSites to initialize centroids and
      # sitesPresent dictionary if they are needed.
#      self.setSiteIndices()
      self._setMapPrefix()
      self._setCellsides(cellsides)
      self.cellsize = cellsize
      self._size = None
      self._setCellMeasurements(size)
      
# ...............................................
   def _createMapPrefix(self):
      """
      @summary: Construct the endpoint of a Lifemapper WMS URL for 
                this object.
      """
      mapprefix = self._earlJr.constructMapPrefix(ftype=LMFileType.SHAPEGRID,
                     usr=self._userId, epsg=self._epsg, lyrname=self.name)
      return mapprefix
    
# ...............................................
   @property
   def mapPrefix(self): 
      return self._mapPrefix
    
   def _setMapPrefix(self, mapprefix=None):
      if mapprefix is None:
         mapprefix = self._createMapPrefix()
      self._mapPrefix = mapprefix
          
# ...............................................
   def _setCellsides(self, cellsides):
      if cellsides == 4 or cellsides == 6:
         self._cellsides = cellsides
      else:
         raise LMError('Invalid cellshape. Only 4 (square) and 6 (hexagon) ' +
                       'sides are currently supported')
         
   def _getCellsides(self):
      return self._cellsides
   
   cellsides = property(_getCellsides)
   
# ...............................................
   def _setCellMeasurements(self, size):
      self._size = None
      if size is not None and isinstance(size, IntType):
         self._size = size
      else:
         self._size = self._getFeatureCount()
               
   def _getSize(self):
      return self._size
   size = property(_getSize)   
            
# ...............................................
   def initSitesPresent(self):
      sitesPresent = {}
      if self._siteIndices is None:
         self.setSiteIndices()
      if self._siteIndices is not None:
         for s in self._siteIndices.keys():
            sitesPresent[s] = True
      return sitesPresent
   
# ...............................................
   def setSiteIndices(self):
      self._siteIndices = {}
      if not(self._features) and self.getDLocation() is not None:
         self.readData()
      if self._features: 
         for siteidx in self._features.keys():
            if siteidx is None:
               print 'WTF?'
            geom = self._features[siteidx][self._geomIdx]
            self._siteIndices[siteidx] = geom

# ...............................................
   def createLocalDLocation(self):
      """
      @summary: Calculates and returns the local _dlocation.
      """
      dloc = self._earlJr.createFilename(LMFileType.SHAPEGRID, lyrname=self.name, 
                usr=self._userId, epsg=self._epsg)
      return dloc

# ...............................................
   def setDLocation(self, dloc=None):
      """
      @summary: Set the Layer._dlocation attribute if it is None.  Use dlocation
                if provided, otherwise calculate it.
      @note: If _dlocation is already present, this does nothing.
      """
      if self.getDLocation() is None:
         if dloc is None:
            dloc = self.createLocalDLocation()
         Vector.setDLocation(self, dloc)

# ...............................................
   def checkbbox(self,minx,miny,maxx,maxy,resol):
      """
      @summary: checks the extent envelop to ensure minx is less than
      maxx etc. 
      """
      
      if (not(minx < maxx) and not(miny < maxy)):
         raise Exception("min x is greater than max x, and min y is greater than max y")
      if not(minx < maxx):
         raise Exception("min x is greater than max x")
      if not(miny < maxy):
         raise Exception("min y is greater than max y")
      
      if ((maxx - minx) < resol):
         raise Exception("resolution of cellsize is greater than x range")
      if ((maxy - miny) < resol):
         raise Exception("resolution of cellsize is greater than y range")
      
# ...................................................
   def cutout(self, cutoutWKT, removeOrig=False, dloc=None):
      """
      @summary: create a new shapegrid from original using cutout
      """
      
      
      if not removeOrig and dloc == None:
         raise LMError("If not modifying existing shapegrid you must provide new dloc")
      ods = ogr.Open(self._dlocation)
      origLayer  = ods.GetLayer(0)
      if not origLayer:
         raise LMError("Could not open Layer at: %s" % self._dlocation)
      if removeOrig:
         newdLoc = self._dlocation
         for ext in SHAPEFILE_EXTENSIONS:
            success, msg = self._deleteFile(self._dlocation.replace('.shp',ext))
      else:
         newdLoc = dloc
         if os.path.exists(dloc):
            raise LMError("Shapegrid file already exists at: %s" % dloc)
         else:
            self._readyFilename(dloc, overwrite=False)
            
      t_srs = osr.SpatialReference()
      t_srs.ImportFromEPSG(self.epsgcode)
      drv = ogr.GetDriverByName('ESRI Shapefile')
      ds = drv.CreateDataSource(newdLoc)
      newlayer = ds.CreateLayer(ds.GetName(), geom_type = ogr.wkbPolygon, srs = t_srs)
      origLyrDefn = origLayer.GetLayerDefn()
      origFieldCnt = origLyrDefn.GetFieldCount()
      for fieldIdx in range(0,origFieldCnt):
         origFldDef = origLyrDefn.GetFieldDefn(fieldIdx)
         newlayer.CreateField(origFldDef)
      # create geom from wkt
      selectedpoly = ogr.CreateGeometryFromWkt(cutoutWKT)
      minx, maxx, miny, maxy = selectedpoly.GetEnvelope()
      origFeature = origLayer.GetNextFeature()
      siteIdIdx = origFeature.GetFieldIndex(self.siteId)
      newSiteId = 0
      while origFeature is not None:
         clone = origFeature.Clone()
         cloneGeomRef = clone.GetGeometryRef()
         if cloneGeomRef.Intersect(selectedpoly):
            #clone.SetField(siteIdIdx,newSiteId)
            newlayer.CreateFeature(clone)
            newSiteId += 1
         origFeature = origLayer.GetNextFeature()
      ds.Destroy()
      return minx,miny,maxx,maxy

# # ...................................................
#    def cutoutNew(self, cutoutWKT):
#       """
#       @summary: Remove any features that do not intersect the cutoutWKT
#       """
#       # create geom from wkt
#       selectedpoly = ogr.CreateGeometryFromWkt(cutoutWKT)
#       minx, maxx, miny, maxy = selectedpoly.GetEnvelope()
#       intersectingFeatures = {}
#       for siteId, featVals in self._features.iteritems():
#          wkt = featVals[self._geomIdx]
#          currgeom = ogr.CreateGeometryFromWkt(wkt)
#          if currgeom.Intersects(selectedpoly):
#             intersectingFeatures[siteId] = featVals
#       self.clearFeatures()
#       self.addFeatures(intersectingFeatures)
#       self._setBBox(minx, miny, maxx, maxy)
      
# ...................................................
   def buildShape(self, cutout=None, overwrite=True):
      """
      @summary: method to build the shapefile for the shapegrid object.
      Calculates the topology for each cell, square or hexagonal.
      @todo: Remove this, it will fail on newer versions of OGR - 
             old: CreateFeature vs new: SetFeature
      """ 
      # After build, setDLocation, write shapefile, and setSiteIndices
      if os.path.exists(self._dlocation):
         print "Shapegrid file already exists at: %s" % self._dlocation
         return 
#         raise LMError("Shapegrid file already exists at: %s" % self._dlocation)
      
      self._readyFilename(self._dlocation, overwrite=overwrite)

      try:
         min_x = self.minX
         min_y = self.minY
         max_x = self.maxX
         max_y = self.maxY
         
         x_res = self.cellsize 
         y_res = self.cellsize
         
         shapepath = self._dlocation
         
         if self.cellsides == 6:
            t_srs = osr.SpatialReference()
            #t_srs.SetFromUserInput('WGS84')
            t_srs.ImportFromEPSG(self.epsgcode)
            drv = ogr.GetDriverByName('ESRI Shapefile')
            ds = drv.CreateDataSource(shapepath)
            layer = ds.CreateLayer(ds.GetName(), geom_type = ogr.wkbPolygon, srs = t_srs)
            layer.CreateField(ogr.FieldDefn(self.siteId, ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn(self.siteX, ogr.OFTReal))
            layer.CreateField(ogr.FieldDefn(self.siteY, ogr.OFTReal))
            
            apothem_y_res = (y_res * .5)*math.sqrt(3)/2
            
            #yc = min_y  # this will miny
            yc = max_y
            y_row = True
            shape_id = 0
            #while yc < max_y:
            while yc > min_y:
               if y_row == True:
                  xc = min_x  # this will be minx
               elif y_row == False:
                  xc = min_x  + (x_res * .75) # this will be minx + 
               while xc < max_x:
                    
                  wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f, %f %f, %f %f))"% \
                                ( xc - (x_res * .5)/2, yc + (y_res * .5)*math.sqrt(3)/2,\
                                  xc + (x_res * .5)/2, yc + (y_res * .5)*math.sqrt(3)/2,\
                                  xc + (x_res * .5), yc,\
                                  xc + (x_res * .5)/2, yc - (y_res * .5)*math.sqrt(3)/2,\
                                  xc - (x_res * .5)/2, yc - (y_res * .5)*math.sqrt(3)/2,\
                                  xc - (x_res * .5), yc,\
                                  xc - (x_res * .5)/2, yc + (y_res * .5)*math.sqrt(3)/2
                                   )
                  xc += x_res * 1.5
                  geom = ogr.CreateGeometryFromWkt(wkt)
                  geom.AssignSpatialReference ( t_srs )
                  c = geom.Centroid()
                  x = c.GetX()
                  y = c.GetY()
                  feat = ogr.Feature(feature_def=layer.GetLayerDefn())
                  feat.SetGeometryDirectly(geom)
                  #feat.SetFID(shape_id)
                  feat.SetField(self.siteX, x)
                  feat.SetField(self.siteY, y)
                  feat.SetField(self.siteId, shape_id)
                  layer.CreateFeature(feat)
                  feat.Destroy()
                  shape_id += 1
               y_row = not(y_row)
               #yc += apothem_y_res
               yc = yc - apothem_y_res
            ds.Destroy()
            
         elif self.cellsides == 4:
            
            t_srs = osr.SpatialReference()
            #t_srs.SetFromUserInput('WGS84')
            t_srs.ImportFromEPSG(self.epsgcode)
            drv = ogr.GetDriverByName('ESRI Shapefile')
            ds = drv.CreateDataSource(shapepath)
            layer = ds.CreateLayer(ds.GetName(), geom_type = ogr.wkbPolygon, srs = t_srs)
            layer.CreateField(ogr.FieldDefn(self.siteId, ogr.OFTInteger))
            
            layer.CreateField(ogr.FieldDefn(self.siteX, ogr.OFTReal))
            layer.CreateField(ogr.FieldDefn(self.siteY, ogr.OFTReal))
            
            shape_id = 0
            for yc in np.arange(max_y,min_y,-y_res):
               for xc in np.arange(min_x, max_x, x_res):
               
            #for xc in xrange(min_x, max_x, x_res):
               #for yc in xrange(min_y,max_y,y_res):
                  wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))"% \
                                ( xc, yc, xc+x_res, yc, xc+x_res,yc-y_res,\
                                 xc,yc-y_res,xc, yc)
                  #print wkt
                  geom = ogr.CreateGeometryFromWkt(wkt)
                  geom.AssignSpatialReference ( t_srs )
                  c = geom.Centroid()
                  x = c.GetX()
                  y = c.GetY()
                  feat = ogr.Feature(feature_def=layer.GetLayerDefn())
                  feat.SetGeometryDirectly(geom)
                  #feat.SetFID(shape_id)
                  feat.SetField(self.siteX, x)
                  feat.SetField(self.siteY, y)
                  feat.SetField(self.siteId,shape_id)
                  layer.CreateFeature(feat)
                  feat.Destroy()
                  shape_id += 1
            
            ds.Destroy()
      except Exception, e:
         raise LMError(e)
           
      else:
         if cutout is not None:
            self.cutout(cutout,removeOrig=True)
         self.setSiteIndices()

   
#................................................   
   def _buildVectorAreaDict(self, fid, layer, shpgridextentpoly, idx, shpgridLayerObj,
                      areaDict):
      """
      @note: This modifies the areaDict parameter
      @summary: against each feature in the species layer build a geometry object
      from the wkt.  it then does a bool check for the intersection of that geometry
      against the extent of the shapegrid, if there is an intersection
      he bbox for that intersection is used to intersect against the 
      r-tree index of shapegrid cell envelopes which returns a list
      of the feature id's of intersection against envelopes
      @param fid: fid from layer features
      @param layer: layer object
      @param shpgridextentpoly: poly ogr geom object for shpgrd 
      @param idx: rtree index
      @param shpgridLayerObj: shpgrid ogr layer object
      @param areaDict: dictionary with two lists, keyed by fid, first list 
      contains all areas of intersection with one shpgrid cell, second list
      contains the area of the cell
      """
      geomfieldname = layer.getGeometryFieldName()                
      poly = ogr.CreateGeometryFromWkt(layer.getFeatureValByFieldName(geomfieldname, fid))
      shpgridcellfids = []
      if poly.Intersect(shpgridextentpoly):
         firstintersection = poly.Intersection(shpgridextentpoly)              
         minx, maxx, miny, maxy = firstintersection.GetEnvelope()
         shpgridcellfids = list(idx.intersection((minx, miny, maxx, maxy))) 
      else:
         minx, maxx, miny, maxy = poly.GetEnvelope()
            
      
      # for each feature id in intersection list check and see if corresponding
      # item in areaDict is already coded with area total of cellshape, this keeps
      # cells from being recoded that have an intersection total that is already
      # equal or larger than the area of the cell      
      if len(shpgridcellfids) > 0:              
         for id in shpgridcellfids:
            shpgrdfeature = shpgridLayerObj.GetFeature(id) 
            gridgeom =  shpgrdfeature.GetGeometryRef() 
            cellarea = gridgeom.GetArea() 
            if sum(areaDict[id][0]) < cellarea:   
               if firstintersection.Contains(gridgeom):                    
                  areaDict[id][0].append(cellarea) 
                  if not areaDict[id][1]:
                     areaDict[id][1].append(cellarea)
               else:                        
                  if firstintersection.Intersect(gridgeom):
                     intersection = firstintersection.Intersection(gridgeom)
                     area = intersection.GetArea()
                     areaDict[id][0].append(area) 
                     if not areaDict[id][1]:
                        areaDict[id][1].append(cellarea)
                     
            del shpgrdfeature
            
#................................................                             
   def _getSpatialFilterBounds(self, layer, sgminx, sgmaxx, sgminy, sgmaxy):   
      minx = max(sgminx, layer.minX)
      miny = max(sgminy, layer.minY)
      maxx = min(sgmaxx, layer.maxX)
      maxy = min(sgmaxy, layer.maxY)
      return minx, miny, maxx,  maxy
         
#................................................          
          
   def _vectorIntersect(self, layer):
      """
      @summary: Takes a vector species layer input and intersects it with the 
                shapegrid.  Returns an array of presence (1), and absence(0)
                for each site
      """
      # Note: this must be disabled, fails in AGoodle, possibly elsewhere
      ogr.UseExceptions()
      maxPresence = layer.maxPresence
      minPresence = layer.minPresence
      percentPresenceDec = layer.percentPresence/100.0
      attrPresence = layer.attrPresence  
      shpgridhandle = ogr.Open(self._dlocation)
      shpgridLayerObj = shpgridhandle.GetLayer(0)
      rowcount = shpgridLayerObj.GetFeatureCount()
      layerArray = np.zeros(rowcount,bool)     
      sgminx, sgmaxx, sgminy, sgmaxy = shpgridLayerObj.GetExtent()             
      # make a polygon for broad intersection from the extent of the shapegrid  
      wktstring = 'Polygon (('+str(sgminx)+' '+str(sgminy)+','+str(sgmaxx)+' '+\
                  str(sgminy)+','+str(sgmaxx)+' '+str(sgmaxy)+','+str(sgminx)+\
                  ' '+str(sgmaxy)+','+str(sgminx)+' '+str(sgminy)+'))'
      shpgridextentpoly = ogr.CreateGeometryFromWkt(wktstring) 
          
      sfminx, sfminy, sfmaxx, sfmaxy = self._getSpatialFilterBounds(layer, sgminx,
                                        sgmaxx, sgminy, sgmaxy)
      shpgridLayerObj.SetSpatialFilterRect(sfminx,sfminy,sfmaxx,sfmaxy)
      
      # an empty rtree index
      idx = rtree.index.Index()
      areaDict = {}
      # because of the spatial filter set above, the while loop will only get the cells
      # within the spatial filter, cell envelopes are then added to the r-tree
      # index
      cell = shpgridLayerObj.GetNextFeature()     
      while cell is not None:
         # GetFieldAsString           
         fid = cell.GetFID()
         #fid = cell.GetFieldAsString(self.siteId)
         areaDict[fid] = ([],[]) 
         geomRef = cell.GetGeometryRef()
         minx, maxx, miny, maxy = geomRef.GetEnvelope()
         idx.insert(fid,(minx,miny,maxx,maxy))      
         cell = shpgridLayerObj.GetNextFeature()        
      if not layer.getFeatures():
         layer.readData()
      layerfeatures = layer.getFeatures()                     
      for fid, attrs in layerfeatures.iteritems():
         if ((layer.getFeatureValByFieldName(attrPresence, fid) <= maxPresence) 
             and (layer.getFeatureValByFieldName(attrPresence, fid) >= minPresence)):
            self._buildVectorAreaDict(fid, layer, shpgridextentpoly, idx, shpgridLayerObj,
                               areaDict)            
                           
      for fid, areas in areaDict.iteritems():
         if len(areas[0]) > 0: 
            if (sum(areas[0]) > (areas[1][0] * percentPresenceDec)):  
               layerArray[fid] = True
      # Note, this must be disabled, fails in AGoodle, possibly elsewhere
      ogr.DontUseExceptions()
      return layerArray
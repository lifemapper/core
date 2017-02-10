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
# import rtree

from LmCommon.common.lmconstants import (SHAPEFILE_EXTENSIONS, 
                              DEFAULT_OGR_FORMAT, JobStatus, ProcessType)
from LmServer.base.layer2 import _LayerParameters, Vector
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import LMFileType, LMServiceType, LMServiceModule
from LmServer.makeflow.cmd import MfRule

# .............................................................................
class ShapeGrid(_LayerParameters, Vector, ProcessObject):
# .............................................................................
   """
   shape grid class inherits from Vector
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, name, userId, epsgcode, cellsides, cellsize, mapunits, bbox, 
                siteId='siteid', siteX='centerX', siteY='centerY', size=None,
                lyrId=None, verify=None, dlocation=None, metadata={}, 
                resolution=None, metadataUrl=None, parentMetadataUrl=None, 
                modTime=None, featureCount=0, featureAttributes={}, 
                features={}, fidAttribute=None, status=None, statusModTime=None):
      """
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @copydoc LmServer.base.layer2._LayerParameters::__init__()
      @copydoc LmServer.base.layer2.Vector::__init__()
      @param cellsides: Number of sides in each cell of a site (i.e. square =4,
                    hexagon = 6).
      @param cellsize: Size in mapunits of each cell.  For cellSides = 6 
                    (hexagon).HEXAGON, this is the measurement between two 
                    vertices.
      @param siteId: Attribute identifying the site number for each cell 
      @param siteX: Attribute identifying the center X coordinate for each cell 
      @param siteY: Attribute identifying the center Y coordinate for each cell 
      @param size: Total number of cells in shapegrid
      """
      # siteIndices are a dictionary of {siteIndex: (FID, centerX, centerY)}
      # created from the data file and passed to LmCommon.common.Matrix for headers
      self._siteIndices = None
      # field with unique values identifying each site
      self.siteId = siteId
      # field with longitude of the centroid of a site
      self.siteX = siteX
      # field with latitude of the centroid of a site
      self.siteY = siteY

      _LayerParameters.__init__(self, userId, paramId=lyrId, modTime=modTime)
      Vector.__init__(self, name, userId, epsgcode, lyrId=lyrId, verify=verify, 
            dlocation=dlocation, metadata=metadata, 
            dataFormat=DEFAULT_OGR_FORMAT, ogrType=ogr.wkbPolygon, 
            mapunits=mapunits, resolution=resolution, bbox=bbox, svcObjId=lyrId, 
            serviceType=LMServiceType.SHAPEGRIDS, moduleType=LMServiceModule.LM,
            metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl, 
            modTime=modTime, featureCount=featureCount, 
            featureAttributes=featureAttributes, features=features,
            fidAttribute=fidAttribute)
      ProcessObject.__init__(self, objId=lyrId, 
                             processType=ProcessType.RAD_BUILDGRID,
                             parentId=None,
                             status=status, statusModTime=statusModTime)
      # Don't necessarily need centroids (requires reading shapegrid), can 
      # explicitly call public method initSites to initialize centroids and
      # sitesPresent dictionary if they are needed.
      self._setMapPrefix()
      self._setCellsides(cellsides)
      self.cellsize = cellsize
      self._size = None
      self._setCellMeasurements(size)
      
# ...............................................
   @classmethod
   def initFromParts(cls, vector, cellsides, cellsize, 
                     siteId='siteid', siteX='centerX', siteY='centerY', size=None,
                     siteIndicesFilename=None, status=None, statusModTime=None):
      shpGrid = ShapeGrid(vector.name, vector.getUserId(), vector.epsgcode, 
                          cellsides, cellsize, vector.mapUnits, vector.bbox, 
                          siteId=siteId, siteX=siteX, siteY=siteY, size=size,
                          lyrId=vector.getLayerId(), verify=vector.verify,
                          dlocation=vector.getDLocation(), 
                          metadata=vector.lyrMetadata, 
                          resolution=vector.resolution, 
                          metadataUrl=vector.metadataUrl, 
                          parentMetadataUrl=vector.parentMetadataUrl, 
                          modTime=vector.modTime,
                          featureCount=vector.featureCount, 
                          featureAttributes=vector.featureAttributes, 
                          features=vector.features, 
                          fidAttribute=vector.fidAttribute,
                          status=status, statusModTime=statusModTime)
      return shpGrid

   # ...............................................
   def updateStatus(self, status, matrixIndex=None, metadata=None, modTime=None):
      """
      @copydoc LmServer.base.serviceobject2.ProcessObject::updateStatus()
      @copydoc LmServer.base.serviceobject2.ServiceObject::updateModtime()
      @copydoc LmServer.base.layer2._LayerParameters::updateParams()
      """
      ProcessObject.updateStatus(self, status, modTime=modTime)
      ServiceObject.updateModtime(self, modTime=modTime)
      _LayerParameters.updateParams(self, matrixIndex=matrixIndex, 
                                    metadata=metadata, modTime=modTime)

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
         
   @property
   def cellsides(self):
      return self._cellsides
   
# ...............................................
   def _setCellMeasurements(self, size=None):
      if size is not None and isinstance(size, IntType):
         self._size = size
      else:
         self._size = self._getFeatureCount()
        
   @property       
   def size(self):
      return self._size
            
# # ...............................................
#    def initSitesPresent(self):
#       sitesPresent = {}
#       if self._siteIndices is None:
#          self.setSiteIndices()
#       if self._siteIndices is not None:
#          for s in self._siteIndices.keys():
#             sitesPresent[s] = True
#       return sitesPresent
   
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
      @todo: Check this, it may fail on newer versions of OGR - 
             old: CreateFeature vs new: SetFeature
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
      drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
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
   def buildShape(self, cutout=None, overwrite=False):
      """
      @summary: method to build the shapefile for the shapegrid object.
      Calculates the topology for each cell, square or hexagonal.
      @todo: Check this, it may fail on newer versions of OGR - 
             old: CreateFeature vs new: SetFeature
      """ 
      # After build, setDLocation, write shapefile, and setSiteIndices
      if os.path.exists(self._dlocation):
         print "Shapegrid file already exists at: %s" % self._dlocation
         self.readData(doReadData=False)
         self._setCellMeasurements()
         return 
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
            drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
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
            drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
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
         # Modify shapegrid by subseting 
         if cutout is not None:
            minx,miny,maxx,maxy = self.cutout(cutout,removeOrig=True)
         # update size and verify attributes
         self._setCellMeasurements()
         self._setVerify()
         self.setSiteIndices()

# ...............................................
   def computeMe(self):
      """
      @summary: Creates a command to intersect a layer and a shapegrid to 
                produce a MatrixColumn.
      """
      rules = []
      # TODO: Put this somewhere!
      cutoutWktFn = None
      # This just works for status 0, 1, assumes Processing or Complete is fine
      # TODO: How to handle Error status 
      if JobStatus.waiting(self.status):
         outFile = self.shapegrid.getDLocation()
         options = ''
         if cutoutWktFn is not None:
            options = "--cutoutWktFn={0}".format(cutoutWktFn)
         
         cmdArguments = [os.getenv('PYTHON'), 
                         ProcessType.getJobRunner(self.processType), 
                         self.shapegrid.getDLocation(), 
                         outFile,
                         self.getMinX(),
                         self.getMinY(),
                         self.getMaxX(),
                         self.getMaxY(),
                         self.cellsize,
                         self.epsgcode,
                         self.cellsides,
                         options]

      cmd = ' '.join(cmdArguments)
      rules.append(MfRule(cmd, [outFile]))
      return rules

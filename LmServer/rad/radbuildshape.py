# """
# @license: gpl2
# @copyright: Copyright (C) 2014, University of Kansas Center for Research
# 
#           Lifemapper Project, lifemapper [at] ku [dot] edu, 
#           Biodiversity Institute,
#           1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
#    
#           This program is free software; you can redistribute it and/or modify 
#           it under the terms of the GNU General Public License as published by 
#           the Free Software Foundation; either version 2 of the License, or (at 
#           your option) any later version.
#   
#           This program is distributed in the hope that it will be useful, but 
#           WITHOUT ANY WARRANTY; without even the implied warranty of 
#           MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
#           General Public License for more details.
#   
#           You should have received a copy of the GNU General Public License 
#           along with this program; if not, write to the Free Software 
#           Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
#           02110-1301, USA.
# """
# import numpy
# from LmCommon.common.lmconstants import JobStatus, FeatureNames
# 
# # ...............................................
# def writeShapefile(dlocation, features, featureAttributes, geomoverwrite=False):
#    """
#    @summary: Writes vector data to a shapefile.  
#    @param overwrite: True if overwrite existing shapefile, False if not
#    @return: boolean for success/failure 
#    @postcondition: The raster file is written to the filesystem.
#    @raise LMError: on failure to write file.
#    """
#    success = False
#    if dlocation is None:
#       dlocation = self._dlocation
#        
#    if not self._features:
#       return success
# 
#    if overwrite:
#       self.deleteData(dlocation=dlocation)
#    elif os.path.isfile(dlocation):
#       print('Dataset exists: %s' % dlocation)
#       return success
#     
#    self.setDLocation(dlocation) 
#    self._readyFilename(self._dlocation)        
#           
#    try:
#       # Create the file object, a layer, and attributes
#       tSRS = osr.SpatialReference()
#       tSRS.ImportFromEPSG(self.epsgcode)
#       drv = ogr.GetDriverByName('ESRI Shapefile')
# 
#       ds = drv.CreateDataSource(self._dlocation)
#       if ds is None:
#          raise LMError('Dataset creation failed for %s' % self._dlocation)
#        
#       lyr = ds.CreateLayer(ds.GetName(), geom_type=self._ogrType, srs=tSRS)
#       if lyr is None:
#          raise LMError('Layer creation failed for %s.' % self._dlocation)
#  
#       # Define the fields
#       for idx in self._featureAttributes.keys():
#          fldname, fldtype = self._featureAttributes[idx]
#          if fldname != FeatureNames.GEOMETRY_WKT:
#             fldDefn = ogr.FieldDefn(fldname, fldtype)
#             # Special case to handle long Canonical, Provider, Resource names
#             if (fldname.endswith('name') and fldtype == ogr.OFTString):
#                fldDefn.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
#             returnVal = lyr.CreateField(fldDefn)
#             if returnVal != 0:
#                raise LMError('CreateField failed for %s in %s' 
#                              % (fldname, self._dlocation)) 
#                 
#       # For each feature
#       for i in self._features.keys():
#          fvals = self._features[i]
#          feat = ogr.Feature( lyr.GetLayerDefn() )
#          try:
#             self._fillOGRFeature(feat, fvals)
#          except Exception, e:
#             print 'Failed to fillOGRFeature, e = %s' % str(e)
#          else:
#             # Create new feature, setting FID, in this layer
#             lyr.CreateFeature(feat)
#             feat.Destroy()
#  
#       # Closes and flushes to disk
#       ds.Destroy()
#       print('Closed/wrote dataset %s' % self._dlocation)
#       success = True
#       try:
#          retcode = subprocess.call(["shptree", "%s" % self._dlocation])
#          if retcode != 0: 
#             print 'Unable to create shapetree index on %s' % self._dlocation
#       except Exception, e:
#          print 'Unable to create shapetree index on %s: %s' % (self._dlocation, 
#                                                                str(e))
#    except Exception, e:
#       raise LMError(['Failed to create shapefile %s' % self._dlocation, str(e)])
#        
#    return success
# 
# # ...................................................
# def _cutoutNew(cutoutWKT):
#    """
#    @summary: Remove any features that do not intersect the cutoutWKT
#    """
#    # create geom from wkt
#    selectedpoly = ogr.CreateGeometryFromWkt(cutoutWKT)
#    minx, maxx, miny, maxy = selectedpoly.GetEnvelope()
#    intersectingFeatures = {}
#    for siteId, featVals in self._features.iteritems():
#       wkt = featVals[self._geomIdx]
#       currgeom = ogr.CreateGeometryFromWkt(wkt)
#       if currgeom.Intersects(selectedpoly):
#          intersectingFeatures[siteId] = featVals
#    self.clearFeatures()
#    self.addFeatures(intersectingFeatures)
#    self._setBBox(minx, miny, maxx, maxy)
#        
# # ...................................................
# def buildShapeNew(minX, minY, maxX, maxY, cellsides, cellsize, epsgcode, cutout=None):
#    """
#    @summary: method to build the shapefile for the shapegrid object.
#    Calculates the topology for each cell, square or hexagonal.
#    self._ogrType must be set to the correct OGR constant
#    @todo: modify this to not use ShapeGrid object so it can be calulated on 
#           ComputeResources without entire LM codebase.
#    """ 
#    from osgeo import ogr
#    from osgeo import osr
#    from math import sqrt
#    from numpy import arange
#    t_srs = osr.SpatialReference()
#    t_srs.ImportFromEPSG(epsgcode)
#    featureAttributes = {
#                0: (FeatureNames.SITE_ID, ogr.OFTInteger),
#                1: (FeatureNames.SITE_X, ogr.OFTReal),
#                2: (FeatureNames.SITE_Y,  ogr.OFTReal),
#                3: (FeatureNames.GEOMETRY_WKT, ogr.OFTString)
#                }
#      
#    x_res = cellsize 
#    y_res = cellsize
#    shape_id = 0         
#    featDict = {}
#    siteIndices = {}
#    try:         
#       if cellsides == 6:
#          apothem_y_res = (y_res * .5) * sqrt(3)/2
#          yc = maxY
#          y_row = True
#          #while yc < max_y:
#          while yc > minY:
#             if y_row == True:
#                xc = minX  # this will be minx
#             elif y_row == False:
#                xc = minX  + (x_res * .75) # this will be minx + 
#             while xc < maxX:
#                  
#                wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f, %f %f, %f %f))" % (
#                            xc - (x_res * .5)/2, yc + (y_res * .5) * sqrt(3)/2,
#                            xc + (x_res * .5)/2, yc + (y_res * .5) * sqrt(3)/2,
#                            xc + (x_res * .5), yc,
#                            xc + (x_res * .5)/2, yc - (y_res * .5) * sqrt(3)/2,
#                            xc - (x_res * .5)/2, yc - (y_res * .5) * sqrt(3)/2,
#                            xc - (x_res * .5), yc,
#                            xc - (x_res * .5)/2, yc + (y_res * .5) * sqrt(3)/2  )
#                xc += x_res * 1.5
#                geom = ogr.CreateGeometryFromWkt(wkt)
#                geom.AssignSpatialReference ( t_srs )
#                c = geom.Centroid()
#                x = c.GetX()
#                y = c.GetY()
#                  
#                featDict[shape_id] = [shape_id, x, y, wkt]
#                shape_id += 1
#             y_row = not(y_row)
#             yc = yc - apothem_y_res
#            
#       elif cellsides == 4:
#          for yc in arange(maxY, minY, -y_res):
#             for xc in arange(minX, maxX, x_res):
#               
#                wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" % (
#                              xc, yc, xc+x_res, yc, xc+x_res, yc-y_res,
#                              xc, yc-y_res, xc, yc )
#                geom = ogr.CreateGeometryFromWkt(wkt)
#                geom.AssignSpatialReference ( t_srs )
#                c = geom.Centroid()
#                x = c.GetX()
#                y = c.GetY()
#                  
#                featDict[shape_id] = [shape_id, x, y, wkt]
#                shape_id += 1
#   
#    except Exception, e:
#       raise LMError(e)
#           
#    if cutout is not None:
#       minx, miny, maxx, maxy = _cutoutNew(cutout,removeOrig=True)
# #       self._setBBox(minx, miny, maxx, maxy)
#   
# #    self.setFeatures(featDict, featureAttributes)
#    self.writeShapefile()
#     
#    # TODO: is this needed??
#    for shape_id in featDict.keys():
#       if shape_id is None:
#          print 'WTF?'
#       geom = featDict[shape_id][3]
#       siteIndices[shape_id] = geom
#    return featDict


"""
@summary: Module containing a class for encoding BioGeographic hypotheses into 
             a contrasts matrix
@author: CJ Grady (originally by Jeff Cavner)
@version: 1.0
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
@see: Leibold, m.A., E.P. Economo and P.R. Peres-Neto. 2010. Metacommunity
         phylogenetics: separating the roles of environmental filters and 
         historical biogeography. Ecology letters 13: 1290-1299.
"""
import numpy as np
import os
from osgeo import ogr

from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.matrix import Matrix
from LmCommon.encoding.encodingException import EncodingException

SITE_FIELD = 'siteid'

# .............................................................................
class BioGeoEncoding(object):
   """
   @summary: This class encodes a set of biogeographic hypothesis and a 
                shapegrid into a matrix of site rows and hypothesis columns
   """
   # ..............................
   def __init__(self, shapegridDLoc):
      """
      @summary: The constructor sets the data location for the shapegrid to be 
                   used for intersection
      @param shapegridDLoc: The file location of the shapegrid to use for 
                               intersection
      """
      self.layers = [] # A list of layer data locations
      self.sortedSites = [] # Will be filled in next step
      
      self._getSortedSitesForIntersection(shapegridDLoc)

   # ..............................
   def addLayers(self, layers, eventField=None):
      """
      @summary: Add a layer or list of layers for encoding
      @param layer: The file location of the layer to add or list of locations
      @param eventField: (optional) If provided, use this field to split a 
                            merged hypothesis shapefile into multiple 
                            hypotheses.  If omitted, assume that the 
                            shapefile(s) only have two features to be used for 
                            the hypothesis.
      @note: The eventField will be used for every layer in the list.  Call this
                method multiple times with different lists if this should not
                be the case.
      """
      if isinstance(layers, basestring): # Just a single value
         layers = [layers] # Make it a list of one item
      
      for lyr in layers:
         self.layers.append((lyr, eventField))

   # ..............................
   def encodeHypotheses(self):
      """
      @summary: Encodes the provided layers into a matrix (B in the literature)
      @todo: Determine how we can add a label to each layer
      @raise IOError: Raised if a layer file does not exist
      """
      encodedLayers = []
      for dloc, eventField in self.layers:
         if not os.path.exists(dloc):
            raise IOError, "File {fn} does not exist".format(fn=dloc)
         if eventField is None:
            features = self._getFeaturesNoEvent(dloc)
         else:
            features = self._getFeaturesWithEvent(dloc, eventField)
         for featureTuple, label in features:
            encodedLayers.append(self._encodeFeatures(featureTuple, label=label))
      
      matrix = Matrix.concatenate(encodedLayers, axis=1)
      return matrix

   # ..............................
   def _encodeFeatures(self, featureTuple, label=None):
      """
      @summary: Encode the feature tuple by intersecting the features within
                   with the shapegrid sites.  If there is only one features, 
                   intersecting values will be coded as 1 and non-intersecting 
                   will be coded as -1.  If there are two features, intersecting
                   with one will be coded as 1, the other -1, and 
                   non-intersecting sites will be coded as 0.
      @todo: Add a label 
      """
      feat1, feat2 = featureTuple
      if feat2 is None: # Only one feature
         defaultValue = -1
      else: # Two values
         defaultValue = 0
      contrast = []
      siteIds = []
      for siteId, site in self.sortedSites:
         val = defaultValue
         intersectedArea = 0.0
         siteGeom = site.GetGeometryRef()
         if siteGeom.Intersect(feat1.GetGeometryRef()):
            intersectedArea = siteGeom.Intersection(feat1.GetGeometryRef()).GetArea()
            val = 1
         if feat2 is not None and siteGeom.Intersect(feat2.GetGeometryRef()):
            area2 = siteGeom.Intersection(feat2.GetGeometryRef()).GetArea()
            if area2 > intersectedArea:
               val = -1
         siteIds.append(siteId)
         contrast.append(val)
      # Assemble headers
      if label is None:
         label = ''
      headers = {'0': siteIds,
                 '1': [label]}
         
      # Make a list of this list and transpose the resulting numpy array so it
      #    is one column wide and number of site rows
      return Matrix(np.array([contrast]).T, headers=headers)
         
   # ..............................
   def _getFeaturesNoEvent(self, layerDL):
      """
      @summary: Get features from a layer without an event field
      @param layerDL: The file location of the layer
      @note: Returns a list with one tuple with one or two features
      @raise EncodingException: If there are zero or more than two features
      """
      lyrDS = self._openShapefile(layerDL)
      lyr = lyrDS.GetLayer(0)
      
      featCount = lyr.GetFeatureCount()
      
      # Make sure feature count is 1 or 2
      if featCount < 1:
         raise EncodingException("Need at least one feature")
      if featCount > 2:
         raise EncodingException("Too many features in layer")
      
      feat1 = None
      feat2 = None
      
      feat1 = lyr.GetNextFeature()#.GetGeometryRef()
      try:
         feat2 = lyr.GetNextFeature()#.GetGeometryRef()
      except: # Second feature is optional
         pass
      
      return [(feat1, feat2), os.path.basename(layerDL)]
   
   # ..............................
   def _getFeaturesWithEvent(self, layerDL, eventField):
      """
      @summary: Get features from a layer using an event field
      @param layerDL: The file location of the layer
      @param eventField: The field in the layer to use to separate hypotheses
      @note: For each distinct event value in the event field, return a tuple
                of one or two features
      @raise EncodingException: If there are zero or more than two features for  
                                   any specific event
      @raise EncodingException: If the event field provided is not found
      """
      featuresList = []
      # Find distinct events
      distinctEvents = []
      # Get the data set name (file base name without extension)
      dsName = os.path.basename(layerDL).replace(LMFormat.SHAPE.ext, '')
      lyrDS = self._openShapefile(layerDL)
      
      # Look for event field
      eventFieldFound = False
      lyrDef = lyrDS.GetLayer(0).GetLayerDefn()
      for i in range(lyrDef.GetFieldCount()):
         fieldName = lyrDef.GetFieldDefn(i).GetName().lower()
         if fieldName == eventField.lower():
            eventFieldFound = True
            break
      
      if not eventFieldFound:
         raise EncodingException("Event field: {0} not found".format(
                                                                  eventField))

      deSQL = "SELECT DISTINCT {field} FROM {dsName}".format(field=eventField,
                                                             dsName=dsName)
      deLyr = lyrDS.ExecuteSQL(deSQL)
      for feat in deLyr:
         distinctEvents.append(feat.GetField(0))
      
      if not distinctEvents: # Empty list
         raise EncodingException(
                  "There are no features in {0} to encode".format(layerDL))
      
      lyr = lyrDS.GetLayer(0) # Get the full layer
      # For each distinct event
      for de in distinctEvents:
      #   Filter for each distinct event
         filter = "{eventField} = '{value}'".format(eventField=eventField,
                                                    value=de)
         lyr.SetAttributeFilter(filter)
         featCount = lyr.GetFeatureCount()
         # Make sure feature count is 1 or 2
         # Don't need to check for at least 1 since we check for distinct events
         if featCount > 2:
            raise EncodingException("Too many features for event: %s" % de)

         feat1 = None
         feat2 = None
         
         feat1 = lyr.GetNextFeature()
         try:
            feat2 = lyr.GetNextFeature()
         except: # Second feature is optional
            pass
         
         featuresList.append(((feat1, feat2), '{} - {}'.format(eventField, de)))

      return featuresList
      
   # ..............................
   def _getSortedSitesForIntersection(self, sgDLoc):
      """
      @summary: Initializes the sorted sites list from the shapegrid
      @param sgDloc: The file location of the shapegrid shapefile
      @note: self.sortedSites is a list of (site id, feature) tuples that is 
                sorted by site id
      @raise IOError: Raised if shapegrid file does not exist
      """
      if not os.path.exists(sgDLoc):
         raise IOError, "Shapegrid: {fn} does not exist".format(fn=sgDLoc)
      
      sgDs = self._openShapefile(sgDLoc)
      lyr = sgDs.GetLayer(0)
      sites = []
      for feature in lyr:
         idIdx = feature.GetFieldIndex(SITE_FIELD)
         siteId = feature.GetFieldAsInteger(idIdx)
         sites.append([siteId, feature])
      self.sortedSites = sorted(sites)

   # ..............................
   def _openShapefile(self, fn):
      """
      @summary: Opens a shapefile and returns the OGR dataset object
      """
      drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
      ds = drv.Open(fn)
      return ds


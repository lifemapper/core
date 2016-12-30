"""
@summary: Module containing classes for Phylogenetic and BioGeographic contrasts
@author: Jeff Cavner (modified by CJ Grady)
@version: 1.0
@status: alpha

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
#TODO: Constants
#TODO: Fix check for keys


# TODO: Check imports
import numpy as np
import csv
from operator import itemgetter
import cPickle
import os, sys
import ntpath
import operator
import json
from osgeo import ogr,gdal

from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.encoding.lmTree import LmTree


# TODO: Spacing
# TODO: Constants
# TODO: Remove references to Jeff's machine

# TODO: Is this needed
ogr.UseExceptions()

# .............................................................................
class BioGeoEncoding(object):
   """
   @summary: The BioGeoEncoding class represents a site by biogeographic
                hypothesis matrix
   @todo: Improve documentation
   """
   
   # ..............................   
   def __init__(self, contrastsdLoc, intersectionLyrDLoc, eventField=False):
      """
      @summary: Constructor for Biogeographic hypothesis encodings
      @note: eventField only for non-collections
      @todo: Function documentation
      @todo: Inline documentation
      """
      self.encMtx = False
      self.contrastColl = False  # list of ogr data sources
      self.eventField = eventField
      fieldName = sPSet = commonSet = True 
      self._mutuallyEx = False
      
      try: 
         if os.path.exists(contrastsdLoc) and os.path.exists(intersectionLyrDLoc) and eventField:
            
            self.contrastsDs = self.openShapefile(contrastsdLoc) 
            fieldName = self._checkEventFieldName()
            
            if not fieldName:
               raise Exception, "incorrect event field"
            
            sPSet = self._setSinglePathValues(eventField, contrastsdLoc)
            commonSet = self._setCommon(intersectionLyrDLoc)
            self._setMutuallyExclusive(self.contrastsDs, eventField)
            
         else:
            raise ValueError('shapefile or eventField missing')
      except Exception, e:
         if fieldName and sPSet and commonSet:
            try:
               # this branch for multiples (collection)
               if len(contrastsdLoc) == 1 and os.path.exists(contrastsdLoc[0]):
                  raise Exception, "list_one"
               
               self.contrastColl = []
               for fn in contrastsdLoc:
                  if os.path.exists(fn):
                     ds = self.openShapefile(fn)
                     self.contrastColl.append(ds)
                     
               self._positions = self.buildContrastPostions(self.contrastColl,
                                                            fromCollection=True)
               
               if not self._eachTwo():
                  self.contrastColl = False
                  raise Exception, "more then two features in .."
               #if not self._checkEventFieldName():  # does a contrast collection need an eventField?
                  #self.contrastColl = False
                  raise Exception, "incorrect event field"
               if os.path.exists(intersectionLyrDLoc):
                  self._setCommon(intersectionLyrDLoc)
               else:
                  self.contrastColl = False
                  raise ValueError('shapefile missing')
               
            except Exception, e: 
               try:
                  if str(e) == 'list_one':
                     if os.path.exists(intersectionLyrDLoc):  
                        
                        self.contrastsDs = self.openShapefile(contrastsdLoc[0])

                        if not self._checkEventFieldName():
                           raise Exception, "incorrect event field"
                        self._setSinglePathValues(eventField, contrastsdLoc[0])
                        self._setCommon(intersectionLyrDLoc)
                        self._setMutuallyExclusive(self.contrastsDs, eventField)
                        
                     else:
                        raise ValueError('shapefile missing')
                  else:
                     raise Exception, str(e)  # was ValueError('unable to build collection')
               except Exception, e:
                  print str(e)
         else:
            print str(e)

   # ..............................   
   def buildContrasts(self):
      # TODO: Function documentation
      # TODO: Inline documentation
      if not self.contrastColl:
         if self._mutuallyEx:
            encMtx = self._buildFromExclusive()
         else:
            encMtx = self._buildFromTwoFeatureSetOrSingleMerged(False)
      else:
         encMtx = self._buildFromTwoFeatureSetOrSingleMerged(True)
      return encMtx

   # ..............................   
   def _setMutuallyExclusive(self, ds, eventFieldName):
      """
      @summary: determine if mutually exclusive
      @note: this might not work under certain circumstances
      @todo: Function documentation
      @todo: Explain why it might not work
      """
      lyr = ds.GetLayer(0)
      fc = lyr.GetFeatureCount()
      # now call get distinct
      fn = ds.name
      name = ntpath.basename(fn).replace(".shp","")
      dE = self.getDistinctEvents(ds, eventFieldName, name)
      if fc == len(dE):
         self._mutuallyEx = True
      else:
         self._mutuallyEx = False
      
   # ..............................   
   def _setCommon(self, intersectionLyrDLoc):
      # TODO: Function documentation
      # TODO: Inline documentation
      set = True
      try:
         self.intersectionDs = self.openShapefile(intersectionLyrDLoc)
         self.sortedSites = self.sortShpGridFeaturesBySiteID(self.intersectionDs.GetLayer(0))
      except:
         set = False
      return set      

   # ..............................   
   def _setSinglePathValues(self, eventField, contrastsdLoc):
      # TODO: Function documentation
      # TODO: Inline documentation
      set = True
      try:
         self.contrastShpName = ntpath.basename(contrastsdLoc)
         #TODO: Constant
         if '.shp' in self.contrastShpName:
            self.contrastShpName = self.contrastShpName.replace('.shp','')
         
         self.distinctEvents = self.getDistinctEvents(self.contrastsDs, eventField, self.contrastShpName) 
         self._positions = self.buildContrastPostions(self.distinctEvents)
      except Exception, e:
         print "error in setSinglePath ",str(e)
         set = False
      return set

   # ..............................   
   def _checkEventFieldName(self):
      """
      @summary: check that in self.eventField exists in contrast shapes
      """
      if not self.contrastColl:
         dataSrcs = [self.contrastsDs]
      else:
         dataSrcs = self.contrastColl
      
      fieldExists = True
      
      for cShpDs in dataSrcs:
         lyr = cShpDs.GetLayer(0)
         lyrDef = lyr.GetLayerDefn()
         if lyrDef.GetFieldIndex(self.eventField) == -1:
            fieldExists = False
            break
         
      return fieldExists   
   
   # ..............................   
   def _eachTwo(self):
      """
      @summary: check to see if each shp file in contrast collection only has two features
      """
      eachTwo = True
      try:
         for cShpDs in self.contrastColl:
            lyr = cShpDs.GetLayer(0)
            fc = lyr.GetFeatureCount()
            if fc != 2:
               fn = cShpDs.name
               name = ntpath.basename(fn)
               raise Exception, "More than 2 features in lyr %s" % (name)
      except Exception,e:
         eachTwo = False
         print str(e)
      return eachTwo

   # ..............................   
   @property
   def positions(self):
      # TODO: Function documentation
      # TODO: Inline documentation
      return self._positions
      
   # ..............................   
   def openShapefile(self,dlocation):
      # TODO: Function documentation
      # TODO: Inline documentation
   
      ogr.RegisterAll()
      drv = ogr.GetDriverByName('ESRI Shapefile')
      try:
         ds = drv.Open(dlocation)
      except Exception, e:
         raise Exception, 'Invalid datasource, %s: %s' % (dlocation, str(e))
      return ds

   # ..............................   
   def sortShpGridFeaturesBySiteID(self, lyr):
      """
      @param lyr: osgeo lyr object
      @return: 2-D list of site features sorted by siteids [siteid,feature],[..]..
      """
      # TODO: Function documentation
      # TODO: Inline documentation
      sites = []
      for feature in lyr:
         idIdx = feature.GetFieldIndex('siteid')
         siteId = feature.GetFieldAsInteger(idIdx)
         sites.append([siteId, feature])
      sortedSites = sorted(sites, key=itemgetter(0))
      return sortedSites

   # ..............................   
   def getDistinctEvents(self, contrastLyrDS, eventFieldName, constrastShpName):
      """
      @summary: returns list of distinct event string values
      """
      # TODO: Function documentation
      # TODO: Inline documentation
      distinctEvents = []
      sql = 'SELECT DISTINCT %s FROM %s' % (eventFieldName, constrastShpName)
      layer = contrastLyrDS.ExecuteSQL(sql)
      for feature in layer:
         distinctEvents.append(feature.GetField(0))
      
      return distinctEvents

   # ..............................   
   def getContrastsData(self):
      """
      @summary: collects the (two) features for each distinct event in a merged shp 
      """
      # TODO: Function documentation
      # TODO: Inline documentation
      
      distinctEvents = self.distinctEvents   
      contrastLyr = self.contrastsDs.GetLayer(0)
      contrasts = []
      for event in distinctEvents:
         filter = "%s = '%s'" % (self.eventField, event)
         contrastLyr.SetAttributeFilter(filter)
         innerList = [event]
         fc = contrastLyr.GetFeatureCount()  # if this is more than 2 throw exception and bail
         if fc != 2:
            contrasts = False
            break
         for feature in contrastLyr:
            innerList.append(feature) #.GetGeometryRef())   
         contrasts.append(innerList)
      
      return contrasts

   # ..............................   
   def buildContrastPostions(self, distinctEvents, fromCollection=False):
      """
      @summary: build look up for contrast methods and visualization of outputs
      @todo: needs to get build for contrast collection too, which means not using distinct
      events, which makes sense since shouldn't need eventField in collection.
      """
      # TODO: Function documentation
      # TODO: Inline documentation
      if not fromCollection:
         refD = {k:v for v,k in enumerate(distinctEvents)}
      else:
         collectionsDs = distinctEvents
         refD = {}
         for v,shp in enumerate(collectionsDs):
            fn = shp.name
            name = ntpath.basename(fn).replace(".shp","")
            refD[name] = v
            
      return refD

   # ..............................   
   def _buildFromExclusive(self):
      """
      @summary: builds from one shapefile where feature is exclusive (no overlap)
      this doesn't use area, could be a problem if site intersects more then one event
      @note: TEST THIS WITH /home/jcavner/TASHI_PAM/Test!!
      @note: have to disallow this method for merged data. # TEST THIS WITH /home/jcavner/TASHI_PAM/Test!!
      """
      # TODO: Function documentation
      # TODO: Inline documentation
      try:
         mds = self.contrastsDs
         meConstrastLyr = mds.GetLayer(0) 
         
         gds = self.intersectionDs
         gLyr = gds.GetLayer(0)
         siteCount = gLyr.GetFeatureCount()
         sortedSites = self.sortedSites
         
         positions = self.positions
         contrastMtx = np.zeros((siteCount,len(positions)), dtype=np.int)
         
         
         for i,site in enumerate(sortedSites):
         #while siteFeature is not None:   
            siteGeom = site[1].GetGeometryRef()
            #siteGeom = siteFeature.GetGeometryRef()
            
            meConstrastLyr.ResetReading()
            contrastFeature = meConstrastLyr.GetNextFeature()
            while contrastFeature is not None:        
               eventGeom = contrastFeature.GetGeometryRef()
               
               eventNameIdx = contrastFeature.GetFieldIndex(self.eventField)
               eventName = contrastFeature.GetFieldAsString(eventNameIdx)
               
               if eventName in positions:
                  colPos = positions[eventName] # columns are contrasts
                  if siteGeom.Intersect(eventGeom):
                     contrastMtx[i][colPos] = 1  
                  else:
                     contrastMtx[i][colPos] = -1      
               contrastFeature = meConstrastLyr.GetNextFeature()
               
            negOnes = np.where(contrastMtx[i] == -1)[0]  #site not in any event
            if len(negOnes) == len(positions):
               contrastMtx[i] = np.zeros(len(positions), dtype=np.int)
            # What if site intersects all events!! might neeed area
            # but then wouldn't work with centroids
      except Exception, e:
         contrastMtx = False
         
      self.encMtx = contrastMtx
     
   # ..............................   
   def _buildFromTwoFeatureSetOrSingleMerged(self, fromCollection):
      """
      @summary: builds from shapefile that inclues all the contrasts.
      This uses area so will only work with a shapegrid (cells)
      @param fromCollection: bool
      @todo: make so it can also centroid (X,Y)
      """
      if not fromCollection:
         contrastData = self.getContrastsData()
      else:
         # build contrastData from collection
         try:
            contrastData = []
            for shpDs in self.contrastColl:
               # coordinate with positions?
               fn = shpDs.name
               name = ntpath.basename(fn).replace(".shp","")
               inner = [name]
               lyr = shpDs.GetLayer(0)
               for feature in lyr:
                  inner.append(feature)
               contrastData.append(inner)   
         except:
            False
      if contrastData:
         try:
            eventPos =  self.positions
            
            gds = self.intersectionDs
            gLyr = gds.GetLayer(0)
            #
            numRow = gLyr.GetFeatureCount()
            numCol = len(contrastData)
            # init Contrasts mtx
            contrastsMtx = np.zeros((numRow, numCol), dtype=np.int)
            sortedSites = self.sortedSites
            #
            for contrast in contrastData:  
               event = contrast[0]
               if event in eventPos:
                  colPos = eventPos[event]
                  for i, site in enumerate(sortedSites):   
                     siteGeom = site[1].GetGeometryRef()
                     A1 = 0.0
                     A2 = 0.0
                     if siteGeom.Intersect(contrast[1].GetGeometryRef()):
                        intersection = siteGeom.Intersection(contrast[1].GetGeometryRef())
                        A1 = intersection.GetArea()
                        contrastsMtx[i][colPos] = -1
                     if siteGeom.Intersect(contrast[2].GetGeometryRef()):
                        if A1 > 0.0:
                           intersection = siteGeom.Intersection(contrast[2].GetGeometryRef())
                           A2 = intersection.GetArea()
                           if A2 > A1:
                              contrastsMtx[i][colPos] = 1     
                        else:
                           contrastsMtx[i][colPos] = 1
               else:
                  break
         except Exception, e:
            contrastsMtx = False
            print str(e)
         
      else:
         contrastsMtx = False
      self.encMtx = contrastsMtx

   # ..............................   
   def writeBioGeoMtx(self,dLoc):
      # TODO: Function documentation
      # TODO: Inline documentation
      
      wrote = False
      if not isinstance(self.encMtx, bool):
         try:
            np.save(dLoc, self.encMtx)
         except Exception,e:
            print str(e)
         else:
            wrote = True
      return wrote

# .............................................................................
class PhyloEncoding(object):
   """
   @summary: The PhyloEncoding class represents the encoding of a phylogenetic
                tree to match a PAM
   """
   # ..............................   
   def __init__(self, treeDict, pam):
      """
      @summary: Base constructor
      @param treeDict: A phylogenetic tree as a dictionary that will be 
                converted to an LmTree object
      @param pam: A numpy array for a PAM
      """
      self.tree = LmTree(treeDict)
      self.pam = pam
      # Check if the PAM and Tree match
      self._checkPamTreeMatch()
   
   # ..............................   
   @classmethod
   def fromFile(cls, treeDLoc, pamDLoc):
      """
      @summary: Creates an instance of the PhyloEncoding class from tree and 
                   pam files
      @param treeDLoc: The location of the tree (in JSON format)
      @param pamDLoc: The location of the PAM (in numpy format)
      @raise IOError: If one or both of the files are not found
      """
      with open(treeDLoc, 'r') as treeF:
         tree = json.load(treeF)
      
      pam = np.load(pamDLoc)
      
      return cls(tree, pam)
      
   # ..............................
   def _checkPamTreeMatch(self):
      """
      @summary: This function checks that the matrix ids present in the tree
                   match those in the PAM metadata.  If they do not, attempt to
                   match them and if that fails, return error
      @raise Exception: If the matrix indices cannot be matched
      @todo: Raise a specific error
      """
      # TODO: This may not work properly, we will need a list of all matrix 
      #          indices in the PAM.  Assuming that metadata has label : index 
      #          values.  Need to confirm this
      #matrixIndices = [value for key, value in self.pam.metadata.iteritems()]
      matrixIndices = range(self.pam.shape[1])
      
      
      treeMatrixIndices = self.tree.getMatrixIndicesInClade()
      
      # Find the intersection between the two indices lists
      intersection = list(set(matrixIndices) & set(treeMatrixIndices))
      
      # Reset the matrix indices if they do not match
      if len(intersection) != len(matrixIndices) or \
             len(intersection) != len(treeMatrixIndices):
         
         # TODO: Reinstate this if we can
         #self.tree.removeMatrixIndices()
         #self.tree.addMatrixIndices(self.pam.metadata)
         
         ## Check again
         #newTreeMatrixIndices = self.tree.getMatrixIndicesInTree()
         #newIntersection = list(set(matrixIndices) & set(newTreeMatrixIndices))
         #if len(newIntersection) != len(matrixIndices) or \
         #        len(newIntersection) != len(newTreeMatrixIndices):
         #   raise Exception, "PAM and Tree matrix indices do not match"
         
         # TODO: For now just fail
         raise Exception, "PAM and Tree matrix indices do not match"
      
   # ..............................   
   def makeP(self):
      """
      @summary: encodes phylogeny into matrix P and checks
      for sps in tree but not in PAM (I), if not in PAM, returns
      new PAM (I) in addition to P
      @note: 'P' is a tips (rows) by internal nodes (columns) matrix representation of the phylogenetic tree
      @todo: Function name
      @todo: Document
      @todo: Function documentation
      """
      ######### make P ###########
      tips, internal, tipsNotInMtx = self.buildTips()
      
      if self.tree.hasBranchLengths():
         P = self._buildPMatrixFromBranchLengths()
      else:
         P = self._buildPMatrixNoBranchLengths()
      
      if len(tipsNotInMtx) > 0:
         I = self.processTipNotInMatrix(tipsNotInMtx, internal, self.pam)
      else:
         I = self.pam  
      return P, I, internal
      
   # ..............................   
   def buildTips(self): 
      """
      @summary: flattens to tips and return list of tip clades(dicts)
      unsure how calculations would reflect/change if more tips in tree
      than in PAM.  If it does it needs to check for matrix key
      @param noColPam: at what point does this arg get set/sent
      @todo: Document
      @todo: Probably take this out completely.  There is a lot of duplication 
                of what is in the tree module
      """ 
      clade = self.tree.tree
      noColPam = self.pam.shape[1]
      # TODO: This just looks like a counter, why would this be a dictionary?
      noMx = {'c':noColPam}  # needs to start with last sps in pam
      tips = []
      tipsNotInMatrix = []
      internal = {}
      
      def buildLeaves(clade):
         if len(clade[PhyloTreeKeys.CHILDREN]) > 0:
            #### just a check, probably take out 
            if len(clade[PhyloTreeKeys.CHILDREN]) > 2:
               print "polytomy ", clade[PhyloTreeKeys.PATH_ID]
            ############    
            internal[clade[PhyloTreeKeys.PATH_ID]] = clade[PhyloTreeKeys.CHILDREN] # both sides
            for child in clade[PhyloTreeKeys.CHILDREN]:  
               buildLeaves(child)
         else:
            if clade.has_key(PhyloTreeKeys.MTX_IDX):
               castClade = clade.copy()
               tips.append(castClade)
               
            else:
               castClade = clade.copy()
               # TODO: Why?
               castClade[PhyloTreeKeys.MTX_IDX] = noMx['c']  # assigns a mx starting at end of pam
               tips.append(castClade)
               tipsNotInMatrix.append(castClade)
               noMx['c'] = noMx['c'] + 1
      buildLeaves(clade)
      
      tips.sort(key=operator.itemgetter(PhyloTreeKeys.MTX_IDX))   
      # TODO: This is sorted by an contrived index, why?
      tipsNotInMatrix.sort(key=operator.itemgetter(PhyloTreeKeys.MTX_IDX))
      
      # tips: List of tip clades sorted by matrix index
      # internal: Dictionary of path id, list of children for that path, I'm sure we can do better
      # tipsNotInMatrix: List of tip clades not in matrix (somehow sorted by matrix id?)
      
      
      return tips, internal, tipsNotInMatrix
   
   # ..............................   
   def getSiblingsMx(self, clade):
      """
      @summary: gets all tips that are siblings that are in PAM, (have 'mx')
      @todo: Document
      @todo: Use the tree version of this
      """
      
      mx = []
      def getMtxIds(clade):
         if len(clade[PhyloTreeKeys.CHILDREN]) > 0:
            for child in clade[PhyloTreeKeys.CHILDREN]:
               getMtxIds(child)
         else:
            if clade.has_key(PhyloTreeKeys.MTX_IDX):
               mx.append(clade[PhyloTreeKeys.MTX_IDX])
      getMtxIds(clade)
      return mx

   # ..............................   
   def processTipNotInMatrix(self, tipsNotInMtx, internal, pam):
      """
      @param tipsNotInMtx: list of tip dictionaries
      @param internal: list of internal nodes made in buildTips
      @todo: Document
      """  
      print "In process tip not in matrix"
      mxMapping = {} 
      for tip in tipsNotInMtx:
         #TODO: Consider reversing path
         parentId = tip[PhyloTreeKeys.PATH][-2]
  
         parentsChildren = internal[parentId]#['children']  
         for sibling in parentsChildren:
            
            # TODO: This checks to see if it is itself or not
            
            if tip[PhyloTreeKeys.PATH_ID] != sibling[PhyloTreeKeys.PATH_ID]:
               # not itself
               if len(sibling[PhyloTreeKeys.CHILDREN]) > 0:
                  # recurse unitl it get to tips with 'mx'
                  
                  
                  # TODO: This looks like it gets all of the matrix indices for this clade
                  mxs = self.getSiblingsMx(sibling)
                  
                  mxMapping[tip[PhyloTreeKeys.MTX_IDX]] = mxs
               else:
                  if sibling.has_key(PhyloTreeKeys.MTX_IDX):
                     mxMapping[tip[PhyloTreeKeys.MTX_IDX]] = [sibling[PhyloTreeKeys.MTX_IDX]]
                  else:
                     mxMapping[tip[PhyloTreeKeys.MTX_IDX]] = 0
      la = [] # list of arrays              
      for k in sorted(mxMapping.keys()):
         
         if isinstance(mxMapping[k], list):
            
            # What are we doing here?
            # TODO: t appears to be an array of the columns in the PAM that have matrix ids for a clade
            t = np.take(pam, np.array(mxMapping[k]), axis = 1)
            # TODO: b is then a vector with ones wherever any of the columns in t had a one
            b = np.any(t, axis = 1)  #returns bool logical or
         else:
            # TODO: Looks like this is just an array of number of rows ones?
            b = np.ones(pam.shape[0], dtype=np.int)
         la.append(b)
      newPam = np.append(pam, np.array(la).T, axis=1)
      return newPam

   # ..............................   
   def _buildPMatrixNoBranchLengths(self):
      """
      @summary: Creates a P matrix when no branch lengths are present
      @note: For this method, we assume that there is a total weight of -1 to 
                the left and +1 to the right for each node.  As we go down 
                (towards the tips) of the tree, we divide the proportion of 
                each previously visited node by 2.  We then recurse with this 
                new visited list down the tree.  Once we reach a tip, we can 
                return that list of proportions because it will match for that 
                tip for each of it's ancestors.
      @note: Example: 
               3
               +--2
               |  +-- 1
               |  |   +--0
               |  |   |  +-- A
               |  |   |  +-- B
               |  |   |
               |  |   +-- C
               |  |
               |  +-- D
               |
               +--4
                  +-- E
                  +-- F
                  
            Step 1: (Node 3) [] 
                      - recurse left with [(3,-1)] 
                      - recurse right with [(3,1)]
            Step 2: (Node 2) [(3,-1)]
                      - recurse left with [(3,-.5),(2,-1)] 
                      - recurse right with [(3,-.5),(2,1)]
            Step 3: (Node 1)[(3,-.5),(2,-1)] 
                      - recurse left with [(3,-.25),(2,-.5),(1,-1)]
                      - recurse right with [3,-.25),(2,-.5),(1,1)]
            Step 4: (Node 0)[(3,-.25),(2,-.5),(1,-1)]
                      - recurse left with [(3,-.125),(2,-.25),(1,-.5),(0,-1)]
                      - recurse right with [(3,-.125),(2,-.25),(1,-.5),(0,1)]
            Step 5: (Tip A) - Return [(3,-.125),(2,-.25),(1,-.5),(0,-1)]
            Step 6: (Tip B) - Return [(3,-.125),(2,-.25),(1,-.5),(0,1)]
            Step 7: (Tip C) - Return [(3,-.25),(2,-.5),(1,1)]
            Step 8: (Tip D) - Return [(3,-.5),(2,1)]
            Step 9: (Node 4) [(3,1)] - recurse left with [(3,.5),(4,-1)]
                                     - recurse right with [(3,.5),(4,1)]
            Step 10: (Tip E) - Return [(3,.5),(4,-1)]
            Step 11: (Tip F) - Return [(3,.5),(4,1)]
            
            Creates matrix:
                   0    1    2     3      4
               A -1.0 -0.5 -0.25 -0.125  0.0
               B  1.0 -0.5 -0.25 -0.125  0.0
               C  0.0  1.0 -0.5  -0.25   0.0
               D  0.0  0.0  1.0  -0.5    0.0
               E  0.0  0.0  0.0   0.5   -1.0
               F  0.0  0.0  0.0   0.5    1.0
      
      @see: Page 1293 of the literature
      """
      # .......................
      # Initialize the matrix
      allPathIds = self.tree.getDescendants(None)
      
      # Internal path ids are the subset of path ids that are not tips
      internalPathIds = list(set(allPathIds) - set(self.tree.tips))
      matrix = np.zeros((len(self.tree.tips), len(internalPathIds)), 
                        dtype=np.float)

      # Get the list of tip proportion lists
      # Note: Which tip each of the lists belongs to doesn't really matter but
      #          recursion will start at the top and go to the bottom of the 
      #          tree tips
      tipProps = self._buildPMatrixTipProportionList(self.tree.tree, visited=[])
      
      # We need a mapping of node path id to matrix column.  I don't think 
      #    order matters
      nodeColumnIndex = dict(zip(internalPathIds, range(len(internalPathIds))))
      
      # "i" will be used as the row index in the matrix for the tip
      for i in range(len(tipProps)):
         for nodePathId, val in tipProps[i]:
            matrix[i][nodeColumnIndex[nodePathId]] = val
            
      return matrix  
   
   # ..............................   
   def _buildPMatrixTipProportionList(self, clade, visited=[]):
      """
      @summary: Recurses through the tree to build a list of tip proportions to 
                   be used to build a P-matrix when no branch lengths are 
                   present
      @param clade: The current clade
      @param visited: A list of [(node matrix index, proportion)]
      @note: Proportion for each visited node is divided by two as we go 
                towards the tips at each hop
      @todo: This appears to be a slightly modified version of the branch 
                lengths version.  Perhaps rewrite these to use the same 
                function and simplify branch length method
      """
      tipProps = []
      if len(clade[PhyloTreeKeys.CHILDREN]) > 0: # Assume this is two
         # First divide all existing visited by two
         newVisited = [(idx, val / 2.0) for idx, val in visited]
         # Recurse.  Left is negative, right is positive
         tipProps.extend(
            self._buildPMatrixTipProportionList(clade[PhyloTreeKeys.CHILDREN][0], 
                            newVisited + [(clade[PhyloTreeKeys.PATH_ID], -1.0)]))
         tipProps.extend(
            self._buildPMatrixTipProportionList(clade[PhyloTreeKeys.CHILDREN][1], 
                             newVisited + [(clade[PhyloTreeKeys.PATH_ID], 1.0)]))
      else: # We are at a tip
         tipProps.append(visited) # Just return a list with one list
      return tipProps
   
   # ..............................   
   def _buildPMatrixFromBranchLengths(self):
      """
      @summary: Build a P matrix from branch lengths in the tree
      @note: Path ids matter.  The order changes if they change.
      @note: The output should be based on position in the tree, not the path id
      @todo: See if this can be combined with no branch lengths method
      """
      lengths = self.tree.getBranchLengths()
      
      # Get all path ids in the tree
      allPathIds = self.tree.getDescendants(None)
      
      # Internal path ids are the subset of path ids that are not tips
      internalPathIds = list(set(allPathIds) - set(self.tree.tips))
      
      # Initialize the matrix
      matrix = np.zeros((len(self.tree.tips), len(internalPathIds)), 
                        dtype=np.float)  # consider it's own init func
      
      # Sort these for processing
      sortedInternalPathIds = sorted(internalPathIds)
      
      numDescTips = self.tree.getNumberOfDescendantTipsDict()

      mtxIdxMapping = self.tree.getMatrixIndicesMapping()
      
      # TODO: Can we do this better than sorting?
      for col, k in enumerate(sortedInternalPathIds):
         
         clade = self.tree.getClade(k)
         # Assume that the tree is binary
         leftChild, rightChild = clade[PhyloTreeKeys.CHILDREN]
         
         leftDescendants = self.tree.getDescendants(leftChild)
         rightDescendants = self.tree.getDescendants(rightChild)
         
         # The left denominator is the sum of all of the branch lengths on the
         #    left side of the clade multiplied by -1
         leftDenominator = sum(lengths[x] for x in leftDescendants) * -1.0
         
         # The right denominator is the same as the left, but for the right 
         #    side of the tree but it is not multiplied
         rightDenominator = sum(lengths[x] for x in rightDescendants)

         # Get the descendants from both sides that are tips of the tree
         leftTips = [x for x in leftDescendants if x in self.tree.tips]
         rightTips = [x for x in rightDescendants if x in self.tree.tips]
         
         # We do the same operation for the left and right children, so rather
         #    than duplicate code, make the operation more generic and loop
         #    through the values
         for tipList, denominator in [(leftTips, leftDenominator), 
                                      (rightTips, rightDenominator)]:
            for pathId in tipList:
               tipPath = self.tree.cladePaths[pathId]
               # Get the position of the current clade since we will only sum 
               #   the branch lengths between that clade and the tip
               # So if we have the following path where the clade is d and the
               #    tip is g, we would only use e and f
               # [a, b, c, d, e, f, g]
               cladePosition = tipPath.index(k)
               
                  # The numerator is the branch length of the tip plus the sum 
                  #    of the weighted lengths between the tip and the current 
                  #    clade
               numerator = lengths[pathId] + sum(
                               [lengths[i] / numDescTips[i] \
                                   for i in tipPath[cladePosition+1:-1]])
         
               matrix[mtxIdxMapping[pathId]][col] = numerator / denominator
         
            
      return matrix   


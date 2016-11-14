"""
@summary Module that contains the RADExperiment class
@author Aimee Stewart
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
import mx.DateTime
from types import StringType, UnicodeType

from LmServer.base.experiment import _Experiment
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import ENVIRONMENTAL_LAYER_KEY, Priority, \
                  ORGANISM_LAYER_KEY, LMFileType, LMServiceType, LMServiceModule
from LmServer.rad.anclayer import _AncillaryValue
from LmServer.rad.matrixlayerset import MatrixLayerset
from LmServer.rad.palayer import _PresenceAbsence
from LmServer.rad.radbucket import RADBucket

# .............................................................................
class RADExperiment(_Experiment):
   """
   The RADExperiment class contains all of the input layers and ancillary 
   information, and all the outputs for a LmRAD macroecology experiment
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, userid, expname, epsgcode, 
                attrMatrixFilename=None, attrTreeFilename=None, 
                email=None, metadataUrl=None,
                keywords=None, lyrIndices=None, orglayers=[], envlayers=[], 
                buckets=[], priority=Priority.REQUESTED, description=None, 
                expId=None, createTime=None, modTime=None):
      """
      @summary Constructor for the Experiment class
      @param userid: the unique identifier for the owner of this experiment.
      @param expname: the human-readable name assigned to this experiment
      @param attrMatrixFilename: Filename for a table with rows containing 
                  layernames 
                  matching the layernames in the orglayerset and columns 
                  containing attributes for those layers.  The table may be 
                  sparsely populated since attributes will be not be applicable
                  to all layers.
      @param attrTree: Filename for a tree describing the relationship 
                  (phylogenetic or otherwise) between organisms in the 
                  orglayerset.  
      @param keywords: a comma-delimited string or sequence of keywords for this 
                       experiment
      @param lyrIndices: A filename or dictionary of MatrixLayerset dictionaries
                            {ORGANISM_LAYER_KEY: dictionary,
                             ENVIRONMENTAL_LAYER_KEY: dictionary} 
      @param orglayers: A list of PresenceAbsenceLayers
      @param envlayers: A list of AncillaryLayers
      @param buckets: A list of RADBuckets using the same layers,
                  but possibly different shapegrids (Area of interest and 
                  resolution) and different species classification information 
                  for this experiment. 
      @param expid: Database id for the RADExperiment 
      @param createTime: Create Time/Date, in Modified Julian Day (MJD) format
      @param modTime: Last modification Time/Date, in MJD format
      """
      _Experiment.__init__(self, expId, userid, epsgcode, createTime, modTime,
                           description=description, metadataUrl=metadataUrl,
                           serviceType=LMServiceType.RAD_EXPERIMENTS, 
                           moduleType=LMServiceModule.RAD)
      self.name = expname
      
      self.orgLayerset = None
      self.envLayerset = None
      self._experimentPath = None
      self._lyridxFname = None
      
      self.priority = priority
      self._email = email

      if keywords is not None:
         if isinstance(keywords, (StringType, UnicodeType)):
            keywords = keywords.split(',')
         else:
            keywords = list(keywords)
         self.keywords = keywords
      else:
         self.keywords = []

      self.bucketList = []
      for bck in buckets:
         self.addBucket(bck)
         
      # Also sets self._experimentPath and expId on all buckets
      if expId is not None:
         self.setId(expId)

      # Do not read attribute files until/unless they are needed
      self._attrMatrixFname = None
      self._attrTreeFname = None
      self._setAttributeMatrixFilename(fname=attrMatrixFilename)
      self._setAttributeTreeFilename(fname=attrTreeFilename)
         
      # lyridxFname is calculated, then read if it exists
      self._setLayerIndicesFilename()
      self._lyrIndices = self.readLayerIndices()
      try:
         self.setOrgLayerset(orglayers, 
                             lyrIndices=self._lyrIndices[ORGANISM_LAYER_KEY])
         self.setEnvLayerset(envlayers, 
                             lyrIndices=self._lyrIndices[ENVIRONMENTAL_LAYER_KEY])
      except LMError, e:
         raise
      except Exception, e:
         raise LMError(currargs=e.args) 

# ...............................................
   @classmethod
   def initFromGrid(cls, userid, expname, shapegrid, keywords=None, 
                expId=None, createTime=None, modTime=None):
      """
      @summary Constructor for the Experiment class
      @param userid: the unique identifier for the owner of this experiment.
      @param expname: the human-readable name assigned to this experiment
      @param shapegrid: Vector layer with polygons representing geographic sites.
      @param keywords: a comma-delimited string of keywords for this experiment
      @param expid: Database id for the RADExperiment 
      @param createTime: Create Time/Date, in Modified Julian Day (MJD) format
      @param modTime: Last modification Time/Date, in MJD format
      """
      exp = RADExperiment(userid, expname, shapegrid.epsgcode, keywords=keywords, 
                          expId=expId, 
                          createTime=createTime, modTime=modTime)
      bck = RADBucket(shapegrid, userId=userid, createTime=createTime, 
                      parentMetadataUrl=exp.metadataUrl)
      exp.addBucket(bck)
      return exp
      
# ...............................................
   @classmethod
   def initFromFiles(cls, userid, expname, epsgcode, keywords=None, 
                      orglayerset=None, envlayerset=None, expId=None,
                      shpgridFilename=None, 
                      attrMatrixFilename=None, attrTreeFilename=None, 
                      pamFilename=None, grimFilename=None, pamsumFilename=None, 
                      indicesFilename=None,  
                      stage=None, stageModTime=None, 
                      status=None, statusModTime=None,
                      createTime=None, modTime=None):
      """
      @summary Constructor for the Matrix class
      @param pamFilename: The filename of the uncompressed PAM 
      @param grimFilename: The filename of the uncompressed GRIM 
      @param pamsumFilename: The filename of the uncompressed GRIM
      @param indicesFilename: The filename of the sitesPresent and layersPresent 
                              dictionaries
      @param attrMatrixFilename: Filename for a table with rows containing 
                  layer identifiers and columns containing attributes for those 
                  layers.
      @param attrTree: Filename for a tree describing the relationship 
                  (phylogenetic or otherwise) between organisms in the 
                  orglayerset.  
      @param stage: The processing stage of the bucket for these files/data
      @param stageModTime: The last time that the stage was modified
      @param status:  The run status of the current stage
      @param statusModTime: The last time that the status was modified
      @return: A RADExperiment with a single bucket 
      """
      bck = RADBucket.initFromFiles(shpgridFilename=shpgridFilename, 
                                    pamFilename=pamFilename, 
                                    grimFilename=grimFilename, 
                                    pamsumFilename=pamsumFilename, 
                                    indicesFilename=indicesFilename,  
                                    stage=stage, stageModTime=stageModTime, 
                                    status=status, statusModTime=statusModTime)
      exp = RADExperiment(userid, expname, epsgcode, keywords=keywords, 
                          orglayerset=orglayerset, envlayerset=envlayerset, 
                          attrMatrixFilename=attrMatrixFilename, 
                          attrTreeFilename=attrTreeFilename, 
                          buckets=[bck], expId=expId, 
                          createTime=createTime, modTime=modTime)
      return exp

# .............................................................................
# Public methods
# .............................................................................
   def setId(self, expid):
      """
      @summary: Sets the database id on the object
      @param expid: The database id for the object
      @note: Overrides (but calls) _Experiment.setId
      """
      _Experiment.setId(self, expid)
      if expid is not None:
         self._experimentPath = self._earlJr.createDataPath(self._userId, 
                                          LMFileType.UNSPECIFIED_RAD,
                                          epsg=self._epsg, radexpId=expid)
         for bkt in self.bucketList:
            bkt.setExperimentId(expid)
                        
# ...............................................
   def divide(self):
      clones = []
      for bkt in self.bucketList:
         exp = RADExperiment(self.getUserId(), self.name, self.epsgcode, 
                             keywords=self.keywords, buckets=[bkt], 
                             priority=self.priority, expId=self.getId(), 
                             createTime=self.createTime, modTime=self.modTime)
         exp.orgLayerset = self.orgLayerset
         exp.envLayerset = self.envLayerset
         clones.append(exp)
      return clones

# ...............................................
# Multiple Attribute Matrices and Trees should be allowed for every experiment
# TODO: move these into another class with a list for Matrices and one for trees
# ...............................................
   
   def readAttributeMatrix(self):
      if self._attrMatrixFname is not None:
         # Read into self.attributeMatrix
         pass

   def writeAttributeMatrix(self):
      if self.attributeMatrix is not None:
         if self._attrMatrixFname is None:
            self._setAttributeMatrixFilename()
         # Write self.attributeMatrix to a file
         pass
   
# ...............................................
   def readAttributeTree(self):
      if self._attrTreeFname is not None:
         pass

   def writeAttributeTree(self, content):
      if self._attrTreeFname is None:
         self._setAttributeTreeFilename()
      try: 
         self._readyFilename(self._attrTreeFname, overwrite=True)
      except:
         raise
      
      print('Writing Attribute Tree %s' % self._attrTreeFname)
      try:
         f = open(self._attrTreeFname, 'wb')
         # write as a string, TODO: sanity check
         f.write(content)
      except Exception, e:
         raise LMError('Error writing file %s' % self._attrTreeFname, str(e))
      finally:
         f.close()
            
# ...............................................
   def _createAttributeMatrixFilename(self):
      """
      @summary: Create the filename for the Attribute Matrix based on the user 
                and experimentId
      @return: String identifying the filename of the Attribute Matrix
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.ATTR_MATRIX,
                    radexpId=self.getId(), pth=self._experimentPath, 
                    usr=self._userId, epsg=self._epsg)
      return fname
# ...............................................
   def _setAttributeMatrixFilename(self, fname=None):
      """
      @summary: Set the filename for the Attribute Matrix
      @return: String identifying the filename of the Attribute Matrix
      """
      if self._attrMatrixFname is None:
         if fname is None:
            fname = self._createAttributeMatrixFilename()
         self._attrMatrixFname = fname
      
# ...............................................
   def clear(self):
      self.clearAttributeMatrixFile()
      self.clearAttributeTreeFile()
      self.clearLayerIndicesFile()

# ...............................................
   def clearAttributeMatrixFile(self):
      if self._attrMatrixFname is None:
         self._setAttributeMatrixFilename()
      success, msg = self._deleteFile(self._attrMatrixFname)
      self._attrMatrixFname = None

# ...............................................
   def _getAttributeMatrixFilename(self):
      """
      @summary: Return the filename for the Attribute Matrix
      @return: String identifying the filename of the Attribute Matrix
      """
      self._setAttributeMatrixFilename()
      return self._attrMatrixFname

# ...............................................
   def _createAttributeTreeFilename(self):
      """
      @summary: Create the filename for the Attribute Tree based on the user 
                and experimentId
      @return: String identifying the filename of the Attribute Tree
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.ATTR_TREE, 
                    radexpId=self.getId(), pth=self._experimentPath, 
                    usr=self._userId, epsg=self._epsg)
      return fname
# ...............................................
   def _setAttributeTreeFilename(self, fname=None):
      """
      @summary: Set the filename for the Attribute Tree
      @return: String identifying the filename of the Attribute Tree
      """
      if self._attrTreeFname is None:
         if fname is None:
            fname = self._createAttributeTreeFilename()
         self._attrTreeFname = fname
      
# ...............................................
   def clearAttributeTreeFile(self):
      if self._attrTreeFname is None:
         self._setAttributeTreeFilename()
      success, msg = self._deleteFile(self._attrTreeFname)
      self._attrTreeFname = None

# ...............................................
   def _getAttributeTreeFilename(self):
      """
      @summary: Return the filename for the Attribute Tree
      @return: String identifying the filename of the Attribute Tree
      """
      self._setAttributeTreeFilename()
      return self._attrTreeFname

   attrMatrixDLocation = property(_getAttributeMatrixFilename)
   attrTreeDLocation = property(_getAttributeTreeFilename)
      
# ...............................................
   def addBucket(self, bucket):
      if isinstance(bucket, RADBucket):
         # Ensure matching UserId
         if bucket.getUserId() is None:
            bucket.setUserId(self.getUserId())
         elif bucket.getUserId() != self.getUserId():
            raise LMError(currargs='Unable to add bucket with User %s to Experiment with User %s' %
                          (str(bucket.getUserId()), str(self.getUserId())))
           
         if self.getBucket(bucket.name) is None: 
            # Ensure matching EPSG
            if bucket.epsgcode == self.epsgcode:
               # Set _experimentId
               if self.getId() is not None:
                  bucket.setExperimentId(self.getId())
               if not bucket.getLayersPresent():
                  # Currently just tracking PA layers present on bucket
                  bucket.setLayersPresent(self.initOrgLayersPresent())
               self.bucketList.append(bucket)
            else:
               raise LMError(currargs='Unable to add bucket with EPSG %s to Experiment with EPSG %s' %
                             (str(bucket.epsgcode), str(self.epsgcode)))
         else:
            print('Bucket %s already present' % self.name)

# ...............................................
   def setEnvLayerset(self, lyrlist, lyrIndices=None):
      # Vestigial name 
      lyrsetname = 'env_' + str(self.getId())
      lyrset = MatrixLayerset(lyrsetname, lyrIndices=lyrIndices, 
                              epsgcode=self.epsgcode, userId=self._userId, 
                              expId=self.getId())
      if len(lyrlist) > 0:
         firstlyr = lyrlist[0]
         if not(isinstance(firstlyr, _AncillaryValue)):
            raise LMError(currargs='Layers are not type _AncillaryValue')
         elif firstlyr.epsgcode != self.epsgcode:
            raise LMError(currargs='Layers are not epsg %s' 
                          % str(self.epsgcode))
         else:
            # initialize the layerset for AncillaryLayers
            lyrset.addLayer(firstlyr)
            
      for i in range(1,len(lyrlist)):
         try:
            lyrset.addLayer(lyrlist[i])
         except LMError, e:
            raise
         except Exception, e:
            raise LMError(currargs='Failed to create a MatrixLayerset (%s)' % str(e))
         
         if (not self._lyrIndices[ENVIRONMENTAL_LAYER_KEY] or 
             not len(self._lyrIndices[ENVIRONMENTAL_LAYER_KEY]) 
              == len(lyrset.getLayerIndices()) ):
            self._lyrIndices[ENVIRONMENTAL_LAYER_KEY] = lyrset.getLayerIndices()

      self.envLayerset = lyrset
                  
# ...............................................
   def addAncillaryLayer(self, lyr):
      self.envLayerset.addLayer(lyr)
      
# ...............................................
   def _createLayerIndicesFilename(self):
      """
      @summary: Create the filename for the SitesPresent and LayersPresent 
                dictionaries based on the user, bucketid, and experimentId
      @return: String identifying the filename of the sitesLayerPresent file
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.LAYER_INDICES, 
                    radexpId=self.getId(), pth=self._experimentPath, 
                    usr=self._userId, epsg=self._epsg)
      return fname
# ...............................................
   def _setLayerIndicesFilename(self, fname=None):
      """
      @summary: Set the filename for the SitesPresent and LayersPresent 
                dictionaries based on the user, bucketid, and experimentId
      @return: String identifying the filename of the sitesLayerPresent file
      """
      if self._lyridxFname is None:
         if fname is None:
            fname = self._createLayerIndicesFilename()
         self._lyridxFname = fname
      
# ...............................................
   @property
   def indicesDLocation(self):
      """
      @summary: Return the filename for the pickled sitesPresent and 
               layersPresent dictionaries
      @return: String identifying the filename containing the sitesPresent and 
               layersPresent
      """
      if self._lyridxFname is None:
         self._setLayerIndicesFilename()
      return self._lyridxFname
      
# ...............................................
   def clearLayerIndicesFile(self):
      if self._lyridxFname is None:
         self._setLayerIndicesFilename()
      success, msg = self._deleteFile(self._lyridxFname)
      self._lyridxFname = None
            
# .............................................................................
   def writeLayerIndices(self):
      """
      @summary: Pickle lyrIndices into _lyridxFname
      @todo: fix this to work when we are also doing AncillaryLayers; now
             it only writes when it populates for a job - one at a time.
      """
      try:
         import cPickle as pickle
      except:
         import pickle
      self._setLayerIndicesFilename()
      
      try: 
         self._readyFilename(self._lyridxFname, overwrite=True)
      except:
         raise
      
      print('Writing Layer Indices %s' % self._lyridxFname)
      try:
         f = open(self._lyridxFname, 'wb')
         # Pickle the list using the highest protocol available.
         pickle.dump(self._lyrIndices, f, protocol=pickle.HIGHEST_PROTOCOL)
      except Exception, e:
         raise LMError('Error pickling file %s' % self._lyridxFname, str(e))
      finally:
         f.close()
      
# ...............................................
   def readLayerIndices(self):
      """
      @summary: Unpickle _presidxFname into sitesPresent and layersPresent
      """
      import os
      try:
         import cPickle as pickle
      except:
         import pickle
      if self._lyridxFname is not None and os.path.isfile(self._lyridxFname):
         try: 
            f = open(self._lyridxFname,'rb')         
            indices = pickle.load(f)
            # TODO: Remove when inconsistent pickles are gone
            if not indices.has_key(ORGANISM_LAYER_KEY):
               indices[ORGANISM_LAYER_KEY] = None
            if not indices.has_key(ENVIRONMENTAL_LAYER_KEY):
               indices[ENVIRONMENTAL_LAYER_KEY] = None
         except Exception, e:
            raise LMError(currargs='Failed to load %s into indices' % 
                          self._lyridxFname, prevargs=str(e))
         finally:
            f.close()
      else:
         indices = {ORGANISM_LAYER_KEY: None, ENVIRONMENTAL_LAYER_KEY: None}
      return indices
         
# ...............................................
   def addPresenceAbsenceLayer(self, lyr):
      """
      @summary: Add a PresenceAbsenceLayer to the orgLayerset on this 
                experiment.
      @note: Layer must be added to the experiment in the database first, to 
             ensure that it is not a duplicate and that it has a matrix index.
      @note: This means that in all buckets, the original PAM must be modified, 
             the Sum must be deleted and all randomizedPamSums deleted.
      """
      self.orgLayerset.addLayer(lyr)
      self.rollback(mx.DateTime.gmt().mjd)

# ...............................................
   def getBucket(self, shapegridName):
      for bck in self.bucketList:
         if bck.shapegrid.name == shapegridName:
            return bck
      return None

# ...............................................
   def getOrgLayer(self, metaurl, paramid):
      return self.orgLayerset.getLayer(metaurl, paramid)
      
# ...............................................
   def getEnvLayer(self, metaurl, paramid):
      return self.envLayerset.getLayer(metaurl, paramid)
            
# ...............................................
   def setOrgLayerset(self, lyrlist, lyrIndices=None):
      # Vestigial name 
      lyrsetname = 'org_' + str(self.getId())
      lyrset = MatrixLayerset(lyrsetname, lyrIndices=lyrIndices, 
                              epsgcode=self.epsgcode, userId=self.getUserId(), 
                              expId=self.getId())
      if len(lyrlist) > 0:
         firstlyr = lyrlist[0]
         if not(isinstance(firstlyr, _PresenceAbsence)):
            raise LMError(currargs='Layers are not type _PresenceAbsence')
         elif firstlyr.epsgcode != self.epsgcode:
            raise LMError(currargs='Layers are not epsg %s' 
                          % str(self.epsgcode))
         else:
            # initialize the layerset for PresenceAbsenceLayers
            lyrset.addLayer(firstlyr)
            
         for i in range(1,len(lyrlist)):
            try:
               lyrset.addLayer(lyrlist[i])
            except LMError, e:
               raise
            except Exception, e:
               raise LMError(currargs='Failed to create a MatrixLayerset (%s)' % str(e))
         if (not self._lyrIndices[ORGANISM_LAYER_KEY] or 
             not len(self._lyrIndices[ORGANISM_LAYER_KEY]) 
              == len(lyrset.getLayerIndices()) ):
            self._lyrIndices[ORGANISM_LAYER_KEY] = lyrset.getLayerIndices()
         
      self.orgLayerset = lyrset
      
# .............................................................................
   def initOrgLayersPresent(self):
      self.readLayerIndices()
      if self.orgLayerset:
         self._lyrIndices[ORGANISM_LAYER_KEY] = self.orgLayerset.initLayersPresent()
      
      layersPresent = {}
      if self._lyrIndices[ORGANISM_LAYER_KEY]:
         for k in self._lyrIndices[ORGANISM_LAYER_KEY].keys():
            layersPresent[k] = True

      return layersPresent
          
# .............................................................................
   def initEnvLayersPresent(self):
      self.readLayerIndices()
      if not self._lyrIndices[ENVIRONMENTAL_LAYER_KEY] and self.envLayerset:
         self._lyrIndices[ENVIRONMENTAL_LAYER_KEY] = self.envLayerset.initLayersPresent()
      
      layersPresent = {}
      if self._lyrIndices[ENVIRONMENTAL_LAYER_KEY]:
         for k in self._lyrIndices[ENVIRONMENTAL_LAYER_KEY].keys():
            layersPresent[k] = True
      
      return layersPresent

# .............................................................................
   def addKeywords(self, keywordSequence):
      """
      @summary Adds keywords to the LayerSet object
      @param keywordSequence: List of keywords to add
      """
      if keywordSequence is not None:
         for k in keywordSequence:
            self._keywords.add(k)
         
   def addKeyword(self, keyword):
      """
      @summary Adds a keyword to the LayerSet object
      @param keyword: Keyword to add
      """
      if keyword is not None:
         self._keywords.add(keyword)

   def _getKeywords(self):
      """
      @summary Gets the keywords of the LayerSet
      @return List of keywords describing the LayerSet
      """
      return self._keywords
         
   def _setKeywords(self, keywords):
      """
      @summary Sets the keywords of the RADBucket
      @param keywords: List or comma-delimited string of keywords that will be 
                       associated with the RADBucket
      """
      if isinstance(keywords, (StringType, UnicodeType)):
         keywords = keywords.split(',')   
      if keywords is not None:
         self._keywords = set(keywords)
      else:
         self._keywords = set()

   def rollback(self, currtime):
      for bucket in self.bucketList:
         bucket.rollback(currtime)

   def getLayerIndices(self):
      return self._lyrIndices

# .............................................................................
# .............................................................................

# ...............................................
   def _getOutputPath(self):
      return self._experimentPath
   
   
# .............................................................................
# Properties
# .............................................................................
   @property
   def email(self):
      return self._email
   
         

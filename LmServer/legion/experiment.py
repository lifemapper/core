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
try:
   import cPickle as pickle
except:
   import pickle
import json
import mx.DateTime
import os
from osgeo import ogr
import subprocess
from types import StringType, UnicodeType

from LmCommon.common.lmconstants import JobStage, JobStatus, RandomizeMethods
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject, ProcessObject
from LmServer.common.lmconstants import LMFileType, LMServiceType, LMServiceModule
from LmServer.rad.matrix import Matrix                                  
from LmServer.rad.pamvim import PamSum

# .............................................................................
class RADBucket(ServiceObject, ProcessObject):
   """
   The RADBucket class contains all of the information for one view (extent and 
   resolution) of a RAD experiment.  
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, shapegrid, metadata={},
                epsgcode=None, keywords=None, 
                fullPam=None, fullGrim=None, pamSum=None, randomPamSums=[],
                pamFname=None, grimFname=None, 
                compressedPamFname=None, sumFname=None,
                sitesPresent=None, layersPresent={}, indicesFilename=None,
                stage=None, stageModTime=None, 
                status=None, statusModTime=None, 
                userId=None, expId=None, bucketId=None, createTime=None,
                metadataUrl=None, parentMetadataUrl=None):
      """
      @summary Constructor for the BgBucket class
      @param shapegrid: Vector layer with polygons representing geographic sites.
      @param fullPAM: A Matrix of the original PresenceAbsenceMatrix
             (PAM) created directly from the input OrganismLayerset intersected
             with the ShapeGrid
      @param fullGRIM: A Matrix of the original Environmental Layers intersected
             with the ShapeGrid
      @param pamSum: The (original) PamSum object that is one Matrix and one 
             dictionary of statistics -  
                1) PAM - compressed version of the fullPAM, with all rows/sites 
                   with no layer present removed and all columns/layers with no 
                   presence in the sites removed.
                2) SUM - A dictionary of calculations, both individual values 
                   and vectors, corresponding to the calculations performed
                   on the compressed PAM 
      @param randomPamSums: A list of PamSums with a randomized, compressed 
             version of the original PAM and a SUM from that randomized PAM. 
      @param indicesFilename: Filename for the 2 dictionaries, sitesPresent and
             layersPresent. This overrides the dictionaries if all are provided.
      @param sitesPresent: A dictionary with keys the unique/record identifiers 
             of the sites in a ShapeGrid, and values a boolean indicating 
             presence in the compressed version of the PAM.  If the PAM has not 
             been compressed, all site keys will have a value of True.
      @param layersPresent: A dictionary with keys the index of each layer
             in a Matrix (and PresenceAbsenceLayerset), and values a boolean 
             indicating presence in the compressed version of the PAM.  If the 
             PAM has not been compressed, all layer keys will have a value of 
             True.
      @param stage: The processing stage of the experiment
      @param stageModTime: The last time that the stage was modified
      @param status:  The run status of the current stage
      @param statusModTime: The last time that the status was modified
      @param userId: id for the owner of these data
      @param expId: database id of the RADExperiment containing this Bucket 
      @param bucketId: database id of object 
      @param createTime: Create Time/Date, in Modified Julian Day (MJD) format
      @param modTime: Last modification Time/Date, in MJD format
      select * from lm3.lm_pullMessageJobs(2,510,1,90,NULL,56593.6974963,'129.237.201.119');
      """
      ServiceObject.__init__(self, userId, bucketId, createTime, statusModTime,
            LMServiceType.BUCKETS, moduleType=LMServiceModule.RAD,
            metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl)
      ProcessObject.__init__(self, objId=bucketId, parentId=expId, status=status,  
            statusModTime=statusModTime, stage=stage, stageModTime=stageModTime) 
      self._bucketPath = None
      self._pamSum = None
      self._randomPamSums = []
      self.metadata = {}
      self.loadMetadata(metadata)
#       self._experimentId = expId

      self._epsg = None
      self.shapegrid = None
      if shapegrid is not None:
         if shapegrid.getUserId() is None:
            shapegrid.setUserId(self._userId)
         self._epsg = shapegrid.epsgcode
      self.shapegrid = shapegrid
      
      # Allow construction without shapegrid 
      if self._epsg is None:
         self._epsg = epsgcode
         
      if bucketId is not None:
         self.setId(bucketId)

      self._setKeywords(keywords)
      
      self._pamFname = None
      self._grimFname = None
      self._presidxFname = None

      self._fullPAM = fullPam
      self._setPAMFilename(pamFname)
      self._fullGRIM = fullGrim
      self._setGRIMFilename(grimFname)
      self._setPAMSUM(pamSum)
#       self._setCmpPamFilename(compressedPamFname)
#       self._setSumFilename(sumFname)
      
      self.setRandomPamSums(randomPamSums)
      
      # indicesFilename is the only filename calculated (if not provided) on construction
      self._setPresenceIndicesFilename(indicesFilename)
      self._readPresenceIndices()
      if not self._layersPresent:
         self._layersPresent = layersPresent
      if not self._sitesPresent and self.shapegrid is not None:
         self._sitesPresent = shapegrid.initSitesPresent()
         if self._bucketPath is not None:
            self.writePresenceIndices()
            
# ...............................................
   @classmethod
   def initFromFiles(cls, shapegrid,  epsgcode=None, keywords=None,
                     pamFilename=None, grimFilename=None, pamsumFilename=None, 
                     indicesFilename=None,
                     stage=None, stageModTime=None, 
                     status=None, statusModTime=None, 
                     userId=None, expId=None, bucketId=None, createTime=None,
                     metadataUrl=None, parentMetadataUrl=None):
      """
      @summary Constructor for the BgBucket class, does not initialize all 
             member objects, just shapeGrid (given) and indices.
      @param shapegrid: Vector layer with polygons representing geographic sites.
      @param pamFilename: Filename for the Matrix of the original 
             PresenceAbsenceMatrix (PAM) 
             created directly from the input OrganismLayerset intersected with 
             the ShapeGrid (or a dlocation containing those data).
      @param grimFilename: Filename for the Matrix of the original Environmental 
             Layers intersected
             with the ShapeGrid (or a dlocation containing those data). 
      @param pamsumFilename: Filename for the PamSum object that is 2 Matrices -  
                1) PAM - compressed version of the fullPAM, with all rows/sites 
                   with no layer present removed and all columns/layers with no 
                   presence in the sites removed.
                2) SUM - The matrix corresponding to the calculations performed
                   on the compressed PAM 
      @param indicesFilename: Filename for the 2 dictionaries, sitesPresent and
             layersPresent.
      """
      pamsum = PamSum.initAndFillFromFile(pamsumFilename, epsgcode=epsgcode)
      bkt = RADBucket(shapegrid, epsgcode=epsgcode, pamSum=pamsum, 
                      userId=userId, expId=expId, 
                      bucketId=bucketId, createTime=createTime,
                      metadataUrl=metadataUrl, 
                      parentMetadataUrl=parentMetadataUrl)

      # Do not read matrices until/unless they are needed
      bkt._pamFname = pamFilename
      bkt._grimFname = grimFilename

      # Read the indices file if it exists and populate dictionaries 
      if indicesFilename is None:
         bkt._sitesPresent = shapegrid.initSitesPresent()
         bkt._layersPresent = {}
      else:
         bkt._presidxFname = indicesFilename
         bkt._readPresenceIndices()
      
      return bkt
      
# ...............................................
   def readPAM(self, fullPAMFilename=None):
      """
      @summary Fill the PAM object from existing file
      @postcondition: The full PAM object will be present
      """
      if fullPAMFilename is not None:
         self._pamFname = fullPAMFilename
      elif self._pamFname is None:
         raise LMError('No fullPAM filename to read')
      
      fullPAM = Matrix.initFromFile(self._pamFname, False)
      self.setFullPAM(fullPAM)      
         
# ...............................................
   def setId(self, id):
      """
      Overrides ServiceObject.setId.  
      @note: ExperimentId should always be set before this is called.
      """
      ServiceObject.setId(self, id)
      self.setPath()

# ...............................................
   def setPath(self):
      if self._bucketPath is None:
         if (self.parentId is not None and self._userId is not None and
             self.getId() is not None):
            self._bucketPath = self._earlJr.createDataPath(self._userId, 
                               epsg=self._epsg, radexpId=self.parentId, 
                               bucketId=self.getId())
      if self._bucketPath is not None:
         if self._pamSum is not None:
            self._pamSum.outputPath = self._bucketPath
         if self._randomPamSums:
            for rps in self._randomPamSums:
               rps.outputPath = self._bucketPath
               
# ...............................................
   def setExperimentId(self, expid):
      self.parentId = expid
      if self._bucketPath is None and self.getId() is not None:
         self._bucketPath = self._earlJr.createDataPath(self._userId, 
                            epsg=self._epsg, radexpId=self.parentId, 
                            bucketId=self.getId())
      
# ...............................................
#    def getExperimentId(self):
#       return self.parentId

# ...............................................
   def addMetadata(self, metadict):
      for key, val in metadict.iteritems():
         self.metadata[key] = val
         
# ...............................................
   def dumpMetadata(self):
      import json
      metastring = None
      if self.metadata:
         metastring = json.dumps(self.metadata)
      return metastring

# ...............................................
   def loadMetadata(self, meta):
      if meta is not None:
         if isinstance(meta, dict): 
            self.addMetadata(meta)
         else:
            try:
               metajson = json.loads(meta)
            except Exception, e:
               print('Failed to load JSON object from {} object {}'
                     .format(type(meta), meta))
            else:
               self.addMetadata(metajson)
   
   @property
   def experimentId(self):
      return self.parentId
   
# ...............................................
   def addShapegrid(self, shpgrid):
      if self.shapegrid is None:
         self.setSitesPresent(shpgrid.initSitesPresent())
      else:
         raise LMError('Shapegrid is already attached')
         
# ...............................................
   def setFullPAM(self, pam):
      self._fullPAM = pam
      if self._pamFname is None:
         self._setPAMFilename()
      
   def getFullPAM(self):
      return self._fullPAM
   
#    fullPAM = property(_getPAM, _setPAM)
# ...............................................
   def setFullGRIM(self, grim):
      self._fullGRIM = grim
      if self._grimFname is None:
         self._setGRIMFilename()
      
   def getFullGRIM(self):
      return self._fullGRIM
   
#    fullGRIM = property(_getGRIM, _setGRIM)
# ...............................................
   def _setPAMSUM(self, pamsum):
      self._pamSum = pamsum
      self._setCmpPamFilename()
      self._setSumFilename()
      if self._pamSum is not None:
         self._pamSum.outputPath = self._bucketPath
      
   def _getPAMSUM(self):
      return self._pamSum
   
   pamSum = property(_getPAMSUM, _setPAMSUM)
   
# ...............................................
   def addRandomPamSum(self, randomPamSum=None, 
                       randomPam=None, method=None, parameters={}, 
                       createTime=None):
      if randomPamSum is None:
         if method == RandomizeMethods.SPLOTCH:
            randomPamSum = PamSum(None, createTime=createTime, 
                                  bucketPath=self._bucketPath, 
                                  bucketId=self.getId(), 
                                  expId=self.parentId,
                                  epsgcode=self._epsg,
                                  userId=self.getUserId(), randomMethod=method, 
                                  randomParameters=parameters, 
                                  splotchPam=randomPam,
                                  parentMetadataUrl=self.metadataUrl)
         elif method == RandomizeMethods.SWAP:
            randomPamSum = PamSum(randomPam, createTime=createTime, 
                                  bucketPath=self._bucketPath,
                                  bucketId=self.getId(), 
                                  expId=self.parentId, 
                                  epsgcode=self._epsg,
                                  userId=self.getUserId(), 
                                  randomMethod=method, 
                                  randomParameters=parameters,
                                  parentMetadataUrl=self.metadataUrl)
         else:
            raise LMError(currargs='Unknown RandomizeMethod %s' % str(method))
      if self._bucketPath is not None:
         randomPamSum.outputPath = self._bucketPath
      self._randomPamSums.append(randomPamSum)
            
   def setRandomPamSums(self, rpamsums):
      self._randomPamSums = []
      if rpamsums:
         for rps in rpamsums:
            self.addRandomPamSum(rps)
   
   def getRandomPamSums(self):
      return self._randomPamSums
   
#    randomPamSums = property(_getRandomPamSums, _setRandomPamSums)
   
# ...............................................
   def readGRIM(self, grimFilename=None):
      """
      @summary Fill the GRIM object from existing file
      @postcondition: The full GRIM object will be present
      """
      if self._grimFname is None and grimFilename is not None:
         self._grimFname = grimFilename
      self._fullGRIM = Matrix.initFromFile(self._grimFname, False)

# # ...............................................
#    def readCompressedPamSum(self, cmpPAMFilename=None, sumFilename=None):
#       """
#       @summary Fill the GRIM object from existing file
#       @postcondition: The full GRIM object will be present
#       """
#       if self._cmpPAMFname is None and cmpPAMFilename is not None:
#          self._cmpPAMFname = cmpPAMFilename
#       self._pamSum = PamSum.initAndFillFromFile(self._cmpPAMFname, 
#                                                 sumFilename=sumFilename)
#       
# # ...............................................
#    def writePamSum(self):
#       """
#       @summary: Write PAM, SUM matrices to files, and sitesPresent, 
#                layersPresent dictionaries to a file. 
#       """
#       self._setPAMSUMFilename()
#       self._setPresenceIndicesFilename()
#       if self._pamSum is not None:
#          self._pamSum.writePam()
#          self._pamSum.writeSum()
#          self.writePresenceIndices()
#       else:
#          raise LMError(currargs='PamSum does not exist')
         
# ...............................................
   def writePam(self):
      """
      @summary: Write PAM matrix to file. 
      """
      self._setPAMFilename()
      if self._fullPAM is not None:
         print('Writing fullPAM ...')
         self._fullPAM.write()
      else:
         print('PAM does not exist')

# ...............................................
   def writeGrim(self):
      """
      @summary: Write GRIM matrix to file. 
      """
      self._setGRIMFilename()
      if self._fullGRIM is not None:
         print('Writing fullGRIM ...')
         self._fullGRIM.write()
      else:         
         print 'GRIM does not exist'

# ...............................................
   def setFilenames(self):
      """
      @summary: Set the filename for the Full PAM, Compressed PAM and SUM, 
                and the file containing sitesPresent and layersPresent indices 
                based on the user, experimentId, and bucketId 
      @return: True on success, False on failure
      """
      if self._bucketPath is not None and self.getId() is not None:
         self._setPAMFilename()
         self._setPAMSUMFilename()
         self._setGRIMFilename()
         self._setPresenceIndicesFilename()
         return True
      else:
         raise LMError(currargs='Unable to set filenames until ExperimentId and BucketId are set', 
                       lineno=self.getLineno())
         
# ................................................
   def createLayerShapefileFromMatrix(self, shpfilename, isPresenceAbsence=True):
      """
      Only partially tested, field creation is not holding
      """
      if isPresenceAbsence:
         matrix = self.getFullPAM()
      else:
         matrix = self.getFullGRIM()
      if matrix is None or self.shapegrid is None:
         return False
      else:
         self.shapegrid.copyData(self.shapegrid.getDLocation(), 
                                 targetDataLocation=shpfilename,
                                 format=self.shapegrid.dataFormat)
         ogr.RegisterAll()
         drv = ogr.GetDriverByName(self.shapegrid.dataFormat)
         try:
            shpDs = drv.Open(shpfilename, True)
         except Exception, e:
            raise LMError(['Invalid datasource %s' % shpfilename, str(e)])
         shpLyr = shpDs.GetLayer(0)

         mlyrCount = matrix.columnCount
         fldtype = matrix.ogrDataType
         # For each layer present, add a field/column to the shapefile
         for lyridx in range(mlyrCount):
            if (not self._layersPresent 
                or (self._layersPresent and self._layersPresent[lyridx])):
               # 8 character limit, must save fieldname
               fldname = 'lyr%s' % str(lyridx)
               fldDefn = ogr.FieldDefn(fldname, fldtype)
               if shpLyr.CreateField(fldDefn) != 0:
                  raise LMError('CreateField failed for %s in %s' 
                                % (fldname, shpfilename))             
         
#          # Debug only
#          featdef = shpLyr.GetLayerDefn()
#          featcount = shpLyr.GetFeatureCount()
#          for i in range(featdef.GetFieldCount()):
#             fld = featdef.GetFieldDefn(i)
#             print '%s  %d  %d' % (fld.name, fld.type, fld.precision)  
#          print  "done with diagnostic loop"
         # For each site/feature, fill with value from matrix
         currFeat = shpLyr.GetNextFeature()
         sitesKeys = sorted(self.getSitesPresent().keys())
         print "starting feature loop"         
         while currFeat is not None:
            #for lyridx in range(mlyrCount):
            for lyridx,exists in self._layersPresent.iteritems():
               if exists:
                  # add field to the layer
                  fldname = 'lyr%s' % str(lyridx)
                  siteidx = currFeat.GetFieldAsInteger(self.shapegrid.siteId)
                  #sitesKeys = sorted(self.getSitesPresent().keys())
                  realsiteidx = sitesKeys.index(siteidx)
                  currval = matrix.getValue(realsiteidx,lyridx)
                  # debug
                  currFeat.SetField(fldname, currval)
            # add feature to the layer
            shpLyr.SetFeature(currFeat)
            currFeat.Destroy()
            currFeat = shpLyr.GetNextFeature()
         #print 'Last siteidx %d' % siteidx
   
         # Closes and flushes to disk
         shpDs.Destroy()
         print('Closed/wrote dataset %s' % shpfilename)
         success = True
         try:
            retcode = subprocess.call(["shptree", "%s" % shpfilename])
            if retcode != 0: 
               print 'Unable to create shapetree index on %s' % shpfilename
         except Exception, e:
            print 'Unable to create shapetree index on %s: %s' % (shpfilename, 
                                                                  str(e))
      return success
      

# .............................................................................
# Private methods
# .............................................................................
# .............................................................................


         
# ...............................................
   def rollback(self, currtime):
      """
      @summary: Rollback processing following an addition to the set of 
                presenceAbsenceLayers.  This indicates one or more layers must
                be intersected into the fullPAM, the compressed PAM and SUM 
               (original PamSum) and all randomPamSums must be deleted.
      @param currtime: Time of status/stage modfication
      @todo: This removes the fullPAM and its dlocation.  In the future, just 
             intersect new layers, then add them to fullPAM
      """
      self.updateStatus(JobStatus.GENERAL, modTime=currtime, 
                        stage=JobStage.GENERAL)
      # @todo: add to fullPAM (update object and files) instead of starting over
      self.clearPAM()
      self.clearPresenceIndicesFile()
      # Delete original and random pamSums
      if self._pamSum is not None:
         self._pamSum.clear()
         self._pamSum = None
      for rps in self._randomPamSums:
         rps.clear()
      self._randomPamSums = []
      
# .............................................................................
# Read-0nly Properties
# .............................................................................
#    ## The run status of the current stage of this bucket (one view of an experiment)
#    status = property(_getStatus)
#    ## The last time the status was updated in modified julian date format
#    statusModTime = property(_getStatusModTime)
# #   ## The run stage of the bucket
#    stage = property(_getStage)
# #   ## The last time the stage was updated in modified julian date format
#    stageModTime = property(_getStageModTime)
#    
#    name = property(_getBucketName)

# .............................................................................
# Public methods
# .............................................................................

# ...............................................
   def populatePAMFromFile(self, pamfname=None):
      if pamfname is None:
         self._fullPAM = Matrix.initFromFile(self._pamFname, False)
      else:
         self._pamFname = pamfname
         self._fullPAM = Matrix.initFromFile(pamfname, False)
         
# ................................................
   def addPAMColumn(self, data, colIdx):
      self._fullPAM.addColumn(data, colIdx)
      
# ...............................................
   def populateGRIMFromFile(self, grimfname=None):
      if grimfname is None:
         self._fullGRIM = Matrix.initFromFile(self._grimFname, False)
      else:
         self._fullGRIM = Matrix.initFromFile(grimfname, False)

# ................................................
   def addGRIMColumn(self, data, colIdx):
      self._fullGRIM.addColumn(data, colIdx)

# ................................................
   def slicePAM(self, columnIdx): 
      if self.isCompressed:
         ids = self._pamSum.getColumnPresence(self._sitesPresent, 
                                              self._layersPresent,
                                              columnIdx)
         return ids
      else:
         raise LMError(currargs='Cannot get column before compression')

## ...............................................
#   def randomizePAM(self, method, pam, iterations=None):
#      if (method == RandomizeMethods.SWAP and  
#          pam.isCompressed):
#         if iterations is not None:
#            self._randomize(pam, iterations = iterations)
#         else:
#            raise LMError('iterations must be specified')
#      elif (method == RandomizeMethods.SPLOTCH 
#            and self.bucket.fullPAM is not None):
#         self._randomize(pam)
#      else:
#         raise LMError(currargs='%s method does not match compression state of PAM' 
#                       % str(method))         

# ...............................................
   def uncompressPAM(self, doDeleteCompressed=False):
      if self._fullPAM is None:
         self._fullPAM = self._pamSum.uncompressPAM(self._sitesPresent, 
                                                    self._layersPresent)
      if doDeleteCompressed:
         self._pamSum = None

# ...............................................
   def uncompressLayer(self, lyridx):
      if self._fullPAM is None:
         lyrdata = self._pamSum.pam.uncompressLayer(lyridx, self._sitesPresent, 
                                                    self._layersPresent)
      return lyrdata
    
# ...............................................
   def getSitesPresent(self):
      """
      @summary: Return an dictionary of site/row identifier keys and boolean values 
                for presence of at least one layer.
      """
      return self._sitesPresent
         
# ...............................................
   def setSitesPresent(self, sitesPresent):
      """
      @summary: Set the dictionary of site/row identifier keys and boolean values 
                for presence of at least one layer.
      """
      if not self._sitesPresent:
         self._sitesPresent = sitesPresent
         
# ...............................................
   def getLayersPresent(self):
      """
      @summary: Return an dictionary of layer/column identifier keys and boolean values 
                for presence in at least one site.
      """
      return self._layersPresent

# ...............................................
   def setLayersPresent(self, layersPresent):
      """
      @summary: Set the dictionary of layer/column identifier keys and boolean values 
                for presence of at least one site.
      @note: Do not reset this if it is already populated.  If necessary, 
             clearLayersPresent first.
      """
      if not self._layersPresent:
         self._layersPresent = layersPresent
         
# ...............................................
   def getCleanLayersPresent(self):
      """
      @summary: Return a new layersPresent, all layers = True
      """
      newLayersPresent = {}
      for idx in self._layersPresent.keys():
         newLayersPresent[idx] = True
      return newLayersPresent

# ...............................................
   def getCleanSitesPresent(self):
      """
      @summary: Reset existing layersPresent and sitesPresent to true for 
                all layers and sites. 
      """
      newSitesPresent = {}
      for idx in self._sitesPresent.keys():
         newSitesPresent[idx] = True
      return newSitesPresent

# ...............................................
   def resetPresenceIndices(self):
      """
      @summary: Reset existing layersPresent and sitesPresent to true for 
                all layers and sites. 
      """
      for idx in self._layersPresent.keys():
         self._layersPresent[idx] = True
      for idx in self._sitesPresent.keys():
         self._sitesPresent[idx] = True

# ...............................................
   def clearPresenceIndices(self):
      """
      @summary: Reset existing layersPresent and sitesPresent to true for 
                all layers and sites. 
      """
      self.clearLayersPresent()
      self.clearSitesPresent()

# ...............................................
   def clearLayersPresent(self):
      self._layersPresent = {}
      
# ...............................................
   def clearSitesPresent(self):
      self._sitesPresent = {}
      
# ...............................................
   def hydrateLayer(self, lyrIdx):
      pass
   
# ...............................................
   def _isCompressed(self):
      if self._pamSum is None:
         return False
      else:
         return True
   isCompressed = property(_isCompressed)
   
# ...............................................
   def createLocalMapFilename(self):
      """
      @summary: Set the filename for the bucket-based mapfile containing 
                shapegrid (now) and computed layers (todo) 
      @return: String identifying the filename mapfile
      """
      mapfname = None
      if self.getId() is not None:
         mapfname = self._earlJr.createFilename(LMFileType.OTHER_MAP,
                        radexpId=self.parentId, bucketId=self.getId(), 
                        pth=self._bucketPath, usr=self._userId, 
                        epsg=self._epsg)
      return mapfname

# ...............................................
   def createMapPrefix(self, lyrName=None):
      """
      @summary Gets the OGC service URL prefix for this object
      @return URL string representing a webservice request for maps of this object
      """
      mapfilename = self.createLocalMapFilename()
      mapname, ext = os.path.splitext(os.path.split(mapfilename)[1])
      mapprefix = self._earlJr.constructMapPrefix(ftype=LMFileType.BUCKET_MAP, 
                                                  mapname=mapname, lyrname=lyrName)
      return mapprefix
          
# ...............................................
   @property
   def mapPrefix(self):
      """
      @summary: Construct the endpoint of a Lifemapper WMS URL for 
                this object.
      """
      return self.createMapPrefix()
   
# ...............................................
   def _createPAMFilename(self):
      """
      @summary: Set the filename for the Uncompressed PAM based on the user and 
                experimentId
      @return: String identifying the filename of the Uncompressed PAM
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.PAM, 
                    radexpId=self.parentId, bucketId=self.getId(), 
                    pth=self._bucketPath, usr=self._userId, epsg=self._epsg)
      return fname
      
# ...............................................
   def _setPAMFilename(self, fname=None):
      """
      @summary: Set the filename for the Uncompressed PAM based on the user and 
                experimentId
      @return: String identifying the filename of the Uncompressed PAM
      @note: Does nothing if the fullPAM is None or the filename is already set
      """
      # self._pamFname parameter takes precedence
      if self._pamFname is None:
         self._pamFname = fname
      if self._pamFname is None:
         self._pamFname = self._createPAMFilename()
         
      if self._fullPAM is None:
         self._fullPAM = Matrix(None, dlocation=self._pamFname, 
                                isCompressed=False)
      elif self._fullPAM.getDLocation() is None:
         self._fullPAM.setDLocation(self._pamFname)
                  
# ...............................................
   def _getPAMFilename(self):
      """
      @summary: Return the filename for the Uncompressed PAM
      @return: String identifying the filename of the Uncompressed PAM
      """
      if self._fullPAM is not None:
         self._setPAMFilename()
         return self._fullPAM.getDLocation()
      else:
         return None

# ...............................................
   def _setCmpPamFilename(self, fname=None):
      """
      @summary: Set the filename for the Compressed PAMSUM based on the 
                user and experimentId
      """
      if self._pamSum is not None:
         self._pamSum._setPAMFilename(fname)
      
# ...............................................
   def _getCmpPamFilename(self):
      """
      @summary: Return the filename for the Compressed PAMSUM PAM
      @return: String identifying the filename of the Compressed PAMSUM PAM
      """
      if self._pamSum is not None:
         return self._pamSum.pamDLocation
      else:
         return None
      
# ...............................................
   def _setSumFilename(self, fname=None):
      """
      @summary: Set the filename for the Compressed PAMSUM based on the 
                user and experimentId
      """
      if self._pamSum is not None:
         self._pamSum._setSumFilename(fname)
      
# ...............................................
   def _getSumFilename(self):
      """
      @summary: Return the filename for the Compressed PAMSUM PAM
      @return: String identifying the filename of the Compressed PAMSUM PAM
      """
      if self._pamSum is not None:
         self._setSumFilename()
         return self._pamSum.sumDLocation
      else:
         return None
      
# ...............................................
   def _createGRIMFilename(self):
      """
      @summary: Set the filename for the GRIM (Geographic Reference Information
                Matrix) based on the user and experimentId
      @return: String identifying the filename of the GRIM
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.GRIM, 
                    radexpId=self.parentId, bucketId=self.getId(), 
                    pth=self._bucketPath, usr=self._userId, epsg=self._epsg)
      return fname
      
# ...............................................
   def _setGRIMFilename(self, fname=None):
      """
      @summary: Set the filename for the GRIM based on the user and 
                experimentId
      @return: String identifying the filename of the GRIM
      @note: Does nothing if the fullGRIM is None or the filename is already set
      """
      # self._grimFname parameter takes precedence
      if self._grimFname is None:
         self._grimFname = fname
      if self._fullGRIM is not None:
         if self._fullGRIM.getDLocation() is None:
            self._grimFname = self._createGRIMFilename()
            self._fullGRIM.setDLocation(self._grimFname)
               
# ...............................................
   def _getGRIMFilename(self):
      """
      @summary: Return the filename for the Uncompressed PAM
      @return: String identifying the filename of the Uncompressed PAM
      """
      if self._fullGRIM is not None:
         self._setGRIMFilename()
         return self._fullGRIM.getDLocation()
      else:
         return None

# ...............................................
   def _createPresenceIndicesFilename(self):
      """
      @summary: Create the filename for the SitesPresent and LayersPresent 
                dictionaries based on the user, bucketid, and experimentId
      @return: String identifying the filename of the sitesLayerPresent file
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.PRESENCE_INDICES, 
                    radexpId=self.parentId, bucketId=self.getId(), 
                    pth=self._bucketPath, usr=self._userId, epsg=self._epsg)
      return fname
   
# ...............................................
   def _setPresenceIndicesFilename(self, fname=None):
      """
      @summary: Set the filename for the SitesPresent and LayersPresent 
                dictionaries based on the user, bucketid, and experimentId
      @return: String identifying the filename of the sitesLayerPresent file
      """
      if self._presidxFname is None:
         if fname is None:
            fname = self._createPresenceIndicesFilename()
         self._presidxFname = fname
      
# ...............................................
   def _getPresenceIndicesFilename(self):
      """
      @summary: Return the filename for the pickled sitesPresent and 
               layersPresent dictionaries
      @return: String identifying the filename containing the sitesPresent and 
               layersPresent
      """
      if self._presidxFname is None:
         self._setPresenceIndicesFilename()
      return self._presidxFname
      
# ...............................................
   def clear(self):
      self.clearGRIM()
      self.clearPAM()
      self.clearPresenceIndices()

# ...............................................
   def clearPresenceIndicesFile(self):
      if self._presidxFname is None:
         self._setPresenceIndicesFilename()
      success, msg = self._deleteFile(self._presidxFname)

# ...............................................
   def clearPAM(self):
      self.clearPresenceIndicesFile()
      if self._pamFname is None:
         self._setPAMFilename()
      success, msg = self._deleteFile(self._pamFname)
      self._fullPAM = None      

# ...............................................
   def clearGRIM(self):
      self.clearPresenceIndicesFile()
      if self._grimFname is None:
         self._setGRIMFilename()
      success, msg = self._deleteFile(self._grimFname)
      self._fullGRIM = None      

# ...............................................
   pamDLocation = property(_getPAMFilename)
   cmpPamDLocation = property(_getCmpPamFilename)
   sumDLocation = property(_getSumFilename)
   grimDLocation = property(_getGRIMFilename)
   indicesDLocation = property(_getPresenceIndicesFilename)
   
#    @property
#    def indicesPresent(self):
#       indices = {'sitesPresent': self._sitesPresent, 
#                  'layersPresent': self._layersPresent}
#       return indices
   
# .............................................................................
   def writePresenceIndices(self):
      """
      @summary: Pickle _sitesPresent and _layersPresent into _presidxFname
      """
      self._setPresenceIndicesFilename()
      if self._presidxFname is None:
         raise LMError('Unable to set Indices Filename')
      
      self._readyFilename(self._presidxFname, overwrite=True)
      
      indices = {'sitesPresent': self._sitesPresent, 
                 'layersPresent': self._layersPresent}
      print('Writing Presence Indices %s' % self._presidxFname)
      try:
         f = open(self._presidxFname, 'wb')
         # Pickle the list using the highest protocol available.
         pickle.dump(indices, f, protocol=pickle.HIGHEST_PROTOCOL)
      except Exception, e:
         raise LMError('Error pickling file %s' % self._presidxFname, str(e))
      finally:
         f.close()
      
# .............................................................................
   def getAllPresenceIndices(self):
      """
      @summary: Pickle _sitesPresent and _layersPresent into _presidxFname
      """
      allIndices = {self.getId(): {'sitesPresent': self._sitesPresent, 
                                   'layersPresent': self._layersPresent}}
      for rps in self.getRandomPamSums():
         if rps.randomMethod == RandomizeMethods.SPLOTCH:
            allIndices[rps.getId()] = {'sitesPresent': 
                                       rps.getSplotchSitesPresent()}
      return allIndices
            
# ...............................................
   def _readPresenceIndices(self):
      """
      @summary: Unpickle _presidxFname into sitesPresent and layersPresent
      """
      # Clear any existing dictionaries
      self._sitesPresent = {}
      self._layersPresent = {}
      if self._presidxFname is not None and os.path.isfile(self._presidxFname):
         try: 
            f = open(self._presidxFname,'rb')         
            indices = pickle.load(f)
         except Exception, e:
            raise LMError(currargs='Failed to load %s into indices' % 
                          self._presidxFname, prevargs=str(e))
         
         self._sitesPresent = indices['sitesPresent']
         self._layersPresent = indices['layersPresent']

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
            
# .............................................................................
# Read-0nly Properties
# .............................................................................
   @property
   def status(self):
      """
      @summary Gets the run status of the RADExperiment for the current stage
      @return The run status of the RADExperiment for the current stage
      """
      return self._status
   
   @property
   def statusModTime(self):
      """
      @summary Gets the last time the status was modified for the current stage
      @return Status modification time in modified julian date format
      """
      return self._statusmodtime

# ...............................................
   @property
   def stage(self):
      """
      @summary Gets the stage of the RADExperiment
      @return The stage of the RADExperiment
      """
      return self._stage

   @property
   def stageModTime(self):
      """
      @summary Gets the last time the stage was modified
      @return Stage modification time in modified julian date format
      """
      return self._stagemodtime
   
# ...............................................
   @property
   def keywords(self):
      """
      @summary Gets the keywords of the LayerSet
      @return List of keywords describing the LayerSet
      """
      return self._keywords   

# ...............................................
   @property
   def name(self):
      name = None
      if self.shapegrid is not None:
         name = self.shapegrid.name
      return name

# ...............................................
   @property
   def outputPath(self):
      return self._bucketPath

# ...............................................
   @property
   def epsgcode(self):
      return self._epsg

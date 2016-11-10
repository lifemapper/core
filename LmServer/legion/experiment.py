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
import json
import mx.DateTime
import os
from osgeo import ogr
import subprocess
from types import StringType, UnicodeType

from LmCommon.common.lmconstants import JobStage, JobStatus, MatrixType
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject, ProcessObject
from LmServer.common.lmconstants import LMFileType, LMServiceType, LMServiceModule
from LmServer.rad.matrix import Matrix                                  
from LmServer.rad.pamvim import PamSum

# .............................................................................
class BigExperiment(ServiceObject, ProcessObject):
   """
   The BigExperiment class contains all of the information for one view (extent and 
   resolution) of a RAD experiment.  
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, metadata={}, 
                shapegrid=None, siteIndices=None, epsgcode=None, 
                pam=None, grim=None, biogeo=None, tree=None,
                status=None, statusModTime=None, 
                userId=None, expId=None, metadataUrl=None):
      """
      @summary Constructor for the BigExperiment class
      @param shapegrid: Vector layer with polygons representing geographic sites.
      @param siteIndices: A dictionary with keys the unique/record identifiers 
             and values the x, y coordinates of the sites in a ShapeGrid or PAM
      @param epsgcode: The EPSG code of the spatial reference system of data.
      @param pam: A Presence Absence Matrix (MatrixType.PAM)
      @param grim: A Matrix of Environmental Values (MatrixType.GRIM)
      @param biogeo: A Matrix of Biogeographic Hypotheses (MatrixType.BIOGEO_HYPOTHESES)
      @param tree: A Tree with taxa matching those in the PAM 
      @param status:  The run status of the current stage
      @param statusModTime: The last time that the status was modified
      @param userId: id for the owner of these data
      @param expId: database id of the RADExperiment containing this Bucket 
      """
      ServiceObject.__init__(self, userId, expId, None, statusModTime,
            LMServiceType.RAD_EXPERIMENTS, moduleType=LMServiceModule.RAD,
            metadataUrl=metadataUrl)
      ProcessObject.__init__(self, objId=expId, 
                             status=status,  statusModTime=statusModTime) 
      self.metadata = {}
      self.loadMetadata(metadata)
      self.shapegrid = shapegrid
      self.siteIndices = None
      self.setIndices(siteIndices, doRead=False)
      self._setEPSG(epsgcode)

      self.setMatrix(MatrixType.PAM, mtxFileOrObj=pam)
      self.setMatrix(MatrixType.GRIM, mtxFileOrObj=grim)
      self.setMatrix(MatrixType.BIOGEO_HYPOTHESES, mtxFileOrObj=biogeo)
      
      if shapegrid is not None:
         if shapegrid.getUserId() is None:
            shapegrid.setUserId(self._userId)
         self._epsg = shapegrid.epsgcode
      self.shapegrid = shapegrid
      
               
            
# ...............................................
   @classmethod
   def initFromFiles(cls):
      pass

# .............................................................................
# Properties
# .............................................................................
   def _setEPSG(self, epsg=None):
      if self._shapegrid is not None:
         self._epsg = self._shapegrid.epsgcode
      else:
         self._epsg = epsg

   def _getEPSG(self):
      if self._epsg is None:
         self._setEPSG()
      return self._epsg

   epsgcode = property(_getEPSG, _setEPSG)

# .............................................................................
# Methods
# .............................................................................

# ...............................................
   def setId(self, expid):
      """
      Overrides ServiceObject.setId.  
      @note: ExperimentId should always be set before this is called.
      """
      ServiceObject.setId(self, expid)
      self.setPath()

# ...............................................
   def setPath(self):
      if self._expPath is None:
         if (self._userId is not None and self.getId() is not None):
            self._bucketPath = self._earlJr.createDataPath(self._userId, 
                               epsg=self._getEPSG(), radexpId=self.getId())
                     
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
   
# ...............................................
   def setIndices(self, indicesFileOrObj=None, doRead=True):
      """
      @summary Fill the siteIndices from dictionary or existing file
      """
      indices = None
      if indicesFileOrObj is not None:
         if isinstance(indicesFileOrObj, StringType) and os.path.exists(indicesFileOrObj):
            if doRead:
               try:
                  f = open(indicesFileOrObj, 'r')
                  indices = f.read()
               except:
                  raise LMError('Failed to read indices {}'.format(indicesFileOrObj))
               finally:
                  f.close()
            else:
               indices = indicesFileOrObj
         elif isinstance(indicesFileOrObj, dict):
            indices = indicesFileOrObj
      self.siteIndices = indices

# ...............................................
   def setMatrix(self, mtxType, mtxFileOrObj=None, doRead=False):
      """
      @summary Fill a Matrix object from Matrix or existing file
      """
      mtx = None
      if mtxFileOrObj is not None:
         if isinstance(mtxFileOrObj, StringType) and os.path.exists(mtxFileOrObj):
            mtx = Matrix(matrixType=mtxType, dlocation=mtxFileOrObj)
            if doRead:
               mtx.readData()            
         elif isinstance(mtxFileOrObj, Matrix):
            mtx = mtxFileOrObj
            
      if mtxType == MatrixType.PAM:
         self._pam = mtx
      elif mtxType == MatrixType.GRIM:
         self._grim = mtx
      elif mtxType == MatrixType.BIOGEO_HYPOTHESES:
         self._biogeo = mtx
                  
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
      @summary: Rollback processing following a change to the layers.  
      @param currtime: Time of status modfication
      """
      pass
#       self.updateStatus(JobStatus.GENERAL, modTime=currtime)

# .............................................................................
# Public methods
# .............................................................................
         
# ................................................
   def addPAMColumn(self, data, colIdx):
      self._fullPAM.addColumn(data, colIdx)
      
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
      @summary Sets the keywords of the BigExperiment
      @param keywords: List or comma-delimited string of keywords that will be 
                       associated with the BigExperiment
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

"""
@summary Module that contains the PamSum class
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
import mx.DateTime
import numpy
import os
from osgeo import ogr

from LmCommon.common.lmconstants import RandomizeMethods, PAMSUMS_SERVICE, \
                                  JobStage, JobStatus
from LmServer.base.layer import Vector
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject, ProcessObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule, LMFileType
from LmServer.rad.matrix import Matrix


# .............................................................................
class PamObject(object):
   """
   Temporary class for exposing PAM object with columnCount without rest of
   object.
   """   
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, columnCount=-1): 
      self.columnCount = columnCount
   
# .............................................................................
class PamSum(ServiceObject, ProcessObject):
   """
   The PamSum class contains 1 (or 2) Matrix objects and a Dictionary: 
     1) Presence/Absence Matrix (PAM) and
       The PAM contains all sites (geographic areas from a ShapeGrid) on one 
       axis (usually rows) intersected with the Presence/Absence for a set of 
       species PALayers on the other axis (usually columns).  This PAM is 
       compressed - it has all zero rows and zero columns removed.
     2) Summary (SUM)
       The SUM is a dictionary of summary statistics - some individual value, 
        some vectors
       calculated variables and indices along the other axis.
     3) Splotch Matrix (optional)
       The splotchPam is an interim step in a matrix randomized with the 
       Splotch method.  It is randomized and uncompressed.  We will keep it 
       until it is clear that it is unnecessary.  
   PamSum objects always work on PAMs that have been compressed - rows and 
   columns of all zeros (a site that has no species present, or a species that
   has no presence in any sites) have been removed.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, presenceAbsenceMatrix, 
                summary=None, pamFilename=None, sumFilename=None, 
                splotchFilename=None,
                status=None, statusModTime=None, 
                stage=None, stageModTime=None, createTime=None, 
                bucketPath=None, userId=None, pamSumId=None, 
                bucketId=None, expId=None, epsgcode=None, 
                metadataUrl=None, parentMetadataUrl=None,
                # random-only attributes 
                randomMethod=None, randomParameters={}, splotchPam=None,
                splotchSitesFilename=None): 
      """
      @summary Constructor for the PamSum class
      @param presenceAbsenceMatrix: A matrix with rows corresponding to sites 
               and columns corresponding to organism presence/absence values.
               The PAM is compressed here.
      @param randomMethod: SWAP or SPLOTCH
      @param randomParameters: a dictionary of key value pairs relating to the
               current random method.
      @param splotchPam: An uncompressed PAM randomized with the Splotch 
             (aka Dye Dispersion) method.
      @param splotchSitesFilename: File containing the pickled dictionary of 
             sitesPresent
      @param summary: A dictionary with values corresponding to
             individual value or vector indices and calculations.
      @param dlocation: Location of the pickled data file containing this object 
      @param status:  The run status of the current stage (submitted/waiting/complete)
      @param statusModTime: The last time that the status was modified
      @param stage: The processing stage of this bucket
      @param stageModTime: The last time that the stage was modified
      @param createTime: Time and date of creation (GMT in Modified Julian Day 
                         format)
      @param userId: Database Id of the User who owns these data 
      @param pamSumId: Database Id of this object record
      """
      ServiceObject.__init__(self,  userId, pamSumId, createTime, statusModTime,
                             LMServiceType.PAMSUMS, moduleType=LMServiceModule.RAD,
                             metadataUrl=metadataUrl, 
                             parentMetadataUrl=parentMetadataUrl)
      ProcessObject.__init__(self, objId=pamSumId, parentId=bucketId, 
                status=status, statusModTime=statusModTime, stage=stage, 
                stageModTime=stageModTime) 
      self._pam = presenceAbsenceMatrix
      self._sum = summary
      self._pamFname = None
      self._sumFname = None
      self._splotchPAM = None
      self._bucketPath = bucketPath
#       self._bucketId = bucketId
      self._experimentId = expId
      self._epsg = epsgcode
      self._setPAMFilename(pamFilename)
      self._setSumFilename(sumFilename)
      self._setSplotchPAMFilename(splotchFilename)
      self._setRandomMethod(randomMethod)
      self._setRandomParameters(randomParameters)
      self.setSplotchPAM(splotchPam)
      self._splotchSitesPresent = None
      self._splotchSitesFname = None
      self._setSplotchSitesFilename(splotchSitesFilename)
      self._readSplotchSites()
   
# ...............................................
   @classmethod
   def initAndFillFromFile(cls, pamFilename, sumFilename=None, 
                           status=None, statusModTime=None, 
                           stage=None, stageModTime=None, createTime=None, 
                           bucketPath=None, bucketId=None, expId=None,
                           epsgcode=None, 
                           metadataUrl=None, parentMetadataUrl=None,
                           userId=None, pamSumId=None, 
                           randomMethod=None, randomParameters={}, 
                           splotchFilename=None,splotchSitesFilename=None):
      """
      @summary Constructor for the PamSum class
      @return: An empty PamSum with two Matrix objects of the given size
      """
      cmpPam = None
      sum = None
      splotch = None
      if pamFilename is not None and os.path.exists(pamFilename):
         cmpPam = Matrix.initFromFile(pamFilename, isCompressed=True)                        
         if sumFilename is not None and os.path.exists(sumFilename):
            try: 
               f = open(sumFilename,'rb')         
               sum = pickle.load(f)
            except Exception, e:
               raise LMError('File %s is not readable by pickle' % sumFilename)
            finally:
               f.close()
      if splotchFilename is not None and os.path.exists(splotchFilename):
         splotch = Matrix.initFromFile(splotchFilename, isCompressed=False)

      ps = PamSum(cmpPam, summary=sum, pamFilename=pamFilename,
                  sumFilename=sumFilename, 
                  status=status, statusModTime=statusModTime, 
                  stage=stage, stageModTime=stageModTime, 
                  createTime=createTime, bucketId=bucketId, expId=expId,
                  epsgcode=epsgcode, 
                  metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl, 
                  pamSumId=pamSumId, 
                  randomMethod=randomMethod, randomParameters=randomParameters,
                  userId=userId, bucketPath=bucketPath, splotchPam=splotch,
                  splotchSitesFilename=splotchSitesFilename)  
      return ps

# ...............................................
   @property
   def epsgcode(self):
      return self._epsg

# .............................................................................
# Public methods
# ..............................................................................
   def getColumnPresence(self, sitesPresent, layersPresent, columnIdx):  
      ids = self._pam.getColumnPresence(sitesPresent, layersPresent, columnIdx)
      return ids 
# ..............................................................................
   def uncompressPAM(self, sitesPresent, layersPresent):
      fullpam = self._pam.createUncompressed(sitesPresent, layersPresent)
      return fullpam
# ...............................................
   def createLayerShapefileFromSum(self, bucket, shpfilename):
      # needs sitesPresent
      # consider sending it the bucket instead, and using indicesDLocation
      # to get the sitepresent pickle
      
      fieldNames = {'speciesRichness-perSite' : 'specrich',      
                    'MeanProportionalRangeSize': 'avgpropRaS',
                    'ProportionalSpeciesDiversity' : 'propspecDi',
                    'Per-siteRangeSizeofaLocality' : 'RaSLoc'
                    }
      # See if we should attach tree stats
      #if self._sum['sites']['MNTD'] is not None:
      fieldNames['MNTD'] = 'mntd'
      fieldNames['PearsonsOfTDandSitesShared'] ='pearsTdSs'
      fieldNames['AverageTaxonDistance'] = 'avgTd'
                    
      if self.randomMethod == RandomizeMethods.NOT_RANDOM or \
      self.randomMethod == RandomizeMethods.SWAP:
         sitesPresent = bucket.getSitesPresent()
      elif self.randomMethod == RandomizeMethods.SPLOTCH:
         sitesPresent = self._splotchSitesPresent
      bucket.shapegrid.copyData(bucket.shapegrid.getDLocation(), 
                                targetDataLocation=shpfilename,
                                format=bucket.shapegrid.dataFormat)
      ogr.RegisterAll()
      drv = ogr.GetDriverByName(bucket.shapegrid.dataFormat)
      try:
         shpDs = drv.Open(shpfilename, True)
      except Exception, e:
         raise LMError(['Invalid datasource %s' % shpfilename, str(e)])
      shpLyr = shpDs.GetLayer(0)
      
      sitesDict = self._sum['sites']
      
      statKeys = [k for k in sitesDict.keys() if sitesDict[k] is not None]
      
      for key in statKeys:
         fldname = fieldNames[key]
         fldtype = ogr.OFTReal
         fldDefn = ogr.FieldDefn(fldname, fldtype)
         if shpLyr.CreateField(fldDefn) != 0:
            raise LMError('CreateField failed for %s in %s' 
                          % (fldname, shpfilename))
      sortedSites = sorted([x[0] for x in sitesPresent.iteritems() if x[1]])
      currFeat = shpLyr.GetNextFeature()         
      while currFeat is not None:
         siteId = currFeat.GetFieldAsInteger(bucket.shapegrid.siteId)
         print siteId
         if sitesPresent[siteId]:
            print "True"
            for statname in statKeys:
               currVector = sitesDict[statname]
               print statname
               currval = currVector[sortedSites.index(siteId)]
               currFeat.SetField(fieldNames[statname], currval)
            # SetFeature used for existing feature based on its unique FID
            shpLyr.SetFeature(currFeat)
            currFeat.Destroy()
         currFeat = shpLyr.GetNextFeature()
      shpDs.Destroy()
      print('Closed/wrote dataset %s' % shpfilename)
      success = True
      return success
# ...............................................
#  def addPresenceAbsenceLayer(self, lyrdata, rows, colIndx):
#     # check to see if a sum exists
#     # if a sum exists then we want to delete it
#     self._pam.addColumn(lyrdata, rows, colIndx)
#
# ...............................................
   def _createSplotchPAMFilename(self):
      """
      @summary: Set the filename for the SplotchPAM  based on the user and 
                experimentId
      @return: String identifying the filename of the SplotchPAM
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.SPLOTCH_PAM, 
                    radexpId=self._experimentId, bucketId=self.parentId, 
                    pamsumId=self.getId(), pth=self._bucketPath, 
                    usr=self._userId, epsg=self._epsg)
      return fname
      
# ...............................................
   def _setSplotchPAMFilename(self, fname=None):
      """
      @summary: Set the filename for the Uncompressed PAM based on the user and 
                experimentId
      @return: String identifying the filename of the Uncompressed PAM
      @note: Does nothing if the fullPAM is None or the filename is already set
      """
      if self._splotchPAM is not None:
         if self._splotchPAM.getDLocation() is None:
            if fname is None:
               fname = self._createSplotchPAMFilename()
            self._splotchPAM.setDLocation(fname)
            
# ...............................................
   def readSplotchPAM(self):
      """
      @summary: Return the filename for the Splotch sitesPresent Pickle
      @return: String of the Splotch sitesPresent Pickle filename
      """
      if self._splotchPAM is not None:
         if self._splotchPAM.getDLocation() is not None:
            self._splotchPAM.readData()
            
# ...............................................
   def _createSplotchSitesFilename(self):
      """
      @summary: Set the filename for the SplotchPAM  based on the user and 
                experimentId
      @return: String identifying the filename of the SplotchPAM
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.SPLOTCH_SITES, 
                    radexpId=self._experimentId, bucketId=self.parentId, 
                    pamsumId=self.getId(), pth=self._bucketPath, 
                    usr=self._userId, epsg=self._epsg)
      return fname
      
# ...............................................
   def _setSplotchSitesFilename(self, fname=None):
      """
      @summary: Set the filename for the (Splotch compressed pam) SitesPresent  
                dictionary based on the user, bucketid, and experimentId
      @return: String identifying the filename of the sitesLayerPresent file
      """
      if self._splotchSitesFname is None:
         if fname is None:
            fname = self._createSplotchSitesFilename()
         self._splotchSitesFname = fname            
      
# ...............................................
   def _getSplotchSitesFilename(self):
      """
      @summary: Return the filename for the Splotch sitesPresent Pickle
      @return: String of the Splotch sitesPresent Pickle filename
      """
      if self._splotchSitesPresent is not None:
         self._setSplotchSitesFilename()
         return self._splotchSitesFname
      else:
         return None

   splotchSitesDLocation = property(_getSplotchSitesFilename)
   
# ...............................................
   def _createSumShapeFilename(self):
      """
      @summary: Set the filename for the SplotchPAM  based on the user and 
                experimentId
      @return: String identifying the filename of the SplotchPAM
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.SUM_SHAPE,
                    radexpId=self._experimentId, bucketId=self.parentId, 
                    pamsumId=self.getId(), pth=self._bucketPath, 
                    usr=self._userId, epsg=self._epsg)
      return fname
      
# ...............................................
   def _setSumShapeFilename(self, fname=None):
      """
      @summary: Set the filename for the (Splotch compressed pam) SitesPresent  
                dictionary based on the user, bucketid, and experimentId
      @return: String identifying the filename of the sitesLayerPresent file
      """
      if self._sumShapeFname is None:
         if fname is None:
            fname = self._createSumShapeFilename()
         self._sumShapeFname = fname            
      
# ...............................................
   def _getSumShapeFilename(self):
      """
      @summary: Sets and returns the filename for the Splotch sitesPresent Pickle
      @return: String of the Splotch sitesPresent Pickle filename
      """
      if self._sumShapeFname is None:
         self._setSumShapeFilename()
      return self._sumShapeFname

   sumShapeDLocation = property(_getSumShapeFilename)

# ...............................................
   def getSplotchSitesPresent(self):
      """
      @summary: Return an dictionary of site/row identifier keys and boolean values 
                for presence of at least one layer.
      """
      return self._splotchSitesPresent
         
# ...............................................
   def setSplotchSitesPresent(self, splotchSitesPresent):
      """
      @summary: Set the dictionary of site/row identifier keys and boolean values 
                for presence of at least one layer.
      """
      if not self._splotchSitesPresent:
         self._splotchSitesPresent = splotchSitesPresent
         
# ...............................................
   def clearSplotchSitesPresent(self):
      self._splotchSitesPresent = {}
   
# ...............................................
   def _getSplotchPAMFilename(self):
      """
      @summary: Return the filename for the Uncompressed PAM
      @return: String identifying the filename of the Uncompressed PAM
      """
      if self._splotchPAM is not None:
         self._setSplotchPAMFilename()
         return self._splotchPAM.getDLocation()
      else:
         return None

   splotchPamDLocation = property(_getSplotchPAMFilename)

# ...............................................
   def _createPAMFilename(self):
      """
      @summary: Set the filename for the compressed PAM  based on the pamsumId
      @return: String identifying the filename of the PAM
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.COMPRESSED_PAM, 
                    radexpId=self._experimentId, bucketId=self.parentId, 
                    pamsumId=self.getId(), pth=self._bucketPath, 
                    usr=self._userId, epsg=self._epsg)
      return fname

# ...............................................
   def _setPAMFilename(self, fname=None):
      """
      @summary: Set the filename for the Compressed PAM based on the pamsumId
      @return: String identifying the filename of the Uncompressed PAM
      """
      if self._pam is None:
         if fname is not None:
            self._pamFname = fname
      elif self._pam.getDLocation() is None:
         if fname is None:
            fname = self._createPAMFilename()
         self._pam.setDLocation(fname)
         self._pamFname = fname
      
# ...............................................
   def _getPAMFilename(self):
      """
      @summary: Return the filename for the Uncompressed PAM
      @return: String identifying the filename of the Uncompressed PAM
      """
      if self._pam is not None:
         return self._pam.getDLocation()
      else:
         return None

   pamDLocation = property(_getPAMFilename)

# ...............................................
   def getPam(self):
      return self._pam

   def setPam(self, pam):
      """
      @param pam: Matrix object that contains a compressed PAM
      """
      self._pam = pam
      
   @property
   def pam(self):
      colCount = None
      if self._pam is not None:
         colCount = self._pam.columnCount
      return PamObject(columnCount=colCount)

# ...............................................
# ...............................................
   def getSum(self):
      return self._sum

   def setSum(self, sum):
      """
      @param sum: Dictionary of individual and vector statistics that 
                  represents biodiversity measures of the compressed PAM
      """
      self._sum = sum
      
# ...............................................
   def readPAM(self, pamFilename=None):
      """
      @summary Fill the PAM object from existing file
      @postcondition: The compressed PAM object will be present
      """
      # Override with incoming filename
      if pamFilename is not None:
         self._pamFname = pamFilename 
#       elif self._pamFname is not None:
#          fname = pamFilename
      if self._pam is None:
         self._pam = Matrix.initFromFile(self._pamFname, False)
      elif self._pamFname is not None:
         self._pam.readData(filename=self._pamFname)
      elif self._pam.getDLocation() is not None:
         self._pam.readData()
      else:
         raise LMError('No compressed PAM filename to read')
      
# ...............................................
   def readSUM(self, fullSUMFilename=None):
      """
      @summary Fill the PAM object from existing file
      @postcondition: The full PAM object will be present
      """
      if fullSUMFilename is not None:
         self._sumFname = fullSUMFilename
      elif self._sumFname is None:
         raise LMError('No SUM filename to read')
      if os.path.exists(self._sumFname):
         try: 
            f = open(self._sumFname,'rb')         
            self._sum = pickle.load(f)
         except Exception, e:
            raise LMError('File %s is not readable by pickle' % self._sumFname)
         finally:
            f.close()
            
# ...............................................
   def _createSumFilename(self):
      """
      @summary: Set the filename for the SplotchPAM  based on the user and 
                experimentId
      @return: String identifying the filename of the SplotchPAM
      """
      fname = None
      if self.getId() is not None:
         fname = self._earlJr.createFilename(LMFileType.SUM_CALCS, 
                    radexpId=self._experimentId, bucketId=self.parentId, 
                    pamsumId=self.getId(), pth=self._bucketPath, 
                    usr=self._userId, epsg=self._epsg)
      return fname

# ...............................................
   def _getSumFilename(self):
      """
      @summary: Return the filename of the pickled SUM
      @return: String identifying the filename of the SUM 
      """
      self._setSumFilename()
      return self._sumFname
   
   def _setSumFilename(self, fname=None):
      """
      @summary: Set the filename of the SUM
      @param fname: String identifying the filename of the pickled SUM dictionary
      @note: this will be set by the enclosing bucket
      """
      if self._sumFname is None:
         if fname is None:
            fname = self._createSumFilename()
         self._sumFname = fname
         
   sumDLocation = property(_getSumFilename)
   
# .............................................................................
   def writeSplotchSites(self, overwrite=True):
      """
      @summary: Pickle _splotchSitesPresent 
      @TODO: Change this to a NetCDF file? 
      """
      self._setSplotchSitesFilename()
      if self._splotchSitesFname is None:
         raise LMError('Unable to set SplotchSites Filename')
      
      self._readyFilename(self._splotchSitesFname, overwrite=overwrite)
      print('Writing splotch sites present %s' % self._splotchSitesFname)
      try:
         f = open(self._splotchSitesFname, 'wb')
         # Pickle the list using the highest protocol available.
         pickle.dump(self._splotchSitesPresent, f, protocol=pickle.HIGHEST_PROTOCOL)
      except Exception, e:
         raise LMError('Error pickling file %s' % self._splotchSitesFname, str(e))
      finally:
         f.close()
            
# ...............................................
   def _readSplotchSites(self):
      """
      @summary: Unpickle _splotchSitesFname into splotchSitesPresent
      """
      # Clear any existing dictionaries
      self._splotchSitesPresent = {}
      if self._splotchSitesFname is not None and os.path.isfile(self._splotchSitesFname):
         try: 
            f = open(self._splotchSitesFname,'rb')         
            self._splotchSitesPresent = pickle.load(f)
         except Exception, e:
            raise LMError(currargs='Failed to load %s into splotchSitesPresent' % 
                          self._splotchSitesFname, prevargs=str(e))
         finally:
            f.close()

# ..............................................................................
   def writePam(self, filename=None, overwrite=True):
      """
      @summary: Write the compressed PAM matrix out to a file
      @param filename: The location on disk to write the file
      """
      self._setPAMFilename(filename)
      print('Writing compressed PAM ...')
      self._pam.write(overwrite=overwrite)
      
# ..............................................................................
   def subsetSum(self, newsitesPresent, origsitesPresent, newlayersPresent,
                 origlayersPresent):
      """
      @summary: returns a subsetted sum given a sitespresent
      and layerspresent
      """
      newSum = self._sum.copy()
      cmpOrigSites = sorted([key for key in origsitesPresent.keys() if origsitesPresent[key]])
      for key in newSum['sites'].keys():
         statVect = newSum['sites'][key]
         newStatVect = []
         for idx, oldShpId in enumerate(cmpOrigSites):
            if oldShpId in newsitesPresent.keys():
               newStatVect.append(statVect[idx])
         newSum['sites'][key] = newStatVect
      cmpOrigSpecies = sorted([key for key in origlayersPresent.keys() if origlayersPresent[key]])
      for key in newSum['species'].keys():
         statVect = newSum['species'][key]
         newStatVect = []
         for idx, oldLyrId in enumerate(cmpOrigSpecies):
            if oldLyrId in newlayersPresent.keys():
               newStatVect.append(statVect[idx])
         newSum['species'][key] = newStatVect
      return newSum                        
      
# ..............................................................................
   def writeSum(self, filename=None, overwrite=True):
      """
      @summary: Write the compressed SUM matrix out to a file
      @param filename: The location on disk to write the file
      """
      if self._sum:
         self._setSumFilename(filename)
         if self._sumFname is None:
            raise LMError('Unable to set Sum Filename')
         
         self._readyFilename(self._sumFname, overwrite=overwrite)
         print 'Writing %s' % self._sumFname
         try:
            f = open(self._sumFname, 'wb')
            # Pickle the list using the highest protocol available.
            pickle.dump(self._sum, f, protocol=pickle.HIGHEST_PROTOCOL)
         except Exception, e:
            raise LMError('Error pickling file %s' % self._sumFname, str(e))
         finally:
            f.close()
            
# ..............................................................................
   def writeSummaryShapefileFromZipdata(self, zipdata, epsgcode, overwrite=True):
      """
      @summary: Write the Summary shapefile to disk
      @param filename: The location on disk to write the shapefile
      """
      shpSum = Vector(epsgcode=epsgcode, ogrType=ogr.wkbPolygon, 
                      ogrFormat='ESRI Shapefile')
      shpSum.writeTempFromZippedShapefile(zipdata, overwrite=overwrite)
      fname = self._getSumShapeFilename()
      shpSum.setDLocation(fname)
      try:
         shpSum.writeShapefile(overwrite=overwrite)
      except Exception, e:
         raise LMError('Error writing summary shapefile %s' % str(fname))
            
# ..............................................................................
   def writeSplotch(self, filename=None, overwrite=True):
      """
      @summary: Write the uncompressed Splotch PAM matrix out to a file
      @param filename: The location on disk to write the file
      """
      self._setSplotchPAMFilename(filename)
      print('Writing splotch PAM ...')
      self._splotchPAM.write(overwrite=overwrite)

# .............................................................................
# Private methods
# .............................................................................
# .............................................................................
   def _getStatus(self):
      """
      @summary Gets the run status of the RADExperiment for the current stage
      @return The run status of the RADExperiment for the current stage
      """
      return self._status
   
   def _getStatusModTime(self):
      """
      @summary Gets the last time the status was modified for the current stage
      @return Status modification time in modified julian date format
      """
      return self._statusmodtime

# ...............................................
   def _getStage(self):
      """
      @summary Gets the stage of the RADExperiment
      @return The stage of the RADExperiment
      """
      return self._stage

   def _getStageModTime(self):
      """
      @summary Gets the last time the stage was modified
      @return Stage modification time in modified julian date format
      """
      return self._stagemodtime

# ...............................................
   def clear(self):
      if self._pam is not None:
         self._pam.clear()
         if self._sum is not None:
            self._deleteFile(self._sumFname, deleteDir=True)
            self._sum = None
         if self._splotchPAM is not None:
            self._splotchPAM.clear()
            success, msg = self._deleteFile(self._splotchSitesFname, 
                                            deleteDir=True)

# ...............................................
   def rollback(self, currtime):
      self.clear()
      self.updateStatus(JobStatus.GENERAL, modTime=currtime, 
                        stage=JobStage.GENERAL)

# ...............................................
   def _getRandomMethod(self):
      return self._randomMethod
      
   def _setRandomMethod(self, randomMethod):
      if randomMethod is None:
         self._randomMethod = RandomizeMethods.NOT_RANDOM
      elif (randomMethod == RandomizeMethods.NOT_RANDOM or
            randomMethod == RandomizeMethods.SWAP or 
            randomMethod == RandomizeMethods.SPLOTCH):
         self._randomMethod = randomMethod
      else:
         raise LMError(currargs='Invalid RandomMethod %s' % str(randomMethod))

   randomMethod = property(_getRandomMethod)

# ...............................................
   def _getOutputPath(self):
      return self._bucketPath
   
   def _setOutputPath(self, pth):
      if self._bucketPath is None:
         self._bucketPath = pth
   
   outputPath = property(_getOutputPath, _setOutputPath)
 
# ...............................................
   def setSplotchPAM(self, splotchPam):
      self._splotchPAM = splotchPam
      self._setSplotchPAMFilename()
      
   def getSplotchPAM(self):
      return self._splotchPAM
   
#    splotchPam = property(_getSplotchPAM, _setSplotchPAM)

# ...............................................
   def _getRandomParameters(self):
      return self._randomParameters

   def dumpRandomParametersAsString(self):
      # Use default protocol 0 here, smaller dictionary can be ascii
      rpstr = pickle.dumps(self._randomParameters)
      return rpstr
   
   def loadRandomParametersFromString(self, rpstring):
      try:
         rp = pickle.loads(rpstring)
      except Exception, e:
         raise LMError(currargs='randomParameters is not a valid pickle: %s' % str(e))
      if isinstance(rp, dict):
         self._randomParameters = rp
      else:
         raise LMError(currargs='randomParameters is not a dictionary')
   
   def _setRandomParameters(self, rndParameters):
      self._randomParameters = {}
      if isinstance(rndParameters, str):
         self.loadRandomParametersFromString(rndParameters)
      elif isinstance(rndParameters, dict):
         self._randomParameters = rndParameters
         
#       if self._randomMethod == RandomizeMethods.SWAP:
#          try: 
#             iterations = self._randomParameters['iterations']
#             if iterations <= 0:
#                raise LMError('Invalid \'iterations\' parameter %d' % iterations)
#          except:
#             raise LMError('Missing or invalid \'iterations\' parameter')
#       elif self._randomMethod == RandomizeMethods.SPLOTCH:      
#          pass
         
   def getRandomParameter(self, key):
      try:
         val = self._randomParameters[key]
      except:
         val = None
      return val

# # .............................................................................
#    def _calculateMarginals(self):
#       """
#       @summary: calculates the marginal totals for a matrix
#       """
#       # N - no. of sites
#       # S - no. of species
#       self._N = float(self._pam._matrix.shape[0])
#       self._N1 = numpy.ones(self._N)
#       self._S = float(self._pam._matrix.shape[1])
#       self._S1 = numpy.ones(self._S)
#       # range size of each species
#       self._omega = numpy.dot(self._N1,self._pam._matrix)
#       # species richness of each site
#       self._alpha = numpy.dot(self._pam._matrix,self._S1)
#       
#       return self._alpha, self._omega
# # .............................................................................
#    def calculateSiteVectors(self):
#       """
#       @summary: calculates site based vectors
#       """
#       if self._omega == None or self._alpha == None:
#          self._calculateMarginals()
#       # Proportional Species Diversity of each site
#       self._alphaprop = self._alpha/self._S # Y axis in sites scatter plot
#        
#       # Per-site range size of a locality 
#       phi = numpy.dot(self._pam._matrix,self._omega) # equivalent to N1 dot A
#       # where A = X dot X.T, but we are trying to avoid matrix matrix multiplication
#            
#       # phiprop is just used to calc phiavgprop, but could be returned?
#       phiprop = phi/self._N
#       
#       # Mean Proportional Range Size     
#       phiavgprop = phiprop/self._alpha # X axis in sites scatter plot
#       
#       return self._alphaprop, phi, phiavgprop
# # .............................................................................
#    def calculateSpeciesVectors(self):
#       """
#       @summary: calculates species based vectors
#       """ 
#       if self._omega == None or self._alpha == None:
#          self._calculateMarginals()
#       # Proportional Range Size of each species
#       self._omegaprop = self._omega/self._N  # this is the Y axis in species scatter plot
#       
#       # Range-richness of a species
#       psi = numpy.dot(self._alpha,self._pam._matrix)
#       
#       # psiprop is just used to calc psiavgprop, but could be returned?  
#       psiprop = psi/self._S
#       
#       # Mean Proportional Species Diversity 
#       psiavgprop = psiprop/self._omega # this is the X axis in species scatter plot
#       
#       return self._omegaprop, psi, psiavgprop
# # ..............................................................................      
#    def betaDiversityIndices(self):
#       """
#       @summary: calculates, Whittakers, Lande's additive, and 
#       Legendre's beta diversity indices
#       """
#       if self._omega == None:
#          self._calculateMarginals()
#       ############# Whittaker's Beta ###########
#       if self._omegaprop == None:
#          self._omegaprop = self._omega/self._N
#       omegameanprop = self._omegaprop.sum()/self._S
#       self._WhittakersBeta = 1.0/omegameanprop
#       ##########################################
#       
#       ########## Lande's Additive Beta #########
#       LAdditiveBeta = self._S*(1-1.0/self._WhittakersBeta)
#       ########################################## 
#       
#       ########## Legendre's Beta ###############
#        
#       #self.LegendreBeta = numpy.sum(pdist(self._pam._matrix,metric='euclidean')**2)/self._N
#       LegendreBeta = self._omega.sum()-((self._omega**2).sum()/self._N) # !!!!!!!!!!
#       ########################################## 
#       
#       return self._WhittakersBeta, LAdditiveBeta, LegendreBeta
# # .............................................................................
#    def _sharedSpeciesSitesShared(self):
#       """
#       @summary: calculates Omega (O) and Alpha (A) matrices, matrices containing
#       the number of sites shared by species and number of shared species between
#       sites, respectively
#       """
#       self._A = numpy.dot(self._pam._matrix,self._pam._matrix.T)
#       self._O = numpy.dot(self._pam._matrix.T,self._pam._matrix)
# # .............................................................................
#    def covarianceMatrices(self):
#       """
#       @summary: calculates the composition of sites and range of species 
#       covariance matrices
#       """
#       if self._alpha == None or self._omega == None:
#          self._calculateMarginals()
#       if self._A == None or self._O == None:
#          self._sharedSpeciesSitesShared()
#              
#       # Matrix of covariance of composition of sites
#       if self._alphaprop == None:
#          self._alphaprop = self._alpha/self._S
#       self._SigmaSites = (self._A/self._S) - numpy.dot(self._alphaprop,self._alphaprop.T)
#       
#       # Matrix of covariance of ranges of species
#       if self._omegaprop == None:
#          self._omegaprop = self._omega/self._N
#       self._SigmaSpecies = (self._O/self._N) - numpy.dot(self._omegaprop,self._omegaprop.T)
#       
#       return self._SigmaSites, self._SigmaSpecies
# # ..............................................................................
#    def SchluterCovariances(self):
#       
#       if self._SigmaSites == None or self._SigmaSpecies == None:
#          self.covarianceMatrices()
#       # Schluter species-ranges covariance 
#       self._Vsps = numpy.dot(self._S1,numpy.dot(self._SigmaSpecies,self._S1))/self._SigmaSpecies.trace()
#       # Schluter sites-composition covariance
#       self._Vsites = numpy.dot(self._N1,numpy.dot(self._SigmaSites,self._N1))/self._SigmaSites.trace() 
#       
#       return self._Vsps, self._Vsites
            
# # ..............................................................................
#    def calculate(self, covMatrix=False, Schluter=False):
#       """
#       @param sitesPresent: sitesPresent dictionary belonging to the bucket
#       @param layersPresent: layersPresent dictionary belonging to the bucket
#       @todo: Fill this!
#       @todo: Test that this is compressed data
#       """ 
# 
#       sum = {}
#       sites = {}
#       species = {}
#       diversity = {}
#       matrices = {}
#       Schluters = {}
#       if self._pam.isCompressed:
#          try:
#             alpha, omega = self._calculateMarginals()
#             alphaprop, phi, phiavgprop = self.calculateSiteVectors()
#             omegaprop, psi, psiavgprop = self.calculateSpeciesVectors()
#             WhittakersBeta, LAdditiveBeta, LegendreBeta = self.betaDiversityIndices()
#             if covMatrix:
#                self.covarianceMatrices()
#             if Schluter:
#                self.SchluterCovariances()
#          except Exception, e:
#             raise LMError('Error calculating', str(e))
#          else: 
#             sites['speciesRichness-perSite'] = alpha         
#             sites['MeanProportionalRangeSize'] = phiavgprop
#             sites['ProportionalSpeciesDiversity'] = alphaprop
#             sites['Per-siteRangeSizeofaLocality'] = phi
#             species['RangeSize-perSpecies'] = omega
#             species['MeanProportionalSpeciesDiversity']  = psiavgprop
#             species['ProportionalRangeSize'] = omegaprop
#             species['Range-richnessofaSpecies'] = psi 
#             diversity['WhittakersBeta'] = WhittakersBeta
#             diversity['LAdditiveBeta'] = LAdditiveBeta
#             diversity['LegendreBeta'] = LegendreBeta
#             matrices['SigmaSpecies'] = self._SigmaSpecies
#             matrices['SigmaSites'] = self._SigmaSites
#             Schluters['Sites-CompositionCovariance'] = self._Vsites
#             Schluters['Species-RangesCovariance'] = self._Vsps
#             sum['sites'] = sites
#             sum['species'] = species
#             sum['diversity'] = diversity
#             sum['matrices'] = matrices
#             sum['Schluter'] = Schluters
#       else:
#          raise LMError('Matrix must be compressed') 
#          
#       return sum 


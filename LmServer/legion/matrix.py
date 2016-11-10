"""
@summary Module that contains the Matrix class
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
import numpy
import ogr
import os
from types import BooleanType

from LmCommon.common.lmconstants import (OFTInteger, OFTReal, OFTBinary, MatrixType)
from LmServer.base.lmobj import LMObject, LMError
from LmServer.base.serviceobject import ProcessObject, ServiceObject
from LmServer.common.lmconstants import LMServiceType,LMServiceModule


# .............................................................................
class Matrix(LMObject):
   """
   The Matrix class contains a 2-dimensional numeric matrix.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrix, 
                matrixType=MatrixType.PAM, 
                metadata={},
                dlocation=None, 
                experimentId=None,
                randomParameters={},
                userId=None,
                experimentId=None,
                matrixId=None,
                status=None, statusModTime=None):
      """
      @param matrix: numpy array
      @param dlocation: file location of the array
      """
      createTime = None
      ServiceObject.__init__(self,  userId, matrixId, createTime, statusModTime,
                             LMServiceType.MATRICES, 
                             moduleType=LMServiceModule.RAD,
                             metadataUrl=metadataUrl, 
                             parentMetadataUrl=parentMetadataUrl)
      ProcessObject.__init__(self, objId=pamSumId, parentId=bucketId, 
                status=status, statusModTime=statusModTime, stage=stage, 
                stageModTime=stageModTime) 
      self._matrix = matrix
      self.matrixType = matrixType
      self.metadata = {}
      self.loadMetadata(metadata)
      self._dlocation = dlocation
      self._setIsCompressed(isCompressed)
      self._randomParameters = randomParameters
      

# ..............................................................................
   @staticmethod
   def initEmpty(siteCount, layerCount):
      matrix = numpy.zeros([siteCount, layerCount])
      return Matrix(matrix)
   
# ..............................................................................
   @staticmethod
   def initFromFile(filename, isCompressed):
      if filename is None:
         return None
      elif os.path.exists(filename):
         try:
            data = numpy.load(filename)
         except Exception, e:
            raise LMError('Matrix File %s is not readable by numpy' 
                          % str(filename))
         if (isinstance(data, numpy.ndarray) and data.ndim == 2):
            return Matrix(data, dlocation=filename, isCompressed=isCompressed)
         else:
            raise LMError('Matrix File %s does not contain a 2 dimensional array' 
                          % str(filename))
      else:
         return None
   
# ...............................................
   def addMetadata(self, metadict):
      for key, val in metadict.iteritems():
         self.metadata[key] = val
         
   def dumpMetadata(self):
      metastring = None
      if self.metadata:
         metastring = json.dumps(self.metadata)
      return metastring

   def loadMetadata(self, meta):
      """
      @note: Adds to dictionary or modifies values for existing keys
      """
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

# .............................................................................
   def readData(self, filename=None):
      # filename overrides dlocation
      if filename is not None:
         self._dlocation = filename
      if os.path.exists(self._dlocation):
         try:
            data = numpy.load(self._dlocation)
         except Exception, e:
            raise LMError('Matrix File %s is not readable by numpy' 
                          % str(self._dlocation))
         if (isinstance(data, numpy.ndarray) and data.ndim == 2):
            self._matrix = data 
         else:
            raise LMError('Matrix File %s does not contain a 2 dimensional array' 
                          % str(self._dlocation))
      else:
         raise LMError('Matrix File %s does not exist' % str(self._dlocation))

# .............................................................................
   @property
   def data(self):
      return self._matrix
   
# .............................................................................
   def getValue(self, siteIdx, layerIdx):
      try:
         val = self._matrix[siteIdx][layerIdx]
      except Exception, e:
         raise LMError('%s: site: %d, layer: %d invalid for %s' 
                       % (str(e), siteIdx, layerIdx, str(self._matrix.shape)))
      return val

# .............................................................................
   def getSiteRow(self, siteIdx):
      return self._matrix[siteIdx]
   
# .............................................................................
   def getLayerColumn(self, layerIdx):
      pass
# ..............................................................................
   def _getRowCount(self):
      return self._matrix.shape[0]
   
# ..............................................................................
   def _getColumnCount(self):
      return self._matrix.shape[1]
   
   def _getNumpyDataType(self):
      return self._matrix.dtype

   def _getOGRDataType(self):
      if self._matrix.dtype in (numpy.float32, numpy.float64, numpy.float128):
         return OFTReal
      elif self._matrix.dtype in (numpy.int8, numpy.int16, numpy.int32):
         return OFTInteger
      elif self._matrix.dtype == numpy.bool:
         return OFTBinary

   rowCount = property(_getRowCount)
   columnCount = property(_getColumnCount)
   numpyDataType = property(_getNumpyDataType)
   ogrDataType = property(_getOGRDataType)

# ..............................................................................
   def _setIsCompressed(self, isCompressed):
      if isinstance(isCompressed, BooleanType):
         self._isCompressed = isCompressed
      else:
         raise LMError('IsCompressed value must be Boolean')


# ...............................................
   def _isCompressed(self):
         return self._isCompressed
   isCompressed = property(_isCompressed)     
# ..............................................................................
   def getDLocation(self): 
      return self._dlocation
   
   def setDLocation(self, dlocation):
      self._dlocation = dlocation

# ..............................................................................
   def convertToBool(self): 
      self._matrix = numpy.bool_(self._matrix)
      
# ..............................................................................

   def transpose(self, axes=None):
      if axes == None:
         self._matrix = numpy.transpose(self._matrix)
      
# ..............................................................................
#   def addColumn(self, data, rows, colIndx):
#      for r in rows.keys():
   def addColumn(self, data, colIdx):
      for row in range(0,len(data)):
         self._matrix[row][colIdx] = data[row]
   

      
#..............................................................................
   def getColumnPresence(self, sitesPresent, layersPresent, columnIdx):
      """
      @summary: return a list of ids from a column in a compressed PAM
      that have presence.  The id will be in uncompressed format, i.e, the
      real id as it would be numbered as the shpid in the shapegrid's shapefile
      @todo: check to see if column exists against layersPresent
      """
      truerowcounter = 0
      ids = []      
      for r in sitesPresent.keys():
         if sitesPresent[r]:
            if self._matrix[truerowcounter,columnIdx] == True:      
               ids.append(r)              
            truerowcounter += 1
         
      return ids
      
# .............................................................................
   def createUncompressed(self, sitesPresent, layersPresent):
      """
      @summary: Uncompress the matrix into a new Matrix object and return it.
      """
      fullpam = numpy.zeros([len(sitesPresent), len(layersPresent)])
      truerowcounter = 0
      for r in sitesPresent.keys():
         if sitesPresent[r]:           
            for c in layersPresent.keys():
               if layersPresent[c]:
                  fullpam[r,c] = self._matrix[truerowcounter,c]
            truerowcounter += 1       
              
      uncompressedPam = Matrix(fullpam, isCompressed=False)
      uncompressedPam.convertToBool()
      
      return uncompressedPam

# ..............................................................................
   def uncompressLayer(self, lyridx, sitesPresent, layersPresent):
      """
      @summary: Uncompress a single layer and return it.
      """
      lyrdata = numpy.zeros([len(sitesPresent)])
      for r in sitesPresent.keys():
         if sitesPresent[r]:
            lyrdata[r] = self._matrix[r,lyridx]
      return lyrdata   

# ..............................................................................
   def write(self, filename=None, overwrite=True):
      """
      @summary: Write the matrix out to a file
      @param filename: The location on disk to write the file
      @todo: test this - there could be a more appropriate numpy method
      """
      if filename is None:
         filename = self._dlocation
      self._readyFilename(filename, overwrite=overwrite)
         
      print '  Writing matrix %s' % filename
      try:
         fnamewoext, ext = os.path.splitext(filename)
         # Numpy automatically adds '.npy' extension
         numpy.save(fnamewoext, self._matrix)
      except Exception, e:
         raise LMError('Error writing file %s' % filename, str(e))
         
# ...............................................
   def clear(self):
      success, msg = self._deleteFile(self._dlocation, deleteDir=True)
      self._matrix = None
      
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

# .............................................................................
   def subsetPamSites(self, newsitesPresent, origsitesPresent):
      """
      @summary: returns a new Matrix object and layersPresent
      subsetted (compressed) from  self._matrix given new a new sitesPresent
      """
      if self.isCompressed:
         numbercols = self._matrix.shape(1)
         newrowidxs = sorted([key for key in newsitesPresent.keys() if newsitesPresent[key]])
         matrix = numpy.zeros([len(newrowidxs),numbercols])
         origrowidxs = sorted([key for key in origsitesPresent.keys() if origsitesPresent[key]])
         for newidx, oldidx in enumerate(newrowidxs):
            matrix[newidx] = self._matrix[origrowidxs.index(oldidx)]
         # now need to compress the columns because a subset of rows will cut some species out
         # now need to make a new layersPresent and return it along with new matrix
         layersPresent = {}
         for col in range(0,matrix.shape(1)):
            if matrix[:,col].max() > 0:
               layersPresent[col] = True
            else:
               layersPresent[col] = False
         colmask = [item[1] for item in sorted(layersPresent.items())]
         cmpMatrix = numpy.compress(colmask, matrix, axis=1)
         compressedPam = Matrix(cmpMatrix, isCompressed=True)
         compressedPam.convertToBool()
      else:
         raise LMError('Matrix must be compressed') 
         
         return compressedPam,layersPresent   
         

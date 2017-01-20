"""
@summary Module that contains the Matrix class
@author Aimee Stewart
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
"""
import numpy
import ogr
import os
import pickle
from types import StringType

from LmCommon.common.lmconstants import (OFTInteger, OFTReal, OFTBinary, 
                                         MatrixType)
from LmServer.base.lmobj import LMObject, LMError
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import LMServiceType,LMServiceModule


# .............................................................................
class Matrix(ServiceObject, ProcessObject):
   """
   The Matrix class contains a 2-dimensional numeric matrix.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrix, 
                matrixType=MatrixType.PAM, 
                processType=None,
                metadata={},
                dlocation=None, 
                layerIndicesFilename=None,
                metadataUrl=None,
                userId=None,
                gridset=None, 
                matrixId=None,
                status=None, statusModTime=None):
      """
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @param gridsetId: parentID for ProcessObject
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @param matrixId: dbId  for ServiceObject
      @param matrix: numpy array
      @param matrixType: Constant from LmCommon.common.lmconstants.MatrixType
      @param metadata: dictionary of metadata using Keys defined in superclasses
      @param dlocation: file location of the array
      @param layerIndicesFilename: file location of layer indices
      @param gridset: parent gridset of this Matrix
      """
      self._gridset = gridset
      ServiceObject.__init__(self,  userId, matrixId, LMServiceType.MATRICES, 
                             moduleType=LMServiceModule.LM, 
                             metadataUrl=metadataUrl, 
                             parentMetadataUrl=gridsetUrl,
                             modTime=statusModTime)
      ProcessObject.__init__(self, objId=matrixId, processType=processType,
                             parentId=gridsetId, 
                             status=status, statusModTime=statusModTime) 
      self._matrix = matrix
      self.matrixType = matrixType
      self._dlocation = dlocation
      self._layerIndicesFilename = layerIndicesFilename
      self._layerIndices = None
      self.mtxMetadata = {}
      self.loadMtxMetadata(metadata)
      

# ..............................................................................
   @staticmethod
   def initEmpty(siteCount, layerCount):
      matrix = numpy.zeros([siteCount, layerCount])
      return Matrix(matrix)
   
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

# ...............................................
   @property
   def gridsetName(self):
      name = None
      if self._gridset is not None:
         name = self._gridset.name
      return name
   
# ...............................................
   @property
   def gridsetId(self):
      gid = None
      if self._gridset is not None:
         gid = self._gridset.getId()
      return gid

# ...............................................
   @property
   def gridsetUrl(self):
      url = None
      if self._gridset is not None:
         url = self._gridset.metadataUrl
      return url

# ...............................................
   def dumpMtxMetadata(self):
      return super(Matrix, self)._dumpMetadata(self.mtxMetadata)
 
# ...............................................
   def loadMtxMetadata(self, newMetadata):
      self.mtxMetadata = super(Matrix, self)._loadMetadata(self, newMetadata)

# ...............................................
   def addMtxMetadata(self, newMetadataDict):
      self.mtxMetadata = super(Matrix, self)._addMetadata(self, newMetadataDict, 
                                  existingMetadataDict=self.mtxMetadata)

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
   def readIndices(self, indicesFilename=None):
      """
      @summary Fill the siteIndices from existing file
      """
      indices = None
      if indicesFilename is None:
         indicesFilename = self._layerIndicesFilename
      if isinstance(indicesFilename, StringType) and os.path.exists(indicesFilename):
         try:
            f = open(indicesFilename, 'r')
            indices = pickle.load(f)
         except:
            raise LMError('Failed to read indices {}'.format(indicesFilename))
         finally:
            f.close()
      self._layerIndices = indices
      
   def getLayerIndicesFilename(self):
      return self._layerIndicesFilename
      

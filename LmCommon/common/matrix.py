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
import json
import numpy
import os
import pickle
from types import StringType

from LmCommon.common.lmconstants import (OFTInteger, OFTReal, OFTBinary, 
                                         MatrixType)


# .............................................................................
class Matrix(object):
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
                columnIndices=None,
                columnIndicesFilename=None,
                matrixId=None):
      """
      @param matrix: numpy array
      @param matrixType: Constant from LmCommon.common.lmconstants.MatrixType
      @param metadata: dictionary of metadata using Keys defined in superclasses
      @param dlocation: file location of the array
      @param columnIndicesFilename: dictionary of column indices
      @param columnIndicesFilename: file location of column indices
      @param matrixId: dbId  for ServiceObject
      """
      self._matrix = matrix
      self.matrixType = matrixType
      self._dlocation = dlocation
      self._columnIndicesFilename = columnIndicesFilename
      self._columnIndices = columnIndices
      self.mtxMetadata = {}
      self.loadMtxMetadata(metadata)
      self._matrixId = matrixId
      

# ..............................................................................
   @staticmethod
   def initEmpty(siteCount, layerCount):
      matrix = numpy.zeros([siteCount, layerCount])
      return Matrix(matrix)
   
# .............................................................................
   def readFromCSV(self, filename=None):
      pass
   
# .............................................................................
   def getMatrixId(self):
      return self._matrixId

# .............................................................................
   def readData(self, filename=None):
      # filename overrides dlocation
      if filename is not None:
         self._dlocation = filename
      if os.path.exists(self._dlocation):
         try:
            data = numpy.load(self._dlocation)
         except Exception, e:
            raise Exception('Matrix File {} is not readable by numpy'
                            .format(self._dlocation))
         if (isinstance(data, numpy.ndarray) and data.ndim == 2):
            self._matrix = data 
         else:
            raise Exception('Matrix File {} does not contain a 2 dimensional array'
                            .format(self._dlocation))
      else:
         raise Exception('Matrix File {} does not exist'.format(self._dlocation))

# ...............................................
   def dumpMtxMetadata(self, metadataDict):
      metadataStr = None
      if metadataDict:
         metadataStr = json.dumps(metadataDict)
      return metadataStr

# ...............................................
   def addMtxMetadata(self, newMetadataDict):
      for key, val in newMetadataDict.iteritems():
         try:
            existingVal = self.mtxMetadata[key]
         except:
            self.mtxMetadata[key] = val
         else:
            # if metadata exists and is ...
            if type(existingVal) is list: 
               # a list, add to it
               if type(val) is list:
                  newVal = list(set(existingVal.extend(val)))
                  self.mtxMetadata[key] = newVal
                  
               else:
                  newVal = list(set(existingVal.append(val)))
                  self.mtxMetadata[key] = newVal
            else:
               # not a set, replace it
               self.mtxMetadata[key] = val

# ...............................................
   def loadMtxMetadata(self, newMetadata):
      """
      @note: Adds to dictionary or modifies values for existing keys
      """
      objMetadata = {}
      if newMetadata is not None:
         if type(newMetadata) is dict: 
            objMetadata = newMetadata
         else:
            try:
               objMetadata = json.loads(newMetadata)
            except Exception, e:
               print('Failed to load JSON object from type {} object {}'
                     .format(type(newMetadata), newMetadata))
      self.mtxMetadata = objMetadata

# .............................................................................
   @property
   def data(self):
      return self._matrix
   
# .............................................................................
   def getValue(self, rowIdx, colIdx):
      try:
         val = self._matrix[rowIdx][colIdx]
      except Exception, e:
         raise Exception('{}: row: {}, column: {} invalid for {}'.format(
                           str(e), rowIdx, colIdx, self._matrix.shape))
      return val

# .............................................................................
   def getRow(self, rowIdx):
      return self._matrix[rowIdx]
   
# .............................................................................
   def getColumn(self, colIdx):
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
   

      
# #..............................................................................
#    def getColumnPresence(self, sitesPresent, layersPresent, columnIdx):
#       """
#       @summary: return a list of ids from a column in a compressed PAM
#       that have presence.  The id will be in uncompressed format, i.e, the
#       real id as it would be numbered as the shpid in the shapegrid's shapefile
#       @todo: check to see if column exists against layersPresent
#       """
#       truerowcounter = 0
#       ids = []      
#       for r in sitesPresent.keys():
#          if sitesPresent[r]:
#             if self._matrix[truerowcounter,columnIdx] == True:      
#                ids.append(r)              
#             truerowcounter += 1
#       return ids
      
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
         
      print '  Writing matrix {}'.format(filename)
      try:
         fnamewoext, ext = os.path.splitext(filename)
         # Numpy automatically adds '.npy' extension
         numpy.save(fnamewoext, self._matrix)
      except Exception, e:
         raise Exception('Error writing file {}, ({})'.format(filename, str(e)))
         
# ...............................................
   def clear(self):
      """
      @note: deleting the file is done in LmServer.legion.Matrix
      """
      if self._dlocation is not None and os.path.exists(self._dlocation):
         try:
            os.remove(self._dlocation)
         except Exception, e:
            print('Failed to remove {}, {}'.format(self._dlocation, str(e)))
      self._matrix = None
      
# ...............................................
   def readColumnIndices(self, colIndicesFname=None):
      """
      @summary Fill the siteIndices from existing file
      """
      indices = None
      if colIndicesFname is None:
         colIndicesFname = self._columnIndicesFilename
      if (isinstance(colIndicesFname, StringType) and 
          os.path.exists(colIndicesFname)):
         try:
            f = open(colIndicesFname, 'r')
            indices = pickle.load(f)
         except:
            raise Exception('Failed to read indices {}'.format(colIndicesFname))
         finally:
            f.close()
      self._columnIndices = indices
      
   def getColumnIndicesFilename(self):
      return self._columnIndicesFilename
      
   def getColumnIndices(self):
      return self._columnIndices

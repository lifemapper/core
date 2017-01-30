"""
@summary Module that contains the Matrix class
@author Aimee Stewart / CJ Grady
@status: alpha
@version: 1.0
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
@todo: Use https://docs.scipy.org/doc/numpy/user/basics.subclassing.html when
          changing this to subclass numpy.ndarray
@todo: Handle multiple rows / columns / etc of metadata (like PAM x, y, site ids)
@todo: Consider adding constants for easy reference to rows and columns
@note: Not tested.  First iteration
"""
import json
import numpy as np
from copy import deepcopy

METADATA_KEY = 'metadata'
DATA_KEY = 'data'

# .............................................................................
class Matrix(object):
   """
   @summary: Lifemapper wrapper for Numpy ndarrays that adds metadata
   """
   # ...........................
   def __init__(self, mtx, metadata=None):
      """
      @summary: Constructor
      @param mtx: A matrix (like) object to use as the base data for the Matrix.
                     This can be None if the data has not been initialized
      @param metadata: Optional headers for this matrix.  This may be either a
                          list of lists, where the index of a list in the lists 
                          will be treated as the axis 
                          (ex. [['Row 1', 'Row 2', 'Row 3'], 
                                ['Column 1', 'Column 2']])
                          Or this could be a dictionary where the key is used
                          for the axis.  (Ex:
                          {
                             1 : ['Column 1', 'Column 2'],
                             0 : ['Row 1', 'Row 2', 'Row 3']
                          }
      @note: If the headers for an axis are a string and not a list, it will be
                treated as a file name
      """
      self.data = mtx
      self.metadata = None
      if metadata is not None:
         self.setMetadata(metadata)
      
   # ...........................
   @classmethod
   def loadFromCSV(cls, flo):
      """
      @summary: Loads a Matrix from a CSV file
      @param flo: A string (filename) or file-like object containing a CSV
      """
      pass
   
   # ...........................
   @classmethod
   def loadFromJsonOrDictionary(cls, obj):
      metadata = obj[METADATA_KEY]
      data = np.array(obj[DATA_KEY])
      return cls(data, metadata=metadata)
   
   # ...........................
   @classmethod
   def concatenate(cls, mtxList, axis=0):
      """
      @summary: Concatenates multiple Matrix objects together to form a new 
                   Matrix object
      @param mtxList: A List of Matrix objects to concatenate together
      @param axis: The axis to concatenate these Matrix objects on
      @note: Assumes that metadata for other axes are the same
      """
      mtxObjs = []
      axisMetadata = []
      for mtx in mtxList:
         axisMetadata.append(mtx.getMetadata(axis=axis))
         mtxObjs.extend(mtx.data)
      # Create a new data matrix
      # TODO: Consider adding capability to append on new axis
      newData = np.concatenate(mtxObjs, axis=axis)
      # Use the first Matrix's metadata as the base
      newMetadata = mtxList[0].getMetadata()
      # Replace the axis of metadata with the concatenated version
      newMetadata[axis] = axisMetadata
      return cls(newData, metadata=newMetadata)
   
   # ...........................
   def append(self, mtx, axis=0):
      """
      @summary: Appends the provided Matrix object to this one
      @param mtx: The Matrix object to append to this one
      @param axis: The axis to append this matrix on
      @note: Only keeps the metadata for the append axis, assumes the other 
                axes are the same
      """
      self.data = np.append(self.data, mtx, axis=axis)
      self.metadata[axis].append(mtx.getMetadata(axis=axis))
   
   # ...........................
   def getColumnMetadata(self):
      """
      @summary: Shortcut to get column metadata
      @todo: Throw a different exception if no column metadat?
      """
      return self.getMetadata(axis=1)
   
   # ...........................
   def getMetadata(self, axis=None):
      """
      @summary: Get the metadata associated with this Matrix, optionally 
                   limited to a specific axis
      @param axis: If provided, return metadata for this axis, else, return all
      """
      if axis is None:
         return self.metadata
      else:
         return self.metadata[axis]
   
   # ...........................
   def getRowMetadata(self):
      """
      @summary: Shortcut to get row metadata
      @todo: Throw a different exception if no row metadata?
      """
      return self.getMetadata(axis=0)
   
   # ...........................
   def save(self, flo):
      """
      @summary: Saves the Matrix object as a JSON document to the file-like 
                   object
      @param flo: The file-like object to write to
      """
      writeObj = {}
      writeObj[METADATA_KEY] = self.metadata
      writeObj[DATA_KEY] = self.data.tolist()
      
      json.dump(writeObj, flo, indent=3)
   
   # ...........................
   def setColumnMetadata(self, metadata):
      """
      @summary: Shortcut to set column metadata
      """
      self.setMetadata(metadata, axis=1)
   
   # ...........................
   def setMetadata(self, metadata, axis=None):
      """
      @summary: Set the metadata for this Matrix, optionally for a specific axis
      @param metadata: Matrix metadata.  Can be a list of lists, a dictionary
                          of lists, or if axis is provided, a single list
      @param axis: If provided, set the metadata for a specific axis, else, 
                      process as if it is for the entire Matrix
      @todo: Validate input for single axis operation?
      @note: Resets metadata dictionary when setting values for all metadata
      @note: Duck types to use list of lists or dictionary to set values for
                different axes
      """
      if axis is not None:
         self.metadata[axis] = metadata
      else:
         self.metadata = {}
         try:
            metadataKeys = metadata.keys()
         except: # Not a dictionary
            # Check if first item is a list
            if isinstance(metadata[0], list):
               # Assume list of lists
               metadataKeys = range(len(metadata))
            else:
               # Convert to a list
               metadata = [metadata]
               metadataKeys = [0]
         
         # We should have a list of keys, which could be either dictionary 
         #    keys or list indices
         for k in metadataKeys:
            self.metadata[k] = metadata[k]
   
   # ...........................
   def setRowMetadata(self, metadata):
      """
      @summary: Shortcut to set row metadata
      """
      self.setMetadata(metadata, axis=0)
   
   # ...........................
   def slice(self, *args):
      """
      @summary: Subsets the matrix and returns a new instance
      @param *args: These are iterables for the indices to retrieve
      @note: The first parameter will be for axis 0, second for axis 1, etc
      """
      newData = np.copy(self.data)
      newMetadata = deepcopy(self.metadata)
      # For each arg in the list
      for i in range(len(args)):
         # Subset the data matrix
         newData = newData.take(args[i], axis=i)
         # Subset the metadata
         tmp = []
         for j in args[i]:
            tmp.append(newMetadata[j])
         newMetadata[i] = tmp
      return Matrix(newData, metadata=newMetadata)
   
   # ...........................
   def writeCSV(self, flo):
      """
      @summary: Write the Matrix object to a CSV file-like object
      @param flo: The file-like object to write to
      @todo: Flatten 3 or more dimensions?
      @todo: Handle header overlap (where the header for one axis is for another 
                axis header
      @todo: Multiple headers along an axis (think site id, x, y for PAMs)
      @note: Currently only works for 2-D tables
      """
      def csvGenerator():
         """
         @summary: This function is a generator that yields rows of values to 
                      be output as CSV
         """
         try:
            rowMetadata = self.metadata[0]
         except:
            # No row metadata
            rowMetadata = []
         
         # Start with the header row, if we have one
         if self.metadata.has_key(1) and self.metadata[1]:
            # Add a blank entry if we have row metadata
            headerRow = [''] if rowMetadata else []
            headerRow.extend(self.metadata[1])
            yield headerRow
         # For each row in the data set
         for i in xrange(self.data.shape[0]):
            # Add the row metadata if exists
            row = [rowMetadata[i]] if rowMetadata else []
            # Get the data from the data array
            row.extend(self.data[i].tolist())
            yield row
            
      for row in csvGenerator():
         flo.write("{0}\n".format(','.join([str(v) for v in row])))
   
   # Initialize an array
   # Update headers
   # Insert row
   # Get row / column shortcuts
   # Insert column / etc
   # Update row / column / etc by index or by header name
   
   
   # To consider
   #   Data could be none?
   #   PAVs
   #   How to read row indices from a shapegrid?
   #   Missing headers?
   #   Multiple header columns
   #   Should headers overlap?
   #   What happens when headers don't match?
   
# ============================================================================
# Old version
# ============================================================================

#"""
#@summary Module that contains the Matrix class
#@author Aimee Stewart
#@license: gpl2
#@copyright: Copyright (C) 2017, University of Kansas Center for Research
#
#          Lifemapper Project, lifemapper [at] ku [dot] edu, 
#          Biodiversity Institute,
#          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
#   
#          This program is free software; you can redistribute it and/or modify 
#          it under the terms of the GNU General Public License as published by 
#          the Free Software Foundation; either version 2 of the License, or (at 
#          your option) any later version.
#  
#          This program is distributed in the hope that it will be useful, but 
#          WITHOUT ANY WARRANTY; without even the implied warranty of 
#          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
#          General Public License for more details.
#  
#          You should have received a copy of the GNU General Public License 
#          along with this program; if not, write to the Free Software 
#          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
#          02110-1301, USA.
#"""
# import json
# import numpy
# import os
# import pickle
# from types import StringType
# 
# from LmCommon.common.lmconstants import (OFTInteger, OFTReal, OFTBinary, 
#                                          MatrixType)
# 
# 
# # .............................................................................
# class Matrix(object):
#    """
#    The Matrix class contains a 2-dimensional numeric matrix.
#    """
# # .............................................................................
# # Constructor
# # .............................................................................
#    def __init__(self, matrix, 
#                 matrixType=MatrixType.PAM, 
#                 metadata={},
#                 dlocation=None, 
#                 columnIndices=None,
#                 columnIndicesFilename=None,
#                 matrixId=None):
#       """
#       @param matrix: numpy array
#       @param matrixType: Constant from LmCommon.common.lmconstants.MatrixType
#       @param metadata: dictionary of metadata using Keys defined in superclasses
#       @param dlocation: file location of the array
#       @param columnIndicesFilename: dictionary of column indices
#       @param columnIndicesFilename: file location of column indices
#       @param matrixId: dbId  for ServiceObject
#       """
#       self._matrix = matrix
#       self.matrixType = matrixType
#       self._dlocation = dlocation
#       self._columnIndicesFilename = columnIndicesFilename
#       self._columnIndices = columnIndices
#       self.mtxMetadata = {}
#       self.loadMtxMetadata(metadata)
#       self._matrixId = matrixId
#       
# 
# # ..............................................................................
#    @staticmethod
#    def initEmpty(siteCount, layerCount):
#       matrix = numpy.zeros([siteCount, layerCount])
#       return Matrix(matrix)
#    
# # .............................................................................
#    def readFromCSV(self, filename=None):
#       pass
#    
# # .............................................................................
#    def readData(self, filename=None):
#       # filename overrides dlocation
#       if filename is not None:
#          self._dlocation = filename
#       if os.path.exists(self._dlocation):
#          try:
#             data = numpy.load(self._dlocation)
#          except Exception, e:
#             raise Exception('Matrix File {} is not readable by numpy'
#                             .format(self._dlocation))
#          if (isinstance(data, numpy.ndarray) and data.ndim == 2):
#             self._matrix = data 
#          else:
#             raise Exception('Matrix File {} does not contain a 2 dimensional array'
#                             .format(self._dlocation))
#       else:
#          raise Exception('Matrix File {} does not exist'.format(self._dlocation))
# 
# # ...............................................
#    def dumpMtxMetadata(self, metadataDict):
#       metadataStr = None
#       if metadataDict:
#          metadataStr = json.dumps(metadataDict)
#       return metadataStr
# 
# # ...............................................
#    def addMtxMetadata(self, newMetadataDict):
#       for key, val in newMetadataDict.iteritems():
#          try:
#             existingVal = self.mtxMetadata[key]
#          except:
#             self.mtxMetadata[key] = val
#          else:
#             # if metadata exists and is ...
#             if type(existingVal) is list: 
#                # a list, add to it
#                if type(val) is list:
#                   newVal = list(set(existingVal.extend(val)))
#                   self.mtxMetadata[key] = newVal
#                   
#                else:
#                   newVal = list(set(existingVal.append(val)))
#                   self.mtxMetadata[key] = newVal
#             else:
#                # not a set, replace it
#                self.mtxMetadata[key] = val
# 
# # ...............................................
#    def loadMtxMetadata(self, newMetadata):
#       """
#       @note: Adds to dictionary or modifies values for existing keys
#       """
#       objMetadata = {}
#       if newMetadata is not None:
#          if type(newMetadata) is dict: 
#             objMetadata = newMetadata
#          else:
#             try:
#                objMetadata = json.loads(newMetadata)
#             except Exception, e:
#                print('Failed to load JSON object from type {} object {}'
#                      .format(type(newMetadata), newMetadata))
#       self.mtxMetadata = objMetadata
# 
# # .............................................................................
#    @property
#    def data(self):
#       return self._matrix
#    
# # .............................................................................
#    def getValue(self, rowIdx, colIdx):
#       try:
#          val = self._matrix[rowIdx][colIdx]
#       except Exception, e:
#          raise Exception('{}: row: {}, column: {} invalid for {}'.format(
#                            str(e), rowIdx, colIdx, self._matrix.shape))
#       return val
# 
# # .............................................................................
#    def getRow(self, rowIdx):
#       return self._matrix[rowIdx]
#    
# # .............................................................................
#    def getColumn(self, colIdx):
#       pass
# # ..............................................................................
#    def _getRowCount(self):
#       return self._matrix.shape[0]
#    
# # ..............................................................................
#    def _getColumnCount(self):
#       return self._matrix.shape[1]
#    
#    def _getNumpyDataType(self):
#       return self._matrix.dtype
# 
#    def _getOGRDataType(self):
#       if self._matrix.dtype in (numpy.float32, numpy.float64, numpy.float128):
#          return OFTReal
#       elif self._matrix.dtype in (numpy.int8, numpy.int16, numpy.int32):
#          return OFTInteger
#       elif self._matrix.dtype == numpy.bool:
#          return OFTBinary
# 
#    rowCount = property(_getRowCount)
#    columnCount = property(_getColumnCount)
#    numpyDataType = property(_getNumpyDataType)
#    ogrDataType = property(_getOGRDataType)
# 
# # ..............................................................................
#    def getDLocation(self): 
#       return self._dlocation
#    
#    def setDLocation(self, dlocation):
#       self._dlocation = dlocation
# 
# # ..............................................................................
#    def convertToBool(self): 
#       self._matrix = numpy.bool_(self._matrix)
#       
# # ..............................................................................
# 
#    def transpose(self, axes=None):
#       if axes == None:
#          self._matrix = numpy.transpose(self._matrix)
#       
# # ..............................................................................
# #   def addColumn(self, data, rows, colIndx):
# #      for r in rows.keys():
#    def addColumn(self, data, colIdx):
#       for row in range(0,len(data)):
#          self._matrix[row][colIdx] = data[row]
#    
# 
#       
# # #..............................................................................
# #    def getColumnPresence(self, sitesPresent, layersPresent, columnIdx):
# #       """
# #       @summary: return a list of ids from a column in a compressed PAM
# #       that have presence.  The id will be in uncompressed format, i.e, the
# #       real id as it would be numbered as the shpid in the shapegrid's shapefile
# #       @todo: check to see if column exists against layersPresent
# #       """
# #       truerowcounter = 0
# #       ids = []      
# #       for r in sitesPresent.keys():
# #          if sitesPresent[r]:
# #             if self._matrix[truerowcounter,columnIdx] == True:      
# #                ids.append(r)              
# #             truerowcounter += 1
# #       return ids
#       
# # ..............................................................................
#    def write(self, filename=None, overwrite=True):
#       """
#       @summary: Write the matrix out to a file
#       @param filename: The location on disk to write the file
#       @todo: test this - there could be a more appropriate numpy method
#       """
#       if filename is None:
#          filename = self._dlocation
#       self._readyFilename(filename, overwrite=overwrite)
#          
#       print '  Writing matrix {}'.format(filename)
#       try:
#          fnamewoext, ext = os.path.splitext(filename)
#          # Numpy automatically adds '.npy' extension
#          numpy.save(fnamewoext, self._matrix)
#       except Exception, e:
#          raise Exception('Error writing file {}, ({})'.format(filename, str(e)))
#          
# # ...............................................
#    def clear(self):
#       """
#       @note: deleting the file is done in LmServer.legion.Matrix
#       """
#       if self._dlocation is not None and os.path.exists(self._dlocation):
#          try:
#             os.remove(self._dlocation)
#          except Exception, e:
#             print('Failed to remove {}, {}'.format(self._dlocation, str(e)))
#       self._matrix = None
#       
# # ...............................................
#    def readColumnIndices(self, colIndicesFname=None):
#       """
#       @summary Fill the siteIndices from existing file
#       """
#       indices = None
#       if colIndicesFname is None:
#          colIndicesFname = self._columnIndicesFilename
#       if (isinstance(colIndicesFname, StringType) and 
#           os.path.exists(colIndicesFname)):
#          try:
#             f = open(colIndicesFname, 'r')
#             indices = pickle.load(f)
#          except:
#             raise Exception('Failed to read indices {}'.format(colIndicesFname))
#          finally:
#             f.close()
#       self._columnIndices = indices
#       
#    def getColumnIndicesFilename(self):
#       return self._layerIndicesFilename
#       

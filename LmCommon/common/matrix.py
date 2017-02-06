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
@todo: Handle multiple rows / columns / etc of headers (like PAM x, y, site ids)
"""
from copy import deepcopy
import json
import numpy as np

HEADERS_KEY = 'headers'
DATA_KEY = 'data'

# .............................................................................
class Matrix(object):
   """
   @summary: Lifemapper wrapper for Numpy ndarrays that adds headers
   """
   # ...........................
   def __init__(self, mtx, headers=None):
      """
      @summary: Constructor
      @param mtx: A matrix (like) object to use as the base data for the Matrix.
                     This can be None if the data has not been initialized
      @param headers: Optional headers for this matrix.  This may be either a
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
      self.headers = None
      if headers is not None:
         self.setHeaders(headers)
      
   # ...........................
   @classmethod
   def load(cls, fn):
      """
      @summary: Attempt to load a Matrix object from a file
      @param fn: File location of a stored Matrix, Numpy array, or ?
      """
      # Try loading Matrix
      try:
         obj = json.load(fn)
         return cls.loadFromJsonOrDictionary(obj)
      except:
         # Try loading numpy array
         try:
            data = np.load(fn)
            return cls(data)
         except:
            raise Exception("Cannot load file: {0}".format(fn))
         
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
      headers = obj[HEADERS_KEY]
      data = np.array(obj[DATA_KEY])
      return cls(data, headers=headers)
   
   # ...........................
   @classmethod
   def concatenate(cls, mtxList, axis=0):
      """
      @summary: Concatenates multiple Matrix objects together to form a new 
                   Matrix object
      @param mtxList: A List of Matrix objects to concatenate together
      @param axis: The axis to concatenate these Matrix objects on
      @note: Assumes that headers for other axes are the same
      """
      mtxObjs = []
      axisHeaders = []
      for mtx in mtxList:
         if not isinstance(mtx, Matrix):
            mtx = Matrix(mtx)
         # Make sure we reshape if necessary if adding new axis (stacking)
         if len(mtx.data.shape) < axis + 1: # Add 1 since zero-based
            newShape = list(mtx.data.shape) + [1]
            mtx.data.reshape(newShape)
            mtx.setHeaders([''], axis=axis)
         
         # TODO: Handle when there are no headers for an axis
         axisHeaders.append(mtx.getHeaders(axis=axis))
         mtxObjs.extend(mtx.data)
         
      # Create a new data matrix
      newData = np.concatenate(mtxObjs, axis=axis)
      # Use the first Matrix's headers as the base
      newHeaders = mtxList[0].getHeaders()
      # Replace the axis of headers with the concatenated version
      newHeaders[axis] = axisHeaders
      return cls(newData, headers=newHeaders)
   
   # ...........................
   def append(self, mtx, axis=0):
      """
      @summary: Appends the provided Matrix object to this one
      @param mtx: The Matrix object to append to this one
      @param axis: The axis to append this matrix on
      @note: Only keeps the headers for the append axis, assumes the other 
                axes are the same
      """
      self.data = np.append(self.data, mtx, axis=axis)
      self.headers[axis].append(mtx.getHeaders(axis=axis))
   
   # ...........................
   def getColumnHeaders(self):
      """
      @summary: Shortcut to get column headers
      @todo: Throw a different exception if no column header?
      """
      return self.getHeaders(axis=1)
   
   # ...........................
   def getHeaders(self, axis=None):
      """
      @summary: Get the headers associated with this Matrix, optionally 
                   limited to a specific axis
      @param axis: If provided, return headers for this axis, else, return all
      """
      if axis is None:
         return self.headers
      else:
         return self.headers[axis]
   
   # ...........................
   def getRowHeaders(self):
      """
      @summary: Shortcut to get row headers
      @todo: Throw a different exception if no row headers?
      """
      return self.getHeaders(axis=0)
   
   # ...........................
   def save(self, flo):
      """
      @summary: Saves the Matrix object as a JSON document to the file-like 
                   object
      @param flo: The file-like object to write to
      """
      writeObj = {}
      writeObj[HEADERS_KEY] = self.headers
      writeObj[DATA_KEY] = self.data.tolist()
      
      json.dump(writeObj, flo, indent=3)
   
   # ...........................
   def setColumnHeaders(self, headers):
      """
      @summary: Shortcut to set column headers
      """
      self.setHeaders(headers, axis=1)
   
   # ...........................
   def setHeaders(self, headers, axis=None):
      """
      @summary: Set the headers for this Matrix, optionally for a specific axis
      @param headers: Matrix headers.  Can be a list of lists, a dictionary
                          of lists, or if axis is provided, a single list
      @param axis: If provided, set the headers for a specific axis, else, 
                      process as if it is for the entire Matrix
      @todo: Validate input for single axis operation?
      @note: Resets headers dictionary when setting values for all headers
      @note: Duck types to use list of lists or dictionary to set values for
                different axes
      """
      if axis is not None:
         self.headers[axis] = headers
      else:
         self.headers = {}
         try:
            headersKeys = headers.keys()
         except: # Not a dictionary
            # Check if first item is a list
            if isinstance(headers[0], list):
               # Assume list of lists
               headersKeys = range(len(headers))
            else:
               # Convert to a list
               headers = [headers]
               headersKeys = [0]
         
         # We should have a list of keys, which could be either dictionary 
         #    keys or list indices
         for k in headersKeys:
            self.headers[k] = headers[k]
   
   # ...........................
   def setRowHeaders(self, headers):
      """
      @summary: Shortcut to set row headers
      """
      self.setHeaders(headers, axis=0)
   
   # ...........................
   def slice(self, *args):
      """
      @summary: Subsets the matrix and returns a new instance
      @param *args: These are iterables for the indices to retrieve
      @note: The first parameter will be for axis 0, second for axis 1, etc
      """
      newData = np.copy(self.data)
      newHeaders = deepcopy(self.headers)
      # For each arg in the list
      for i in range(len(args)):
         # Subset the data matrix
         newData = newData.take(args[i], axis=i)
         # Subset the headers
         tmp = []
         for j in args[i]:
            tmp.append(newHeaders[j])
         newHeaders[i] = tmp
      return Matrix(newData, headers=newHeaders)
   
   # ...........................
   def sliceByHeader(self, header, axis):
      """
      @summary: Gets a slice of the Matrix matching the header provided
      @param header: The name of a header to use for slicing
      @param axis: The axis to find this header
      @raise ValueError: If the header is not found for the specified axis
      @todo: Add capability to slice over multiple axes and multiple headers
                Maybe combine with other slice method and provide method to 
                search for header indices
      """
      idx = self.headers[axis].index(header)
      
      newData = np.copy(np.take(self.data, idx, axis=axis))
      
      # Need to reshape the result.  Take the existing shape and change the 
      #    query axis to 1
      newShape = list(self.data.shape)
      newShape[axis] = 1
      
      # Copy the headers and set the header for the axis to just be the search 
      #    header
      newHeaders = deepcopy(self.headers)
      newHeaders[axis] = [header]
      
      # Return a new Matrix
      return Matrix(newData, headers=newHeaders)
      
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
      # .....................
      # Inner function
      def csvGenerator():
         """
         @summary: This function is a generator that yields rows of values to 
                      be output as CSV
         """
         try:
            rowHeaders = self.headers[0]
         except:
            # No row headers
            rowHeaders = []
         
         # Start with the header row, if we have one
         if self.headers.has_key(1) and self.headers[1]:
            # Add a blank entry if we have row headers
            headerRow = [''] if rowHeaders else []
            headerRow.extend(self.headers[1])
            yield headerRow
         # For each row in the data set
         for i in xrange(self.data.shape[0]):
            # Add the row headers if exists
            row = [rowHeaders[i]] if rowHeaders else []
            # Get the data from the data array
            row.extend(self.data[i].tolist())
            yield row
            
      # .....................
      # Main writeCSV function
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
   

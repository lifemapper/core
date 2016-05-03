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
import numpy
import os
from types import BooleanType

from LmCommon.common.lmconstants import OFTInteger, OFTReal, OFTBinary
from LmServer.base.lmobj import LMObject, LMError


# .............................................................................
class Matrix(LMObject):
   """
   The Matrix class contains a 2-dimensional numeric matrix.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrix, dlocation=None, isCompressed=False, 
                randomParameters={}):
      """
      @param matrix: numpy array
      @param dlocation: file location of the array
      """
      self._matrix = matrix
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
   

   
# ..............................................................................
#    def compress(self, sitesPresent, layersPresent):
#       totalrows = 0
#       for r,rk in enumerate(sorted(sitesPresent.keys())):
#          
#          if self._matrix[r].max() > 0:
#             sitesPresent[rk] = True
#             totalrows += 1
#          else:
#             sitesPresent[rk] = False
#       
#       totalcols = 0
#       for c,ck in enumerate(sorted(layersPresent.keys())):
#          if self._matrix[:,c].max() > 0:
#             layersPresent[ck] = True
#             totalcols += 1
#          else:
#             layersPresent[ck] = False
#       
#       cpam = numpy.zeros([totalrows, totalcols])
#       #if self.sum is not None:
#       #   cvim = numpy.zeros([totalrows, totalcols])
#       #else:
#       #   cvim = None
#          
#       currRow = 0
#       #currCol = 0
#       for r,rk in enumerate(sorted(sitesPresent.keys())):
#          if sitesPresent[rk]:
#             currCol = 0
#             for c,ck in enumerate(sorted(layersPresent.keys())):
#                if layersPresent[ck]:
#                   cpam[currRow, currCol] = self._matrix[r,c]
#                   #if self.sum is not None:
#                   #   cvim[currRow, currCol] = self.sum[r,c]
# 
#                   currCol += 1
#             currRow += 1
#             
#       compressedPam = Matrix(cpam, isCompressed=True)
#       compressedPam.convertToBool()
#       return (compressedPam, sitesPresent, layersPresent)
      
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
         
# # .............................................................................
#    def createSplotch(self, shapegrid, siteCount, layersPresent):
#       """
#       @param shapegrid:  shapegrid object belonging to the experiment
#       @param sitesPresent: sitesPresent dictionary belonging to the bucket
#       @param layersPrsent: layersPresent dictionary belonging to the bucket
#       @todo: Test that this is uncompressed data
#       """      
#       import pysal
#       dlocation = shapegrid.getDLocation()
#       if not self._isCompressed:     
#          if shapegrid._cellsides == 4:
#             numberedges = shapegrid._cellsides
#             neighbormatrix = pysal.rook_from_shapefile(dlocation)
#          elif shapegrid._cellsides == 6:
#             numberedges = shapegrid._cellsides
#             neighbormatrix = pysal.queen_from_shapefile(dlocation)
#          M = self._matrix.copy()
#          numberofcells = neighbormatrix.n
#          for column in layersPresent.keys():
#             edges = {}
#             totalareaincells = sum(M[:,column])
#             zerocolumn = numpy.zeros(siteCount,dtype=numpy.dtype(bool))
#             if totalareaincells > 0:
#                id = random.randrange(0,numberofcells)
#                zerocolumn[id] = True
#                n1 = neighbormatrix.neighbors[id]
#                firstlength = len(n1)
#                edges[id] = numberedges - firstlength
#                area = 1
#                while area < (totalareaincells):
#                   n2 =  neighbormatrix.neighbors[id]
#                   neighborlength = len(n2)
#                   move = 0
#                   while move == 0:
#                      r = random.randrange(0,neighborlength)
#                      nextid = n2[r]
#                      feature = zerocolumn[nextid]
#                      if feature ^ True:
#                         zerocolumn[nextid] = True
#                         n3 = neighbormatrix.neighbors[nextid]
#                         nlength = len(n3)
#                         edges[nextid] = numberedges - nlength
#                         for neighbor in n3:
#                            feature = zerocolumn[neighbor]
#                            if feature and True:
#                               edges[nextid] +=1
#                               edges[neighbor] += 1                                             
#                               if edges[neighbor] == numberedges:
#                                  del edges[neighbor] 
#             
#                         if edges[nextid] == numberedges:
#                            del edges[nextid]       
#                         items = edges.items()
#                         id = items[random.randrange(0,len(edges))][0]
#                         move = 1
#                         area += 1
#             # replace column in M with zerocolumn
#             #print len(zerocolumn)
#             #print M.shape
#             M[:,column] = zerocolumn
#          splotchedPam = Matrix(M, isCompressed=False)
#       else:
#          raise  LMError('Matrix must be uncompressed')
#       return splotchedPam
   
        
# # ..............................................................................
#    def createSwap(self, iterations, sitesPresent, layersPresent):
#       """
#       @summary: This does something cool
#       """
#       if self.isCompressed:
#          # could just do a shape on self._matrix
#          rowLen = sitesPresent.values().count(True)
#          colLen = layersPresent.values().count(True)
#          if rowLen > 1 and colLen > 1:          
#             counter = 0
#             matrixCopy = self._matrix.copy()
#             for x in range(0, iterations):           
#                column1 = random.randrange(0, colLen)
#                column2 = random.randrange(0, colLen)
#                row1 = random.randrange(0, rowLen)                   
#                while column2 == column1:
#                   column2 = random.randrange(0, colLen)             
#                firstcorner = matrixCopy[row1][column1]     
#                if firstcorner ^ matrixCopy[row1][column2]:
#                   row2 = random.randrange(0, rowLen)              
#                   while row2 == row1:
#                         row2 = random.randrange(0, rowLen)
#                   if ((firstcorner ^ matrixCopy[row2][column1]) and 
#                       (not(firstcorner) ^ matrixCopy[row2][column2])):
#                         matrixCopy[row1][column2] = firstcorner
#                         matrixCopy[row2][column1] = firstcorner
#                         matrixCopy[row2][column2] = not(firstcorner)
#                         matrixCopy[row1][column1] = not(firstcorner)                 
#                         counter += 1
#             rdmParams = {'numberOfSwaps': counter, 'numberOfIterations': iterations}
#             swappedPam = Matrix(matrixCopy, isCompressed=True, randomParameters=rdmParams)
#          else:
#             raise LMError('Matrix must have more than one column or row') 
#       else:
#          raise LMError('Matrix must be compressed')
#          
#       return swappedPam



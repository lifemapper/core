"""
@summary: Module containing functions necessary for matrix compression
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import JobStatus

def _getSortedKeysWhereTrue(myDictionary):
   keys = []
   for k in myDictionary.keys():
      if myDictionary[k]:
         keys.append(k)
   return sorted(keys)


# .............................................................................
def compress(matrix, sortedSites):
   """
   @summary: Compress a matrix
   @param matrix: the matrix to be compressed
   @param sitesPresent: a dictionary of siteId keys, corresponding to the 
                        polygons in the shapegrid used to create the matrix, 
                        with all values = True 
   @param layersPresent: a dictionary of layer matrix indexes, corresponding 
                        to the layers used to create the matrix, with all 
                        values = True 
   @return: A compressed matrix, and dictionaries sitesPresent and 
           layersPresent.  Both dictionaries have True or False to correspond 
           with whether the site or layer, respectively, is present in the 
           compressed matrix
   """
   sitesPresent = {}
   
   # sp is like sitesPresent but it is for the sites in the matrix and it is 
   #    numbered sequentially from zero instead of having the feature id as a 
   #    key.  This could probably be combined but would like be too confusing
   sp = {}
   layersPresent = {}
   cmpMtx = None
   totalrows = totalcols = 0
   numRows, numCols = matrix.shape
   try:
      for i in xrange(numRows):
         #for r,rk in enumerate(sorted(sitesPresent.keys())):
         # r and rk should always be the same if there are no gaps
         #if matrix[i].max() > 0: 
         #   sitesPresent[i] = True
         #   totalrows += 1
         #else:
         #   sitesPresent[i] = False
         sitesPresent[sortedSites[i]] = bool(matrix[i].max())
         sp[i] = bool(matrix[i].max())
         totalrows += int(sitesPresent[sortedSites[i]])
      
      #for c,ck in enumerate(sorted(layersPresent.keys())):
      for i in xrange(numCols):
         layersPresent[i] = bool(matrix[:,i].max())
         totalcols += int(layersPresent[i])
#          if matrix[:,i].max() > 0:
#             layersPresent[i] = True
#             totalcols += 1
#          else:
#             layersPresent[i] = False
       
      cpam = numpy.zeros([totalrows, totalcols])
      cols = _getSortedKeysWhereTrue(layersPresent)
      #rows = _getSortedKeysWhereTrue(sitesPresent)
      rows = _getSortedKeysWhereTrue(sp)
      
      for j in xrange(len(rows)):
         for i in xrange(len(cols)):
            cpam[j, i] = matrix[rows[j], cols[i]]
#       for r,rk in enumerate(sorted(sitesPresent.keys())):
#          if sitesPresent[rk]:
#             currCol = 0
#             for c,ck in enumerate(sorted(layersPresent.keys())):
#                if layersPresent[ck]:
#                   cpam[currRow, currCol] = matrix[r,c]
#                   currCol += 1
#             currRow += 1
             
      cmpMtx = numpy.bool_(cpam)
      status = JobStatus.COMPLETE
   except Exception, e:
      print str(e)
      status = JobStatus.RAD_COMPRESS_ERROR
      
   return status, cmpMtx, sitesPresent, layersPresent


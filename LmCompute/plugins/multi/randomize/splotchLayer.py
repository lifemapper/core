"""
@summary: Module containing methods to perform a splotch randomization of a 
             layer

@version: 3.0.0
@status: beta

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
import pysal
from random import randrange

# .............................................................................
def splotchLayer(neighborMtx, totalAreaInCells, 
                 cellCount, cellsideCount, siteCount, fileName):
   """
   @summary: Randomize an uncompressed matrix using the Splotch method
   @param matrix: the uncompressed matrix to be Splotched
   @param cellsideCount: number of sides in each shapefile cell, either 4 or 6
   @param shapefile: location of gridded shapefile (written as a temporary ESRI  
                     shapefile to the filesystem) describing the area to be 
                     randomized with the splotch algorithm
   @param siteCount: a count of all polygons in the shapegrid used to create 
                     the matrix
   @param layersPresent: a dictionary of layer matrix indexes, corresponding 
                        to the layers used to create the matrix, with 
                        values = True/False, corresponding with layer
                        presence in the compressed matrix
   @return: An uncompressed, splotched matrix
   """
   edges = {}
   if totalAreaInCells <> siteCount:
      newColumn = numpy.zeros(siteCount, dtype=numpy.dtype(bool))
      if totalAreaInCells > 0:
         id = randrange(0, cellCount)
         newColumn[id] = True
         n1 = neighborMtx.neighbors[id]
         firstlength = len(n1)
         edges[id] = cellsideCount - firstlength
         area = 1
         while area < (totalAreaInCells):
            n2 =  neighborMtx.neighbors[id]
            neighborlength = len(n2)
            move = 0
            while move == 0:
               r = randrange(0, neighborlength)
               nextid = n2[r]
               feature = newColumn[nextid]
               if feature ^ True:
                  newColumn[nextid] = True
                  n3 = neighborMtx.neighbors[nextid]
                  nlength = len(n3)
                  edges[nextid] = cellsideCount - nlength
                  for neighbor in n3:
                     feature = newColumn[neighbor]
                     if feature and True:
                        edges[nextid] +=1
                        edges[neighbor] += 1                                             
                        if edges[neighbor] == cellsideCount:
                           del edges[neighbor] 
      
                  if edges[nextid] == cellsideCount:
                     del edges[nextid]       
                  items = edges.items()
                  id = items[randrange(0,len(edges))][0]
                  move = 1
                  area += 1
   else:
      newColumn = numpy.ones(siteCount,dtype=numpy.dtype(bool))
   return newColumn

# .............................................................................
def boolStr(val):
   if val.strip().lower() == "false":
      return False
   else:
      return True

# .............................................................................
def getAttributeOrDefault(obj, attribute, default=None, func=str):
   try:
      return func(obj.__getattribute__(attribute).strip())
   except:
      return default
   
# .............................................................................
if __name__ == "__main__":
   import sys
   from LmCommon.common.lmXml import deserialize, fromstring
   if len(sys.argv) >= 2:
      obj = deserialize(fromstring(sys.argv[1]))
      
      cellSideCount = int(obj.SplotchLayerObj.cellsideCount)
      
      if cellSideCount == 4:
         neighborMtx = pysal.rook_from_shapefile(obj.SplotchLayerObj.shapegridLocation)
      elif cellSideCount == 6:
         neighborMtx = pysal.queen_from_shapefile(obj.SplotchLayerObj.shapegridLocation)

      cellCount = neighborMtx.n
      
      layerArray = splotchLayer(neighborMtx, 
                                int(obj.SplotchLayerObj.totalAreaInCells), 
                                cellCount, 
                                cellSideCount, 
                                int(obj.SplotchLayerObj.siteCount),
                                obj.SplotchLayerObj.fileName)
       
      numpy.save(obj.SplotchLayerObj.fileName, layerArray)

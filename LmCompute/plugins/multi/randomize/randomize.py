"""
@summary: Module containing functions to randomize a RAD PAM
@author: Lifemapper Team; lifemapper@ku.edu
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
import os
# import numpy
# import pysal
from random import randrange

from LmCommon.common.lmconstants import JobStatus, OutputFormat
from LmBackend.common.subprocessManager import SubprocessManager, \
                                               VariableContainer


# TODO: Move to constants file
SPLOTCH_LAYER_SCRIPT = "rad/randomize/splotchLayer.py"

MAX_TRIES_WITHOUT_SWAP = 1000000

# .............................................................................
def swap(matrix, numSwaps):
   """
   @summary: Randomize a compressed matrix using the Swap method
   @param matrix: the compressed matrix to be Swapped
   @param sitesPresent: a dictionary of siteId keys, corresponding to the 
                        polygons in the shapegrid used to create the matrix, 
                        with values = True/False, corresponding with site
                        presence in the compressed matrix
   @param layersPresent: a dictionary of layer matrix indices, corresponding 
                        to the layers used to create the matrix, with 
                        values = True/False, corresponding with layer
                        presence in the compressed matrix
   @param numSwaps: The number of successful swaps to perform
   @return: A compressed, randomized matrix and the number of swaps executed
   """
   counter = 0
   numTries = 0
   swappedMtx = None
   rowLen,colLen = matrix.shape

   if rowLen <= 1 or colLen <= 1:
      status = JobStatus.RAD_SWAP_TOO_FEW_COLUMNS_OR_ROWS_ERROR
   else:          
      try:
         swappedMtx = matrix.copy()
         while counter < numSwaps and numTries < MAX_TRIES_WITHOUT_SWAP: #numTries is a safety to kill the loop if nothing is ever found
            numTries += 1
            column1 = randrange(0, colLen)
            column2 = randrange(0, colLen)
            row1 = randrange(0, rowLen)                   
            while column2 == column1:
               column2 = randrange(0, colLen)             
            firstcorner = swappedMtx[row1][column1]     
            if firstcorner ^ swappedMtx[row1][column2]:
               row2 = randrange(0, rowLen)              
               while row2 == row1:
                     row2 = randrange(0, rowLen)
               if ((firstcorner ^ swappedMtx[row2][column1]) and 
                   (not(firstcorner) ^ swappedMtx[row2][column2])):
                     swappedMtx[row1][column2] = firstcorner
                     swappedMtx[row2][column1] = firstcorner
                     swappedMtx[row2][column2] = not(firstcorner)
                     swappedMtx[row1][column1] = not(firstcorner)                 
                     counter += 1
                     numTries = 0
         status = JobStatus.COMPLETE
         
      except Exception, e:
         status = JobStatus.RAD_SWAP_ERROR
      
   return status, swappedMtx, counter
   
# .............................................................................
class SplotchLayerObj(object):
   """
   @summary: Splotch Layer object that goes inside of a VariableContainer for 
             serialization and then passed to a subprocess
   """
   def __init__(self, shapegridLocation, totalAreaInCells, cellsideCount, siteCount, fileName):
#    def __init__(self, neighborMtx, totalAreaInCells, cellCount, cellsideCount, 
#                 siteCount, fileName):
#       self.neighborMtx = neighborMtx 
      self.shapegridLocation = shapegridLocation
      self.totalAreaInCells = totalAreaInCells  
#       self.cellCount = cellCount
      self.cellsideCount = cellsideCount
      self.siteCount = siteCount
      self.fileName = fileName

# .............................................................................
def splotch(matrix, shapegrid, siteCount, layersPresent, env, outputDir=None):
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
   scriptLocation = os.path.join(env.getPluginsPath(), SPLOTCH_LAYER_SCRIPT)
   lyrArrays = {}
   splotchedMtx = None
   cellsideCount = int(shapegrid['cellsideCount'])
#    try:
#       if cellsideCount == 4:
#          neighborMtx = pysal.rook_from_shapefile(shapegrid['dlocation'])
#       elif cellsideCount == 6:
#          neighborMtx = pysal.queen_from_shapefile(shapegrid['dlocation'])
#    except Exception, e:
#       status = JobStatus.RAD_SPLOTCH_PYSAL_NEIGHBOR_ERROR
#       return status, splotchedMtx

   try:
      splotchedMtx = matrix.copy()
   except Exception, e:
      print 'Unable to copy matrix'
      status = JobStatus.RAD_SPLOTCH_ERROR
      return status, splotchedMtx
   
   try:
#       cellCount = neighborMtx.n
      cmds = []
      layerArrayFnames = {}
      for mtxIdx in layersPresent.keys():
         totalAreaInCells = sum(splotchedMtx[:,int(mtxIdx)]).ravel().tolist()[0][0]
         fname = env.getTemporaryFilename(OutputFormat.NUMPY, base=outputDir)
         
         shapegridLocation = shapegrid['dlocation']
         
         vc = VariableContainer(SplotchLayerObj(shapegridLocation, totalAreaInCells, 
                                #cellCount, 
                                cellsideCount, siteCount, fname))
         layerArrayFnames[mtxIdx] = fname
         
         cmd = "{python} {scriptLocation} \"{args}\"".format(
                  python=env.getPythonCmd(),
                  scriptLocation=scriptLocation,
                  args=str(vc))
         cmds.append(cmd)
         
      spm = SubprocessManager(commandList=cmds)
      spm.runProcesses()
      
      for key in layerArrayFnames.keys():
         fname = layerArrayFnames[key]
         lyrArrays[key] = fname
      
#       splotchedMtx[:,column] = newColumn   
      status = JobStatus.COMPLETE

   except Exception, e:
      print str(e)
      status = JobStatus.RAD_SPLOTCH_ERROR

   return status, splotchedMtx
      


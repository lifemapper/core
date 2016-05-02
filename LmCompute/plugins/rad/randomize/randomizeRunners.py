"""
@summary: Module containing process runners to perform RAD randomizations
@author: CJ Grady
@version: 3.0.0
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
import os

from LmCommon.common.lmconstants import JobStatus, OutputFormat, ProcessType

from LmCompute.common.layerManager import LayerManager
from LmCompute.common.lmconstants import LayerFormat
from LmCompute.jobs.runners.pythonRunner import PythonRunner
from LmCompute.plugins.rad.common.matrixTools import getNumpyMatrixFromCSV
from LmCompute.plugins.rad.randomize.randomize import splotch, swap
from LmCompute.plugins.rad.randomize.grady import gradyRandomize

# .............................................................................
class _RandomizeRunner(PythonRunner):
   """
   @summary: RAD randomize job runner base class
   """
   PROCESS_TYPE = None
   
   # .......................................
   def _push(self):
      """
      @summary: Pushes the results of the job to the job server
      """
      if self.status < JobStatus.GENERAL_ERROR:
         self.status = JobStatus.COMPLETE
         component = "matrix"
         contentType = "application/octet-stream"
         
         self.outputMatrxFN = self.env.getTemporaryFilename(OutputFormat.NUMPY)
         print "Writing randomized matrix to:", self.outputMatrxFN 
         # Save to temporary file
         numpy.save(self.outputMatrxFN, self.randomMatrix)
         
         content = open(self.outputMatrxFN, 'rb').read()
         
         self._update()
         try:
            self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, 
                                contentType, component)
         except Exception, e:
            try:
               self.log.debug(str(e))
            except: # Log not initialized
               pass
            self.status = JobStatus.PUSH_FAILED
            self._update()
      else:
         component = "error"
         content = None

# .............................................................................
class RandomizeGradyRunner(_RandomizeRunner):
   """
   @summary: RAD randomize Grady job runner
   """
   PROCESS_TYPE = ProcessType.RAD_GRADY

   # ...................................
   def _doWork(self):

      self.status, self.randomMatrix = gradyRandomize(self.matrix)
      
   # ...................................
   def _processJobInput(self):
      self.log.debug("Start of process job input")
      
      import time
      t1 = time.clock()
      self.matrix = getNumpyMatrixFromCSV(csvUrl=self.job.matrix.url)
      t2 = time.clock()
      print("Time to convert: %s seconds" % (t2-t1))

# .............................................................................
class RandomizeSplotchRunner(_RandomizeRunner):
   """
   @summary: RAD randomize splotch job runner
   """
   PROCESS_TYPE = ProcessType.RAD_SPLOTCH
   
   # ...................................
   def _doWork(self):
      self.status, self.randomMatrix = splotch(self.matrix, self.shapegrid, 
                                            self.siteCount, self.layersPresent, self.env)
   
   # ...................................
   def _processJobInput(self):
      self.log.debug("Start of process job input")
      print self.job.matrix.url
       
      lyrMgr = LayerManager(self.env.getJobDataPath())
      sgLayerId = self.job.shapegrid.identifier
      sgUrl = self.job.shapegrid.shapegridUrl
      
      self.shapegrid = {
                   'dlocation' : lyrMgr.getLayerFilename(sgLayerId, 
                                                         LayerFormat.SHAPE, 
                                                         layerUrl=sgUrl),
                   'localIdIdx' : self.job.shapegrid.localIdIndex,
                   'cellsideCount' : self.job.shapegrid.cellSides
                  }
      
      self.matrix = getNumpyMatrixFromCSV(csvUrl=self.job.matrix.url)
      
      # Needs to be a dictionary
      self.layersPresent = {}
      for lyr in self.job.layersPresent.layer:
         # Only need the keys for splotch
         self.layersPresent[lyr.key] = True
   
      self.siteCount = len(self.job.sitesPresent.site)
   
# .............................................................................
class RandomizeSwapRunner(_RandomizeRunner):
   """
   @summary: RAD randomize swap job runner
   """
   PROCESS_TYPE = ProcessType.RAD_SWAP
   
   # ...................................
   def _doWork(self):

      self.status, self.randomMatrix, self.counter = swap(self.matrix,  
                                                           self.numSwaps)
   # ...................................
   def _processJobInput(self):
      self.log.debug("Start of process job input")
      
      self.matrix = getNumpyMatrixFromCSV(csvUrl=self.job.matrix.url)

      self.numSwaps = int(self.job.numSwaps)

"""
@summary: Module containing process runners to perform RAD compressions
@author: CJ Grady
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
import os
import pickle
from StringIO import StringIO
import zipfile
#from LmCompute.common.layerManager import getAndStoreShapefile, LayerManager

from LmCommon.common.lmconstants import JobStatus, OutputFormat, ProcessType
from LmCommon.common.lmXml import Element, SubElement, tostring

from LmCompute.jobs.runners.pythonRunner import PythonRunner
from LmCompute.plugins.rad.common.matrixTools import getNumpyMatrixFromCSV
from LmCompute.plugins.rad.compress.compress import compress

# .............................................................................
class CompressRunner(PythonRunner):
   """
   @summary: RAD compress job runner class
   """
   PROCESS_TYPE = ProcessType.RAD_COMPRESS
   
   # .......................................
   def _finishJob(self):
      """
      @summary: Move outputs we want to keep to the specified location
      @todo: Determine if anything else should be moved
      @todo: Should we take a name parameter?
      @todo: What should file names be?
      """
      # Options to keep:
      #  metrics
      
      if self.outDir is not None:
         writeDir = self.outDir
      else:
         writeDir = self.workDir

      # Write compressed matrix         
      self.outputMatrixFN = os.path.join(writeDir, "{jobName}{ext}".format(
               jobName=self.jobName, ext=OutputFormat.NUMPY))
      
      print "Writing compressed matrix to:", self.outputMatrxFN 
      numpy.save(self.outputMatrxFN, self.compressedMatrix)

      # Write sites present dictionary as pickle
      spPklFn = os.path.join(writeDir, "{jobName}-sitesPresent{ext}".format(
         jobName=self.jobName, ext=OutputFormat.PICKLE))
      pickle.dump(self.sitesPresentMod, spPklFn)

      # Write layers present dictionary as pickle
      lpPklFn = os.path.join(writeDir, "{jobName}-layersPresent{ext}".format(
         jobName=self.jobName, ext=OutputFormat.PICKLE))
      pickle.dump(self.layersPresentMod, lpPklFn)

   # ...................................
   def _doWork(self):
      self.status, self.compressedMatrix, self.sitesPresentMod, \
         self.layersPresentMod = compress(self.matrix, self.sortedSites)

   # ...................................
   def _processJobInput(self):
      self.log.debug("Start of process job input")
      
      self.sortedSites = []
      for site in self.job.sitesPresent.site:
         self.sortedSites.append(int(site.key))
      self.sortedSites.sort()
      
      self.matrix = getNumpyMatrixFromCSV(csvUrl=self.job.matrix.url)

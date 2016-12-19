"""
@summary: Module containing GBIF process runners
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
import glob
import os
import shutil
from StringIO import StringIO
from time import sleep
import zipfile

from LmCommon.common.lmconstants import JobStatus, ProcessType, \
                                        SHAPEFILE_EXTENSIONS

from LmCompute.common.lmObj import LmException
from LmCompute.jobs.runners.pythonRunner import PythonRunner

from LmCompute.plugins.single.gbif.gbif import parseGBIFData

# .............................................................................
class GBIFRetrieverRunner(PythonRunner):
   """
   @summary: Process runner to retrieve occurrence data from GBIF from a 
                download key
   """
   PROCESS_TYPE = ProcessType.GBIF_TAXA_OCCURRENCE

   # ...................................
   def __init__(self, pointsCsvFn, count, maxPoints, jobName=None, outName=None, 
                      outDir=None, workDir=None, metricsFn=None, logFn=None, 
                      logLevel=None, statusFn=None):
      """
      @summary: Constructor for GBIF points processing
      @param pointsCsvFn: A path to a CSV file with raw points
      @param count: The reported count for the raw csv file
      @param maxPoints: The maximum number of points to include before subsetting
      @param outName: (optional) This will be used to name the output shapefiles
      """
      if os.path.exists(pointsCsvFn):
         with open(pointsCsvFn) as inF:
            self.csvInputBlob = inF.read()
      else:
         raise LmException(JobStatus.IO_NOT_FOUND, 
                           "Could not open raw CSV: {0}".format(pointsCsvFn))
      
      self.count = count
      self.maxPoints = maxPoints
      PythonRunner.__init__(self, jobName=jobName, outDir=outDir, 
                            workDir=workDir, metricsFn=metricsFn, logFn=logFn,
                            logLevel=logLevel, statusFn=statusFn)
      if outName is None:
         self.outName = self.jobName
      else:
         self.outName = outName

   # ...................................
   def _processJobInput(self):
      # Get the job inputs
      self.count = int(self.job.count)
      if self.count < 0:
         self.count = len(self.csvInputBlob.split('\n'))
      # Set job outputs
      self.shapefileLocation = None 
      self.subsetLocation = None
      
   # ...................................
   def _doWork(self):
      # Write and optionally subset points
      self.shapefileLocation, self.subsetLocation = parseGBIFData(self.count, 
                                                               self.csvInputBlob, 
                                                               self.outputPath, 
                                                               self.maxPoints,
                                                               self.outName)
      
   # ...................................
   def _getFiles(self, shapefileName):
      if shapefileName is not None:
         return glob.iglob('%s*' % os.path.splitext(shapefileName)[0])
      else:
         return []
      
   # .......................................
   def _finishJob(self):
      """
      @summary: Move outputs we want to keep to the specified location
      @todo: Determine if anything else should be moved
      """
      # Options to keep:
      #  metrics
      
      if self.outDir is not None:
         # Main shapefile
         for f in self._getFiles(self.shapefileLocation):
            ext = os.path.splitext(f)[1]
            if ext in SHAPEFILE_EXTENSIONS:
               shutil.move(f, self.outDir)
         
         # Subset
         if self.subsetLocation is not None:
            for f in self._getFiles(self.subsetLocation):
               ext = os.path.splitext(f)[1]
               if ext in SHAPEFILE_EXTENSIONS:
                  shutil.move(f, self.outDir)
         

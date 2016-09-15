"""
@summary: Module containing Bison job runner hooks
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
from urllib import urlencode
import urllib2
import zipfile

from LmCommon.common.lmconstants import BISON_FILTERS, BISON_OCC_FILTERS, \
                                   JobStatus, ProcessType, SHAPEFILE_EXTENSIONS

from LmCompute.jobs.runners.pythonRunner import PythonRunner
from LmCompute.plugins.single.bison.bison import createBisonShapefileFromUrl

# .............................................................................
class BisonRetrieverRunner(PythonRunner):
   """
   @summary: Process runner to retrieve occurrence data from a BISON url
   """
   PROCESS_TYPE = ProcessType.BISON_TAXA_OCCURRENCE

   # ...................................
   def _processJobInput(self):
      # Get the job inputs
      self.pointsUrl = self.job.pointsUrl
      
      # TODO: Remove testing URL below; fix parsing of URL string in 
      #       LmCompute.environment.testEnv  TestEnv.requestJob
      moreFilters = BISON_OCC_FILTERS.copy()
      for k,v in BISON_FILTERS.iteritems():
         moreFilters[k] = v
      moreUrl = urlencode(moreFilters)
      self.pointsUrl = '&'.join([self.job.pointsUrl, moreUrl])

      self.maxPoints = int(self.job.maxPoints)
      # Set job outputs
      self.shapefileLocation = None 
      self.subsetLocation = None
      
   # ...................................
   def _doWork(self):
      # Write and optionally subset points
      try:
         self.shapefileLocation, self.subsetLocation = \
            createBisonShapefileFromUrl(self.pointsUrl, self.workDir, 
                                        self.maxPoints, self.jobName)
      except urllib2.HTTPError, e:
         # The HTTP_GENERAL_ERROR status is 4000, each of the HTTP error codes
         #   corresponds to 4000 + the HTTP error code
         #    Ex: HTTP 500 error -> HTTP_SERVER_INTERNAL_SERVER_ERROR = 4500
         self.status = JobStatus.HTTP_GENERAL_ERROR + int(e.code)
      except Exception, e:
         self.status = JobStatus.UNKNOWN_CLUSTER_ERROR
      
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
      @todo: Should we take a name parameter?
      @todo: What should file names be?
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
         
      
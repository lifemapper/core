"""
@summary: Module containing GBIF process runners
@author: CJ Grady
@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
from StringIO import StringIO
from time import sleep
import urllib2
import zipfile

from LmCommon.common.lmconstants import JobStatus, ProcessType, \
                                        SHAPEFILE_EXTENSIONS
from LmCompute.jobs.runners.pythonRunner import PythonRunner
from LmCompute.plugins.sdm.idigbio.idigbio import parseIDigData

SLEEP_TIME = 600 # Ten minutes

# .............................................................................
class IDIGBIORetrieverRunner(PythonRunner):
   """
   @summary: Process runner to retrieve occurrence data from GBIF from a 
                download key
   """
   PROCESS_TYPE = ProcessType.IDIGBIO_TAXA_OCCURRENCE

   # ...................................
   def _processJobInput(self):
      # Get the job inputs
      self.taxonKey = self.job.taxonKey
      self.maxPoints = int(self.job.maxPoints)
      # Set job outputs
      self.shapefileLocation = None 
      self.subsetLocation = None
      
   # ...................................
   def _doWork(self):
      # Write and optionally subset points
      try:
         self.shapefileLocation, self.subsetLocation = parseIDigData(
                  self.taxonKey, self.outputPath, self.env, self.maxPoints)
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
      
   # ...................................
   def _wait(self):
      sleep(SLEEP_TIME)
      
   # ...................................
   def _push(self):
      """
      @summary: Pushes the results of the job to the job server
      """
      self._pushPackage()
   
   # ...................................
   def _pushPackage(self):
      """
      @summary: Assembles and pushes the GBIF data package
      """
      contentType = "application/zip"
      component = "package"
      
      outStream = StringIO()
      
      with zipfile.ZipFile(outStream, 'w', compression=zipfile.ZIP_DEFLATED,
                              allowZip64=True) as zf:
         # Main shapefile
         for f in self._getFiles(self.shapefileLocation):
            ext = os.path.splitext(f)[1]
            if ext in SHAPEFILE_EXTENSIONS:
               zf.write(f, 'points-%s%s' % (self.job.jobId, ext))
         
         if self.subsetLocation is not None:
            for f in self._getFiles(self.subsetLocation):
               ext = os.path.splitext(f)[1]
               if ext in SHAPEFILE_EXTENSIONS:
                  zf.write(f, 'subset-%s%s' % (self.job.jobId, ext))
      outStream.seek(0)
      content = outStream.getvalue()
      self._update()
      
      try:
         self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, 
                          contentType, component)
      except Exception, e:
         try:
            self.log.debug(str(e))
         except:
            pass
         self.status = JobStatus.PUSH_FAILED
         self._update()
   
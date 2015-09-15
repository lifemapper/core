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
import zipfile

from LmCommon.common.lmconstants import JobStatus, ProcessType, \
                                        SHAPEFILE_EXTENSIONS

from LmCompute.jobs.runners.pythonRunner import PythonRunner

from LmCompute.plugins.sdm.gbif.gbif import parseGBIFData

SLEEP_TIME = 600 # Ten minutes

# .............................................................................
class GBIFRetrieverRunner(PythonRunner):
   """
   @summary: Process runner to retrieve occurrence data from GBIF from a 
                download key
   """
   PROCESS_TYPE = ProcessType.GBIF_TAXA_OCCURRENCE

   # ...................................
   def _processJobInput(self):
      # Get the job inputs
      self.maxPoints = int(self.job.maxPoints)
      self.csvInputBlob = self.job.points
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
                                                               self.env,
                                                               self.maxPoints)
      
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
      #self._pushShapefile("shapefile", self._getFiles(self.shapefileLocation))
      #if self.subsetLocation is not None:
      #   self._pushShapefile("subset", self._getFiles(self.subsetLocation))
   
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
   
#    # ...................................
#    def _pushShapefile(self, component, fileList):
#       """
#       @summary: Performs the push for a shapefile
#       """
#       contentType = "application/zip"
#       # Read shapefile into StringIO object
#       outStream = StringIO()
#       with zipfile.ZipFile(outStream, 'w') as zf:
#          # Determine all files to include
#          for f in fileList:
#             if os.path.splitext(f)[1] in SHAPEFILE_EXTENSIONS:
#                zf.write(f, os.path.basename(f))
#       # Post shapefile
#       outStream.seek(0)
#       content = outStream.getvalue()
#       self._update()
#       try:
#          self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, 
#                           contentType, component)
#       except Exception, e:
#          try:
#             self.log.debug(str(e))
#          except:
#             pass
#          self.status = JobStatus.PUSH_FAILED
#          self._update()

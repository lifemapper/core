"""
@summary: Module containing process runners to perform RAD compressions
@author: CJ Grady
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
import os
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
   def _push(self):
      """
      @summary: Pushes the results of the job to the job server
      """
      if self.status < JobStatus.GENERAL_ERROR:
         self.status = JobStatus.COMPLETE

         component = "package"
         contentType = "application/zip"
         
         # Write matrix to temporary file
         self.outputMatrxFN = self.env.getTemporaryFilename(OutputFormat.NUMPY)
         print "Writing randomized matrix to:", self.outputMatrxFN 
         numpy.save(self.outputMatrxFN, self.compressedMatrix)

         # Sites present XML document
         spEl = Element("sitesPresent")
         for key in self.sitesPresentMod.keys():
            SubElement(spEl, 'site', value=str(self.sitesPresentMod[key]),
                       attrib={'id': str(key)})
         
         # Layers present XML document
         lyrEl = Element('layersPresent')
         for key in self.layersPresentMod.keys():
            SubElement(lyrEl, 'layer', value=str(self.layersPresentMod[key]),
                       attrib={'id': str(key)})

         # Initialize zip file
         outStream = StringIO()
         with zipfile.ZipFile(outStream, 'w', compression=zipfile.ZIP_DEFLATED, 
                                 allowZip64=True) as zf:
            # Write log file
            zf.write(self.jobLogFile, os.path.split(self.jobLogFile)[1])
            
            # Write XML files
            sitesPresXmlString = StringIO(tostring(spEl))
            sitesPresXmlString.seek(0)
            zf.writestr("sitesPresent.xml", sitesPresXmlString.getvalue())

            lyrsPresXmlString = StringIO(tostring(lyrEl))
            lyrsPresXmlString.seek(0)
            zf.writestr("layersPresent.xml", lyrsPresXmlString.getvalue())

            # Write Matrix
            zf.write(self.outputMatrxFN, 'pam.npy')
            

         outStream.seek(0)
         content = outStream.getvalue()
         

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

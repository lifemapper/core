"""
@summary: Module containing the job runner for the RAD intersect process
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
import os
from StringIO import StringIO
import zipfile

from LmCommon.common.lmAttObject import LmAttList
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.lmXml import Element, SubElement, tostring

from LmCompute.common.layerManager import LayerManager
from LmCompute.jobs.runners.pythonRunner import PythonRunner
from LmCompute.plugins.rad.intersect.radIntersect import intersect
from LmCompute.common.lmconstants import LayerFormat

# .............................................................................
class IntersectRunner(PythonRunner):
   """
   @summary: RAD intersect job runner
   """
   PROCESS_TYPE = ProcessType.RAD_INTERSECT
   
   # ...................................
   def _processJobInput(self):
      self.log.debug("Start of process job input")
      lyrMgr = LayerManager(self.env.getJobDataPath())
      
      self.log.debug("Layer manager has been initialized")
      sgLayerId = self.job.shapegrid.identifier
      sgUrl = self.job.shapegrid.shapegridUrl
      
      self.shapegrid = {
                        #TODO: Make sure this works correctly
                   'dlocation' : lyrMgr.getLayerFilename(sgLayerId, 
                                                         LayerFormat.SHAPE, 
                                                         layerUrl=sgUrl),
                   #getAndStoreShapefile(sgUrl, vectorPath),
                   #'dlocation' : lyrMgr.getLayerFilename(sgUrl),
                   'localIdIdx' : self.job.shapegrid.localIdIndex
                  }
      
      self.layers = {}
      self.log.debug("Processing layers")
      
      if isinstance(self.job.layerSet.layer, LmAttList):
         lyrObjs = self.job.layerSet.layer
      else:
         lyrObjs = [self.job.layerSet.layer]
      
      for lyr in lyrObjs:
         self.log.debug(" -- Processing layer %s" % lyr.index)
         lyrVals = {
                   }
         if lyr.isRaster.lower() != "false":
            lyrVals['isRaster'] = True
            lyrVals['resolution'] = lyr.resolution
            lyrFrmt = LayerFormat.GTIFF
         else:
            lyrVals['isRaster'] = False
            lyrFrmt = LayerFormat.SHAPE
            
         lyrVals['dlocation'] = lyrMgr.getLayerFilename(lyr.identifier, 
                                             lyrFrmt, layerUrl=lyr.layerUrl)

         try:
            lyrVals['isOrganism'] = True
            lyrVals['attrPresence'] = lyr.attrPresence
            lyrVals['minPresence'] = lyr.minPresence
            lyrVals['maxPresence'] = lyr.maxPresence
            lyrVals['percentPresence'] = lyr.percentPresence
            lyrVals['attrAbsence'] = lyr.attrAbsence
            lyrVals['minAbsence'] = lyr.minAbsence
            lyrVals['maxAbsence'] = lyr.maxAbsence
            lyrVals['percentAbsence'] = lyr.percentAbsence
         except:
            try:
               lyrVals['isOrganism'] = False
               lyrVals['attrValue'] = lyr.attrValue
               lyrVals['weightedMean'] = lyr.weightedMean
               lyrVals['largestClass'] = lyr.largestClass
               lyrVals['minPercent'] = lyr.minPercent
            except:
               raise Exception('Must provide PA or Anc Layer')
         self.layers[lyr.index] = lyrVals
      
   # ...................................
   def _doWork(self):
      self.status, self.layerArrays = intersect(self.layers, self.shapegrid, 
                                           self.env, outputDir=self.outputPath)
   
   # .......................................
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
         
   def _push(self):
      """
      @summary: Pushes the results of the job to the job server
      """
      if self.status < JobStatus.GENERAL_ERROR:
         self.status = JobStatus.COMPLETE
         component = "pam"
         contentType = "application/zip"
         
         # Initialize XML document
         lyrEl = Element("layers")
         
         # Initialize zip file
         outStream = StringIO()
         with zipfile.ZipFile(outStream, 'w', compression=zipfile.ZIP_DEFLATED,
                                 allowZip64=True) as zf:
            for key in self.layerArrays.keys():
               fullFilePath = self.layerArrays[key]
               self.log.debug("Adding %s" % fullFilePath)
               fn = os.path.split(fullFilePath)[1]
               # Add layer entry to XML
               el = SubElement(lyrEl, "layer")
               SubElement(el, "index", value=key)
               SubElement(el, "filename", value=fn)
               zf.write(fullFilePath, fn)
            
            # Write XML file
            xmlString = StringIO(tostring(lyrEl))
            xmlString.seek(0)
            zf.writestr("layerIndex.xml", xmlString.getvalue())

            # Write log file
            zf.write(self.jobLogFile, os.path.split(self.jobLogFile)[1])

         outStream.seek(0)
         content = outStream.getvalue()
         
         try:
            self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, 
                                contentType, component)
            #self.status = JobStatus.COMPLETE
            #self._update()
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

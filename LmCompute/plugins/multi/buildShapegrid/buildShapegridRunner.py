"""
@summary: Module containing build shapegrid process runner
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
from StringIO import StringIO
import zipfile

from LmCommon.common.lmconstants import JobStatus, ProcessType, \
                                        SHAPEFILE_EXTENSIONS

from LmCompute.jobs.runners.pythonRunner import PythonRunner
from LmCompute.plugins.multi.buildShapegrid.radBuildShapegrid import buildShapegrid

# .............................................................................
class BuildShapegridRunner(PythonRunner):
   """
   @summary: Process runner to build a shapegrid from the provided inputs
   """
   PROCESS_TYPE = ProcessType.RAD_BUILDGRID

   # ...................................
   def _processJobInput(self):
      # Get the job inputs
      self.minX = float(self.job.minX)
      self.minY = float(self.job.minY)
      self.maxX = float(self.job.maxX)
      self.maxY = float(self.job.maxY)
      self.cellSize = float(self.job.cellSize)
      self.epsgCode = int(self.job.epsgCode)
      self.cellSides = int(self.job.cellSides)
      
      try:
         self.siteId = self.job.siteId
      except:
         self.siteId = 'siteid'
      
      try:
         self.siteX = self.job.siteX
      except:
         self.siteX = 'centerX'
         
      try:
         self.siteY = self.job.siteY
      except:
         self.siteY = 'centerY'
         
      try:
         self.cutoutWKT = self.job.cutoutWKT
      except:
         self.cutoutWKT = None
      
   # ...................................
   def _doWork(self):
      # Build the shapegrid
      self.shapegridLocation, self.status = buildShapegrid(
                                                   self.workDir,
                                                   self.minX,
                                                   self.minY,
                                                   self.maxX,
                                                   self.maxY,
                                                   self.cellSize,
                                                   self.epsgCode,
                                                   self.cellSides,
                                                   siteId=self.siteId,
                                                   siteX=self.siteX,
                                                   siteY=self.siteY,
                                                   cutoutWKT=self.cutoutWKT)
      
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
         # Write shapegrid to output location
         for f in self._getFiles(self.shapegridLocation):
            ext = os.path.splitext(f)[1]
            if ext in SHAPEFILE_EXTENSIONS:
               shutil.move(f, os.path.join(self.outDir, "{name}{ext}".format(
                                                  name=self.jobName, ext=ext)))
         
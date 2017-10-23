"""
@summary: Module containing the job runners for openModeller models and 
             projections
@author: CJ Grady
@version: 4.0.0
@status: beta

@note: Commands are for openModeller library version 1.3.0
@note: Commands may be backwards compatible

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
@todo: Metrics keys
"""
import json
import ogr
import os
import shutil
import time
import zipfile

from LmBackend.common.subprocessManager import SubprocessRunner
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCompute.common.layerManager import LayerManager
from LmCompute.common.lmObj import LmException
from LmCompute.common.lmconstants import LayerFormat
from LmCompute.common.localconstants import SHARED_DATA_PATH
from LmCompute.common.log import LmComputeLogger
from LmCompute.common.lmconstants import BIN_PATH
from LmCompute.plugins.single.openModeller.constants import (OM_VERSION,
                                          DEFAULT_LOG_LEVEL, OM_MODEL_CMD, 
                                          OM_PROJECT_CMD)
from LmCompute.plugins.single.openModeller.omRequest import (OmModelRequest, 
                                                          OmProjectionRequest)

# .............................................................................
class OpenModellerModel(object):
   """
   @summary: Class containing methods to create an SDM model using openModeller
   """
   PROCESS_TYPE = ProcessType.OM_MODEL
   # ...................................
   def __init__(self, jobName, pointsFn, layersJson, rulesetFn, paramsJson, 
                packageFn=None, workDir=None, metricsFn=None, logFn=None, 
                statusFn=None, mask=None):
      """
      @summary: Constructor for OM model
      @param pointsFn: The file location of the shapefile containing points
      @param layersJson: JSON string of layer information
      @param rulesetFn: The location to write the resulting ruleset
      @param paramsJson: JSON string of algorithm parameter information
      @param packageFn: If provided, write the package output here
      @param workDir: If provided, use this directory for work
      @param metricsFn: If provided, write the metrics to this location
      @param logFn: If provide, write the output log to this location
      @param statusFn: If provided, write the status to this location
      @param mask: If provided, use this file as the mask
      """
      self.metrics = {}
      self.metrics['processType'] = self.PROCESS_TYPE
      # Process inputs
      if workDir is not None:
         self.workDir = workDir
      else:
         self.workDir = os.getcwd()

      # Create the work directory if it does not exist
      if not os.path.exists(self.workDir):
         os.makedirs(self.workDir)

      # Logs
      if logFn is not None:
         addFile=True
      else:
         addFile=False
      self.log = LmComputeLogger(jobName, addConsole=True, addFile=addFile, 
                                 logFilename=logFn)
   
      points, pointsWKT = self._processPoints(pointsFn)

      # layers
      layerFns, maskFn = self._processLayers(layersJson)
      
      # If a mask was provided, use it
      if mask is not None:
         maskFn = mask

      # parameters
      algoParams = self._processParameters(paramsJson)
      self.metrics['algorithmCode'] = self.algoCode
      
      # Command inputs
      self.modelFile = os.path.join(self.workDir, 
                                    "{0}-model.xml".format(jobName))
      self.modelRequestFile = os.path.join(self.workDir, 
                                           "{0}-request.xml".format(jobName))
      self.modelLogFile = os.path.join(self.workDir, "{0}.log".format(jobName))
      
      # Output files
      self.rulesetFn = rulesetFn
      self.pkgFn = packageFn
      self.metricsFn = metricsFn
      self.statusFn = statusFn
      
      # Build request here
      omr = OmModelRequest(points, jobName, pointsWKT, layerFns, algoParams,
                           maskFn=maskFn)
      with open(self.modelRequestFile, 'w') as reqF:
         cnt = omr.generate()
         # As of openModeller 1.3, need to remove <?xml ... first line
         if cnt.startswith("<?xml version"):
            tmp = cnt.split('\n')
            cnt = '\n'.join(tmp[1:])

         reqF.write(cnt)

   # ...................................
   def _buildCommand(self):
      """
      @summary: Builds the command that will generate a model
      @return: A bash command to run
      
      @note: om_model version 1.3.0
      @note: Usage - om_model [options [args]]
         --help,       -h          Displays this information
         --version,    -v          Display version info
         --xml-req,    -r <args>   Model creation request file in XML
         --model-file, -m <args>   File to store the generated model
         --log-level <args>        Set the log level (debug, warn, info, error)
         --log-file <args>         Log file
         --prog-file <args>        File to store model creation progress
      """
      mdlBinary = os.path.join(BIN_PATH, OM_MODEL_CMD)
      cmd = "%s -r %s -m %s --log-level %s --log-file %s " % \
               (mdlBinary, 
                self.modelRequestFile, self.modelFile,
                DEFAULT_LOG_LEVEL, self.modelLogFile)

      if not os.path.exists(mdlBinary):
         raise LmException(JobStatus.LM_JOB_APPLICATION_NOT_FOUND,  
                           "%s not found" % mdlBinary)

      return cmd

   # ...................................
   def _findError(self, stdErr):
      """
      @summary: Checks for information about the error
      @param stdErr: Standard error from the process
      """
      status = JobStatus.OM_GENERAL_ERROR
      
      # Log standard error
      self.log.debug("Checking standard error")
      if stdErr is not None:
         self.log.debug(stdErr)
         # openModeller logs the error in a file.  Not sure what could be in 
         #    standard error, but if something is found, check for it here
         
      self.log.debug("Checking output")
      if os.path.exists(self.modelLogFile):
         with open(self.modelLogFile) as logF:
            omLog = logF.read()
         
         self.log.debug("openModeller error log")
         self.log.debug("-----------------------")
         self.log.debug(omLog)
         self.log.debug("-----------------------")
      
         if omLog.find("[Error] No presence points available") >= 0:
            status = JobStatus.OM_MOD_REQ_POINTS_MISSING_ERROR
         elif omLog.find(
             "[Error] Cannot use zero presence points for sampling") >= 0:
            status = JobStatus.OM_MOD_REQ_POINTS_MISSING_ERROR
         elif omLog.find("[Error] Algorithm could not be initialized") >= 0:
            status = JobStatus.OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR
         elif omLog.find(
             "[Error] Cannot create model without any presence or absence"
             ) >= 0:
            status = JobStatus.OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR
         elif omLog.find("[Error] XML Parser fatal error: not well-formed"
                         ) >= 0:
            status = JobStatus.OM_MOD_REQ_ERROR
         elif omLog.find("[Error] Unable to open file") >= 0:
            status = JobStatus.OM_MOD_REQ_LAYER_ERROR
         elif omLog.find("[Error] Algorithm %s not found" % \
                                               self.job.algorithm.code) >= 0:
            status = JobStatus.OM_MOD_REQ_ALGO_INVALID_ERROR
         elif omLog.find("[Error] Parameter") >= 0:
            if omLog.find("not set properly.\n", 
                                        omLog.find("[Error] Parameter")) >= 0:
               status = JobStatus.OM_MOD_REQ_ALGOPARAM_MISSING_ERROR

      return status
   
   # ...................................
   def _processLayers(self, layersJson):
      """
      @summary: Read the layers JSON and process the layers accordingly
      @param layersJson: JSON string with layer information
      @param layersDir: The directory to create layer symbolic links
      """
      lyrMgr = LayerManager(SHARED_DATA_PATH)
      lyrs, mask = lyrMgr.processLayersJSON(layersJson, 
                                 layerFormat=LayerFormat.GTIFF)
      return lyrs, mask
   
   # .................................
   def _processParameters(self, paramsJson):
      """
      @summary: Process provided algorithm parameters JSON and return string
      @param paramsJson: Json string of algorithm information
      @todo: Use constants
      """
      self.algoCode = paramsJson['algorithmCode']

      return paramsJson
      
   # ...................................
   def _processPoints(self, pointsShpFn):
      """
      @summary: Read the points and returns them as a list of tuples
      @param pointsShpFn: The file location of the shapefile with points
      """
      points = []
      driver = ogr.GetDriverByName("ESRI Shapefile")
      dataSource = driver.Open(pointsShpFn, 0)
      layer = dataSource.GetLayer()
      
      srs = layer.GetSpatialRef()
      pointsWKT = srs.ExportToWkt()
      
      i = 1
      for feature in layer:
         geom = feature.GetGeometryRef()
         ptGeom = geom.GetPoint()
         # Append species name, x, y
         points.append((i, ptGeom[0], ptGeom[1]))
         i += 1

      self.metrics['numPoints'] = len(points)
      
      return points, pointsWKT
   
   # ...................................
   def run(self):
      """
      @summary: Run openModeller, create the model, write outputs appropriately
      """
      try:
         cmd = self._buildCommand()
         spr = SubprocessRunner(cmd)
         startTime = time.time()
         procExitStatus, procStdErr = spr.run()
         endTime = time.time()
         self.metrics['runningTime'] = endTime - startTime
         status = JobStatus.COMPUTED
         
         # Check for output
         if procExitStatus > 0 or not os.path.exists(self.modelFile):
            # Error
            status = self._findError(procStdErr)
      except LmException, lme:
         status = lme.status
         
      # Write outputs
      # Ruleset
      shutil.move(self.modelFile, self.rulesetFn)
      
      # Status
      if self.statusFn is not None:
         with open(self.statusFn, 'w') as outStatus:
            outStatus.write(str(status))
      
      # Metrics
      if self.metricsFn is not None:
         with open(self.metricsFn, 'w') as outMetrics:
            for k, v in self.metrics.iteritems():
               outMetrics.write("{0}: {1}\n".format(k, v))
      
      # Package
      if self.pkgFn is not None:
         with zipfile.ZipFile(self.pkgFn, 'w', compression=zipfile.ZIP_DEFLATED,
                               allowZip64=True) as zf:
            for base, _, files in os.walk(self.workDir):
               for f in files:
                  # Don't add zip files
                  if f.find('.zip') == -1:
                     zf.write(os.path.join(base, f), 
                           os.path.relpath(os.path.join(base, f), self.workDir))

# .............................................................................
class OpenModellerProjection(object):
   """
   @summary: Class containing methods to create an SDM projection using 
                openModeller
   """
   PROCESS_TYPE = ProcessType.OM_PROJECT
   
   # ...................................
   def __init__(self, jobName, rulesetFn, layersJson, outTiffFn, workDir=None, 
                metricsFn=None, logFn=None, statusFn=None, packageFn=None,
                mask=None):
      """
      @summary: Constructor for ME model
      @param pointsFn: The file location of the shapefile containing points
      @param layersJson: JSON string of layer information
      @param rulesetFn: The location to write the resulting ruleset
      @param workDir: If provided, use this directory for work
      @param metricsFn: If provided, write the metrics to this location
      @param logFn: If provide, write the output log to this location
      @param statusFn: If provided, write the status to this location
      @param mask: If provided, use this file as a mask
      """
      self.metrics = {}
      self.metrics['algorithmCode'] = 'ATT_MAXENT'
      self.metrics['processType'] = self.PROCESS_TYPE
      # Process inputs
      if workDir is not None:
         self.workDir = workDir
      else:
         self.workDir = os.getcwd()

      # Create the work directory if it does not exist
      if not os.path.exists(self.workDir):
         os.makedirs(self.workDir)

      # Logs
      if logFn is not None:
         addFile=True
      else:
         addFile=False
      self.log = LmComputeLogger(jobName, addConsole=True, addFile=addFile, 
                                 logFilename=logFn)
   
      # layers
      layerFns, maskFn = self._processLayers(layersJson)
      
      # If a mask was explicitly provided, use it
      if mask is not None:
         maskFn = mask
      
      # Other
      self.outTiffFn = outTiffFn
      self.metricsFn = metricsFn
      self.statusFn = statusFn
      self.pkgFn = packageFn

      # OM files
      self.projRequestFile = os.path.join(self.workDir, 
                                          "{0}-request.xml".format(jobName))
      self.projectionFile = os.path.join(self.workDir, 
                                         "{0}.tif".format(jobName))
      self.projLogFile = os.path.join(self.workDir, "{0}.log".format(jobName))
      self.projStatsFile = os.path.join(self.workDir, 
                                        "{0}.stats".format(jobName))
      
      # Build request
      omr = OmProjectionRequest(rulesetFn, layerFns, maskFn=maskFn)
      
      with open(self.projRequestFile, 'w') as reqF:
         cnt = omr.generate()
         # As of openModeller 1.3, need to remove <?xml ... first line
         if cnt.startswith("<?xml version"):
            tmp = cnt.split('\n')
            cnt = '\n'.join(tmp[1:])

         reqF.write(cnt)
      
   # .......................................
   def _buildCommand(self):
      """
      @summary: Builds the command that will generate a projection
      @note: om_project version 1.3.0
      @note: Usage: om_project [options [args]]
        --help,      -h          Displays this information
        --version,   -v          Display version info
        --xml-req,   -r <args>   Projection request file in XML
        --model,     -o <args>   File with serialized model (native projection)
        --template,  -t <args>   Raster template for the distribution map 
                                    (native projection)
        --format,    -f <args>   File format for the distribution map 
                                    (native projection)
        --dist-map,  -m <args>   File to store the generated model
        --log-level <args>       Set the log level (debug, warn, info, error)
        --log-file <args>        Log file
        --prog-file <args>       File to store projection progress
        --stat-file <args>       File to store projection statistics
      """
      prjBinary = os.path.join(BIN_PATH, OM_PROJECT_CMD)
      cmd = "%s -r %s -m %s --log-level %s --log-file %s --stat-file %s" % \
            (prjBinary, self.projRequestFile, self.projectionFile,
             DEFAULT_LOG_LEVEL, self.projLogFile, self.projStatsFile)
      
      if not os.path.exists(prjBinary):
         self.status = JobStatus.LM_JOB_APPLICATION_NOT_FOUND
         raise LmException(JobStatus.LM_JOB_APPLICATION_NOT_FOUND,  
                           "%s not found" % prjBinary)

      return cmd

   # ...................................
   def _findError(self, stdErr):
      """
      @summary: Checks for information about the error
      @param stdErr: Standard error produced by the process
      """
      status = JobStatus.OM_PROJECTION_ERROR
      
      # Log standard error
      self.log.debug("Checking standard error")
      if stdErr is not None:
         self.log.debug(stdErr)
         # No specific errors are known to present in standard error
         
      self.log.debug("Checking output")
      
      # Add the openModeller log
      if os.path.exists(self.projLogFile):
         with open(self.projLogFile, 'r') as f:
            logContent = f.read()
         self.log.debug("---------------------------------------")
         self.log.debug(logContent)
         self.log.debug("---------------------------------------")

      return status
   
   # ...................................
   def _processLayers(self, layersJson):
      """
      @summary: Read the layers JSON and process the layers accordingly
      @param layersJson: JSON string of layer information
      @param layersDir: The directory to create layer symbolic links
      """
      lyrMgr = LayerManager(SHARED_DATA_PATH)
      lyrs, mask = lyrMgr.processLayersJSON(layersJson, 
                                 layerFormat=LayerFormat.GTIFF)
      return lyrs, mask
   
   # ...................................
   def run(self):
      """
      @summary: Run openModeller, create the projection, write outputs 
                   appropriately
      @todo: Assess
      """
      try:
         cmd = self._buildCommand()
         spr = SubprocessRunner(cmd)
         startTime = time.time()
         procExitStatus, procStdErr = spr.run()
         endTime = time.time()
         self.metrics['runningTime'] = endTime - startTime
         status = JobStatus.COMPUTED
         
         # Check outputs            
         if procExitStatus > 0 or not os.path.exists(self.projectionFile):
            # Error
            status = self._findError(procStdErr)
      except LmException, lme:
         status = lme.status
         
      # Write outputs
      # Move projection output to proper location
      shutil.move(self.projectionFile, self.outTiffFn)
      
      # Status
      if self.statusFn is not None:
         with open(self.statusFn, 'w') as outStatus:
            outStatus.write(str(status))
      
      # Metrics
      if self.metricsFn is not None:
         with open(self.metricsFn, 'w') as outMetrics:
            for k, v in self.metrics.iteritems():
               outMetrics.write("{0}: {1}\n".format(k, v))

      # Package
      if self.pkgFn is not None:
         with zipfile.ZipFile(self.pkgFn, 'w', compression=zipfile.ZIP_DEFLATED,
                               allowZip64=True) as zf:
            for base, _, files in os.walk(self.workDir):
               for f in files:
                  # Don't add zip files
                  if f.find('.zip') == -1:
                     zf.write(os.path.join(base, f), 
                           os.path.relpath(os.path.join(base, f), self.workDir))

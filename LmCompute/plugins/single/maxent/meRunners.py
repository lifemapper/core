"""
@summary: Module containing the job runners for Maximum Entropy models and 
             projections
@author: CJ Grady
@version: 4.0.0
@status: beta

@note: Commands are for maxent library version 3.3.3e
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
"""
import json
import os
from osgeo import ogr
import shutil
import time
import zipfile

from LmBackend.common.subprocessManager import SubprocessRunner

from LmCommon.common.lmconstants import JobStatus, ProcessType

from LmCompute.common.layerManager import LayerManager
from LmCompute.common.localconstants import SHARED_DATA_PATH
from LmCompute.common.lmObj import LmException

from LmCompute.common.lmconstants import (LayerFormat, ME_CMD, MDL_TOOL, 
                                          PRJ_TOOL, JAVA_CMD)
from LmCompute.common.log import LmComputeLogger
from LmCompute.plugins.single.maxent.constants import PARAMETERS

# .............................................................................
class MaxentModel(object):
   """
   @summary: Class containing methods to create an SDM model using Maxent
   """
   PROCESS_TYPE = ProcessType.ATT_MODEL
   # ...................................
   def __init__(self, jobName, pointsFn, layersFn, rulesetFn, paramsFn=None, 
                packageFn=None, workDir=None, metricsFn=None, logFn=None, 
                logLevel=None, statusFn=None):
      """
      @summary: Constructor for ME model
      @param pointsFn: The file location of the shapefile containing points
      @param layersFn: The file location of the JSON layers file
      @param rulesetFn: The location to write the resulting ruleset
      @param paramsFn: The JSON file location of algorithm parameters, if not
                          provided, use defaults
      @param packageFn: If provided, write the package output here
      @param workDir: If provided, use this directory for work
      @param metricsFn: If provided, write the metrics to this location
      @param logFn: If provide, write the output log to this location
      @param logLevel: The log level to use when logging
      @param statusFn: If provided, write the status to this location
      """
      self.metrics = {}
      self.metrics['algorithmCode'] = 'ATT_MAXENT'
      self.metrics['processType'] = self.PROCESS_TYPE
      # Process inputs
      if workDir is not None:
         self.workDir = workDir
      else:
         self.workDir = os.getcwd()

      # Logs
      if logFn is not None:
         addFile=True
      else:
         addFile=False
      self.log = LmComputeLogger(jobName, addConsole=True, addFile=addFile, 
                                 logFilename=logFn)
   
      self.samplesFile = os.path.join(self.workDir, 'points.csv')
      self._processPoints(pointsFn, self.samplesFile)

      # layers
      self.layersDir = os.path.join(self.workDir, 'layers')
      try:
         os.makedirs(self.layersDir)
      except:
         pass
      self._processLayers(layersFn, self.layersDir)

      # parameters
      self.params = self._processParameters(paramsFn)
      
      # Need species name?
      self.occName = os.path.splitext(os.path.basename(pointsFn))[0]
      self.lambdasFile = os.path.join(self.workDir, 
                                      "{0}.lambdas".format(self.occName))

      # Output files
      self.rulesetFn = rulesetFn
      self.pkgFn = packageFn
      self.metricsFn = metricsFn
      self.statusFn = statusFn
      
   # ...................................
   def _buildCommand(self):
      """
      @summary: Builds the command that will generate a model
      @return: A bash command to run
      @todo: Update
      @note: MaxEnt version 3.3.3e
      """
      baseCmd = "{0} {1} {2}".format(JAVA_CMD, ME_CMD, MDL_TOOL)
      samples = "-s {0}".format(self.samplesFile)
      envParams = "-e {0}".format(self.layersDir)
      outputParams = "-o {0}".format(self.workDir)
      options = "nowarnings nocache autorun -z"
      # Application path, me command, model tool
      cmd = "{0} {1} {2} {3} {4} {5}".format(baseCmd, samples, envParams, 
                                            outputParams, self.params, options)
      if not os.path.exists(ME_CMD):
         raise LmException(JobStatus.LM_JOB_APPLICATION_NOT_FOUND, 
                           "Could not find: %s" % ME_CMD)

      self.log.debug(cmd)
      
      return cmd
   
   # ...................................
   def _findError(self, stdErr):
      """
      @summary: Checks for information about the error
      """
      status = JobStatus.ME_GENERAL_ERROR
      
      # Log standard error
      self.log.debug("Checking standard error")
      if stdErr is not None:
         self.log.debug(stdErr)
         
         status = JobStatus.GENERAL_ERROR
         if stdErr.find("Couldn't get file lock.") >= 0:
            status = JobStatus.ME_FILE_LOCK_ERROR
         elif stdErr.find("Could not reserve enough space for object heap") >= 0:
            status = JobStatus.ME_HEAP_SPACE_ERROR
         elif stdErr.find("Too small initial heap for new size specified") >= 0:
            status = JobStatus.ME_HEAP_SPACE_ERROR
         elif stdErr.find("because it has 0 training samples") >= 0:
            status = JobStatus.ME_POINTS_ERROR

      self.log.debug("Checking output")
      errfname = os.path.join(self.workDir, 'maxent.log')

      # Look at Maxent error (might be more specific)
      if os.path.exists(errfname):
         with open(errfname, 'r') as f:
            logContent = f.read()
         self.log.debug("---------------------------------------")
         self.log.debug(logContent)
         self.log.debug("---------------------------------------")

         if logContent.find('have different geographic dimensions') >= 0:
            status = JobStatus.ME_MISMATCHED_LAYER_DIMENSIONS
         elif logContent.find('NumberFormatException') >= 0:
            status = JobStatus.ME_CORRUPTED_LAYER
         elif logContent.find('because it has 0 training samples') >= 0:
            status = JobStatus.ME_POINTS_ERROR
         elif logContent.find('is missing from') >= 0: #ex: Layer vap6190_ann is missing from layers/projectionScn
            status = JobStatus.ME_LAYER_MISSING
         elif logContent.find('No background points with data in all layers') >= 0:
            status = JobStatus.ME_POINTS_ERROR
         elif logContent.find('No features available: select more feature types') >= 0:
            status = JobStatus.ME_NO_FEATURES_CLASSES_AVAILABLE

      return status
   
   # ...................................
   def _processLayers(self, layersFn, layersDir):
      """
      @summary: Read the layers JSON and process the layers accordingly
      @param layersFn: Location of JSON file with layer information
      @param layersDir: The directory to create layer symbolic links
      """
      with open(layersFn) as inLayers:
         lyrJson = json.load(inLayers)
         
      lyrMgr = LayerManager(SHARED_DATA_PATH)
      lyrs, mask = lyrMgr.processLayersJSON(lyrJson, 
                                 layerFormat=LayerFormat.MXE, symDir=layersDir)
      return lyrs, mask
   
   # .................................
   def _processParameters(self, paramsFn):
      """
      @summary: Process provided algorithm parameters JSON and return string
      @param paramsFn: File location of algorithm parameters JSON
      @todo: Use constants
      """
      algoParams = []
      if paramsFn is not None:
         with open(paramsFn) as inParams:
            paramsJson = json.load(inParams)
            for param in paramsJson['parameters']:
               paramName = param['name']
               paramValue = param['value']
               defParam = PARAMETERS[paramName]
               if paramValue is not None and paramValue == 'None':
                  v = defParam['process'](paramValue)
                  # Check for options
                  if defParam.has_key('options'):
                     v = defParam['options'][v]
                  if v != defParam['default']:
                     algoParams.append("{0}={1}".format(paramName, v))
      return ' '.join(algoParams)
               
   # ...................................
   def _processPoints(self, pointsShpFn, outPointsCsv):
      """
      @summary: Read the points and write them in the appropriate format
      """
      points = []
      driver = ogr.GetDriverByName("ESRI Shapefile")
      dataSource = driver.Open(pointsShpFn, 0)
      layer = dataSource.GetLayer()
      
      for feature in layer:
         geom = feature.GetGeometryRef()
         ptGeom = geom.GetPoint()
         # Append species name, x, y
         points.append((self.occName, ptGeom[0], ptGeom[1]))

      self.metrics['numPoints'] = len(points)
      
      with open(outPointsCsv, 'w') as outCsv:
         outCsv.write("Species, X, Y\n")
         for name, x, y in points:
            outCsv.write("{0}, {1}, {2}\n".format(name, x, y))
   
   # ...................................
   def run(self):
      """
      @summary: Run Maxent, create the model, write outputs appropriately
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
         outputs = False
         fnParts = os.path.splitext(self.lambdasFile)
         multiLambda = "{0}_0{1}".format(fnParts[0], fnParts[1])
         if os.path.exists(self.lambdasFile) or os.path.exists(multiLambda):
            outputs = True
            
         if procExitStatus > 0 or not outputs:
            # Error
            status = self._findError(procStdErr)
      except LmException, lme:
         status = lme.status
         
      # Write outputs
      # Ruleset
      if os.path.exists(self.lambdasFile):
         shutil.move(self.lambdasFile, self.rulesetFn)
      
      # Status
      if self.statusFn is not None:
         with open(self.statusFn, 'w') as outStatus:
            outStatus.write(status)
      
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
               # Add everything but layers
               if base.find('layers') == -1:
                  for f in files:
                     zf.write(os.path.join(base, f), 
                              os.path.relpath(os.path.join(base, f), 
                                              self.workDir))

# .............................................................................
class MaxentProjection(object):
   """
   @summary: Class containing methods to create an SDM projection using Maxent
   """
   PROCESS_TYPE = ProcessType.ATT_PROJECT
   
   # ...................................
   def __init__(self, jobName, rulesetFn, layersFn, outAsciiFn, paramsFn=None, 
                workDir=None, metricsFn=None, logFn=None, 
                logLevel=None, statusFn=None):
      """
      @summary: Constructor for ME model
      @param pointsFn: The file location of the shapefile containing points
      @param layersFn: The file location of the JSON layers file
      @param rulesetFn: The location to write the resulting ruleset
      @param paramsFn: The JSON file location of algorithm parameters, if not
                          provided, use defaults
      @param workDir: If provided, use this directory for work
      @param metricsFn: If provided, write the metrics to this location
      @param logFn: If provide, write the output log to this location
      @param logLevel: The log level to use when logging
      @param statusFn: If provided, write the status to this location
      """
      self.metrics = {}
      self.metrics['algorithmCode'] = 'ATT_MAXENT'
      self.metrics['processType'] = self.PROCESS_TYPE
      # Process inputs
      if workDir is not None:
         self.workDir = workDir
      else:
         self.workDir = os.getcwd()

      # Logs
      if logFn is not None:
         addFile=True
      else:
         addFile=False
      self.log = LmComputeLogger(jobName, addConsole=True, addFile=addFile, 
                                 logFilename=logFn)
   
      # layers
      self.layersDir = os.path.join(self.workDir, 'layers')
      try:
         os.makedirs(self.layersDir)
      except:
         pass
      self._processLayers(layersFn, self.layersDir)

      # parameters
      self.params = self._processParameters(paramsFn)
      
      # Other
      self.asciiOut = outAsciiFn
      #self.asciiOut = os.path.join(self.workDir, 'output.asc')
      self.rulesetFn = rulesetFn

      self.metricsFn = metricsFn
      self.statusFn = statusFn
      
   # .......................................
   def _buildCommand(self):
      """
      @summary: Builds the command that will generate a projection
      @note: MaxEnt version 3.3.3e
      @note: Usage: java -cp maxent.jar density.Project lambdaFile gridDir outFile [args]

Here lambdaFile is a .lambdas file describing a Maxent model, and gridDir is a 
directory containing grids for all the predictor variables described in the 
.lambdas file.  As an alternative, gridDir could be an swd format file.  The 
optional args can contain any flags understood by Maxent -- for example, a 
"grd" flag would make the output grid of density.Project be in .grd format.
      """
      baseCmd = "{0} {1} {2}".format(JAVA_CMD, ME_CMD, PRJ_TOOL)

      args = "nowarnings nocache autorun -z"
      #cmd = "{baseCmd} {lambdaFile} {gridDir} {outFile} {args}".format(
      #            baseCmd=baseCmd, gridDir=gridDir, outFile=outFile, args=args)
      cmd = "{0} {1} {2} {3} {4} {5}".format(baseCmd, self.lambdasFile, 
                                               self.layersDir, self.asciiOut, 
                                               self.params, args)
      self.log.debug(cmd)
      
      if not os.path.exists(ME_CMD):
         raise LmException(JobStatus.LM_JOB_APPLICATION_NOT_FOUND, 
                           "Could not find: %s" % ME_CMD)
      return cmd
   
   # ...................................
   def _findError(self, stdErr):
      """
      @summary: Checks for information about the error
      """
      status = JobStatus.ME_GENERAL_ERROR
      
      # Log standard error
      self.log.debug("Checking standard error")
      if stdErr is not None:
         self.log.debug(stdErr)
         
         status = JobStatus.GENERAL_ERROR
         if stdErr.find("Couldn't get file lock.") >= 0:
            status = JobStatus.ME_FILE_LOCK_ERROR
         elif stdErr.find("Could not reserve enough space for object heap") >= 0:
            status = JobStatus.ME_HEAP_SPACE_ERROR
         elif stdErr.find("Too small initial heap for new size specified") >= 0:
            status = JobStatus.ME_HEAP_SPACE_ERROR
         elif stdErr.find("because it has 0 training samples") >= 0:
            status = JobStatus.ME_POINTS_ERROR

      self.log.debug("Checking output")
      errfname = os.path.join(self.workDir, 'maxent.log')

      # Look at Maxent error (might be more specific)
      if os.path.exists(errfname):
         with open(errfname, 'r') as f:
            logContent = f.read()
         self.log.debug("---------------------------------------")
         self.log.debug(logContent)
         self.log.debug("---------------------------------------")

         if logContent.find('have different geographic dimensions') >= 0:
            status = JobStatus.ME_MISMATCHED_LAYER_DIMENSIONS
         elif logContent.find('NumberFormatException') >= 0:
            status = JobStatus.ME_CORRUPTED_LAYER
         elif logContent.find('because it has 0 training samples') >= 0:
            status = JobStatus.ME_POINTS_ERROR
         elif logContent.find('is missing from') >= 0: #ex: Layer vap6190_ann is missing from layers/projectionScn
            status = JobStatus.ME_LAYER_MISSING
         elif logContent.find('No background points with data in all layers') >= 0:
            status = JobStatus.ME_POINTS_ERROR
         elif logContent.find('No features available: select more feature types') >= 0:
            status = JobStatus.ME_NO_FEATURES_CLASSES_AVAILABLE

      return status
   
   # ...................................
   def _processLayers(self, layersFn, layersDir):
      """
      @summary: Read the layers JSON and process the layers accordingly
      @param layersFn: Location of JSON file with layer information
      @param layersDir: The directory to create layer symbolic links
      """
      with open(layersFn) as inLayers:
         lyrJson = json.load(inLayers)
         
      lyrMgr = LayerManager(SHARED_DATA_PATH)
      lyrs, mask = lyrMgr.processLayersJSON(lyrJson, 
                                 layerFormat=LayerFormat.MXE, symDir=layersDir)
      return lyrs, mask
   
   # .................................
   def _processParameters(self, paramsFn):
      """
      @summary: Process provided algorithm parameters JSON and return string
      @param paramsFn: File location of algorithm parameters JSON
      @todo: Use constants
      """
      algoParams = []
      if paramsFn is not None:
         with open(paramsFn) as inParams:
            paramsJson = json.load(inParams)
            for param in paramsJson['parameters']:
               paramName = param['name']
               paramValue = param['value']
               defParam = PARAMETERS[paramName]
               if paramValue is not None and paramValue == 'None':
                  v = defParam['process'](paramValue)
                  # Check for options
                  if defParam.has_key('options'):
                     v = defParam['options'][v]
                  if v != defParam['default']:
                     algoParams.append("{0}={1}".format(paramName, v))
      return ' '.join(algoParams)
   
   # ...................................
   def run(self):
      """
      @summary: Run Maxent, create the model, write outputs appropriately
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
         if procExitStatus > 0 or not os.path.exists(self.asciiOut):
            # Error
            status = self._findError(procStdErr)
      except LmException, lme:
         status = lme.status
         
      # Write outputs
      
      # Status
      if self.statusFn is not None:
         with open(self.statusFn, 'w') as outStatus:
            outStatus.write(status)
      
      # Metrics
      if self.metricsFn is not None:
         with open(self.metricsFn, 'w') as outMetrics:
            for k, v in self.metrics.iteritems():
               outMetrics.write("{0}: {1}\n".format(k, v))
      
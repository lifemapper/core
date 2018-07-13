"""
@summary: Module containing the job runners for Maximum Entropy models and 
             projections
@author: CJ Grady
@version: 4.0.0
@status: beta

@note: Commands are for maxent library version 3.3.3e
@note: Commands may be backwards compatible

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
from osgeo import ogr
import shutil
import socket
import time
import zipfile

from LmBackend.common.layerTools import convertAsciisToMxes, processLayersJSON
from LmBackend.common.metrics import LmMetricNames, LmMetrics
from LmBackend.common.subprocessManager import SubprocessRunner

from LmCommon.common.lmconstants import JobStatus, ProcessType, LMFormat

from LmCompute.common.lmObj import LmException

from LmCompute.common.lmconstants import (ME_CMD, MDL_TOOL, 
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
   def __init__(self, jobName, pointsFn, layersJson, rulesetFn, paramsJson=None, 
                packageFn=None, workDir=None, metricsFn=None, logFn=None, 
                statusFn=None, mask=None):
      """
      @summary: Constructor for ME model
      @param pointsFn: The file location of the shapefile containing points
      @param layersJson: JSON string containing layer information
      @param rulesetFn: The location to write the resulting ruleset
      @param paramsJson: JSON string of algorithm parameters, if not
                          provided, use defaults
      @param packageFn: If provided, write the package output here
      @param workDir: If provided, use this directory for work
      @param metricsFn: If provided, write the metrics to this location
      @param logFn: If provide, write the output log to this location
      @param statusFn: If provided, write the status to this location
      @param mask: If provided, use this file as a mask
      """
      # Always track metrics, won't write if file name is None
      self.metrics = LmMetrics(metricsFn)
      self.metrics.add_metric(LmMetricNames.ALGORITHM_CODE, 'ATT_MAXENT')
      self.metrics.add_metric(LmMetricNames.PROCESS_TYPE, self.PROCESS_TYPE)
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
   
      self.samplesFile = os.path.join(self.workDir, 'points.csv')
      self.occName = os.path.splitext(os.path.basename(pointsFn))[0]
      self._processPoints(pointsFn, self.samplesFile)

      # layers
      self.layersDir = os.path.join(self.workDir, 'layers')
      try:
         os.makedirs(self.layersDir)
      except:
         pass
      _ = self._processLayers(layersJson, self.layersDir)

      # parameters
      self.params = self._processParameters(paramsJson)
      
      # Process mask if provided
      if mask is not None:
         # TODO: Evaluate if we need to convert this to be the same format as
         #          the other layers or if we can mix
         maskFn = os.path.join(self.layersDir, 'mask{}'.format(LMFormat.MXE.ext))
         #os.symlink(mask, maskFn)
         if not os.path.exists(maskFn):
            convertAsciisToMxes(os.path.split(mask)[0])
            
            shutil.move('{}{}'.format(os.path.splitext(
                                          os.path.abspath(mask))[0], 
                                      LMFormat.MXE.ext), maskFn)
         
         self.params += ' togglelayertype=mask'

      # Need species name?
      self.lambdasFile = os.path.join(self.workDir, 
                                      "{0}.lambdas".format(self.occName))

      # Output files
      self.rulesetFn = rulesetFn
      self.pkgFn = packageFn
      self.statusFn = statusFn
      
      # Check for zero length files
      checkFiles = []
      checkFiles.extend(glob.glob(os.path.join(self.layersDir, '*')))
      checkFiles.append(self.samplesFile)
      for fn in checkFiles:
         if os.path.getsize(fn) == 0:
            raise Exception, 'File: {} has size 0'.format(fn)
      
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
         
         if stdErr.find("Couldn't get file lock.") >= 0:
            status = JobStatus.ME_FILE_LOCK_ERROR
         elif stdErr.find("Could not reserve enough space for object heap") >= 0:
            status = JobStatus.ME_HEAP_SPACE_ERROR
         elif stdErr.find("Too small initial heap for new size specified") >= 0:
            status = JobStatus.ME_HEAP_SPACE_ERROR
         elif stdErr.find("because it has 0 training samples") >= 0:
            status = JobStatus.ME_POINTS_ERROR
         elif stdErr.find('Attempt to evaluate layer') >= 0: 
            #1 at sample with no value
            status = JobStatus.ME_CORRUPTED_LAYER

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
   def _processLayers(self, layersJson, layersDir):
      """
      @summary: Read the layers JSON and process the layers accordingly
      @param layersJson: JSON string with layer information
      @param layersDir: The directory to create layer symbolic links
      """
      lyrs = processLayersJSON(layersJson, symDir=layersDir)
      return lyrs
   
   # .................................
   def _processParameters(self, paramsJson):
      """
      @summary: Process provided algorithm parameters JSON and return string
      @param paramsFn: File location of algorithm parameters JSON
      @todo: Use constants
      """
      algoParams = []
      for param in paramsJson['parameters']:
         paramName = param['name']
         paramValue = param['value']
         defParam = PARAMETERS[paramName]
         if paramValue is not None and paramValue != 'None':
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
      points = set([])
      
      driver = ogr.GetDriverByName("ESRI Shapefile")
      dataSource = driver.Open(pointsShpFn, 0)
      layer = dataSource.GetLayer()
      
      for feature in layer:
         geom = feature.GetGeometryRef()
         ptGeom = geom.GetPoint()
         # Add point to set
         points.add((ptGeom[0], ptGeom[1]))

      self.metrics.add_metric(LmMetricNames.NUMBER_OF_FEATURES, len(points))
      
      with open(outPointsCsv, 'w') as outCsv:
         outCsv.write("Species, X, Y\n")
         for x, y in list(points):
            outCsv.write("{0}, {1}, {2}\n".format(self.occName, x, y))
   
   # ...................................
   def run(self):
      """
      @summary: Run Maxent, create the model, write outputs appropriately
      """
      try:
         cont = True
         triesLeft = 3
         while cont:
            cont = False
            cmd = self._buildCommand()
            spr = SubprocessRunner(cmd)
            startTime = time.time()
            procExitStatus, procStdErr = spr.run()
            endTime = time.time()
            self.metrics.add_metric(LmMetricNames.RUNNING_TIME, 
                                                            endTime - startTime)
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
               
               if status == JobStatus.ME_CORRUPTED_LAYER:
                  self.log.debug('Status {}'.format(status))
                  self.log.debug('Corrupted layer issue.  Retrying... ({})'.format(triesLeft))
                  self.log.debug('Machine {}'.format(socket.gethostname()))
                  if triesLeft > 0:
                     cont = True
                     triesLeft -= 1
                     
      except LmException, lme:
         status = lme.status
         
      # Write outputs
      # Ruleset
      if os.path.exists(self.lambdasFile):
         shutil.move(self.lambdasFile, self.rulesetFn)
      
      # Status
      self.metrics.add_metric(LmMetricNames.STATUS, status)
      if self.statusFn is not None:
         with open(self.statusFn, 'w') as outStatus:
            outStatus.write(str(status))
      
      # Package
      if self.pkgFn is not None:
         with zipfile.ZipFile(self.pkgFn, 'w', compression=zipfile.ZIP_DEFLATED,
                               allowZip64=True) as zf:
            for base, _, files in os.walk(self.workDir):
               # Add everything but layers
               if base.find('layers') == -1:
                  for f in files:
                     # Don't add zip files
                     if f.find('.zip') == -1:
                        zf.write(os.path.join(base, f), 
                              os.path.relpath(os.path.join(base, f), 
                                              self.workDir))

      # Metrics
      # Get size of output directory
      # Metrics
      self.metrics.add_metric(LmMetricNames.OUTPUT_SIZE,
                              self.metrics.get_dir_size(self.workDir))
      self.metrics.write_metrics()

# .............................................................................
class MaxentProjection(object):
   """
   @summary: Class containing methods to create an SDM projection using Maxent
   """
   PROCESS_TYPE = ProcessType.ATT_PROJECT
   
   # ...................................
   def __init__(self, jobName, rulesetFn, layersJson, outAsciiFn, 
                paramsJson=None, workDir=None, metricsFn=None, logFn=None, 
                statusFn=None, packageFn=None, mask=None):
      """
      @summary: Constructor for ME projection
      @param pointsFn: The file location of the shapefile containing points
      @param layersJson: JSON string of layer information
      @param rulesetFn: The location to write the resulting ruleset
      @param paramsJson: JSON string of algorithm parameters, if not
                          provided, use defaults
      @param workDir: If provided, use this directory for work
      @param metricsFn: If provided, write the metrics to this location
      @param logFn: If provide, write the output log to this location
      @param statusFn: If provided, write the status to this location
      @param mask: If provided, use this file as a mask
      """
      self.metrics = LmMetrics(metricsFn)
      self.metrics.add_metric(LmMetricNames.ALGORITHM_CODE, 'ATT_MAXENT')
      self.metrics.add_metric(LmMetricNames.PROCESS_TYPE, self.PROCESS_TYPE)
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
      self.layersDir = os.path.join(self.workDir, 'layers')
      try:
         os.makedirs(self.layersDir)
      except:
         pass
      _ = self._processLayers(layersJson, self.layersDir)

      # parameters
      self.params = self._processParameters(paramsJson)
      
      # Process mask if provided
      if mask is not None:
         # TODO: Evaluate if we need to convert this to be the same format as
         #          the other layers or if we can mix
         maskFn = os.path.join(self.layersDir, 'mask{}'.format(
            LMFormat.MXE.ext))
         #os.symlink(mask, maskFn)
         if not os.path.exists(maskFn):
            convertAsciisToMxes(os.path.split(mask)[0])
            
            shutil.move('{}{}'.format(os.path.splitext(
                                          os.path.abspath(mask))[0], 
                                      LMFormat.MXE.ext), maskFn)
         #os.symlink(maskFn, symMaskFn)
         self.params += ' togglelayertype=mask'

      # Other
      self.asciiOut = outAsciiFn
      #self.asciiOut = os.path.join(self.workDir, 'output.asc')
      self.rulesetFn = rulesetFn

      self.statusFn = statusFn
      self.pkgFn = packageFn
      
      # Check for zero length files
      checkFiles = []
      checkFiles.extend(glob.glob(os.path.join(self.layersDir, '*')))
      checkFiles.append(self.rulesetFn)
      for fn in checkFiles:
         if os.path.getsize(fn) == 0:
            raise Exception, 'File: {} has size 0'.format(fn)
      
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
      cmd = "{0} {1} {2} {3} {4} {5}".format(baseCmd, self.rulesetFn, 
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
   def _processLayers(self, layersJson, layersDir):
      """
      @summary: Read the layers JSON and process the layers accordingly
      @param layersJson: JSON string with layer information
      @param layersDir: The directory to create layer symbolic links
      """
      lyrs = processLayersJSON(layersJson, symDir=layersDir)
      return lyrs
   
   # .................................
   def _processParameters(self, paramsJson):
      """
      @summary: Process provided algorithm parameters JSON and return string
      @param paramsJson: JSON string of algorithm parameters
      @todo: Use constants
      """
      algoParams = []
      for param in paramsJson['parameters']:
         paramName = param['name']
         paramValue = param['value']
         defParam = PARAMETERS[paramName]
         if paramValue is not None and paramValue != 'None':
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
      @summary: Run Maxent, create the projection, write outputs appropriately
      """
      try:
         cmd = self._buildCommand()
         spr = SubprocessRunner(cmd)
         startTime = time.time()
         procExitStatus, procStdErr = spr.run()
         endTime = time.time()
         self.metrics.add_metric(LmMetricNames.RUNNING_TIME,
                                                         endTime - startTime)
         status = JobStatus.COMPUTED
         
         # Check outputs            
         if procExitStatus > 0 or not os.path.exists(self.asciiOut):
            # Error
            status = self._findError(procStdErr)
      except LmException, lme:
         status = lme.status
         
      # Write outputs
      
      # Status
      self.metrics.add_metric(LmMetricNames.STATUS, status)
      if self.statusFn is not None:
         with open(self.statusFn, 'w') as outStatus:
            outStatus.write(str(status))
      
      # Package
      if self.pkgFn is not None:
         with zipfile.ZipFile(self.pkgFn, 'w', compression=zipfile.ZIP_DEFLATED,
                               allowZip64=True) as zf:
            for base, _, files in os.walk(self.workDir):
               # Add everything but layers
               if base.find('layers') == -1:
                  for f in files:
                     # Don't add zip files
                     if f.find('.zip') == -1:
                        zf.write(os.path.join(base, f), 
                              os.path.relpath(os.path.join(base, f), 
                                              self.workDir))

      # Metrics
      self.metrics.add_metric(LmMetricNames.OUTPUT_SIZE,
                              self.metrics.get_dir_size(self.workDir))
      self.metrics.write_metrics()
 

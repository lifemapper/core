"""
@summary: Module containing the job runners for Maximum Entropy models and 
             projections
@author: CJ Grady
@version: 4.0.0
@status: beta

@note: Commands are for maxent library version 3.3.3e
@note: Commands may be backwards compatible

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

from LmCommon.common.lmconstants import JobStatus, ProcessType, OutputFormat
from LmCommon.common.unicode import fromUnicode, toUnicode

from LmCompute.common.layerManager import (convertAndModifyAsciiToTiff, 
                                           LayerManager)
from LmCompute.common.localconstants import BIN_PATH, JOB_DATA_PATH
from LmCompute.common.lmconstants import LayerFormat
from LmCompute.common.lmObj import LmException

from LmCompute.jobs.runners.applicationRunner import ApplicationRunner

from LmCompute.plugins.sdm.maxent.constants import PARAMETERS
from LmCompute.plugins.sdm.maxent.localconstants import JAVA_CMD, MDL_TOOL, \
                                                        ME_CMD, ME_VERSION, \
                                                        PRJ_TOOL

# .............................................................................
class MEModelRunner(ApplicationRunner):
   """
   @summary: MaxEnt model job runner
   """
   PROCESS_TYPE = ProcessType.ATT_MODEL
   # ...................................
   def _buildCommand(self):
      """
      @summary: Builds the command that will generate a model
      @return: A bash command to run
      
      @note: MaxEnt version 3.3.3e
      """
      baseCmd = "{1} {2} {3}".format(BIN_PATH, JAVA_CMD, ME_CMD, MDL_TOOL)
      samples = "-s {0}".format(self.samplesFile)
      envParams = "-e {0}".format(self.jobLayerDir)
      outputParams = "-o {0}".format(self.workDir)
      algoOptions = getAlgorithmOptions(self.job.algorithm)
      options = "nowarnings nocache autorun -z"
      # Application path, me command, model tool
      cmd = "{0} {1} {2} {3} {4} {5}".format(baseCmd, samples, envParams, 
                                            outputParams, algoOptions, options)
      if not os.path.exists(ME_CMD):
         self.status = JobStatus.LM_JOB_APPLICATION_NOT_FOUND
         raise LmException(JobStatus.LM_APPLICATION_NOT_FOUND, "Could not find: %s" % ME_CMD)

      self.log.debug(cmd)
      
      return cmd
   
   # ...................................
   def _checkStdErr(self):
      try:
         self.log.debug("Something went wrong, check stderr")
         print self.stderr
         self.log.debug(self.stderr)

         if self.stderr.find("Couldn't get file lock.") >= 0:
            self.status = JobStatus.ME_FILE_LOCK_ERROR
         elif self.stderr.find("Could not reserve enough space for object heap") >= 0:
            self.status = JobStatus.ME_HEAP_SPACE_ERROR
         elif self.stderr.find("Too small initial heap for new size specified") >= 0:
            self.status = JobStatus.ME_HEAP_SPACE_ERROR
         elif self.stderr.find("because it has 0 training samples") >= 0:
            self.status = JobStatus.ME_POINTS_ERROR
      except Exception, e: # Might happen if there is no stderr 
         pass
   
   # ...................................
   def _checkOutput(self):
      """
      @summary: Checks the output of MaxEnt to see if any errors occurred
      """
      self.log.debug("Checking output")
      # If an output raster does not exist, an error occurred
      if not os.path.exists(self.lambdasFile):
         # If the main lambdas file is not present, look for one of the 
         #    multiples.  This happens if there are replicates
         fnParts = os.path.splitext(self.lambdasFile)
         multiLambdasFile = ''.join([fnParts[0], '_0', fnParts[1]])
         if not os.path.exists(multiLambdasFile):
            errfname = os.path.join(self.workDir, 'maxent.log')
            # Initialize error status
            self.status = JobStatus.ME_GENERAL_ERROR
            # Check stderr for error first, we may end up updating with 
            #    error from maxent (can happen with lock errors that are really 
            #    just warnings)
            self._checkStdErr()
            # Look at Maxent error (might be more specific)
            if os.path.exists(errfname):
               with open(errfname, 'r') as f:
                  logContent = f.read()
                  self.log.debug("---------------------------------------")
                  self.log.debug(logContent)
                  self.log.debug("---------------------------------------")
                  print("-----------------------------")
                  print logContent
                  print("-----------------------------")
               if logContent.find('have different geographic dimensions') >= 0:
                  self.status = JobStatus.ME_MISMATCHED_LAYER_DIMENSIONS
               elif logContent.find('NumberFormatException') >= 0:
                  self.status = JobStatus.ME_CORRUPTED_LAYER
               elif logContent.find('because it has 0 training samples') >= 0:
                  self.status = JobStatus.ME_POINTS_ERROR
               elif logContent.find('is missing from') >= 0: #ex: Layer vap6190_ann is missing from layers/projectionScn
                  self.status = JobStatus.ME_LAYER_MISSING
               elif logContent.find('No background points with data in all layers') >= 0:
                  self.status = JobStatus.ME_POINTS_ERROR
               elif logContent.find('No features available: select more feature types') >= 0:
                  self.status = JobStatus.ME_NO_FEATURES_CLASSES_AVAILABLE
               else:
                  self.status = JobStatus.ME_GENERAL_ERROR
            
   # ...................................
   def _processJobInput(self):
      """
      @summary: Initializes a model to be ran by MaxEnt
      """
      self.log.debug("Processing job input")
      self.metrics['jobId'] = self.job.jobId
      self.metrics['algorithm'] = 'MAXENT'
      self.metrics['processType'] = self.PROCESS_TYPE
      #self.metrics['numPoints'] = len(self.job.points.point)
      #self.matrics['numLayers'] = len(self.job.layers.layer)
      
      self.dataDir = JOB_DATA_PATH
      self.jobLayerDir = os.path.join(self.workDir, 'layers')
      self.samplesFile = os.path.join(self.workDir, 'samples.csv')

      # Fix for species names with author strings
      self.spName = self.job.points.displayName.replace(',', '_').replace('(', '_').replace(')', '_')
      lambdaSpName = toUnicode(self.spName).replace(toUnicode(' '), toUnicode('_'))
      
      self.lambdasFile = os.path.join(toUnicode(self.workDir), 
                                      toUnicode(toUnicode('{0}.lambdas'
                                                       ).format(lambdaSpName)))

      #self.lambdasFile = os.path.join(self.workDir, '{0}.lambdas'.format(
      #                          self.spName.replace(' ', '_')))
      
      if not os.path.exists(self.jobLayerDir):
         os.makedirs(self.jobLayerDir)
      
      self.log.debug("MaxEnt Version: %s" % ME_VERSION)
      self.log.debug("-------------------------------------------------------")

      # Layers
      layers = []
      for lyr in self.job.layers:
         try:
            lUrl = lyr.layerUrl
         except:
            lUrl = None
         layers.append((lyr.identifier, lUrl))
      
      try:
         mEl = self.job.mask
         try:
            mUrl = mEl.layerUrl
         except:
            mUrl = None
         mask = (mEl.identifier, mUrl)
      except: # mask not provided
         mask = (None, None)
      
      self.log.debug("Acquiring inputs...")
      self.status = JobStatus.ACQUIRING_INPUTS
      self._update()

      handleLayers(layers, self.env, self.dataDir, self.jobLayerDir, mask=mask)
      self.log.debug("Inputs acquired...")

      # Points
      self._writePoints()
      
   # .......................................
   def _finishJob(self):
      """
      @summary: Move outputs we want to keep to the specified location
      @todo: Determine if anything else should be moved
      @todo: Should we take a name parameter?
      @todo: Move entire model package?
      """
      # Options to keep:
      #  self.modelLogFile
      #  self.modelRequestFile
      #  metrics
      #  Maxent package of outputs
      
      if self.outDir is not None:
         # CJG - 06/22/2015
         #   If a Maxent model has multiple lambdas files, don't push one back
         #      as the "ruleset" for the whole model.  Rather, just add them
         #      all to the package and we won't allow projections for these
         #      experiments
         shutil.move(self.lambdasFile, self.outDir)
         
   # ...................................
   def _writePoints(self):
      """
      @summary: Writes out the points of the job in a format MaxEnt can read
      """
      self.log.debug("Writing points")
      f = open(self.samplesFile, 'w')
      f.write("Species, X, Y\n")
      
      for pt in self.job.points:
         a = toUnicode("{0}, {1}, {2}\n")
         b = a.format(self.spName, toUnicode(pt.x), toUnicode(pt.y))
         #f.write("{0}, {1}, {2}\n".format(self.spName, pt.x,pt.y))
         f.write(fromUnicode(b))
      f.close()
      self.metrics['numPoints'] = len(self.job.points)
      
# .............................................................................
class MEProjectionRunner(ApplicationRunner):
   """
   @summary: openModeller projection job runner
   """
   PROCESS_TYPE = ProcessType.ATT_PROJECT
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
      baseCmd = "{1} {2} {3}".format(BIN_PATH, JAVA_CMD, ME_CMD, PRJ_TOOL)
      outFile = os.path.join(self.workDir, 'output.asc')

      algoOptions = getAlgorithmOptions(self.job.algorithm)
      args = "nowarnings nocache autorun -z"
      #cmd = "{baseCmd} {lambdaFile} {gridDir} {outFile} {args}".format(
      #            baseCmd=baseCmd, gridDir=gridDir, outFile=outFile, args=args)
      cmd = "{0} {1} {2} {3} {4} {5}".format(baseCmd, self.lambdasFile, 
                                               self.jobLayerDir, outFile, 
                                               algoOptions, args)
      self.log.debug(cmd)
      
      if not os.path.exists(ME_CMD):
         self.status = JobStatus.LM_JOB_APPLICATION_NOT_FOUND
         raise LmException(JobStatus.LM_JOB_APPLICATION_NOT_FOUND, 
                           "Could not find: %s" % ME_CMD)
      return cmd
   
   # .......................................
   def _checkOutput(self):
      """
      @summary: Checks the output of openModeller to see if any errors occurred
      """
      self.log.debug("Checking output")
      # If an output raster does not exist, an error occurred
      if not os.path.exists(self.outputFile):
         errfname = os.path.join(self.workDir, 'maxent.log')
         if os.path.exists(errfname):
            with open(errfname) as f:
               logContent = f.read()
               self.log.debug("---------------------------------------")
               self.log.debug(logContent)
               self.log.debug("---------------------------------------")
               print("-----------------------------------------------")
               print logContent
               print("-----------------------------------------------")
            if logContent.find('have different geographic dimensions') >= 0:
               self.status = JobStatus.ME_MISMATCHED_LAYER_DIMENSIONS
            elif logContent.find('NumberFormatException') >= 0:
               self.status = JobStatus.ME_CORRUPTED_LAYER
            elif logContent.find('because it has 0 training samples') >= 0:
               self.status = JobStatus.ME_POINTS_ERROR
            elif logContent.find('is missing from') >= 0: #ex: Layer vap6190_ann is missing from layers/projectionScn
               self.status = JobStatus.ME_LAYER_MISSING
            else:
               self.status = JobStatus.ME_GENERAL_ERROR
         else:
            self.log.debug("Missing %s file" % errfname)
            self.status = JobStatus.ME_GENERAL_ERROR
   
   # .......................................
   def _processJobInput(self):
      """
      @summary: Initializes a projection for generation
      """
      self.log.debug("Processing job input")
      self.metrics['jobId'] = self.job.jobId
      self.metrics['algorithm'] = 'MAXENT'
      self.metrics['processType'] = self.PROCESS_TYPE

      self.dataDir = JOB_DATA_PATH
      self.jobLayerDir = os.path.join(self.workDir, 'layers')
      self.lambdasFile = os.path.join(self.workDir, 'input.lambdas')
      self.outputFile = os.path.join(self.workDir, 'output.asc')

      if not os.path.exists(self.jobLayerDir):
         os.makedirs(self.jobLayerDir)

      self.log.debug("MaxEnt Version: %s" % ME_VERSION)
      self.log.debug("-------------------------------------------------------")
      
      # Layers
      layers = []
      for lyr in self.job.layers:
         try:
            lUrl = lyr.layerUrl
         except:
            lUrl = None
         layers.append((lyr.identifier, lUrl))

      try:
         mEl = self.job.mask
         try:
            mUrl = mEl.layerUrl
         except:
            mUrl = None
         mask = (mEl.identifier, mUrl)
      except: # mask not provided
         mask = (None, None)
      
      self.log.debug("Acquiring inputs...")
      self.status = JobStatus.ACQUIRING_INPUTS
      self._update()
      
      handleLayers(layers, self.env, self.dataDir, self.jobLayerDir, mask=mask)
      #handleLayers(self.job.layers, self.env, self.dataDir, self.jobLayerDir)
      
      
      ## Get omission file from package
      ##      This is needed for cumulative projections
      #packageUrl = "%s/package" % self.job.parentUrl
      #
      ## Pull value of url and put it in StringIO object
      #pkgZipString = StringIO(urllib2.urlopen(packageUrl).read())
      #
      ## Make it a zip file
      #with zipfile.ZipFile(pkgZipString, mode="r", allowZip64=True) as zf:
      #   # Look for omissions file in name list
      #   for fn in zf.namelist():
      #      if fn.find("_omission.csv") >= 0:
      #         # Extract the file if found
      #         omissionFn = os.path.join(self.workDir, 'input_omission.csv')
      #         omissionContents = zf.read(fn)
      #         with open(omissionFn, 'w') as oF:
      #            oF.write(omissionContents)
      #         #zf.extract(fn, omissionFn)
      
      
      self.log.debug("Inputs acquired...")
      
      # Look for a layer that is outside of the range of the layers provided
      #   This will be layerXX where XX is the length of the layers list
      #   because the list is zero based.  Replace that layer with mask
      print self.job.lambdas
      lambdas = self.job.lambdas.replace('layer{0}'.format(len(layers)), 'mask')
      print lambdas
      
      
      f = open(self.lambdasFile, 'w')
      f.write(lambdas)
      f.close()
      
      # Handle scenario where lambdas includes a mask but one is not provided for the projection
      if mask is None:
         # Look to see if the model was created with a mask
         #   New maxent models will have a mask layer named 'mask'
         #   Old models will have a mask layer named layerX where X is equal to
         #      the number of layers in the scenario

         fn = None         
         if lambdas.find('mask') >= 0:
            fn = 'mask.asc'
         elif lambdas.find('layer{0}'.format(len(layers))) >= 0:
            fn = 'layer{0}.asc'.format(len(layers))
      
         if fn is not None: # Mask was used in model creation
            # Pick the first layer in the layers directory (layer0.asc will exist if valid)
            basedOnLayer = os.path.join(self.jobLayerDir, 'layer0.asc')
            maskFn = os.path.join(self.jobLayerDir, fn)
            try:
               createMaskLayer(maskFn, basedOnLayer)
            except:
               self.status = JobStatus.IO_LAYER_READ_ERROR
               self._update()
      
      self.log.debug("Ready to compute")

   # .......................................
   def _finishJob(self):
      """
      @summary: Move outputs we want to keep to the specified location
      @todo: Determine if anything else should be moved
      @todo: Should we take a name parameter?
      @todo: Move entire package?
      """
      # Options to keep:
      #  projection LogFile
      #  proj req file
      #  metrics
      #  Maxent package of outputs
      
      #TODO: Move these to their own post-processing scripts
      outFn = os.path.join(self.workDir, '%s.tif' % self.jobName)
      # Look to see if the layer should be scaled
      try: # Will fail if scale element does not exist and will use plain convert
         scaleDataType = self.job.postProcessing.scale.dataType

         if scaleDataType.strip().lower() == "int":
            scaleMin = int(self.job.postProcessing.scale.scaleMin)
            scaleMax = int(self.job.postProcessing.scale.scaleMax)
         else:
            scaleMin = float(self.job.postProcessing.scale.scaleMin)
            scaleMax = float(self.job.postProcessing.scale.scaleMax)
         
         convertAndModifyAsciiToTiff(self.outputFile, outFn, 
                                     scale=(scaleMin, scaleMax), 
                                     dataType=scaleDataType)
      except:
         try: # See if the output should be multiplied instead
            multiplyDataType = self.job.postProcessing.multiply.dataType
            multiplier = int(self.job.postProcessing.multiply.multiplier)
            convertAndModifyAsciiToTiff(self.outputFile, outFn, 
                                        multiplier=multiplier, 
                                        dataType=multiplyDataType)
         except:
            convertAndModifyAsciiToTiff(self.outputFile, outFn)
      
      
      if self.outDir is not None:
         # CJG - 06/22/2015
         #   If a Maxent model has multiple lambdas files, don't push one back
         #      as the "ruleset" for the whole model.  Rather, just add them
         #      all to the package and we won't allow projections for these
         #      experiments
         shutil.move(outFn, self.outDir)

# .................................
def getAlgorithmOptions(algo):
   """
   @summary: Processes the algorithm parameters provided to generate 
                command-line options
   """
   params = []
   if algo.parameter is not None:
      for param in algo.parameter:
         p = processParameter(param.id, param.value)
         if p is not None:
            params.append(p)
   return ' '.join(params)
         
# .................................
def processParameter(param, value):
   """
   @summary: Processes an individual parameter and value
   """
   p = PARAMETERS[param]
   if value is None or value == 'None':
      return None
   v = p['process'](value)
   if p.has_key('options'):
      v = p['options'][v]
   if v != p['default']:
      return "{parameter}={value}".format(parameter=param, value=v)
   else:
      return None

# .................................
def handleLayers(layers, env, dataDir, jobLayerDir, mask=(None, None)):
   """
   @summary: Iterates through the list of layer urls and stores them on the 
                file system if they are not there yet.  Then creates links
                in the job layer directory so that the layers may be stored
                long term on the machine but still used per job.
   @param layers: A list of tuples of the form layer id, layer url
   @param layerUrls: List of layer urls
   @param env: The environment to operate in
   @param dataDir: Directory to store layers
   @param jobLayerDir: The layer directory of the job
   @param mask: (optional) Mask layer to be added if provided (layer id, layer url)
   """
   lyrs = []
   lyrMgr = LayerManager(dataDir)
   
   for layerId, layerUrl in layers:
      lyrs.append(lyrMgr.getLayerFilename(layerId, LayerFormat.MXE, layerUrl))
      
   for i in range(len(lyrs)):
      os.symlink("{0}/layer{1}{2}".format(jobLayerDir, i, OutputFormat.MXE), 
                 lyrs[i])
   
   if mask[0] is not None:
      mskLyr = lyrMgr.getLayerFilename(mask[0], LayerFormat.MXE, mask[1])
      os.symlink("{0}/mask{1}".format(jobLayerDir, OutputFormat.MXE), 
                     mskLyr)
      
   lyrMgr.close()

# .................................
def createMaskLayer(maskFn, basedOnFn):
   """
   @summary: This function will create a dummy mask layer that has the same 
                headers as the layer specified by 'basedOnFn'
   @param maskFn: The mask layer will be written to this file path
   @param basedOnFn: The new mask layer will be created to match this layer 
                        with respect to extent and resolution
   @todo: This would be a good place to use a tool built for area refine
   """
   if os.path.exists(maskFn):
      raise Exception, "Mask filename already exists"
   
   if os.path.exists(basedOnFn):
      # Read layer headers
      with open(basedOnFn) as inF:
         cont = True
         while cont:
            line = inF.next()
            if line.startswith('ncols'):
               nCols = int(line.split('ncols')[1].strip())
            elif line.startswith('nrows'):
               nRows = int(line.split('nrows')[1].strip())
            elif line.startswith('xllcorner'):
               xll = line
            elif line.startswith('yllcorner'):
               yll = line
            elif line.startswith('cellsize'):
               cellsize = line
            elif line.startswith('NODATA_value'):
               nodata = line
            elif line.startswith('dx'):
               cellsize = line.replace('dx', 'cellsize')
            elif line.startswith('dy'):
               pass
            else:
               cont = False
      
      with open(maskFn, 'w') as outF:
         outF.write('ncols     {0}\n'.format(nCols))
         outF.write('nrows     {0}\n'.format(nRows))
         outF.write(xll)
         outF.write(yll)
         outF.write(cellsize)
         # Write no data line without end line to make output of 1s easier
         outF.write(nodata.replace('\n', ''))
         
         # Write out 1s
         for y in xrange(nRows):
            row = ['1'] * nCols
            outF.write('\n{0}'.format(' '.join(row)))
   else:
      raise Exception, "Climate layer to be used in mask generation does not exist"

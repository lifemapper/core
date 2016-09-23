"""
@summary: Module containing the job runners for openModeller models and 
             projections
@author: CJ Grady
@version: 3.0.0
@status: beta

@note: Commands are for openModeller library version 1.3.0
@note: Commands may be backwards compatible

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
import os

from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCompute.jobs.runners.applicationRunner import ApplicationRunner
from LmCompute.common.lmconstants import BIN_PATH
from LmCompute.plugins.sdm.openModeller.constants import (OM_VERSION,
                                          DEFAULT_LOG_LEVEL, OM_MODEL_CMD, 
                                          OM_PROJECT_CMD)
from LmCompute.plugins.sdm.openModeller.omRequest import (OmModelRequest, 
                                                          OmProjectionRequest)

# .............................................................................
class OMModelRunner(ApplicationRunner):
   """
   @summary: openModeller model job runner
   """
   PROCESS_TYPE = ProcessType.OM_MODEL
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
      cmd = "%s -r %s -m %s --log-level %s --log-file %s --prog-file %s" % \
               (mdlBinary, self.modelRequestFile, self.modelResultFile,
                self.modelLogLevel, self.modelLogFile, self.modelProgressFile)

      if not os.path.exists(mdlBinary):
         self.status = JobStatus.LM_JOB_APPLICATION_NOT_FOUND
         self._update()

      return cmd
   
   # ...................................
   def _checkApplication(self):
      """
      @summary: Checks the openModeller output files to get the progress and 
                   status of the running model.
      """
      f = open(self.modelProgressFile)
      self.progress = int(''.join(f.readlines()))
      f.close()
   
   # ...................................
   def _checkOutput(self):
      """
      @summary: Checks the output of openModeller to see if any errors occurred
      """
      if self.progress == -2:
         self.status = self._getModelErrorStatus()
      elif self.progress == 100:
         self.status = JobStatus.COMPUTED
      else:
         # Probably a seg fault or killed by signal
         self.status = self._getModelErrorStatus()
         
   # ...................................
   def _processJobInput(self):
      """
      @summary: Initializes a model to be ran by openModeller
      """
      self.metrics['jobId'] = self.job.jobId
      self.metrics['processType'] = self.PROCESS_TYPE
      self.metrics['algorithm'] = self.job.algorithm.code
      self.metrics['numPoints'] = len(self.job.points.point)


      self.modelLogFile = "%s/modLog-%s.log" % (self.outputPath, self.job.jobId)
      self.modelProgressFile = "%s/modProg-%s.txt" % (self.outputPath, 
                                                      self.job.jobId)
      self.modelRequestFile = "%s/modReq-%s.xml" % (self.outputPath, 
                                                    self.job.jobId)
      self.modelResultFile = "%s/mod-%s.xml" % (self.outputPath, self.job.jobId)

      self.log.debug("openModeller Version: %s" % OM_VERSION)
      self.log.debug("-------------------------------------------------------")

      self.modelLogLevel = DEFAULT_LOG_LEVEL
      
      self.status = JobStatus.ACQUIRING_INPUTS
      self._update()
      self.log.debug("Acquiring inputs")
      # Generate a model request file and write it to the file system
      req = OmModelRequest(self.job, self.env.getJobDataPath())
      reqFile = open(self.modelRequestFile, "w")
      cnt = req.generate()
      self.log.debug("Inputs acquired")
     
      # At least as of openModeller 1.3, need to remove the xml version line
      if cnt.startswith("<?xml version"):
         tmp = cnt.split('\n')
         cnt = '\n'.join(tmp[1:])
      reqFile.write(cnt)
      reqFile.close()
   
   # .......................................
   def _getModelErrorStatus(self):
      """
      @summary: Checks the model log file to determine what error occurred. 
      """
      f = open(self.modelLogFile)
      omLog = ''.join(f.readlines())
      f.close()
      
      print("openModeller error log")
      print("-----------------------")
      print(omLog)
      print("-----------------------")
      
      status = JobStatus.UNKNOWN_ERROR
      
      if omLog.find("[Error] No presence points available") >= 0:
         status = JobStatus.OM_MOD_REQ_POINTS_MISSING_ERROR
      elif omLog.find(
          "[Error] Cannot use zero presence points for sampling") >= 0:
         status = JobStatus.OM_MOD_REQ_POINTS_MISSING_ERROR
      elif omLog.find("[Error] Algorithm could not be initialized") >= 0:
         status = JobStatus.OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR
      elif omLog.find(
          "[Error] Cannot create model without any presence or absence point."
          ) >= 0:
         status = JobStatus.OM_MOD_REQ_POINTS_OUT_OF_RANGE_ERROR
      elif omLog.find("[Error] XML Parser fatal error: not well-formed") >= 0:
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
   
   # .......................................
   def _push(self):
      """
      @summary: Pushes the results of the job to the job server
      """
      if self.status < JobStatus.GENERAL_ERROR:
         self.status = JobStatus.PUSH_REQUESTED
         component = "model"
         contentType = "application/xml"
         content = open(self.modelResultFile).read()
         self.log.debug("\n\n-------------\nModel Results\n-------------\n\n")
         self.log.debug(content)
         self._update()
         try:
            self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, 
                                                        contentType, component)
            try:
               self._pushPackage()
            except:
               pass
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
   def _pushPackage(self):
      """
      @summary: Pushes the entire package back to the job server
      @note: Does not push back layers directory
      """
      from StringIO import StringIO
      import zipfile

      component = "package"
      contentType = "application/zip"
      
      outStream = StringIO()
      zf = zipfile.ZipFile(outStream, 'w', compression=zipfile.ZIP_DEFLATED,
                              allowZip64=True)
      zf.write(self.modelLogFile, os.path.split(self.modelLogFile)[1])
      zf.write(self.modelResultFile, os.path.split(self.modelResultFile)[1])
      zf.write(self.modelRequestFile, os.path.split(self.modelRequestFile)[1])
      zf.write(self.jobLogFile, os.path.split(self.jobLogFile)[1])
      zf.writestr("metrics.txt", self._getMetricsAsStringIO().getvalue())
      zf.close()      
      outStream.seek(0)
      content = outStream.getvalue()      
      self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, contentType, 
                                                                     component)
   
# .............................................................................
class OMProjectionRunner(ApplicationRunner):
   """
   @summary: openModeller projection job runner
   """
   PROCESS_TYPE = ProcessType.OM_PROJECT
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
      cmd = "%s -r %s -m %s --log-level %s --log-file %s --prog-file %s --stat-file %s" % \
            (prjBinary, self.projRequestFile, self.projResultFile,
             self.projLogLevel, self.projLogFile, self.projProgressFile,
             self.projStatFile)
      
      if not os.path.exists(prjBinary):
         self.status = JobStatus.LM_JOB_APPLICATION_NOT_FOUND
         self._update()

      return cmd
   
   # .......................................
   def _checkApplication(self):
      """
      @summary: Checks the openModeller output files to get the progress and 
                   status of the running projection.
      """
      f = open(self.projProgressFile)
      self.progress = int(''.join(f.readlines()))
      f.close()
      
   # .......................................
   def _checkOutput(self):
      """
      @summary: Checks the output of openModeller to see if any errors occurred
      """
      if self.progress == -2:
         self.status = self._getProjectionErrorStatus()
      elif self.progress == 100:
         self.status = JobStatus.COMPUTED
      else:
         # Probably a seg fault or killed by signal
         self.status = self._getProjectionErrorStatus()
   
   # .......................................
   def _getProjectionErrorStatus(self):
      """
      @summary: Checks the projection log file to determine what error occurred
      @todo: Look for errors in log file
      """
      f = open(self.projLogFile)
      omLog = ''.join(f.readlines())
      f.close()
      print("openModeller error log")
      print("-----------------------")
      print(omLog)
      print("-----------------------")
      
      # Need to look for specific projection errors
      status = JobStatus.OM_PROJECTION_ERROR
      
      return status
   
   # .......................................
   def _processJobInput(self):
      """
      @summary: Initializes a projection for generation
      """
      self.metrics['jobId'] = self.job.jobId
      self.metrics['processType'] = self.PROCESS_TYPE


      self.projLogFile = "%s/projLog-%s.log" % (self.outputPath, self.job.jobId)
      self.projLogLevel = DEFAULT_LOG_LEVEL
      self.projProgressFile = "%s/projProg-%s.txt" % (self.outputPath, 
                                                                self.job.jobId)
      self.projRequestFile = "%s/projReq-%s.xml" % (self.outputPath, 
                                                                self.job.jobId)
      self.projResultFile = "%s/proj-%s.tif" % (self.outputPath, self.job.jobId)
      self.projStatFile = "%s/projStats-%s.txt" % (self.outputPath, 
                                                                self.job.jobId)
      
      
      self.status = JobStatus.ACQUIRING_INPUTS
      self._update()
      self.log.debug("Acquiring inputs")
      # Generate a projection request file and write it to the file system
      req = OmProjectionRequest(self.job, self.env.getJobDataPath())
      reqFile = open(self.projRequestFile, "w")
      cnt = req.generate()
      self.log.debug("Inputs acquired")
     
      # At least as of openModeller 1.3, need to remove the xml version line
      if cnt.startswith("<?xml version"):
         tmp = cnt.split('\n')
         cnt = '\n'.join(tmp[1:])
      reqFile.write(cnt)
      reqFile.close()

   # .......................................
   def _push(self):
      """
      @summary: Pushes the results of the job to the job server
      """
      if self.status < JobStatus.GENERAL_ERROR:
         self.status = JobStatus.PUSH_REQUESTED
         component = "projection"
         contentType = "image/tiff"
         content = open(self.projResultFile).read()
         self._update()
         try:
            self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, 
                                                        contentType, component)
            try:
               self._pushPackage()
            except IOError, ioe:
               print(str(ioe))
               print("Log file (%s) exist? %s" % (self.projLogFile, os.path.exists(self.projLogFile)))
               print("Request file (%s) exist? %s" % (self.projRequestFile, os.path.exists(self.projRequestFile)))
               print("Statistics file (%s) exist? %s" % (self.projStatFile, os.path.exists(self.projStatFile)))
               print("Job log file (%s) exist? %s" % (self.jobLogFile, os.path.exists(self.jobLogFile)))
            except Exception, e:
               print("Exception while trying to post package:")
               print(str(e))
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
   def _pushPackage(self):
      """
      @summary: Pushes the entire package back to the job server
      @note: Does not push back layers directory
      """
      from StringIO import StringIO
      import zipfile

      component = "package"
      contentType = "application/zip"
      
      outStream = StringIO()
      zf = zipfile.ZipFile(outStream, 'w', compression=zipfile.ZIP_DEFLATED,
                              allowZip64=True)
      
      zf.write(self.projLogFile, os.path.split(self.projLogFile)[1])
      zf.write(self.projRequestFile, os.path.split(self.projRequestFile)[1])
      zf.write(self.projStatFile, os.path.split(self.projStatFile)[1])
      zf.write(self.jobLogFile, os.path.split(self.jobLogFile)[1])
      zf.writestr("metrics.txt", self._getMetricsAsStringIO().getvalue())
      print(self.metrics)

      zf.close()      
      outStream.seek(0)
      content = outStream.getvalue()      
      self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, contentType, 
                                                                     component)

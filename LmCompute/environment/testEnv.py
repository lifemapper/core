"""
@summary: Contains the Lifemapper test environment class
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
import os
from random import choice
import uuid

from LmCommon.common.lmconstants import ProcessType
from LmCommon.common.lmXml import deserialize, fromstring

from LmCompute.environment.environmentMethods import _EnvironmentMethods
from LmCompute.common.layerManager import LayerManager
from LmCompute.common.localconstants import BIN_PATH, JOB_DATA_PATH, \
                                    JOB_OUTPUT_PATH, PLUGINS_DIR, PYTHON_CMD, \
                                    SAMPLE_JOBS_PATH, SAMPLE_LAYERS_PATH, \
                                    TEMPORARY_FILE_PATH

# Create a constant to save space
VALID_TYPES = [ProcessType.OM_MODEL,
          ProcessType.OM_PROJECT,
          ProcessType.ATT_MODEL,
          ProcessType.ATT_PROJECT,
          #ProcessType.RAD_INTERSECT,
          #ProcessType.RAD_COMPRESS,
          #ProcessType.RAD_SWAP,
          #ProcessType.RAD_SPLOTCH,
          #ProcessType.RAD_CALCULATE,
          ProcessType.GBIF_TAXA_OCCURRENCE,
          ProcessType.BISON_TAXA_OCCURRENCE,
          ProcessType.IDIGBIO_TAXA_OCCURRENCE]

# .............................................................................
class TestEnv(_EnvironmentMethods):
   """
   @summary: Lifemapper cluster environment methods.
   """
   # ..................................
   def __init__(self, job):
      """
      @summary: Constructor
      """
      self._initialize()
      
   def _initialize(self):
      # check for existing layers db
      lyrBasePath = os.path.join(self.getJobDataPath(), "layers")
      dbFile = os.path.join(lyrBasePath, "layers.db")

      # if it doesn't exist, seed layers
      if not os.path.exists(dbFile):
         lyrMgr = LayerManager(self.getJobDataPath())
         seedLayers = []
         f = open(SAMPLE_LAYERS_PATH)
         seedLayers = [tuple(line.split(', ')) for line in f.readlines()]
         f.close()
         for lyrUrl, fn in seedLayers:
            lyrMgr.seedLayer(lyrUrl, os.path.join(lyrBasePath, fn))
         lyrMgr.close()
   
   # ..................................
   def createLink(self, fromPath, toPath):
      """
      @summary: Creates a link between two paths
      @param fromPath: The new file / directory
      @param toPath: The file / directory that it points to
      """
      if not os.path.islink(fromPath):
         os.symlink(toPath, fromPath)

   # ..................................
   def finalizeJob(self, jobType, jobId):
      """
      @summary: Finalizes a job and does any necessary cleanup
      @note: The test environment doesn't need to do anything with this at this 
                time
      """
      pass
   
   # ..................................
   def getApplicationPath(self):
      """
      @summary: Gets the application path for this environment
      @return: The base path for applications in this environment
      @rtype: String
      """
      return BIN_PATH
   
   # ..................................
   def getJobDataPath(self):
      """
      @summary: Gets the job input data path for this environment
      @return: The base path for job input data in this environment
      @rtype: String
      """
      return JOB_DATA_PATH
   
   # ..................................
   def getJobOutputPath(self):
      """
      @summary: Gets the job output data path for this environment
      @return: The base path for job output data in this environment
      @rtype: String
      """
      return JOB_OUTPUT_PATH
   
   # ..................................
   def getPluginsPath(self):
      """
      @summary: Gets the path to the plugins directory
      @return: The base path for Lifemapper compute plugins
      @rtype: String
      """
      return PLUGINS_DIR
   
   # ..................................
   def getPythonCmd(self):
      """
      @summary: Gets the command to run python for this machine
      @return: Path to the python executable
      @rtype: String
      """
      return PYTHON_CMD

   # ..................................
   def getTemporaryFilename(self, extension, base=None):
      """
      @summary: Gets a filename for a temporary file
      @return: A temporary filename
      @rtype: String
      """
      if base is None:
         base = TEMPORARY_FILE_PATH
      return os.path.join(base, "%s%s" % (uuid.uuid4(), extension))
   
   # ..................................
   def postJob(self, jobType, jobId, content, contentType, component):
      """
      @summary: Posts (part of) a job via the environment
      @param jobType: The type of job being posted
      @param jobId: The id of the job being posted
      @param content: The content of the post
      @param contentType: The MIME-type of the content
      @param component: The part of the job being posted (data, log, error, etc)
      @note: The test environment does not actually push the data back, so 
                return None
      """
      return None
   
   # ..................................
   def requestJob(self, validTypes=VALID_TYPES, parameters={}):
      """
      @summary: Requests a job to run
      @param validTypes: A list of the job types this environment can process
      @param parameters: An optional dictionary of parameters specifying a 
                            subset of jobs that this environment is willing to 
                            compute
      """
      jt = choice(validTypes)
      sampleJobsPath = SAMPLE_JOBS_PATH
      if jt in [ProcessType.OM_MODEL]:
         jobXmlFn = os.path.join(sampleJobsPath, 'omModel.xml')
      elif jt in [ProcessType.OM_PROJECT]:
         jobXmlFn = os.path.join(sampleJobsPath, 'omProjection.xml')
      elif jt in [ProcessType.ATT_MODEL]:
         jobXmlFn = os.path.join(sampleJobsPath, 'meModel.xml')
      elif jt in [ProcessType.ATT_PROJECT]:
         jobXmlFn = os.path.join(sampleJobsPath, 'meProjection.xml')
      elif jt in [ProcessType.RAD_INTERSECT]:
         jobXmlFn = os.path.join(sampleJobsPath, 'radIntersect.xml')
      elif jt in [ProcessType.RAD_COMPRESS]:
         jobXmlFn = os.path.join(sampleJobsPath, 'radCompress.xml')
      elif jt in [ProcessType.RAD_SWAP]:
         jobXmlFn = os.path.join(sampleJobsPath, 'radSwap.xml')
      elif jt in [ProcessType.RAD_SPLOTCH]:
         jobXmlFn = os.path.join(sampleJobsPath, 'radSplotch.xml')
      elif jt in [ProcessType.RAD_CALCULATE]:
         jobXmlFn = os.path.join(sampleJobsPath, 'radCalculate.xml')
      elif jt in [ProcessType.GBIF_TAXA_OCCURRENCE]:
         jobXmlFn = os.path.join(sampleJobsPath, 'gbifPoints.xml')
      elif jt in [ProcessType.BISON_TAXA_OCCURRENCE]:
         jobXmlFn = os.path.join(sampleJobsPath, 'bisonPoints.xml')
      elif jt in [ProcessType.IDIGBIO_TAXA_OCCURRENCE]:
         jobXmlFn = os.path.join(sampleJobsPath, 'idigbioPoints.xml')
      else:
         raise Exception, "Unknown job type requested"
      job = deserialize(fromstring(open(jobXmlFn).read()))
      return job

   # ..................................
   def updateJob(self, jobType, jobId, status, progress):
      """
      @summary: Updates the job status information in whatever manages it for
                   this environment
      @param status: The new status of the job
      @param progress: The new progress of the job
      @return: Value indicating success
      @rtype: Boolean
      @note: The test environment doesn't actually do this, so return True
      """
      return True
   
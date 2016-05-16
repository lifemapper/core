"""
@summary: Module containing Lifemapper Makeflow document builder
@author: CJ Grady
@status: alpha
@version: 0.1
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

@note: Inputs could be objects or job objects
@todo: Many functions can take a subworkflow or a pre-existing object, handle that
@note: Start by using job objects
@note: Let job server handle object updates for now
"""
import os

from LmServer.common.lmconstants import JobFamily
from LmCompute.common.localconstants import JOB_REQUEST_DIR, PYTHON_CMD
from LmServer.common.localconstants import APP_PATH
from LmCommon.common.lmconstants import JobStatus

# For now, most of the jobs follow the same pattern, build the request, submit
BASE_JOB_COMMAND = """\
# Build job request
$JOB_REQUESTS/{processType}-{jobId}Req.xml: {dependency}
   $PYTHON $MAKE_JOB_REQUEST {objectFamily} {jobId} -f $JOB_REQUESTS/{processType}-{jobId}Req.xml

# Run job
{dLocation}: $JOB_REQUESTS/{processType}-{jobId}Req.xml
   $PYTHON $RUNNER $JOB_REQUESTS/{processType}-{jobId}Req.xml
   
"""

JOB_REQUEST_FILENAME = "$JOB_REQUESTS/{processType}-{jobId}Req.xml"
BUILD_JOB_REQUEST_CMD = "$PYTHON $MAKE_JOB_REQUEST {objectFamily} {jobId} -f {jrFn}"
LM_JOB_RUNNER_CMD = "$PYTHON $RUNNER {jrFn}"

# .............................................................................
class LMMakeflowDocument(object):
   """
   @summary: Class used to generate a Makeflow document with Lifemapper 
                computational jobs
   """
   # ...........................
   def __init__(self):
      """
      @summary: Constructor
      """
      self.jobs = []
      self.inputs = []
      self.outputs = []
      self.headers = []
      # Calling this automatically.  We will have conditional headers someday
      self._addDocumentHeaders()
   
   # ...........................
   def _addJobCommand(self, outputs, cmd, dependencies=[], comment=''):
      """
      @summary: Adds a job command to the document
      @param outputs: A list of output files created by this job
      @param cmd: The command to execute
      @param dependencies: A list of dependencies (files that must exist before 
                              this job can run
      """
      job = """\
# {comment}
{outputs}: {dependencies}
   {cmd}
""".format(outputs=' '.join(outputs), cmd=cmd, comment=comment, 
           dependencies=' '.join(dependencies))
      self.jobs.append(job)
      self.outputs.extend(outputs)
   
   # ...........................
   def _addDocumentHeaders(self):
      self.headers.append("PYTHON={pyCmd}".format(pyCmd=PYTHON_CMD))
      self.headers.append("RUNNER={factoryPath}".format(
                                       factoryPath=os.path.join(APP_PATH, 
                                         "LmCompute/jobs/runners/factory.py")))
      self.headers.append("JOB_REQUESTS={jrDir}".format(jrDir=JOB_REQUEST_DIR))
      self.headers.append("MAKE_JOB_REQUEST={jrScript}".format(
                                 jrScript=os.path.join(APP_PATH, 
                                      "LmBackend/makeflow/makeJobRequest.py")))
      #REQ_FILL=$HOME/git/core/LmCompute/scripts/fillProjectionRequest.py
      
   # ...........................
   def buildOccurrenceSet(self, occJob):
      """
      @summary: Adds commands to build an occurrence set
      @todo: Consider adding an option to force a rebuild
      @note: We will continue to use the job server (for now) to update the job
      @todo: To remove job server, need to have a script update the db here
      """
      jobId = occJob.getId()
      jrFn = JOB_REQUEST_FILENAME.format(processType=occJob.processType, 
                                         jobId=jobId)
      jrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.SDM, 
                                           jobId=jobId, jrFn=jrFn)
      # Add job to create job request
      self._addJobCommand([jrFn], jrCmd, 
                  comment='Build occurrence set {0} job request'.format(jobId))
      
      # Add job to create occurrence set
      self._addJobCommand([occJob.outputObj.getDLocation()], 
                          LM_JOB_RUNNER_CMD.format(jrFn=jrFn),
                          dependencies=[jrFn], 
                          comment="Build occurrence set {0}".format(jobId))

   # ...........................
   def buildModel(self, mdlJob):
      """
      @summary: Adds commands to build an SDM model
      @param mdlJob: A Lifemapper model job
      @note: Output is model ruleset, this will be used in other jobs 
                potentially
      """
      # Determine if there is a dependent job to build an occurrence set
      if mdlJob.outputObj.occurrenceSet.status == JobStatus.COMPLETE:
         dep = []
      else:
         dep = [mdlJob.outputObj.occurrenceSet.getDLocation()]

      jobId = mdlJob.getId()
      jrFn = JOB_REQUEST_FILENAME.format(processType=mdlJob.processType, 
                                         jobId=jobId)
      jrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.SDM, 
                                           jobId=jobId, jrFn=jrFn)
      # Add job to create job request
      self._addJobCommand([jrFn], jrCmd, dependencies=dep,
                  comment='Build model {0} job request'.format(jobId))
      
      # Add job to create model
      self._addJobCommand([mdlJob.outputObj.getDLocation()], 
                          LM_JOB_RUNNER_CMD.format(jrFn=jrFn),
                          dependencies=[jrFn], 
                          comment="Build model {0}".format(jobId))
   
   # ...........................
   def buildProjection(self, prjJob):
      """
      @summary: Adds commands to create an SDM projection
      """
      # Determine if there is a dependent job to build a model
      if prjJob.outputObj.getModel().status == JobStatus.COMPLETE:
         dep = ''
      else:
         dep = prjJob.outputObj.getModel().getDLocation()

      jobId = prjJob.getId()
      jrFn = JOB_REQUEST_FILENAME.format(processType=prjJob.processType, 
                                         jobId=jobId)
      jrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.SDM, 
                                           jobId=jobId, jrFn=jrFn)
      # Add job to create job request
      self._addJobCommand([jrFn], jrCmd, dependencies=dep,
                  comment='Build projection {0} job request'.format(jobId))
      
      # Add job to create projection
      self._addJobCommand([prjJob.outputObj.getDLocation()], 
                          LM_JOB_RUNNER_CMD.format(jrFn=jrFn),
                          dependencies=[jrFn], 
                          comment="Build projection {0}".format(jobId))
   
   # ...........................
   def addProjectionToGlobalPAM(self, prj, shapegrid):
      """
      @todo: Do I need to pass Solr connection information or name GPAM?
      @note: Not ready
      """
      job = """\
# Intersect one layer
# Run through script
# Delete layer

  
"""
      #self.jobs.append(job)

   # ...........................
   def addNotification(self, toAddresses, subject, message, dependencies=[]):
      """
      @summary: Adds a notification job to the workflow
      """
      emailCmd = "$NOTIFIER {toAddrs} -s {subject} -m {msg}".format(
                              toAddrs=' -t '.join(toAddresses),
                              subject=subject, msg=message)
      self._addJobCommand([], emailCmd, dependencies=dependencies, 
                          comment="Notify user")

   # ...........................
   def addIntersectJob(self, intJob):
      # Loop through job inputs to see if layers have an incomplete status
      # Add completes to inputs?
      
      job = BASE_JOB_COMMAND.format(processType=intJob.processType, 
                                    jobId=intJob.getId(), 
                                    objectFamily=JobFamily.RAD, 
                                    dLocation=intJob.outputObj.getDLocation(),
                                    dependency='')
      self.jobs.append(job)
      self.outputs.append(intJob.outputObj.getDLocation())
   
   # ...........................
   def addCalculateAndCompressPAM(self, calcJob, compJob):
      # Calculate
      job = BASE_JOB_COMMAND.format(processType=calcJob.processType, 
                                    jobId=calcJob.getId(), 
                                    objectFamily=JobFamily.RAD, 
                                    dLocation=calcJob.outputObj.getDLocation(),
                                    dependency='')
      self.jobs.append(job)
      self.outputs.append(calcJob.outputObj.getDLocation())
      # Compress
      job = BASE_JOB_COMMAND.format(processType=compJob.processType, 
                                    jobId=compJob.getId(), 
                                    objectFamily=JobFamily.RAD, 
                                    dLocation=compJob.outputObj.getDLocation(),
                                    dependency='')
      self.jobs.append(job)
      self.outputs.append(compJob.outputObj.getDLocation())
   
   # ...........................
   #def randomizePAM(self, pam, method, iterations):
   def addRandomizeJob(self, randJob):
      job = BASE_JOB_COMMAND.format(processType=randJob.processType, 
                                    jobId=randJob.getId(), 
                                    objectFamily=JobFamily.RAD, 
                                    dLocation=randJob.outputObj.getDLocation(),
                                    dependency='')
      self.jobs.append(job)
      self.outputs.append(randJob.outputObj.getDLocation())

   # ...........................
   def write(self, filename):
      with open(filename, 'w') as outF:
         for header in self.headers:
            outF.write("%s\n" % header) # Assume that they need newlines
         
         for job in self.jobs:
            outF.write(job) # These have built-in newlines
   





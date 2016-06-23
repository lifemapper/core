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
@todo: Job request dir and python command constants need to move out of compute
@todo:   Or, use $PYTHON if that works and job requests will be generated in workspace
"""
import os

from LmCommon.common.lmconstants import JobStatus

from LmCompute.common.localconstants import JOB_REQUEST_DIR, PYTHON_CMD

from LmServer.base.lmobj import LMObject
from LmServer.common.lmconstants import JobFamily
from LmServer.common.localconstants import APP_PATH
from LmServer.sdm.sdmJob import SDMOccurrenceJob, SDMModelJob, SDMProjectionJob
from types import TupleType, ListType

JOB_REQUEST_FILENAME = "$JOB_REQUESTS/{processType}-{jobId}Req.xml"
BUILD_JOB_REQUEST_CMD = "LOCAL $PYTHON $MAKE_JOB_REQUEST {objectFamily} {jobId} -f {jrFn}"
LM_JOB_RUNNER_CMD = "$PYTHON $RUNNER {jrFn}"

# .............................................................................
class LMMakeflowDocument(LMObject):
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
      #TODO: Get these from constants
      self.headers.append("PYTHON={pyCmd}".format(pyCmd=PYTHON_CMD))
      self.headers.append("RUNNER={factoryPath}".format(
                                       factoryPath=os.path.join(APP_PATH, 
                                         "LmCompute/jobs/runners/factory.py")))
      self.headers.append("JOB_REQUESTS={jrDir}".format(jrDir=JOB_REQUEST_DIR))
      self.headers.append("MAKE_JOB_REQUEST={jrScript}".format(
                                 jrScript=os.path.join(APP_PATH, 
                                      "LmServer/makeflow/makeJobRequest.py")))
      #REQ_FILL=$HOME/git/core/LmCompute/scripts/fillProjectionRequest.py
   
   # ...........................
   def addProcessesForList(self, itemList):
      """
      @summary: Adds all of the processes necessary for the jobs or objects in 
                   the list
      @param itemList: A list of jobs or objects
      """
      for item in itemList:
         try: # See if we have a self-aware object
            for targets, cmd, deps, comment in item.getMakeflowProcess():
               pass
         except: # Should fail until we implement functions on objects
            if isinstance(item, SDMOccurrenceJob):
               self.buildOccurrenceSet(item)
            elif isinstance(item, SDMModelJob):
               self.buildModel(item)
            elif isinstance(item, SDMProjectionJob):
               self.buildProjection(item)
            else:
               raise Exception, "Don't know how to build Makeflow process for: %s" % item.__class__
         
   # ...........................
   #def addProcessesForChain(self, jobChain):
   #   """
   #   @summary: Adds all of the processes necessary for jobs in the chain
   #   @param jobChain: A recursive iterable of (item, [dependencies])
   #   @note: This job chain will start as jobs, but will switch to objects
   #   """
   #   
   #   # ......................
   #   def addProcessForItem(item, deps=[]):
   #      """
   #      @summary: Internal function to process recursive structure of chain
   #      @param item: Job or object to add process for
   #      """
   #      try: # See if we have a self-aware object
   #         for targets, cmd, deps, comment in item.getMakeflowProcess():
   #            pass
   #      except: # Should fail until we implement functions on objects
   #         if isinstance(item, SDMOccurrenceJob):
   #            self.buildOccurrenceSet(item)
   #         elif isinstance(item, SDMModelJob):
   #            self.buildModel(item)
   #         elif isinstance(item, SDMProjectionJob):
   #            self.buildProjection(item)
   #         else:
   #            raise Exception, "Don't know how to build Makeflow process for: %s" % item.__class__
   #      
   #      for iTup in deps:
   #         if isinstance(iTup, (TupleType, ListType)):
   #            i, d = iTup
   #         else:
   #            i = iTup
   #            d = []
   #         addProcessForItem(i, d)
   #      
   #   item, deps = jobChain
   #   addProcessForItem(item, deps)
   
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
      self._addJobCommand([occJob.outputObj.createLocalDLocation()], 
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
         dep = [mdlJob.outputObj.occurrenceSet.createLocalDLocation()]

      jobId = mdlJob.getId()
      jrFn = JOB_REQUEST_FILENAME.format(processType=mdlJob.processType, 
                                         jobId=jobId)
      jrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.SDM, 
                                           jobId=jobId, jrFn=jrFn)
      # Add job to create job request
      self._addJobCommand([jrFn], jrCmd, dependencies=dep,
                  comment='Build model {0} job request'.format(jobId))
      
      # Add job to create model
      self._addJobCommand([mdlJob.dataObj.createLocalDLocation()], 
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
         dep = []
      else:
         mdl = prjJob.outputObj.getModel()
         dep = [mdl.createLocalDLocation()]

      jobId = prjJob.getId()
      jrFn = JOB_REQUEST_FILENAME.format(processType=prjJob.processType, 
                                         jobId=jobId)
      jrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.SDM, 
                                           jobId=jobId, jrFn=jrFn)
      # Add job to create job request
      self._addJobCommand([jrFn], jrCmd, dependencies=dep,
                  comment='Build projection {0} job request'.format(jobId))
      
      # Add job to create projection
      self._addJobCommand([prjJob.outputObj.createLocalDLocation()], 
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
      """
      @summary: Adds an intersect job to the workflow
      @note: Currently expects all input layers to be ready
      @todo: Figure out output object
      """
      # TODO: When an entire end to end chain can be built, fill in this variable
      deps = []
      
      jobId = intJob.getId()
      jrFn = JOB_REQUEST_FILENAME.format(processType=intJob.processType, 
                                         jobId=jobId)
      jrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.RAD, 
                                           jobId=jobId, jrFn=jrFn)
      # Add job to create job request
      self._addJobCommand([jrFn], jrCmd, dependencies=deps,
                  comment='Build intersect {0} job request'.format(jobId))
      
      # Add job to create intersect
      self._addJobCommand([intJob.outputObj.createLocalDLocation()], 
                          LM_JOB_RUNNER_CMD.format(jrFn=jrFn),
                          dependencies=[jrFn], 
                          comment="Build intersect {0}".format(jobId))
   
   # ...........................
   def addCalculateAndCompressPAM(self, calcJob, compJob):
      """
      @summary: Adds calculate and compress jobs to the workflow
      @todo: Establish dependencies
      """
      # Calculate
      calcDep = []
      calcOutput = calcJob.outputObj.createLocalDLocation()
      
      calcJobId = calcJob.getId()
      calcJrFn = JOB_REQUEST_FILENAME.format(processType=calcJob.processType, 
                                         jobId=calcJobId)
      calcJrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.RAD, 
                                           jobId=calcJobId, jrFn=calcJrFn)
      # Add job to create job request
      self._addJobCommand([calcJrFn], calcJrCmd, dependencies=calcDep,
                  comment='Build calculate {0} job request'.format(calcJobId))
      
      # Add job to calculate stats
      self._addJobCommand([calcJob.outputObj.createLocalDLocation()], 
                          LM_JOB_RUNNER_CMD.format(jrFn=calcJrFn),
                          dependencies=[calcJrFn], 
                          comment="Calculate stats {0}".format(calcJobId))
      
      # Compress
      compDep = [calcOutput]
      compJobId = compJob.getId()
      compJrFn = JOB_REQUEST_FILENAME.format(processType=compJob.processType, 
                                         jobId=compJobId)
      compJrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.RAD, 
                                           jobId=compJobId, jrFn=compJrFn)
      # Add job to create job request
      self._addJobCommand([compJrFn], compJrCmd, dependencies=compDep,
                  comment='Build compress {0} job request'.format(compJobId))
      
      # Add job to create model
      self._addJobCommand([compJob.outputObj.createLocalDLocation()], 
                          LM_JOB_RUNNER_CMD.format(jrFn=compJrFn),
                          dependencies=[compJrFn], 
                          comment="Compress {0}".format(compJobId))
   
   # ...........................
   #def randomizePAM(self, pam, method, iterations):
   def addRandomizeJob(self, randJob):
      """
      @summary: Add a randomize job to the workflow
      @todo: Establish dependencies
      """
      dep = []
      jobId = randJob.getId()
      jrFn = JOB_REQUEST_FILENAME.format(processType=randJob.processType, 
                                         jobId=jobId)
      jrCmd = BUILD_JOB_REQUEST_CMD.format(objectFamily=JobFamily.RAD, 
                                           jobId=jobId, jrFn=jrFn)
      # Add job to create job request
      self._addJobCommand([jrFn], jrCmd, dependencies=dep,
                  comment='Randomize {0} job request'.format(jobId))
      
      # Add job to create model
      self._addJobCommand([randJob.outputObj.createLocalDLocation()], 
                          LM_JOB_RUNNER_CMD.format(jrFn=jrFn),
                          dependencies=[jrFn], 
                          comment="Randomize {0}".format(jobId))

   # ...........................
   def write(self, filename):
      didWrite = False
      if self.jobs:
         success = self._readyFilename(filename, overwrite=True)
         if success:
            try:
               with open(filename, 'w') as outF:
                  for header in self.headers:
                     # Assume that they need newlines
                     outF.write("{}\n".format(header)) 
                  for job in self.jobs:
                     # These have built-in newlines
                     outF.write(job) 
               didWrite = True
            except Exception, e:
               print ('Failed to write file {} ({})'.format(filename, str(e)))
      return didWrite
   

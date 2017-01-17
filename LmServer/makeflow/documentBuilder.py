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

@todo: Many functions can take a subworkflow or a pre-existing object, handle that
@note: Let job server handle object updates for now
@todo: Job request dir and python command constants need to move out of compute
@todo:   Or, use $PYTHON if that works and job requests will be generated in workspace
@todo: Should we tell dependencies to compute if needed?
@todo: remove status files
"""
import os

from LmCommon.common.lmconstants import JobStatus, ProcessType

from LmCompute.common.localconstants import JOB_REQUEST_PATH, PYTHON_CMD

from LmServer.base.lmobj import LMObject
from LmServer.common.lmconstants import JobFamily
# TODO: Okay that this in server?
from LmServer.common.localconstants import APP_PATH
from LmServer.makeflow.makeJobCommand import (makeBisonOccurrenceSetCommand,
                                             makeGbifOccurrenceSetCommand,
                                             makeIdigbioOccurrenceSetCommand,
                                             makeMaxentSdmModelCommand,
                                             makeMaxentSdmProjectionCommand,
                                             makeOmSdmModelCommand,
                                             makeOmSdmProjectionCommand)

from LmServer.sdm.sdmJob import SDMOccurrenceJob, SDMModelJob, SDMProjectionJob

JOB_REQUEST_FILENAME = "$JOB_REQUESTS/{processType}-{jobId}Req.xml"
BUILD_JOB_REQUEST_CMD = "LOCAL $PYTHON $MAKE_JOB_REQUEST {objectFamily} {jobId} -f {jrFn}"
LM_JOB_RUNNER_CMD = "$PYTHON $RUNNER {jrFn}"
UPDATE_DB_CMD = "$PYTHON $UPDATE_DB_SCRIPT {processType} {objId} {status}"

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
      self.headers.append("JOB_REQUESTS={jrDir}".format(jrDir=JOB_REQUEST_PATH))
      self.headers.append("MAKE_JOB_REQUEST={jrScript}".format(
                                 jrScript=os.path.join(APP_PATH, 
                                      "LmServer/makeflow/makeJobRequest.py")))
      self.headers.append("SINGLE_SPECIES_SCRIPTS_PATH={sssPath}".format(
                             sssPath=SINGLE_SPECIES_SCRIPTS_PATH))
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
   def addBisonOccurrenceSet(self, occ):
      """
      @summary: Adds tasks related to filling a BISON occurrence set
      """
      # Fill the occurrence set command
      occCmd, occStatusFn = makeBisonOccurrenceSetCommand(occ)
      
      # Add entry to fill occurrence set
      self._addJobCommand([occ.createLocalDLocation(), occStatusFn],
                          occCmd, dependencies=[],
                          comment="Fill BISON occurrence set {0}".format(occ.getId()))
   
      # Move outputs
      # If we decide to create outputs in a temporary space, we'll need to move
      #   them to their final location
      
      # Update DB
      updateDbCmd = UPDATE_DB_CMD.format(
                       processType=ProcessType.BISON_TAXA_OCCURRENCE,
                       objId=occ.getId(), statusFn=occStatusFn)
      
   # ...........................
   def addGbifOccurrenceSet(self, occ):
      """
      @summary: Adds tasks related to filling a GBIF occurrence set
      """
      # Need the raw data location to fill the occurrence set
      dep = [occ.rawDLocation()]
         
      # Fill the occurrence set command
      occCmd, occStatusFn = makeGbifOccurrenceSetCommand(occ)
      
      # Add entry to fill occurrence set
      self._addJobCommand([occ.createLocalDLocation(), occStatusFn],
                          occCmd, dependencies=dep,
                          comment="Fill GBIF occurrence set {0}".format(occ.getId()))
   
      # Move outputs
      # If we decide to create outputs in a temporary space, we'll need to move
      #   them to their final location
      
      # Update DB
      updateDbCmd = UPDATE_DB_CMD.format(processType=ProcessType.GBIF_TAXA_OCCURRENCE,
                                         objId=occ.getId(), statusFn=occStatusFn)
      
   # ...........................
   def addIdigbioOccurrenceSet(self, occ):
      """
      @summary: Adds tasks related to filling an iDigBio occurrence set
      """
      # Fill the occurrence set command
      occCmd, occStatusFn = makeIdigbioOccurrenceSetCommand(occ)
      
      # Add entry to fill occurrence set
      self._addJobCommand([occ.createLocalDLocation(), occStatusFn],
                          occCmd, dependencies=[],
                          comment="Fill iDigBio occurrence set {0}".format(occ.getId()))
   
      # Move outputs
      # If we decide to create outputs in a temporary space, we'll need to move
      #   them to their final location
      
      # Update DB
      updateDbCmd = UPDATE_DB_CMD.format(
                       processType=ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                       objId=occ.getId(), statusFn=occStatusFn)
      
   # ...........................
   def addUserOccurrenceSet(self, occ):
      """
      @summary: This is just like GBIF occurrence sets
      """
      self.addGbifOccurrenceSet(occ)
      
   # ...........................
   def addSdmModel(self, mdl):
      """
      @summary: Adds all of the processes necessary for computing an SDM model
      """
      # Determine which type of model to add
      
      # TODO: Use a constant!
      if mdl.algorithmCode == 'ATT_MAXENT':
         self.addMaxentModel(mdl)
      else:
         self.addOmModel(mdl)
         
   # ...........................
   def addMaxentModel(self, mdl):
      """
      @summary: Adds all of the processes necessary for computing a Maxent model
      """
      # Determine if there are dependencies
      if mdl.occurrenceSet.status == JobStatus.COMPLETE:
         dep = []
      else:
         dep = [mdl.occurrenceSet.createLocalDLocation()]
         
      # Generate request filename
      jrFn = JOB_REQUEST_FILENAME.format(processType=ProcessType.ATT_MODEL,
                                         jobId=mdl.getId())
      # Generate request command
      jobReqCmd = BUILD_JOB_REQUEST_CMD.format(
               processType=ProcessType.ATT_MODEL, jobId=mdl.getId(), jrFn=jrFn)

      # Create model command
      mdlCmd, mdlStatusFn = makeMaxentSdmModelCommand(mdl, jrFn)
      
      # Add entry to create job request
      self._addJobCommand([jrFn], jobReqCmd, dependencies=dep, 
                        comment="Build model {0} request".format(mdl.getId()))
      # Add entry to build model
      self._addJobCommand([mdl.createLocalDLocation(), mdlStatusFn],
                          mdlCmd, dependencies=[jrFn],
                          comment="Build model {0}".format(mdl.getId()))
   
      # Move outputs
      # If we decide to create outputs in a temporary space, we'll need to move
      #   them to their final location
      
      # Update DB
      updateDbCmd = UPDATE_DB_CMD.format(processType=ProcessType.ATT_MODEL,
                                         objId=mdl.getId(), statusFn=mdlStatusFn)
      
   # ...........................
   def addOmModel(self, mdl):
      """
      @summary: Adds all of the processes necessary for computing an 
                   openModeller model
      """
      # Determine if there are dependencies
      if mdl.occurrenceSet.status == JobStatus.COMPLETE:
         dep = []
      else:
         dep = [mdl.occurrenceSet.createLocalDLocation()]
         
      # Generate request filename
      jrFn = JOB_REQUEST_FILENAME.format(processType=ProcessType.OM_MODEL,
                                         jobId=mdl.getId())
      # Generate request command
      jobReqCmd = BUILD_JOB_REQUEST_CMD.format(
               processType=ProcessType.OM_MODEL, jobId=mdl.getId(), jrFn=jrFn)

      # Create model command
      mdlCmd, mdlStatusFn = makeMaxentSdmModelCommand(mdl, jrFn)
      
      # Add entry to create job request
      self._addJobCommand([jrFn], jobReqCmd, dependencies=dep, 
                        comment="Build model {0} request".format(mdl.getId()))
      # Add entry to build model
      self._addJobCommand([mdl.createLocalDLocation(), mdlStatusFn],
                          mdlCmd, dependencies=[jrFn],
                          comment="Build model {0}".format(mdl.getId()))
   
      # Move outputs
      # If we decide to create outputs in a temporary space, we'll need to move
      #   them to their final location
      
      # Update DB
      updateDbCmd = UPDATE_DB_CMD.format(processType=ProcessType.OM_MODEL,
                                         objId=mdl.getId(), statusFn=mdlStatusFn)
      
   # ...........................
   def addSdmProjection(self, prj):
      """
      @summary: Adds all of the processes necessary for computing an SDM 
                   projection
      """
      # Determine which type of projection to add
      
      # TODO: Use a constant!
      if prj.getModel().algorithmCode == 'ATT_MAXENT':
         self.addMaxentProjection(prj)
      else:
         self.addOmProjection(prj)
         
   # ...........................
   def addMaxentProjection(self, prj):
      """
      @summary: Adds all of the processes necessary for computing a Maxent 
                   projection
      """
      # Determine if there are dependencies
      mdl = prj.getModel()
      if mdl.status == JobStatus.COMPLETE:
         dep = []
      else:
         dep = [mdl.createLocalDLocation()]
         
      # Generate request filename
      jrFn = JOB_REQUEST_FILENAME.format(processType=ProcessType.ATT_PROJECT,
                                         jobId=prj.getId())
      # Generate request command
      jobReqCmd = BUILD_JOB_REQUEST_CMD.format(
             processType=ProcessType.ATT_PROJECT, jobId=prj.getId(), jrFn=jrFn)

      # Create projection command
      prjCmd, prjStatusFn = makeMaxentSdmProjectionCommand(prj, jrFn)
      
      # Add entry to create job request
      self._addJobCommand([jrFn], jobReqCmd, dependencies=dep, 
                        comment="Build projection {0} request".format(
                           prj.getId()))
      # Add entry to build projection
      self._addJobCommand([prj.createLocalDLocation(), prjStatusFn],
                          prjCmd, dependencies=[jrFn],
                          comment="Build projection {0}".format(prj.getId()))
   
      # Move outputs
      # If we decide to create outputs in a temporary space, we'll need to move
      #   them to their final location
      
      # Update DB
      updateDbCmd = UPDATE_DB_CMD.format(processType=ProcessType.ATT_PROJECT,
                                       objId=prj.getId(), statusFn=prjStatusFn)
      
   # ...........................
   def addOmProjection(self, prj):
      """
      @summary: Adds all of the processes necessary for computing an  
                   openModeller projection
      """
      # Determine if there are dependencies
      mdl = prj.getModel()
      if mdl.status == JobStatus.COMPLETE:
         dep = []
      else:
         dep = [mdl.createLocalDLocation()]
         
      # Generate request filename
      jrFn = JOB_REQUEST_FILENAME.format(processType=ProcessType.OM_PROJECT,
                                         jobId=prj.getId())
      # Generate request command
      jobReqCmd = BUILD_JOB_REQUEST_CMD.format(
             processType=ProcessType.OM_PROJECT, jobId=prj.getId(), jrFn=jrFn)

      # Create projection command
      prjCmd, prjStatusFn = makeOmSdmProjectionCommand(prj, jrFn)
      
      # Add entry to create job request
      self._addJobCommand([jrFn], jobReqCmd, dependencies=dep, 
                        comment="Build projection {0} request".format(
                           prj.getId()))
      # Add entry to build projection
      self._addJobCommand([prj.createLocalDLocation(), prjStatusFn],
                          prjCmd, dependencies=[jrFn],
                          comment="Build projection {0}".format(prj.getId()))
   
      # Move outputs
      # If we decide to create outputs in a temporary space, we'll need to move
      #   them to their final location
      
      # Update DB
      updateDbCmd = UPDATE_DB_CMD.format(processType=ProcessType.OM_PROJECT,
                                       objId=prj.getId(), statusFn=prjStatusFn)
      
   # Occurrence set (3+ flavors)
   # Create intersect parameters
   # Intersect PAM
   # Intersect layer
   # Calculate
   # MCPA
   # Encoding (tree and biogeo)
   # Compress?
   # Randomizations
   
         
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
   def addPAMIntersect(self, shapegrid, matrixColumns):
      """
      @summary: Adds tasks to intersect a set of matrix columns against a 
                   shapegrid for a PAM
      @param shapegrid: Shapegrid object (May want to change this to a file 
                           location later, or look for file name.  Just so it 
                           is a bit more flexible)
      @param matrixColumns: A list of one or more matrix columns to intersect.
      @note: Matrix column needs a input dLocation, the intersect parameters, 
                and a final data location 
      @todo: Generalize
      """
      for mtxCol in matrixColumns:
         # Add command to intersect the matrix column
         # Add command to move the PAV to the final location
         pass
   

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
   

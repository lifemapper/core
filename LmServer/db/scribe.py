# coding=utf-8
""" 
    Module to write to the Lifemapper Catalog (databases PBJ, MAL)

    @status: alpha
    @author: Aimee Stewart
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
import mx.DateTime
import os
from types import IntType

from LmCommon.common.lmconstants import (JobStage, JobStatus, ProcessType, 
                                        RandomizeMethods, Instances)

from LmServer.base.layer import Vector
from LmServer.base.lmobj import LMError, LMMissingDataError
from LmServer.common.lmconstants import (DbUser, JobFamily, Priority, 
                                        ReferenceType)
from LmServer.common.localconstants import (ARCHIVE_USER, POINT_COUNT_MIN, 
                                            DATASOURCE)
from LmServer.common.notifyJob import NotifyJob
from LmServer.db.peruser import Peruser
from LmServer.rad.pamvim import PamSum
from LmServer.rad.radJob import (RADIntersectJob, RADCompressJob, RADGradyJob,
                  RADSwapJob, RADSplotchJob, RADCalculateJob, RADBuildGridJob)
from LmServer.sdm.envlayer import EnvironmentalLayer, EnvironmentalType
from LmServer.sdm.sdmJob import (SDMOccurrenceJob, SDMModelJob, 
                                SDMProjectionJob)
from LmServer.sdm.sdmmodel import SDMModel
from LmServer.sdm.sdmprojection import SDMProjection
from LmServer.sdm.sdmexperiment import SDMExperiment

# .............................................................................
class Scribe(Peruser):
# .............................................................................
   """
   Class to write to the LM database(s)
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger, dbUser=DbUser.Pipeline, overrideDB=None):
      """
      @summary Scribe constructor
      @param logger: Logger for informational, debugging, and error messages
      @param overrideDB: optional parameter for overriding default database
                         to connect to.  Only used when debugging data on 
                         production or beta from development environment.  
                         Expects the database hostname, options are HL_HOST, 
       """
      Peruser.__init__(self, logger, dbUser=dbUser, overrideDB=overrideDB)

# .............................................................................
# Public functions
# .............................................................................
   def insertProjection(self, proj):
      """
      @summary: Insert a Projection for an existing model into the database
      @param proj: A Projection object 
      """
      try:
         self._mal.insertProjection(proj)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         self.log.error(str(e))
         raise e

# ...............................................
   def insertAlgorithm(self, alg):
      """
      @summary Inserts an Algorithm into the database
      @param alg: The algorithm to add
      """
      algid = self._mal.insertAlgorithm(alg)
      return algid

# ...............................................
# Job/Experiment (SDM)
# ...............................................
# ...............................................
   def initSDMModelProjectionJobs(self, occset, mdlScen, prjScenList, alg, usr, 
                                  priority, modtime=mx.DateTime.gmt().mjd, 
                                  mdlMask=None, prjMask=None, email=None, 
                                  name=None, description=None):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      jobs = []
      mdlJob, notJob = self.initSDMModelJobs(occset, mdlScen, alg, usr, 
                                             priority, modtime=modtime, 
                                             mdlMask=mdlMask, 
                                             email=email, name=name, 
                                             description=description)
      if mdlJob is not None:
         jobs.append(mdlJob)
         if notJob:
            jobs.append(notJob)
            
         for pscen in prjScenList:
            prjJob = self.initSDMProjectionJob(mdlJob.dataObj, pscen, priority, 
                                               modtime=modtime, prjMask=prjMask)
            jobs.append(prjJob)
      return jobs
   
# ...............................................
   def initSDMModelJobs(self, occset, mdlScen, alg, usr, priority, 
                        modtime=mx.DateTime.gmt().mjd,
                        mdlMask=None, email=None, name=None, description=None):
      """
      @summary: Initialize model for inputs/algorithm.
      @note: If model is ready and occurrence data is missing; abort
      """
      mdlJob = notJob = None
      if modtime is None:
         modtime = mx.DateTime.gmt().mjd
         
      stat = JobStatus.GENERAL
      # TODO: if occset.status is None, ERROR, this should be set now
      if occset.status is None or occset.status == JobStatus.COMPLETE:
         stat = JobStatus.INITIALIZE
         
      mdl = SDMModel(priority, occset, mdlScen, alg, mask=mdlMask, email=email,
                     status=stat, statusModTime=modtime,
                     createTime=modtime, userId=usr, name=name, 
                     description=description)
      self._mal.insertModel(mdl)
      if alg.code == 'ATT_MAXENT':
         processType = ProcessType.ATT_MODEL
      else:
         processType = ProcessType.OM_MODEL
         
      try:
         mdlJob = SDMModelJob(mdl, processType=processType, createTime=modtime, 
                           retryCount=0)
      except LMMissingDataError, e:
         self.log.error('Missing/invalid data for occ {}; will rollback'
                        .format(occset.getId()))
         self.rollbackOccurrenceDeleteDependents(occset)
      except Exception, e:
         raise e
      else:
         mdlJob = self._mal.insertJob(mdlJob)
         # Init Notify
         if email is not None:
            notJob = NotifyJob(obj=mdl, objType=ReferenceType.SDMModel , 
                             parentUrl=mdl.metadataUrl, jobFamily=JobFamily.SDM, 
                             email=email, status=JobStatus.GENERAL, 
                             statusModTime=modtime, priority=priority, 
                             createTime=modtime)
            notJob = self._mal.insertJob(notJob)
         
      return mdlJob, notJob

# ...............................................
   def reinitSDMExperiment(self, occ):
      jobs = []
      # make sure raw input data is valid
      processtype = None
      if occ.getRawDLocation() is not None:
         if DATASOURCE == Instances.BISON:
            processtype = ProcessType.BISON_TAXA_OCCURRENCE
         elif DATASOURCE == Instances.GBIF and os.path.exists(occ.getRawDLocation()):
            processtype = ProcessType.GBIF_TAXA_OCCURRENCE
         elif DATASOURCE == Instances.IDIGBIO and os.path.exists(occ.getRawDLocation()):
            processtype = ProcessType.IDIGBIO_TAXA_OCCURRENCE
         else:
            raise LMError(currargs='Unknown DATASOURCE {}'.format(DATASOURCE))
      # inits jobs for occset, model and projections
      if processtype is None:
         if occ.status < JobStatus.COMPLETE:
            occ.updateStatus(JobStatus.LM_RAW_POINT_DATA_ERROR)
            self.updateOccset(occ)
      
      if occ.status <= JobStatus.COMPLETE:
         try:
            jobs = self.initSDMChain(ARCHIVE_USER, occ, self.algs, 
                                      self.modelScenario, 
                                      self.projScenarios, 
                                      occJobProcessType=processtype,
                                      priority=Priority.OBSOLETE, 
                                      intersectGrid=None,
                                      minPointCount=POINT_COUNT_MIN)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
                     
      self.log.debug('Created %d chained jobs for ready occset %d' 
                     % (len(jobs), occ.getId()))
      return len(jobs)

# ...............................................
   def reinitSDMModel(self, mdl, priority, modtime=mx.DateTime.gmt().mjd, 
                      doRollback=True):
      mdlJob = notJob = None
      if doRollback:
         # back to JobStatus.Initialize if occset is complete, GENERAL if not
         mdl.rollback(modtime, newPriority=priority)
         success = self._mal.rollbackModel(mdl, JobStatus.GENERAL_ERROR)
      
      if mdl.algorithmCode == 'ATT_MAXENT':
         processType = ProcessType.ATT_MODEL
      else:
         processType = ProcessType.OM_MODEL
      mdlJob = SDMModelJob(mdl, processType=processType, createTime=modtime, 
                           retryCount=0)
      # If job exists, this just rolls it back if status > GENERAL
      mdlJob = self._mal.insertJob(mdlJob)
      
      # Init Notify
      if mdl.email is not None:
         notJob = NotifyJob(obj=mdl, objType=ReferenceType.SDMModel , 
                         parentUrl=mdl.metadataUrl, jobFamily=JobFamily.SDM, 
                         email=mdl.email, status=JobStatus.GENERAL, 
                         statusModTime=modtime, priority=priority, 
                         createTime=modtime)
         notJob = self._mal.insertJob(notJob)
         
      return mdlJob, notJob
   
# ...............................................
   def initSDMProjectionJob(self, model, prjScen, priority, 
                            modtime=mx.DateTime.gmt().mjd, prjMask=None):
      prj = SDMProjection(model, prjScen, mask=prjMask, priority=priority, 
                          status=JobStatus.GENERAL, statusModTime=modtime,
                          createTime=modtime)
      self._mal.insertProjection(prj)
      if model.algorithmCode == 'ATT_MAXENT':
         processType = ProcessType.ATT_PROJECT
      else:
         processType = ProcessType.OM_PROJECT
      prjJob = SDMProjectionJob(prj, processType=processType, 
                                createTime=modtime, retryCount=0)
      prjJob = self._mal.insertJob(prjJob)
      
      return prjJob

# ...............................................
   def reinitSDMProjection(self, prj, priority, modtime=mx.DateTime.gmt().mjd, 
                           doRollback=True):
      if doRollback:
         prj.rollback(modtime)
         prj.priority = priority
         self._mal.updateProjection(prj, None, None)
      
      if prj.algorithmCode == 'ATT_MAXENT':
         processType = ProcessType.ATT_PROJECT
      else:
         processType = ProcessType.OM_PROJECT
      job = SDMProjectionJob(prj, processType=processType, createTime=modtime, 
                             retryCount=0)
      job = self._mal.insertJob(job)
      
      return job
      
# ...............................................
   def insertJob(self, job):
      if job.jobFamily == JobFamily.SDM:
         updatedjob = self._mal.insertJob(job)
#       elif job.jobFamily == JobFamily.RAD:
#          updatedjob = self._rad.insertJob(job)
      return updatedjob
   
# ...............................................
   def findExistingJobs(self, occId=None, mdlId=None, prjId=None, status=None):
      existingJob = None
      jobs = self._mal.findExistingJobs(occId=occId, mdlId=mdlId, prjId=prjId, 
                                        status=status)
      return jobs

# ...............................................
   def deleteExperiment(self, model):
      """
      @summary Delete a model and any projections associated with it.
      @param model: model to remove
      @todo: when we update the database to include occurrences within the MAL,
             we will want to delete orphaned occurrences here.
      """
      success = True
      model.clearModelFiles()
      model.clearLocalMapfile()
      projs = self._mal.getProjectionsForModel(model.getId(), status=None)
      for prj in projs:
         prj.clearProjectionFiles()
         deleted = self._mal.deleteProjection(prj.getId())
         if not deleted:
            self.log.error('Unable to delete projection %d' % prj.getId())
            success = False
      deleted = self._mal.deleteModel(model.getId())
      if not deleted:
         self.log.error('Unable to delete model %d' % model.getId())
         success = False
      return success

# ...............................................
   def deleteProjection(self, prj):
      """
      @summary Delete a projection and all files associated with it.
      @param prj: projectionId or projection object to remove
      """
      success = True
      if isinstance(prj, IntType):
         prj = self._mal.getProjectionById(prj)
      if prj is not None:
         prj.clearProjectionFiles()
         deleted = self._mal.deleteProjection(prj.getId())
         if not deleted:
            self.log.error('Unable to delete projection %d' % prj.getId())
            success = False
      return success


# # ...............................................
#    def deleteOccDependentObjects(self, occ):
#       """
#       @summary Delete a model and any projections associated with it.
#       @param model: model to remove
#       @todo: when we update the database to include occurrences within the MAL,
#              we will want to delete orphaned occurrences here.
#       """
#       success = True
#       self._mal.deleteOccAndDependentObjects(occ.getId())
#       model.clearModelFiles()
#       model.clearLocalMapfile()
#       projs = self._mal.getProjectionsForModel(model.getId(), status=None)
#       for prj in projs:
#          prj.clearProjectionFiles()
#          deleted = self._mal.deleteProjection(prj.getId())
#          if not deleted:
#             self.log.error('Unable to delete projection %d' % prj.getId())
#             success = False
#       deleted = self._mal.deleteModel(model.getId())
#       if not deleted:
#          self.log.error('Unable to delete model %d' % model.getId())
#          success = False
#       return success

# ...............................................
   def _rollbackExp(self, oldexp, modtime=mx.DateTime.gmt().mjd, 
                    newpriority=None, errstatus=None):
      """
      @summary: Reinitialize (or setting to ERROR) an existing experiment by 
                  * clearing all related files
                  * resetting the model and projection statuses
                  * updating the priorities if the new one is higher
                  * update existing projections 
               The model and projections of this experiment are now ready to be
               processed by the Pipeline.
      @param exp: Experiment to be reset.
      @param oldmodel: Existing model with the same input parameters.
      @param newpriority: New Priority for experiment calculation
      @param errstatus: New Error Status for experiment if something has failed
      @postcondition: The model and projections of this experiment are 
                  inserted or updated with an INITIALIZED status in the 
                  database.  Old model, projection, and mapfiles are deleted.
      """
      success = True
      if errstatus is not None:
         oldexp.model.rollback(modtime, status=errstatus, priority=newpriority)
         success = self._mal.rollbackModel(oldexp.model, JobStatus.GENERAL_ERROR)
         if not success:
            self.log.error('Failed to rollback experiment {}'.format(oldexp.getId()))
         else:
            self.log.info('Rolled back experiment {}'.format(oldexp.getId()))
      else:
         mdlJob, notJob = self.reinitSDMModel(oldexp.model, newpriority, 
                                              modtime=modtime)
         self.log.info('Re-initialized experiment {}'.format(oldexp.getId()))
         
      # Update existing projections
      for oldprj in oldexp.projections:
         if errstatus is not None:
            oldprj.rollback(modtime, priority=newpriority, status=errstatus)
            psuccess = self._mal.rollbackProjection(oldprj)
            if not psuccess:
               self.log.error('Failed to update projection {}'.format(oldprj.getId()))
         else:
            prjJob = self.reinitSDMProjection(oldprj, newpriority)
      return success
      
# ...............................................
   def _extendExp(self, exp, existingExp):
      """
      @summary: Reinitialize an existing experiment by 
                  * clearing all related files
                  * resetting the model and projection statuses
                  * updating the priorities if the new one is higher
                  * add any new projections (new scenario/mask combos for 
                    this model) from the provided experiment
                  * update existing projections 
               The model and projections of this experiment are now ready to be
               processed by the Pipeline.
      @param exp: Experiment to be re-initialized.
      @param existingExp: Existing experiment (from database) with the same 
                         input parameters.
      @postcondition: The model and projections of this experiment are 
                  inserted or updated with an INITIALIZED status in the 
                  database.  Old model and projections are cleared, and mapfiles are deleted.
      """
      success = True
      oldpairs = {}
      newpairs = {}

      # Find new projection scenario/mask combinations
      for newprj in exp.projections:
         newpairs[newprj.getScenario().getId()] = newprj.getMask().getId()

      # Find existing projection scenario/mask combinations
      for oldprj in existingExp.projections:
         oldscen = oldprj.getScenario().getId()
         oldmask = oldprj.getMask().getId()
         oldpairs[oldscen] = oldmask
         # If this scenario-mask pair projection isn't in Experiment, add it
         if not (newpairs.has_key(oldscen) and newpairs[oldscen] == oldmask):
            exp.projections.append(oldprj)
            
      # Does this mutate exp or not? 
      exp.model = existingExp.model
               
      # Experiment exp now has *all* projections   
      for newprj in exp.projections:
         newscen = newprj.getScenario().getId()
         newmask = newprj.getMask().getId()
         # If this scenario-mask pair  projection isn't in database, insert it
         if not (oldpairs.has_key(newscen) and oldpairs[newscen] == newmask):
            self._mal.insertProjection(newprj)
      
      return success

# ...............................................
# Layer
# ...............................................
   def insertLayer(self, lyr, scenarioid=None):
      """
      @summary: Insert a Raster Layer into the appropriate table 
      @param lyr: Layer to be inserted
      @param scenarioid: Id of scenario to join this EnvironmentalLayer 
      @note: In SDM, if scenarioid is given, join the layer to that Scenario.
      """
      if isinstance(lyr, EnvironmentalLayer):
         if lyr.isValidDataset():
            existingLayers = self.getEnvLayersByNameUserEpsg(lyr.name, 
                                                             lyr.getUserId(), 
                                                             lyr.epsgcode)
            if len(existingLayers) > 0:
               exLyr = existingLayers[0]
               if exLyr.typeCode == lyr.typeCode:
                  lyr = exLyr
               else:
                  raise LMError(currargs='Layer {} already exists with typeCode \'{}\''
                                .format(exLyr.getId(), exLyr.typeCode))
            else:
               lyr = self._mal.insertEnvLayer(lyr, scenarioId=scenarioid)
         else:
            raise LMError(currargs='Invalid environmental layer: {}'
                                    .format(lyr.getDLocation()), 
                          lineno=self.getLineno())
      else:
         raise LMError(currargs='Call Scribe.insertRADLayer instead')
      return lyr

# ...............................................
   def insertRADLayer(self, lyr, lyrContent=None, lyrTempFile=None):
      """
      @summary: Insert a Raster Layer into the appropriate table(s)
      @param lyr: Layer to be inserted
      @param lyrContent: If layer is vector, features and featureAttributes may
         `               be on the layer object, and can be written from memory
      """
      # Clearing and resetting dlocation earlier so that it is correct in the 
      #    database before inserting or updating
      # This was done because the lm_updateLayer stored procedure is not working
      #TODO: Evaluate this
      lyr.clearDLocation()
      lyr.setDLocation()
      updatedLyr, isNewLyr = self._rad.insertOrUpdateLayer(lyr)
      if isNewLyr:
         if lyrContent:
            updatedLyr.writeLayer(srcData=lyrContent)
         elif lyrTempFile:
            updatedLyr.writeLayer(srcFile=lyrTempFile)
            updatedLyr.deleteData(dlocation=lyrTempFile, isTemp=True)
         elif updatedLyr.features is not None:
            updatedLyr.writeLayer(overwrite=True)
         else:
            self.log.error('Missing input data to write RAD layer {}'
                           .format(updatedLyr.getId()))
      return updatedLyr, isNewLyr

# ...............................................
   def rollbackRADExperiment(self, radexp, modtime=mx.DateTime.gmt().mjd, bucketId=None):
      if bucketId is not None:
         # Make sure we have the right bucket
         fullexp = self.getRADExperimentWithOneBucket(radexp.getUserId(), bucketId)
         fullexp.bucketList[0].rollback(modtime)
         # rollback bucket stage/status, delete PamSums
         pamsumDeleteCount = self._rad.rollbackBucket(fullexp.bucketList[0], 
                                                      JobStatus.GENERAL, 
                                                      JobStage.GENERAL)
         count = pamsumDeleteCount
      else:
         # Make sure we have all buckets
         fullexp = self.getRADExperiment(radexp.getUserId(), expid=radexp.getId())
         # for each bucket, clears PAMs and presenceIndices
         fullexp.rollback(modtime)
         # for each bucket, rollback stage/status, delete PamSums
         bucketRollbackCount = self._rad.rollbackExperiment(fullexp, 
                                                            JobStatus.GENERAL, 
                                                            JobStage.GENERAL)
         count = bucketRollbackCount
      return count
   
# ...............................................
# @todo: This should be parallel to addAncillaryLayerToExperiment
   def insertPresenceAbsenceLayer(self, palyr, radexp, 
                                  lyrContent=None, rollback=False):
      updatedlyr = None
      if hasattr(palyr, 'attrPresence'):
         
         # If external data, reset layer location
         if lyrContent is not None:
            palyr.clearDLocation()
            palyr.setDLocation()
            
         # Modifies palyr, adding layerid, presenceabsenceid
         updatedlyr = self._rad.insertPresenceAbsenceLayer(palyr, radexp.getId())
         
         # If external data, write it to filesystem
         if lyrContent:
            updatedlyr.writeLayer(srcData=lyrContent, overwrite=True)
            
         if updatedlyr and not radexp.getOrgLayer(updatedlyr.metadataUrl, 
                                                  updatedlyr.getParametersId()):
            # This 'rolls-back' the bucket, deletes all random pamsums, clears 
            # the original pamsum matrices and their files  
            radexp.addPresenceAbsenceLayer(updatedlyr)
            
            if rollback:
               bucketRollbackCount = self.rollbackRADExperiment(radexp)
      else:
         raise LMError(currargs='Wrong object type', lineno=self.getLineno())
      return updatedlyr
   

# ...............................................
   def updateLayer(self, lyr):
      success = self._rad.updateLayer(lyr)
      return success
      
# ...............................................
   def insertScenarioLayer(self, lyr, scenarioid):
      updatedLyr = None
      if isinstance(lyr, EnvironmentalLayer):
         if lyr.isValidDataset():
            updatedLyr = self._mal.insertEnvLayer(lyr, scenarioId=scenarioid)
         else:
            raise LMError(currargs='Invalid environmental layer: {}'
                                    .format(lyr.getDLocation()), 
                          lineno=self.getLineno())
      return updatedLyr

# ...............................................
   def getOrInsertLayerTypeCode(self, envType):
      etypeid = None
      if isinstance(envType, EnvironmentalType):
         existingET = self._mal.getEnvironmentalType(envType.typeCode, 
                                          envType.getParametersUserId())
         if existingET is not None:
            envType = existingET
            etypeid = envType.getParametersId()
         else:
            etypeid = self._mal.insertEnvironmentalType(envType)
      else:
         raise LMError(currargs='Invalid object for EnvironmentalType insertion')
      return etypeid
      
# ...............................................
   def insertVectorLayer(self, lyr):
      """
      @summary: Insert an Vector into the Layer table in the MAL 
                database.  Currently only used for ChangeThinking display 
                layers.  Another object might be more appropriate, but this has 
                all the attributes we need.
      @todo: Handle arbitrary map service display/query layers 
      """
      if isinstance(lyr, Vector):
         if lyr.isValidDataset() or lyr.isFilled():
            lyrid = self._mal.insertVectorLayer(lyr)
         else:
            raise LMError(currargs='Invalid vector layer: {}'
                                    .format(lyr.getDLocation()), 
                          lineno=self.getLineno())
      else:
         raise LMError(currargs='Error %s insertion is not supported'
                                 .format(type(lyr)), 
                       lineno=self.getLineno())
      return lyrid

# ...............................................
# Job/Experiment (SDM)
# ...............................................
# ...............................................
   def moveDependentJobs(self, job):
      """
      @summary Updates a job in the database with current object attributes
      @param job: The job to update
      @note: This updates object properties besides status, stage and modtime.
      """
      currtime = mx.DateTime.gmt().mjd
      completeStat = JobStatus.COMPLETE 
      errorStat = JobStatus.GENERAL_ERROR
      notReadyStat = JobStatus.GENERAL
      readyStat = JobStatus.INITIALIZE
      # Update Job and object attributes, then move dependent jobs
      if job.jobFamily == JobFamily.SDM:
         dsuccess = self._mal.moveDependentJobs(job, completeStat, errorStat, 
                                                notReadyStat, readyStat, currtime)
      elif job.jobFamily == JobFamily.RAD:
         dsuccess = self._rad.moveDependentJobs(job, completeStat, errorStat,
                                                notReadyStat, readyStat, currtime)
      else:
         raise LMError('Invalid JobFamily {}'.format(job.jobFamily))
      
      return dsuccess


# ...............................................
   def _writeMapIfCompleteExperiment(self, job):
      complete = False
      if job.processType in (ProcessType.ATT_PROJECT, ProcessType.OM_PROJECT):
         proj = job.outputObj
         unfinishedCount = self.countProjections(userId=job.jobData.userId, 
                                                 inProcess=True, 
                                                 mdlId=proj.getModel().getId())
         if unfinishedCount == 0:
            proj.clearLocalMapfile()
            proj.setLocalMapFilename()
            occId = proj.getOccurrenceSet().getId()
            expLayerset = self._mal.getOccMapservice(occId)
            expLayerset.writeMap()
      return complete
   
# ...............................................
   def updateJob(self, job, errorStat=JobStatus.GENERAL_ERROR):
      """
      @summary Updates a job in the database with current object attributes
      @param job: The job to update
      @note: This updates object properties besides status, stage and modtime.
      """
      incrementRetry = False
      if job.status == JobStatus.PULL_COMPLETE:
         incrementRetry = True

      # Reset error NotifyJob so it retries
      elif job.processType == ProcessType.SMTP and job.status >= errorStat:
         job.status = JobStatus.INITIALIZE

      # Update Job and object attributes, then move dependent jobs
      if job.jobFamily == JobFamily.SDM:
         osuccess, jsuccess = self._mal.updateSDMJob(job, errorStat, incrementRetry)
         try: # TODO: Evaluate what we want to do for other installations that don't have maps
            written = self._writeMapIfCompleteExperiment(job)
         except:
            pass
         
      elif job.jobFamily == JobFamily.RAD:
         osuccess, jsuccess = self._rad.updateRADJob(job, errorStat, incrementRetry)
      else:
         raise LMError('Invalid JobFamily {}'.format(job.jobFamily))
      
      if not osuccess:
         self.log.error('Failed to update object {}, {}'
                        .format(type(job.outputObj), job.outputObj.getId()))
      if not jsuccess:
         self.log.error('Failed to update family {}, type {}, id {}, job {}'
                        .format(job.jobFamily, job.outputObjType, 
                                job.outputObj.getId(), job.getId() ))
      return osuccess and jsuccess
      
# ...............................................
   def updateJobAndObjectStatus(self, jobFamily, jobid, computeIP, status,
                                progress):
      """
      @summary Updates the status on a job and its corresponding object
      @param jobFamily: The module/database to update
      @param jobid: The job record to update
      @param computeName: The name of the ComputeResource computing the job
      @param status: The JobStatus
      @param progress: Percent complete
      @note: This updates compute info: compute resource, progress, retryCount,  
                                        status, modtime, lastheartbeat.
      """
      incrementRetry = False
      if status == JobStatus.PULL_COMPLETE:
         incrementRetry = True
      if jobFamily == JobFamily.SDM:
         success = self._mal.updateJobAndObjectStatus(jobid, computeIP, 
                                                      status, progress,
                                                      incrementRetry)
      elif jobFamily == JobFamily.RAD:
         success = self._rad.updateJobAndObjectStatus(jobid, computeIP, 
                                                      status, progress,
                                                      incrementRetry)
      return success
   
# ...............................................
   def updateExperimentMapfile(self, model=None, proj=None):
      if model is not None:
         occId = model.occurrenceSet.getId()
         obj = model
      elif proj is not None:
         occId = proj.getOccurrenceSet().getId()
         obj = proj
      else:
         raise LMError(lmerr='Must provide a model or projection')
        
      obj.clearLocalMapfile()
      obj.setLocalMapFilename()
      expLayerset = self._mal.getOccMapservice(occId)
      expLayerset.writeMap()
         
# ...............................................
# Layers (SDM)
# ...............................................
   def updateEnvLayer(self, envLyr):
      """
      @summary: Updates an environmental layer in the database
      @param lyr: an EnvironmentalLayer to update
      @return: True if environmental layer is modified successfully; False if 
               unsuccessful or if environmental layer does not exist.
      """
      if envLyr is not None:
         success = self._mal.updateEnvLayer(envLyr)
         return success
      return False
   
# ...............................................
   def deleteEnvLayer(self, envLyr):
      """
      @summary: Deletes an environmental layer in the database, assuming it is 
                unconnected to any Scenarios
      @param lyr: an EnvironmentalLayer to update
      @return: True if EnvironmentalLayer is deleted successfully; False if 
               unsuccessful or if EnvironmentalLayer is still joined to scenario.
      """
      if envLyr is not None:
         #envLyr.clearDataFile()
         envLyr.deleteData()
         success = self._mal.deleteEnvLayer(envLyr.getId())
         return success
      return False

# ...............................................
   def deleteLayerType(self, typeId):
      """
      @summary: Deletes a LayerType in the database, assuming it is 
                unconnected to any environmental layers
      @param typeId: unique LayerType id
      @return: True if LayerType is deleted successfully; False if 
               unsuccessful or if it is still joined to a layer.
      """
      if typeId is not None:
         #envLyr.clearDataFile()
         success = self._mal.deleteLayerType(typeId)
         return success
      return False

# ...............................................
   def updateLayerKeywords(self, layerid, typekeywords):
      """
      @todo: Delete later - only used in updating existing layers to new db schema.
      @summary Insert keywords associated with a layerType and its attributes 
               into the database
      @param layerid: Primary key of the layer
      @param typekeywords: Sequence of keywords for the layertype
      @return: True or False for success
      """
      for kw in typekeywords:
         typeid = self._mal.updateLayerTypeKeyword(layerid, None, kw)
         if typeid == -1: 
            self.log.warning('Error inserting {} for layertype {}'
                             .format(kw, layerid))
      return typeid
# ...............................................
# Model
# ...............................................
         
# ...............................................
   def moveAllDependentJobs(self):
      currtime = mx.DateTime.gmt().mjd
      notReadyStat = JobStatus.GENERAL
      readyStat = JobStatus.INITIALIZE
      completeStat = JobStatus.COMPLETE
      errorStat = JobStatus.GENERAL_ERROR

      radCount = self._rad.moveAllDependentJobs(completeStat, errorStat, 
                                                notReadyStat, readyStat, 
                                                currtime)
      sdmCount = self._mal.moveAllDependentJobs(completeStat, errorStat, 
                                                notReadyStat, readyStat, 
                                                currtime)
      self.log.debug('Moved {}/{} dependent SDM/RAD jobs to ready (status=1)'
                     .format(sdmCount, radCount))
      return sdmCount, radCount
      
# ...............................................
   def rollbackLifelessJobs(self, cutoffTime, jobFamily=None):
      sct = rct = 0
      if jobFamily is None or jobFamily == JobFamily.SDM:
         sct = self._mal.rollbackLifelessJobs(cutoffTime, JobStatus.PULL_COMPLETE, 
                                        JobStatus.INITIALIZE, JobStatus.COMPLETE)
      if jobFamily is None or jobFamily == JobFamily.RAD:
         rct = self._rad.rollbackLifelessJobs(cutoffTime, JobStatus.PULL_COMPLETE, 
                                        JobStatus.INITIALIZE, JobStatus.COMPLETE)
      return sct + rct

# ...............................................
   def getJobChainTopDown(self, occ):
      """
      @return: a nested tuple of dependent jobs and objects as:
         (occObj, [(mdlObj, [(prjObj, [(pavJob, None)]), (prjJob, None)]), 
                   (mdlJob, [(prjJob, [(pavJob, None), (pavJob, None)])]) ])
      @note: The top occurrence object will be a Job if it must be computed
      """
      (topOcc, occDependents) = self._mal.getJobChainTopDown(occ)
      return (topOcc, occDependents)
      
# ...............................................
   def resetTopDownJobChain(self, oldstat, startstat, depstat, 
                            top=ReferenceType.OccurrenceSet, usr=None):
      '''
      @summary: Reset all job chains, starting with a 'top' level dependency, 
                then all jobs dependent on completion of those objects 
      @param oldstat: target status to change
      @param startstat: desired status for top level jobs
      @param depstat: desired status for dependent jobs
      @param top: LmServer.common.lmconstants.ReferenceType, could start at 
                  OccurrenceSet, SDMModel, or SDMProjection.  
      @param usr: optional filter by userId
      @todo: put dependencies in subclasses of LmServer.base._JobData or 
             LmServer.base._Job, or in object classes
      '''
      if ReferenceType.isSDM(top):
         cnt = self._mal.resetSDMChain(top, oldstat, startstat, depstat, usr)
      elif ReferenceType.isRAD(top):
         raise LMError(lmerr='resetRADJobs is not yet implemented')
      return cnt
      
# ...............................................
   def pullJobs(self, count, computeIP, 
                processTypes=[ProcessType.ATT_MODEL, ProcessType.ATT_PROJECT, 
                              ProcessType.OM_MODEL, ProcessType.OM_PROJECT, 
                              ProcessType.RAD_INTERSECT, 
                              ProcessType.RAD_COMPRESS, ProcessType.RAD_SWAP, 
                              ProcessType.RAD_SPLOTCH, ProcessType.RAD_CALCULATE, 
                              ProcessType.SMTP], 
                userIds=[], inputTypes=[]):
      """
      @summary Gets Jobs from the MAL with status = startStat,  
               updates them in the database to endStat, and returns them
      @param count: The number of jobs to move from startStat to endStat 
                    and return.  If count is None, move all available. 
      @param computeIP: IP address for the ComputeResource requesting jobs
      @param processTypes: filter jobs by all acceptable required software;
                           requires at least one acceptable type.
                           uses @type LmCommon.common.lmconstants.ProcessType
      @param userIds: filter jobs by all acceptable userIds; will take any if 
                      empty list
      @param inputTypes: filter jobs by all acceptable input data; will 
                         take any if empty list; 
                         uses @type LmCommon.common.lmconstants.InputDataType 
      @return: a list of Jobs
      """
      jobs = []
      filters = self._getJobFilters(count, processTypes, userIds, inputTypes)
      for ptCount, ptType, usr, inType in filters:
         if ptType == ProcessType.SMTP:
            eachcount, rem = divmod(ptCount,2) 
            sdmcount = eachcount + rem
            currjobs = self._mal.pullJobs(eachcount+rem, ptType, JobStatus.INITIALIZE,
                                          JobStatus.PULL_REQUESTED, usr, inType, 
                                          computeIP)
            morejobs = self._rad.pullJobs(eachcount, ptType, JobStatus.INITIALIZE,
                                          JobStatus.PULL_REQUESTED, usr, inType, 
                                          computeIP)
            currjobs.extend(morejobs)
            self.log.debug('Pulled {} SMTP Jobs'.format(len(morejobs)))
            
         if ProcessType.isRAD(ptType):
            currjobs = self._rad.pullJobs(ptCount, ptType, JobStatus.INITIALIZE,
                                          JobStatus.PULL_REQUESTED, usr, inType, 
                                          computeIP)
            
         elif ProcessType.isSDM(ptType):
            currjobs = self._mal.pullJobs(ptCount, ptType, JobStatus.INITIALIZE,
                                          JobStatus.PULL_REQUESTED, usr, inType, 
                                          computeIP)
         
         jobs.extend(currjobs)
      return jobs
   
# ...............................................
   def resetObjectsJobsFromStatus(self, reftype, oldstat, newstat, usr=None):
      '''
      @summary: Change status of multiple objects at oldstat and any related jobs
      @param reftype: LmServer.common.lmconstants.ReferenceType
      @param oldstat: target status to change
      @param newstat: desired status
      @param usr: optional filter by userId
      '''
      if ReferenceType.isSDM(reftype):
         cnt = self._mal.resetObjectsJobsFromStatus(reftype, oldstat, newstat, usr)
      elif ReferenceType.isRAD(reftype):
         raise LMError(lmerr='RAD resetObjectsJobsFromStatus is not yet implemented')
      return cnt
      
# ...............................................
   def resetObjectAndJobs(self, reftype, objid, newstat):
      '''
      @summary: Change status of one object and any related jobs
      @param reftype: LmServer.common.lmconstants.ReferenceType
      @param objid: primary id of object to change
      @param newstat: desired status
      '''
      if ReferenceType.isSDM(reftype):
         cnt = self._mal.resetObjectAndJobs(reftype, objid, newstat)
      elif ReferenceType.isRAD(reftype):
         raise LMError(lmerr='RAD resetObjectAndJobs is not yet implemented')
      return cnt
      
# ...............................................
   def _getJobFilters(self, count, processTypes, userIds, inputTypes):
      from random import choice
      combos = []
      if not userIds:
         userIds = [None]
      if not inputTypes:
         inputTypes = [None]
         
      for ptype in processTypes:
         combos.append((count, ptype, choice(userIds), choice(inputTypes)))
      return combos
   
# ...............................................
   def moveSDMJobs(self, count, startStat, endStat, computeId=None, 
                   usr=None, inputType=None, computeType=None, 
                   jobTypes=[SDMProjectionJob, SDMModelJob]):
      """
      @summary Gets Jobs from the MAL with status = startStat,  
               updates them in the database to endStat, and returns them
      @param count: The number of jobs to move from startStat to endStat 
                    and return.  If count is None, move all available. 
      @param startStat: Beginning status code of Jobs to move
      @param endStat: New status code for moved jobs
      @param computeId: Identifier for the ComputeResource requesting jobs
      @param usr: filter for returned jobs by required userId
      @param inputType: filter for returned jobs by required input data; uses 
                        @type LmCommon.common.lmconstants.InputDataType: 
      @param computeType: filter for returned jobs by required software; uses 
                        @type LmCommon.common.lmconstants.ProcessType
      @return: a list of SDMJobs
      """
      jobs = []
      currjobs = []
      jbCount = max(count, count/len(jobTypes))
      for jobType in jobTypes:
         currjobs = self._mal.moveSDMJobs(jobType, jbCount, startStat, 
                                          endStat, usr, inputType, 
                                          computeType, computeId)
         jobs.extend(currjobs)
         if len(jobs) >= count:
            break
      return jobs

# ...............................................
   def moveNotifyJobs(self, count, startStat, endStat, computeId=None, 
                      usr=None, inputType=None, computeType=None):
      """
      @summary Gets Jobs from the MAL with status = startStat,  
               updates them in the database to endStat, and returns them
      @param count: The number of jobs to move from startStat to endStat 
                    and return.  If count is None, move all available. 
      @param startStat: Beginning status code of Jobs to move
      @param endStat: New status code for moved jobs
      @param computeId: Identifier for the ComputeResource requesting jobs
      @param usr: filter for returned jobs by required userId
      @param inputType: filter for returned jobs by required input data; uses 
                        @type LmCommon.common.lmconstants.InputDataType: 
      @param computeType: filter for returned jobs by required software; uses 
                        @type LmCommon.common.lmconstants.ProcessType
      @return: a list of SDMJobs
      """
      jobs = []
      currjobs = []
      jbCount = max(count, count/2)
      currjobs = self._mal.moveNotifyJobs(jbCount, startStat, 
                                       endStat, usr, inputType, 
                                       computeType, computeId)
      jobs.extend(currjobs)
      return jobs

# ...............................................
   def findProblemObjects(self, cutofftime, startStat=None, endStat=None,
                      count=None, ignoreUser=None):
      """
      @summary: Method to notify developers of a problem in the pipeline or a 
                user experiment.
      """
      mdls, projs = self._mal.findProblemObjects(cutofftime, startStat, 
                                                   endStat, count, ignoreUser)
      return  mdls, projs
             
# ...............................................
# OccurrenceLayers
# ...............................................
   def insertAndSaveOccurrenceSet(self, occ):
      """
      Save an occurrence set provided by the webservice/user.  
      @deprecated: No longer save SpecimenPoint list in the object - 
                   now use Vector._features.
      @note: Called from userdata.postOccurrenceSet and JOD.inject only.
      @param occ: New OccurrenceLayer to save to MAL.  
      """
      # Save user points reference to MAL and points to shapefile set
      if occ.getId() is None:
         try:
            # Insert into database
            id = self._mal.insertOccurrenceSetMetadata(occ)
            
            # If points are present, calculate shapefile location and write
            if occ.getFeatures():
               occ.writeShapefile()
               # Update database record with featureCount and geometry 
               success = self._mal.updateOccurrenceSetMetadataAndStatus(occ)
               if not success:
                  raise LMError(currargs='Failed to update occurrenceset {}'
                                          .format(occ.getId()), 
                                lineno=self.getLineno())
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs='Failed', prevargs=e.args, 
                           lineno=self.getLineno())
            raise e
      else:
         self.log.error('OccurrenceLayer {} already contains a database ID'
                        .format(occ.getId()))
            
# ...............................................
   def insertOccurrenceSet(self, occ):
      """
      Save a new occurrence set, metadata only, no points, from GBIF.   
      @param occ: New OccurrenceSet to save to MAL.  
      @note: updates db with count, the actual count on the object (likely zero 
             on initial insertion)
      """
      # Save user points reference to MAL
      if occ.getId() is None :
         id = self._mal.insertOccurrenceSetMetadata(occ)
      else:
         self.log.error('OccurrenceLayer {} already contains a database ID'
                        .format(occ.getId()))
      return id
         
# ...............................................
   def removeOccurrenceSetExperiments(self, occ):
      """
      @summary: Remove an OccurrenceLayer from database (metadata)
                and all experiments for that occurrenceset (regardless of owner).  
                Also remove mapfile and shapefiles if they exist.
      @occ: OccurrenceLayer object to delete
      """
      models = self.getModelsForOccurrenceSet(occ.getId(), status=None,
                                              userid=occ.getUserId())
      count = 0
      for mdl in models:
         success = self.deleteExperiment(mdl)
         if success:
            count += 1
      if models:
         occ.clearLocalMapfile()
      return count
      
# ...............................................
   def completelyRemoveOccurrenceSet(self, occ):
      """
      @summary: Remove an OccurrenceLayer from database
                and all experiments for that occurrenceset (regardless of owner).  
                Also remove mapfile and shapefiles if they exist.
      @occ: OccurrenceLayer object to delete
      """
      # If this is an Atom, get the full object first
      try:
         pth = occ.getAbsolutePath()
      except:
         occ = self.getOccurrenceSet(occ.getId())
         pth = occ.getAbsolutePath()
         
      # ARCHIVE_USER dependent experiments are all in occurrenceId-based path 
      if occ.getUserId() == ARCHIVE_USER:
         if os.path.exists(pth):
            import glob
            fnames = glob.glob(os.path.join(pth, '*'))
            for fn in fnames:
               os.remove(fn)                            
            os.rmdir(pth)
      # Workaround for non-occurrenceId-based paths for users
      # TODO: remove this when UserData structure parallels ArchiveData
      else:
         exps = self.getExperimentsForOccurrenceSet(occ.getId(), 
                                                    userid=occ.getUserId())
         for exp in exps:
            exp.model.clearModelFiles()
            exp.model.clearLocalMapfile()
            for prj in exp.projections:
               prj.clearProjectionFiles()
         occ.deleteData()
      success = self._mal.deleteOccAndDependentObjects(occ.getId(), 
                                                       occ.getUserId())
      if not success:
         self.log.info ('  Failed to remove occurrence set %d' % (occ.getId()))

      return success
      
# ...............................................
   def rollbackOccurrenceDeleteDependents(self, occ):
      """
      @summary: Rollback an OccurrenceLayer without raw or cooked data to
                status = 0, delete all experiments for that occurrenceset.  
                Also remove mapfile if it exists.
      @param occ: OccurrenceLayer object to rollback
      """
      # If this is an Atom, get the full object first
      try:
         pth = occ.getAbsolutePath()
      except:
         occ = self.getOccurrenceSet(occ.getId())
         pth = occ.getAbsolutePath()
         
      exps = self.getExperimentsForOccurrenceSet(occ.getId(), status=None,
                                                userid=occ.getUserId())
      for exp in exps:
         exp.model.clearModelFiles()
         exp.model.clearLocalMapfile()
         for prj in exp.projections:
            prj.clearProjectionFiles()
         self._mal.deleteModel(exp.model.getId())
      occ.deleteData()
      occ.updateStatus(JobStatus.GENERAL, queryCount=-1)
      success = self.updateOccState(occ)
      if not success:
         self.log.error ('  Failed to remove experiments for occurrence set %d' 
                        % (occ.getId()))
      return success
      
# ...............................................
   def setSpeciesEnvironment(self, occ, primaryEnvCode):
      """
      @summary: Remove an OccurrenceLayer from database
                and all experiments for that occurrenceset (regardless of owner).  
                Also remove mapfile and shapefiles if they exist.
      @occ: OccurrenceLayer object to delete
      """
      success = self._mal.setSpeciesEnvironment(occ.getId(), primaryEnvCode)
      if not success:
         self.log.error ('Failed to set occurrence set %d to environment %d' % 
                        (occ.getId(), primaryEnvCode))

      return success

# ...............................................
   def clearOccurrenceSet(self, occ):
      """
      @summary: Remove any points from the occurrenceset, the 
                mapfile and shapefiles if they exist, and clear the metadata 
                (querycount = -1 and new timestamp).  Delete experiments
                using this occurrenceset, filtering on Occurrenceset userid.    
      @note: This could leave orphaned experiments. 
      @occ: OccurrenceLayer object to clear
      """
      models = self.getModelsForOccurrenceSet(occ.getId(), status=None, 
                                              userid=occ.getUserId())
      for mdl in models:
         success = self.deleteExperiment(mdl)
         if not success:
            self.log.error('Failed to delete experiment %d' % mdl.getId())
         
      # Should be present if GBIF or uploaded data
      occ.deleteData()
      occ.clearLocalMapfile()
      occ.queryCount = -1
      
      success = self._mal.updateOccurrenceSetMetadataAndStatus(occ)
      if not success:
         self.log.error('Failed to update occurrenceset {}'.format(occ.getId()))
   
# ...............................................         
   def updateExperimentsForOcc(self, occ, userid, occSuccess=True):
      # Get experiments for occurrenceset and (default) user
      try:
         exps = self.getExperimentsForOccurrenceSet(occ.getId(), status=None, 
                                                    userid=userid)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs='Failed getting experiments', 
                        prevargs=e.args, lineno=self.getLineno())
         raise e
   
      for exp in exps:
         # low priority or mid-execution, raise to higher priority
         newpriority = None
         if (exp.model.status != JobStatus.INITIALIZE or
             exp.model.priority < Priority.OBSOLETE):
            newpriority = Priority.OBSOLETE
         # then re-init or set to error
         try:
            if occSuccess:
               success = self._rollbackExp(exp, newpriority=newpriority)
            else:
               # If failed to update OccurrenceLayer, exp=ERROR
               success = self._rollbackExp(exp, 
                               errstatus=JobStatus.LM_PIPELINE_UPDATEOCC_ERROR)
               self.log.error('Rolled back experiments to status {} for failed occurrenceset {} ({})'
                              .format(JobStatus.LM_PIPELINE_UPDATEOCC_ERROR,
                                      occ.getId(), occ.displayName ))
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs='Failed updating old exp {}'.format(exp.getId()), 
                           prevargs=e.args, lineno=self.getLineno())
            raise e
         
# ...............................................         
   def _deleteOverlyComplexOccset(self, occ):
      deleted = False
      # Delete if overly-complex name
      if len(occ.displayName.split()) > 2:
         self.completelyRemoveOccurrenceSet(occ)
         deleted = True
      return deleted
               
   
# ...............................................
   def rollbackOrDeleteExperiment(self, exp, priority):
      """
      @summary: Rollback SDMExperiments for re-calculation
      """
      jobs = []
      currtime = mx.DateTime.gmt().mjd
      if exp.model.occurrenceSet.queryCount < POINT_COUNT_MIN:
         try:
            self.deleteExperiment(exp.model)
         except Exception, e:
            self.log.error('Failed to delete experiment {} with {} points' % 
                           (exp.getId(), exp.model.occurrenceSet.queryCount))
      else:
         try:
            mdlJob, notJob = self.reinitSDMModel(exp.model, priority, 
                                                 modtime=currtime)
            jobs.append(mdlJob)
            if notJob:
               jobs.append(notJob)
               
            for prj in exp.projections:
               prjJob = self.reinitSDMProjection(prj, priority, modtime=currtime)
               jobs.append(prjJob)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs='Failed updating old exp {}'
                                    .format(exp.getId()),
                           prevargs=e.args, lineno=self.getLineno())
            self.log.debug('Failed rollback exp, deleting instead ({})'.format(e))
            self.deleteExperiment(exp.model)
            raise e
         self.log.info('Rolled back experiment {} ({}) to status {}'
                       .format(exp.getId(), exp.model.occurrenceSet.displayName, 
                               JobStatus.INITIALIZE))
      return jobs
# ...............................................
   def updateModel(self, mdl):
      """
      @summary: Update Model attributes: status, priority, dLocation, qualityControl
      @param occ: Model to be updated
      @return: True/False for successful update.
      """
      success = self._mal.updateModel(mdl, None, None)
      return success

# ...............................................
   def updateProjection(self, prj):
      """
      @summary: Update Projection attributes: status, priority, dLocation, 
                gdalType, bbox, epsgcode
      @param prj: Projection to be updated
      @return: True/False for successful update.
      """
      success = self._mal.updateProjection(prj, None, None)
      return success

# ...............................................
   def updateOccset(self, occ):
      """
      @summary: Update OccurrenceLayer attributes: filename, count, 
                modtime, epsgcode, geometry
      @param occ: OccurrenceLayer to be updated.  OccurrenceLayer._features must be 
                present to correctly update geometry.
      @return: True/False for successful update.
      """
      success = self._mal.updateOccurrenceSetMetadataAndStatus(occ)
      return success

# ...............................................
   def updateOccState(self, occ):
      """
      @summary: Update OccurrenceLayer attributes: filename/s, count, status,
                statusmodtime, touchtime, parammodtime, 
      @param occ: OccurrenceLayer to be updated.  
      @return: True/False for successful update.
      """
      success = self._mal.updateOccurrenceState(occ)
      return success

# ...............................................
   def touchOccset(self, occ):
      """
      @summary: Update OccurrenceLayer datelastchecked
      @param occ: OccurrenceLayer to be updated.  
      @return: True/False for successful update.
      """
      success = self._mal.touchOccurrenceSet(occ)
      return success
   
# ...............................................
   def resetOccsetAndExperiments(self, occ):
      occ.queryCount = -1
      success = self._mal.updateOccurrenceSetMetadataAndStatus(occ)
      exps = self.getExperimentsForOccurrenceSet(occ.getId(), status=None, 
                                                    userid=occ.getUserId())
      for exp in exps:
         self._rollbackExp(exp)
      return success, len(exps)
   
# ...............................................
# Scenario
# ...............................................
   def insertScenario(self, scen):
      lyrIds = []
      scenId = self._mal.insertScenario(scen)
      for lyr in scen.layers:
         updatedLyr = self.insertScenarioLayer(lyr, scenId)
         lyrIds.append(updatedLyr.getId())
      return scenId, lyrIds

# ...............................................
   def deleteScenario(self, scen):
      """
      @summary: Deletes a Scenario in the database, assuming it is 
                unconnected to any Models or Projections
      @param lyr: an Scenario to update
      @return: True if Scenario is deleted successfully; False if 
               unsuccessful or if Scenario is still joined to Experiments.
      """
      if scen is not None:
         success = self._mal.deleteScenario(scen.getId())
         return success
      return False

# ...............................................
# Statistics
# ...............................................
   def updateStatistic(self, key, qry):
      success = self._mal.updateStatistic(key, qry)
      return success

# ...............................................
   def insertStatisticRow(self, key, query, description):
      self._mal.insertStatisticRow(key, query, description)

# ...............................................
# ...............................................
   def registerComputeResource(self, compResource, crContact):
      """
      @summary: Insert a compute resource of this Lifemapper system.  
      @param usr: LMComputeResource object to insert
      @return: True on success, False on failure (i.e. IPAddress is not unique)
      """
      existingCR = self._mal.getComputeResourceByIP(compResource.ipAddress,
                                                    ipMask=compResource.ipMask)
      if existingCR is None:
         usr = self._mal.getUser(crContact.getUserId())
         if usr is None:
            self._mal.insertUser(crContact)
         compResource = self._mal.insertComputeResource(compResource)
      return compResource

# ...............................................
   def insertUser(self, usr):
      """
      @summary: Insert a user of the Lifemapper system.  Allows 
            on-demand-modeling with user-submitted point data.
      @param usr: LMUser object to insert
      @return: True on success, False on failure (i.e. userid is not unique)
      @note: since inserting the same record in both databases, userid is identical
      """
      existingUser = self._mal.findUser(usr.userid, usr.email)
      if existingUser is not None:
         uid = existingUser.userid
      else:
         uid = self._mal.insertUser(usr)
      return uid

# ...............................................
   def deleteUser(self, usr):
      """
      @summary: Delete a user of the Lifemapper system.  
      @param usr: LMUser object to delete
      @return: True on success, False on failure
      """
      success = self._mal.deleteUser(usr)
      return success

# ...............................................
   def updateUser(self, usr):
      """
      @summary: Update a user of the Lifemapper system.  All fields are updated,
             even if null, so fill all of them appropriately before calling this 
             method.
      @param usr: LMUser object to insert
      @return: True on success, False on failure
      """
      success = self._mal.updateUser(usr)
      return success
   
# ...............................................
# Miscellaneous
# ...............................................
   def insertTaxonomySource(self, taxSourceName, taxSourceUrl):
      currtime = mx.DateTime.gmt().mjd
      taxSourceId = self._mal.insertTaxonSource(taxSourceName, taxSourceUrl, 
                                                currtime)
      return taxSourceId

# ...............................................
   def insertTaxon(self, sciName):
      self._mal.insertTaxon(sciName)
      
# ...............................................
   def deleteTaxon(self, sciName):
      self._mal.deleteTaxon(sciName)
      
   
# .............................................................................
# RAD functions
# .............................................................................   
# ...............................................
# Job/Experiment (RAD)
# ...............................................
   def insertRADExperiment(self, radexp):
      """
      @note: Assumes any 
      """
      existExp = self.getRADExperiment(radexp.getUserId(), expid=radexp.getId(), 
                                       expname=radexp.name, fillLayers=True)
      if existExp is not None:
         self.log.warning('Using existing RADExperiment {} for user {}'
                          .format(radexp.name, radexp.getUserId()))
         # Make sure all the buckets exist too
         for currbuck in radexp.bucketList:
            if existExp.getBucket(currbuck.name) is None:
               newexistBucket = self.insertBucket(currbuck, existExp)
               existExp.addBucket(newexistBucket)
         updatedExp = existExp
      else:
         # Insert experiment and insert/update each bucket
         expid = self._rad.insertExperiment(radexp)
                     
         updatedLayers = []
         for palyr in radexp.orgLayerset.layers:
            updatedLyr = self._rad.insertPresenceAbsenceLayer(palyr, expid)
            updatedLayers.append(updatedLyr)
         radexp.orgLayerset.setLayers(updatedLayers)
         
         updatedLayers = []
         for anclyr in radexp.envLayerset.layers:
            updatedLyr = self._rad.insertAncillaryLayer(anclyr, expid)
            updatedLayers.append(updatedLyr)
         radexp.envLayerset.setLayers(updatedLayers)
         updatedExp = radexp
         
      return updatedExp
            
# ...............................................
   def deleteRADExperiment(self, exp):
      success = False
      for bck in exp.bucketList:
         bck.clear()
         bsuccess = self.deleteBucket(bck)
         if not bsuccess:
            self.log.error('Failed to delete bucket {}'.format(bck.getId()))
      exp.clear()
      success = self._rad.deleteExperiment(exp.getId())
      return success
   
# ...............................................
   def deleteRADExperimentJobs(self, exp):
      jobcount = self._rad.deleteRADJobsForExperiment(exp.getId())
      return jobcount
   
# ...............................................
   def deleteRADJob(self, job):
      success = self._rad.deleteRADJob(job.getId())
      return success

# ...............................................
   def deleteSDMJob(self, job):
      success = self._mal.deleteJob(job.getId())
      return success
   
# ...............................................
   def updateRADExperiment(self, exp):
      success = self._rad.updateExperiment(exp)
      return success
         
# ...............................................
   def rollbackIncompleteJobs(self, queuedStatus):
      """
      @summary Reset jobs that were begun but not
               yet dispatched (JobStatus.PULL_REQUESTED) for whatever reason.
               Set back to JobStatus.GENERAL to re-check that dependencies 
               are met.
      """
      sdmcount = self._mal.rollbackSDMJobs(queuedStatus)
      radcount = self._rad.rollbackRADJobs(queuedStatus)
      return sdmcount + radcount

# ...............................................
# Bucket (RAD)
# ...............................................
   def insertBucket(self, bucket, exp):
      newexistingBucket = self._rad.insertBucket(bucket, exp.getId())
      return newexistingBucket
   
   def findBucket(self, exp, bucket):
      bkt = None
      if bucket.getId() is not None:
         bkt = self._rad.getBucket(bucket.getId())
      elif exp.getId() is not None:
         bkt = self._rad.getBucketByShape(exp.getId(), 
                                          shpName=bucket.shapegrid.name, 
                                          shpId=bucket.shapegrid.getParametersId())
      return bkt
   
# ...............................................
   def deleteBucket(self, bucket):
      bucket.clear()
      success = self._rad.deleteBucket(bucket.getId())
      return success
   
# ...............................................
   def deletePamSum(self, pamsum):
      """
      @summary: Deletes PamSum db record, associated files and jobs
      @param pamsum: PamSum Id or object.  
      """
      success = True
      if isinstance(pamsum, IntType):
         pamsum = self._rad.getPamSum(pamsum)
      if pamsum is not None:
         pamsum.clear()
         success = self._rad.deletePamSum(pamsum.getId())
         if not success:
            self.log.error('Unable to delete PamSum {}'.format(pamsum.getId()))
      return success
   
# ...............................................
   def updateRADFilenames(self, bucketOrPamSum):
      try:
         bucketOrPamSum.getFullPAM()
         success = self._rad.updateBucketInfo(bucketOrPamSum)
      except:
         bucketOrPamSum.sum
         success = self._rad.updatePamSumFilenames(bucketOrPamSum)
      return success

# ...............................................
# Layer (RAD)
# ...............................................
# ...............................................
   def deletePresenceAbsenceLayerSet(self, radexp):
      setSuccess = True
      if radexp.orgLayerset is not None:
         for palyr in radexp.orgLayerset.layers:
            success = self._rad.deletePresenceAbsenceLayer(palyr, radexp.getId())
            if not success:
               setSuccess = False
            else:
               for bkt in radexp.bucketList:
                  bkt.clearPresenceIndicesFile()
      return setSuccess

   def deletePresenceAbsenceLayer(self, palyr, expid):
      success = self._rad.deletePresenceAbsenceLayer(palyr, expid)
      return success
   
   def updatePresenceAbsenceLayer(self, palyr, expid):
      newPAId = self._rad.updatePresenceAbsenceLayer(palyr, expid)
      return newPAId 

# ...............................................
# ...............................................
   def insertAncillaryLayer(self, anclyr, radexp):
      updatedlyr = None
      if hasattr(anclyr, 'weightedMean'):
         # The function inserts new Layer, Ancillary Values 
         # and joins Layer, Ancillary Values, Experiment
         updatedlyr = self._rad.insertAncillaryLayer(anclyr, radexp.getId())
         if updatedlyr and not radexp.getEnvLayer(anclyr.metadataUrl, anclyr.getParametersId()):
            radexp.addAncillaryLayer(updatedlyr)
      else:
         raise LMError(currargs='Wrong object type', lineno=self.getLocation())
      return updatedlyr

# ...............................................
   def deleteAncillaryLayer(self, anclyr, expid):
      success = self._rad.deleteAncillaryLayer(anclyr, expid)
      return success

# ...............................................
   def deleteAncillaryLayerSet(self, radexp):
      setSuccess = True
      if radexp.envLayerset is not None:
         for alyr in radexp.envLayerset.layers:
            success = self._rad.deleteAncillaryLayer(alyr, radexp.getId())
            if not success:
               setSuccess = False
      return setSuccess
   
# ...............................................
   def getAncillaryLayersForAncValues(self, anclyr):
      anclyrs = self._rad.getLayersForAncillary(anclyr.getUserId(), anclyr.getId())
      return anclyrs

# ...............................................
   def updateAncillaryLayer(self, anclyr, expid):
      new_ancid = self._rad.updateAncillaryLayer(anclyr, expid)
      return new_ancid

# ...............................................
# ...............................................
   def deleteRADLayer(self, lyr):
      dbSuccess = self._rad.deleteBaseLayer(lyr.getUserId(), lyr.getId())
      fileSuccess = lyr.deleteData()
      return dbSuccess and fileSuccess

# ...............................................
# ShapeGrid
# ...............................................
   def insertShapeGrid(self, shpgrd, cutout=None):
      if shpgrd.getParametersId() is not None:
         existSG = self.getShapeGrid(shpgrd.getUserId(),shpid=shpgrd.getId())
      elif shpgrd.name is not None:
         existSG = self.getShapeGrid(shpgrd.getUserId(),shpname=shpgrd.name)

      if existSG is not None:
         self.log.warning('Using existing ShapeGrid {} for user {}'
                          .format(shpgrd.name, shpgrd.getUserId()))
         return existSG
         
      else:
         newshpgrd = self._rad.insertShapeGrid(shpgrd, cutout)
         return newshpgrd
   
   # ...............................................
   def deleteShapeGrid(self, shpgrd):
      """
      @note: uses parameterId (shapegridId)
      """
      success = shpgrd.deleteData() and self._rad.deleteShapeGrid(shpgrd.getParametersId())
      return success
   
   # ...............................................
   def updateShapeGrid(self, shpgrd, expid):
      pass

   # ...............................................
   def renameShapeGrid(self, shpgrd, newshpname):
      success = self._rad.renameShapeGrid(shpgrd, newshpname)
      return success
   
# ...............................................
   def initSDMOccJob(self, usr, occ, occJobProcessType, modtime,  
                     priority=Priority.NORMAL): 
      occJob = self.getJobOfType(JobFamily.SDM, occ)
      if occJob is None:
         try:
            if occ.status != JobStatus.INITIALIZE:
               occ.updateStatus(JobStatus.INITIALIZE, modTime=modtime)
            occJob = SDMOccurrenceJob(occ, processType=occJobProcessType,
                                      status=JobStatus.INITIALIZE, 
                                      statusModTime=modtime, createTime=modtime,
                                      priority=Priority.NORMAL)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self.log.error('   Failed to update occurrenceset or create Job for {} ({})'
                           .format(occ.getId()), str(e))
            raise e
            
         try:
            success = self.updateOccState(occ)
            updatedOccJob = self.insertJob(occJob)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self.log.error('   Failed to insert occurrenceSet Job for {} ({})'
                           .format(occ.getId()), str(e))
            raise e
         else:
            occJob = updatedOccJob
      return occJob

# ...............................................
   def initSDMChain(self, usr, occ, algList, mdlScen, prjScenList, 
                    occJobProcessType=ProcessType.GBIF_TAXA_OCCURRENCE,
                    priority=Priority.NORMAL, mdlMask=None, prjMask=None,
                    intersectGrid=None, minPointCount=None):
      """
      @summary: Initialize LMArchive job chain (models, projections, 
                optional intersect) for occurrenceset.
      """
      jobs = []
      currtime = mx.DateTime.gmt().mjd
      # ........................
      # OccurrenceJobs
      if occJobProcessType is not None:
         occJob = self.initSDMOccJob(usr, occ, occJobProcessType, 
                                     currtime, priority=priority)
         jobs.append(occJob)
      # ........................
      # ModelJobs and ProjectionJobs
      if minPointCount is None or occ.queryCount >= minPointCount: 
         for alg in algList:
            rightModels = [] 
            mdls = self.listModels(0, 500, userId=usr, occSetId=occ.getId(), 
                                   algCode=alg.code, atom=False)
            # TODO: Add scenario filter to listModels and listProjections
            for mdl in mdls:
               if mdl.scenarioCode == mdlScen.code:
                  rightModels.append(mdl)
            if not rightModels:
               morejobs = self.initSDMModelProjectionJobs(occ, mdlScen, 
                                 prjScenList, alg, usr, priority, 
                                 modtime=currtime, 
                                 mdlMask=mdlMask, prjMask=prjMask)
               jobs.extend(morejobs)

            else:
               desiredScenCodes = set([s.code for s in prjScenList])
               for mdl in rightModels:
                  # ........................
                  # Reinit existing Experiments
                  projs = self.getProjectionsForModel(mdl, None)
                  exp = SDMExperiment(mdl, projs)
                  self.rollbackOrDeleteExperiment(exp, priority)
                  # ........................
                  # Add any missing projections
                  existingScenCodes = set([p.scenarioCode for p in projs])
                  neededScenCodes = existingScenCodes.symmetric_difference(desiredScenCodes)
                  for prjScen in prjScenList:
                     if prjScen.code in neededScenCodes:
                        prjJob = self.initSDMProjectionJob(mdl, prjScen, priority, 
                                             modtime=currtime, prjMask=prjMask)
                        jobs.append(prjJob)
      # ........................
      # @TODO: IntersectJobs for one layer + one grid + parameters
      
      return jobs

# ...............................................
   def initRADBuildGrid(self, usr, shapegrid, cutoutWKT=None, 
                        priority=Priority.NORMAL):
      """
      @param usr: UserId for this experiment
      @param shapegrid: Shapegrid object containing all the parameters for a 
                        polygon shapefile to be built.
      """
      jobs = []
      currtime = mx.DateTime.gmt().mjd
         
      job = RADBuildGridJob(shapegrid, cutoutWKT=cutoutWKT, 
                               status=JobStatus.INITIALIZE, 
                               statusModTime=currtime, 
                               priority=priority, createTime=currtime)
      updatedJob = self._rad.insertJobNew(job)
      jobs.append(updatedJob)
      return jobs
      
# # ...............................................
#    def initRADIntersect(self, usr, bucketId, doSpecies=True, priority=None):
#       """
#       @param usr: UserId for this experiment
#       @param bucketId: Id for a RADBucket containing a ShapeGrid to be 
#                        intersected with the layers in its RADExperiment 
#       @note: Initializes Intersect, Compress, Calculate
#       """
#       jobs = []
#       currtime = mx.DateTime.gmt().mjd
#       exp = self.getRADExperimentWithOneBucket(usr, bucketId, fillLayers=True)
#       
#       # Rollback existing bucket and delete pamsums
#       self.rollbackRADExperiment(exp, bucketId=bucketId)
#       
#       bkt = exp.bucketList[0]
#       
#       # Init Intersect
#       job = RADIntersectJob(exp, doSpecies=doSpecies, 
#                                status=JobStatus.INITIALIZE, 
#                                statusModTime=currtime, 
#                                priority=priority, createTime=currtime)
#       updatedIJob = self._rad.insertJobNew(job)
#       jobs.append(updatedIJob)

# ...............................................
   def initRADIntersectPlus(self, usr, bucketId, doSpecies=True, priority=None):
      """
      @param usr: UserId for this experiment
      @param expId: Id for a RADExperiment containing Layers to be intersected 
                    with ShapeGrids in each bucket
      @param bucketId: Id for a RADBucket containing a ShapeGrid to be 
                       intersected with the layers in its RADExperiment 
      @note: Initializes Intersect, Compress, Calculate
      """
      jobs = []
      currtime = mx.DateTime.gmt().mjd
      exp = self.getRADExperimentWithOneBucket(usr, bucketId, fillLayers=True)
      
      # Rollback existing bucket and delete pamsums
      self.rollbackRADExperiment(exp, bucketId=bucketId)
      
      bkt = exp.bucketList[0]
      
      # Init Intersect
      job = RADIntersectJob(exp, doSpecies=doSpecies, 
                               status=JobStatus.INITIALIZE, 
                               statusModTime=currtime, 
                               priority=priority, createTime=currtime)
      updatedIJob = self._rad.insertJobNew(job)
      jobs.append(updatedIJob)
      
      # Init Compress
      origps = PamSum(None, createTime=currtime, 
                      bucketId=bkt.getId(), expId=bkt.parentId, 
                      epsgcode=bkt.epsgcode, bucketPath=bkt.outputPath, userId=usr, 
                      status=JobStatus.GENERAL, statusModTime=currtime, 
                      stage=JobStage.COMPRESS, stageModTime=currtime, 
                      randomMethod=RandomizeMethods.NOT_RANDOM,
                      parentMetadataUrl=bkt.metadataUrl)
      bkt.pamSum = origps
      job = RADCompressJob(exp, origps, status=JobStatus.GENERAL, 
                              statusModTime=currtime, priority=priority, 
                              createTime=currtime)
      updatedCJob = self._rad.insertJobNew(job)
      jobs.append(updatedCJob)
      
      # Init Calculate
      job = RADCalculateJob(exp, origps, status=JobStatus.GENERAL, 
                               statusModTime=currtime, priority=priority,  
                               createTime=currtime)
      updatedCaJob = self._rad.insertJobNew(job)
      jobs.append(updatedCaJob)
      
      # Init Notify
      if exp.email is not None:
         job = NotifyJob(obj=bkt, objType=ReferenceType.Bucket , 
                         parentUrl=exp.metadataUrl, jobFamily=JobFamily.RAD, 
                         email=exp.email, status=JobStatus.GENERAL, 
                         statusModTime=currtime, priority=priority, 
                         createTime=currtime)
         jobs.append(job)
      return jobs

# ...............................................
   def initRADRandomizePlus(self, usr, bucketId, priority=None,
                           method=RandomizeMethods.SPLOTCH, numSwaps=50):
      """
      @param usr: UserId for this experiment
      @note: Intersection (and Compression for Swap) must be complete prior to
             initializing a job.
      @note: Initializes:
                Swap, Calculate  OR
                Splotch, Compress, Calculate
      """
      jobs = []
      currtime = mx.DateTime.gmt().mjd
      if method == RandomizeMethods.SWAP:
         params = {'numSwaps': numSwaps}
         stage = JobStage.SWAP
      else:
         params = {}
         stage = JobStage.SPLOTCH

      exp = self.getRADExperimentWithOneBucket(usr, bucketId, fillLayers=True)
      bkt = exp.bucketList[0]
      origPS = bkt.pamSum
      
      # Init Randomize PS object
      rndps = PamSum(None, createTime=currtime, 
                     bucketId=bkt.getId(), expId=bkt.parentId, 
                     epsgcode=bkt.epsgcode, userId=usr, 
                     bucketPath=bkt.outputPath,
                     randomMethod=method, randomParameters=params,
                     status=JobStatus.GENERAL, statusModTime=currtime,
                     stage=stage, stageModTime=currtime,
                     parentMetadataUrl=bkt.metadataUrl)
      bkt.addRandomPamSum(rndps)

      if method in (RandomizeMethods.SWAP, RandomizeMethods.GRADY):
         # Randomize is ready if original Pam is compressed
         if (origPS.stage in (JobStage.COMPRESS, JobStage.CALCULATE) and
             origPS.status == JobStatus.COMPLETE):
            rndps.updateStatus(JobStatus.INITIALIZE, modTime=currtime)
         # Init Swap or Grady Job
         if method == RandomizeMethods.SWAP:
            rJob = RADSwapJob(exp, rndps, numSwaps=numSwaps, status=rndps.status, 
                              statusModTime=currtime, priority=priority, 
                              createTime=currtime)
         elif method == RandomizeMethods.GRADY:
            rJob = RADGradyJob(exp, rndps, status=rndps.status, 
                               statusModTime=currtime, priority=priority, 
                               createTime=currtime)
         # Insert Randomize Job
         updatedRJob = self._rad.insertJobNew(rJob)
         jobs.append(updatedRJob)
         
         # then Init Calculate Job
         cJob = RADCalculateJob(exp, rndps, status=JobStatus.GENERAL, 
                                   statusModTime=currtime, priority=priority, 
                                   createTime=currtime)
         updatedCaJob = self._rad.insertJobNew(cJob)
         jobs.append(updatedCaJob)
         
      elif method == RandomizeMethods.SPLOTCH:
         if bkt.status == JobStatus.COMPLETE:
            rndps.updateStatus(JobStatus.INITIALIZE, modTime=currtime)
         # Init Splotch Job
         rJob = RADSplotchJob(exp, rndps, status=rndps.status, 
                                 statusModTime=currtime, priority=priority, 
                                 createTime=currtime)
         updatedRJob = self._rad.insertJobNew(rJob)
         jobs.append(updatedRJob)
         
         # then Init Compress Job
         cJob = RADCompressJob(exp, rndps, status=JobStatus.GENERAL, 
                                  statusModTime=currtime, priority=priority, 
                                  createTime=currtime)
         updatedCJob = self._rad.insertJobNew(cJob)
         jobs.append(updatedCJob)
         
         # finally Init Calculate Job
         job = RADCalculateJob(exp, rndps, status=JobStatus.GENERAL, 
                                  statusModTime=currtime, priority=priority, 
                                  createTime=currtime)
         updatedCaJob = self._rad.insertJobNew(job)
         jobs.append(updatedCaJob)
      
      else:
         raise LMError(currargs='Invalid Randomization Method')
         
      return jobs

# ...............................................
   def _getAndDivideExperimentForJobs(self, usr, expId=None, bucketId=None, pamsumId=None):
      if pamsumId is not None:
         exp = self._rad.getPamSumWithBucketAndExperiment(pamsumId)
         return [exp]
      
      elif bucketId is not None:
         # Experiment with one bucket
         exp = self.getRADExperimentWithOneBucket(usr, bucketId, fillLayers=True)
         return [exp]

      elif expId is not None:
         # Experiment with all buckets
         exp = self.getRADExperiment(usr, expid=expId, fillLayers=True)
         clones = exp.divide()
         return clones
         
      else:
         raise LMError(currargs='Must provide an experimentId or bucketId to initialize Intersect')
   

# .............................................................................
if __name__ == "__main__":
   from LmServer.common.log import ScriptLogger
   
   scribe = Scribe(ScriptLogger('scribeTest'))
   scribe.openConnections()
   comps = scribe.getAllComputeResources()
   print 'Output:'
   for c in comps:
      print '  ', c.name, c.ipAddress+'/'+c.ipMask

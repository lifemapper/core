"""
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
   
from LmCommon.common.lmconstants import JobStatus, DEFAULT_EPSG, ProcessType

from LmServer.base.dbpgsql import DbPostgresql
from LmServer.base.job import _Job
from LmServer.base.layer import Raster, Vector
from LmServer.base.taxon import ScientificName
from LmServer.base.layerset import MapLayerSet                                  
from LmServer.base.lmobj import LMError
from LmServer.common.computeResource import LMComputeResource
from LmServer.common.lmconstants import (ALGORITHM_DATA, LMServiceModule,
                  DEFAULT_PROJECTION_FORMAT, JobFamily, MAL_STORE, ReferenceType)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import ARCHIVE_USER
from LmServer.common.notifyJob import NotifyJob
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.envlayer import EnvironmentalType, EnvironmentalLayer
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.sdmJob import SDMModelJob, SDMProjectionJob, SDMOccurrenceJob
from LmServer.sdm.scenario import Scenario
from LmServer.sdm.sdmmodel import SDMModel
from LmServer.sdm.sdmprojection import SDMProjection

# .............................................................................
class MAL(DbPostgresql):
   """
   Class to control modifications to the MAL database.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger, dbHost, dbPort, dbUser, dbKey):
      """
      @summary Constructor for MAL class
      @param logger: LmLogger to use for MAL
      @param dbHost: hostname for database machine
      @param dbPort: port for database connection
      """
      DbPostgresql.__init__(self, logger, db=MAL_STORE, user=dbUser, 
                            password=dbKey, host=dbHost, port=dbPort)
            
# .............................................................................
# Public functions
# .............................................................................
   def insertModel(self, mdl):
      """
      @summary: Inserts a Model into the database, then sets the object id
                to the new database id for that model.
      @param mdl: a Model object filled with all attributes for calculating a model
      @note: lm_insertModel(varchar, int, varchar, int, int, int, int, double, int) 
             returns int
      """
      scen = mdl.getScenario()
      
      try:
         scencode = scen.code
      except:
         scencode = scen.title
         
      maskid = None
      if mdl.getMask() is not None:
         maskid = mdl.getMask().getId()
      elif len(scen.layers) > 0:
         maskid = scen.layers[0].getId()

      # returns new or pre-existing model id 
      # (same occurrenceset, scenario, algorithm and parameters)
      apdict = mdl.getAlgorithm().dumpParametersAsString()
      mdlid = self.executeInsertFunction('lm_insertSDMModel', 
                                         mdl.getUserId(),
                                         mdl.name,
                                         mdl.description,
                                         mdl.occurrenceSet.getId(), 
                                         scencode, scen.getId(),
                                         maskid,
                                         mdl.getAlgorithm().code,
                                         apdict,
                                         mdl.email,
                                         mdl.status, mdl.createTime, 
                                         mdl.priority)
      mdl.setId(mdlid)
      if mdlid < 0:
         raise LMError(currargs='Unable to insert model for occ: %d, scen %d' %
                       (mdl.occurrenceSet.getId(), mdl.getScenario().getId()),
                        location=self.getLocation(), logger=self.log)
      
# ...............................................
   def insertProjection(self, proj):   
      """
      @summary: Inserts a Projection into the database, then sets the object id
                to the new database id for that projection.
      @param proj: a Projection object to be inserted
      @raise LMError: on failure to insert Projection. 
      """
      maskid = None
      if proj.getMask() is not None:
         maskid = proj.getMask().getId()
      elif len(proj.getScenario().layers) > 0:
         maskid = proj.getScenario().layers[0].getId()
               
      pid = self.executeInsertFunction('lm_insertProjection', 
                                      proj.verify,
                                      proj.squid,
                                      proj.getModel().getId(), 
                                      proj.getScenario().getId(), 
                                      maskid,
                                      proj.createTime, 
                                      proj.status, 
                                      proj.priority, 
                                      proj.mapUnits, 
                                      proj.resolution, 
                                      proj.getCSVExtentString(), 
                                      proj.epsgcode, 
                                      proj.getWkt(), 
                                      proj.metadataUrl)
      if pid > 0:
         proj.setId(pid)
      else:
         raise LMError(currargs='Failed to initialize projection for model %s; scenario %s'
                       % (str(proj.getModel().getId()), str(proj.getScenario().getId())),
                       location=self.getLocation(),
                       logger=self.log)
   
# # ...............................................
#    def selectOccurrenceSetsToUpdate(self, count, expDate):
#       """
#       @summary Get occurrencesets which have been cleared, or were updated prior 
#                to the expiration date.
#       @param count: Number of occurrences objects to return
#       @return List of OccurrenceLayer objects
#       @note lm_getOccurrenceSetsToUpdate(int, double) 
#               returns setof occurrenceset 
#       """
#       occurrenceSetLst = []
#       
#       rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetsToUpdate', 
#                                                   count, ARCHIVE_USER, expDate)
#       for r in rows:
#          occ = self._createOccurrenceSet(r, idxs)
#          occurrenceSetLst.append(occ)
#       
#       return occurrenceSetLst  
   
# ...............................................
   def updateOccurrenceState(self, occset):
      success = self.executeModifyFunction('lm_updateOccurrenceStatus',
                                           occset.getId(),
                                           occset.getDLocation(),
                                           occset.queryCount,
                                           occset.getRawDLocation(),
                                           occset.status, 
                                           occset.statusModTime, 
                                           occset.getTouchTime(),
                                           occset.parametersModTime)
      return success
      
# ...............................................
   def updateOccurrenceSetMetadataAndStatus(self, occ):
      """
      @todo: Obsolete? This does not touch status, is it still useful?
      @summary Method to update an occurrenceSet object in the MAL database with 
               the metadataUrl, dlocation, queryCount, bbox, geom, status and modtime.
      @param occ the occurrences object to update
      @note: if point features are present, use actual count, otherwise use 
             queryCount, set when GBIF cache is queried and/or shapefile write. 
             This allows update without unnecessary re-population of points. 
      """
      bbstr = polywkt = pointswkt = None
      pointtotal = occ.queryCount
      
      if not occ.getFeatures():
         occ.readShapefile()
      if occ.getFeatures():
         pointtotal = occ.featureCount
         if occ.epsgcode == DEFAULT_EPSG:
            polywkt = occ.getConvexHullWkt()
            pointswkt = occ.getWkt()
      try:
         success = self.executeModifyFunction('lm_updateOccurrenceSet', 
                                              occ.getId(), 
                                              occ.metadataUrl, 
                                              occ.getDLocation(), 
                                              occ.getRawDLocation(), 
                                              pointtotal, 
                                              occ.epsgcode, 
                                              occ.getCSVExtentString(), 
                                              polywkt, 
                                              pointswkt,
                                              occ.status, 
                                              occ.statusModTime, 
                                              occ.getTouchTime(),
                                              occ.parametersModTime)
      except Exception, e:
         raise e
      return success


# ...............................................
   def findSuspiciousOccurrenceSets(self, count, offset, expDate):
      """
      @note: function returns setof int
      """
      rows, idxs = self.executeSelectManyFunction('lm_findOldUnmodeledOccurrencesets',
                                                  ARCHIVE_USER, expDate, count, offset)
      occsetids = [r[0] for r in rows]
      return occsetids      
   
# ...............................................
   def findUnfinishedJoblessOccurrenceSets(self, count):
      """
      @summary Return occurrences which are unfinished, but have no job.
      @param count: Number of occurrences objects to return
      @note: function returns setof occurrenceset
      """
      occs = []
      rows, idxs = self.executeSelectManyFunction('lm_findUnfinishedJoblessOccurrenceSets', 
                                       ARCHIVE_USER, count, JobStatus.COMPLETE)
      for r in rows:
         occ = self._createOccurrenceSet(r, idxs)
         occs.append(occ)
      return occs  

# ...............................................
   def selectUnModeledOccurrenceSets(self, usr, total, primaryEnv, minNumPoints,
                                     algcode, mdlscenarioId, errstat):
      """
      @summary Return occurrences which are current, but have not yet been 
             modeled with the given algorithm/scenario parameters.
      @param total: Number of occurrences objects to return
      @param primaryEnv: PrimaryEnvironment.TERRESTRIAL or MARINE
      @param minNumPoints: Minimum number of points required for OccurrenceLayer 
             to be suitable for modeling.
      @param algcode: Code of algorithm to be used for models created with returned 
             OccurrenceLayers.
      @param mdlscenarioId: Id of environmental scenario to be used for models
             created with returned occurrencesets.
      @return List of OccurrenceLayer objects
      @note: function returns setof occurrenceset
      """
      occurrenceSetLst = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetsMissingModels', 
                        usr, primaryEnv, total, minNumPoints, algcode, 
                        mdlscenarioId, errstat)
      for r in rows:
         occ = self._createOccurrenceSet(r, idxs)
         occurrenceSetLst.append(occ)
            
      return occurrenceSetLst  
   
# ...............................................
   def findUnprojectedArchiveModels(self, count, algcode, mdlscenarioId, 
                                    prjscenarioId):
      """
      @note: function returns setof lm_fullmodel 
      """
      mdls = []
      rows, idxs = self.executeSelectManyFunction('lm_getArchiveModelsNotProjectedOntoScen', 
                                                  ARCHIVE_USER, count, algcode, 
                                                  mdlscenarioId, prjscenarioId)
      for r in rows:
         mdls.append(self._createModel(r, idxs, doFillScenario=False))
         
      return mdls
   
# ...............................................
   def getModelsByStatus(self, count, status, doFillScenario=False):
      """
      @note: function returns setof lm_fullmodel
      """
      mdls = []
      rows, idxs = self.executeSelectManyFunction('lm_getModelsByStatus', count, status)
      for r in rows:
         model = self._createModel(r, idxs, doFillScenario=doFillScenario)
         mdls.append(model)
      return mdls
   
# ...............................................
   def getModelsNeedingJobs(self, count, userid, readyStat):
      mdls = []
      rows, idxs = self.executeSelectManyFunction('lm_getModelsNeedingJobs', 
                                                  count, userid, readyStat)
      for r in rows:
         mdls.append(self._createModel(r, idxs, doFillScenario=False))
      return mdls

# # ...............................................
#    def getModelsWithUpdatedOccurrencesets(self, count, lastdate):
#       """
#       @summary: Gets models calculated before the last GBIF update whose 
#                 occurrenceset was modified after the last update.
#       @param count: Limit results to this number
#       @param lastdate: Date of last GBIF cache update
#       @note: lm_getExpiredModels(int, int) returns setof lm_fullmodel
#       """
#       mdls = []
#       rows, idxs = self.executeSelectManyFunction('lm_getExpiredModels', count, lastdate)
#       for r in rows:
#          model = self._createModel(r, idxs, doFillScenario=False)
#          mdls.append(model)
#       return mdls
#    
## ...............................................
#   def reprioritizeExperiment(self, modelId, priority):
#      """
#      @summary Method to update an experiment's priority
#      @param modelId: The id of the model to update
#      @param priority: The new priority for the model and its projections
#      @return: True for success, False for failure
#      @note: lm_reprioritizeExperiment(int, int) returns 0/-1
#      """
#      success = self.executeModifyFunction('lm_reprioritizeExperiment', modelId, 
#                                           priority)
#      return success

# ...............................................
   def updateModel(self, mdl, jobId, computeId, errorStat=JobStatus.GENERAL_ERROR):
      """
      @summary Method to store a model's ruleset on the filesystem and its 
               metadata in the MAL.
      @param modelId: The id of the model to update
      @param status: The new run status of the model
      @param priority: The new priority for the model
      @param ruleset: The new ruleset for the model
      @param qc: The new quality control for the model
      @param jobid: The unique job id returned by the modeling service
      @param errorStat: minimum (General) error status.  If model is set to
                        any error, related projections will also be set to 
                        this General error.
      @return: True for success, False for failure
      @note: lm_reprioritizeExperiment(int, int) returns 0/-1
      """
#       success = self.executeModifyFunction('lm_updateModel', modelId, 
#                                            status, mx.DateTime.utc().mjd, 
#                                            priority, ruleset, qc, jobId, errorStat)
      success = self.executeModifyFunction('lm_updateModel', mdl.getId(), 
                                           mdl.status, mx.DateTime.utc().mjd, 
                                           mdl.priority, 
                                           mdl.getDLocation(), 
                                           mdl.qualityControl, jobId, 
                                           computeId, errorStat,
                                           JobStatus.MODEL_ERROR)
      return success

# ...............................................
   def rollbackModel(self, mdl, errorStat):
      """
      @summary Method to rollback a model's status.
      @param mdl: The model to update
      @param errorStat: minimum (General) error status.  If model is set to
                        any error, related projections will also be set to 
                        this General error.
      @return: True for success, False for failure
      @todo: Remove jobid - only for debugging
      """
      success = self.updateModel(mdl, None, None, errorStat)
      return success
   
## ...............................................
#   def updateObjectByCommand(self, cmd):
#      """
#      @summary Updates a job in the database with current object members
#      @param job: The job to update
#      """
#      row, idxs = self.executeCommandFunction(cmd)
#      if row[0] == 0:
#         return True
#      else:
#         return False
  
# ...............................................
   # TODO: No need to store jobid - update below
   def updateProjection(self, prj, jobId, computeId):
      """
      @summary Method to store a model's projection metadata 
              (including jobId and computeResourceId for debugging) in the MAL.
      @param prj: The projection to update
      @param jobId: The unique LMJob table ID used when this projection has been 
                    initialized for computation
      @param computeId: The unique ComputeResource ID used to calculate this 
                    projection
      @return: True for success, False for failure
      @note: lm_updateProjectionInfo(int, int) returns 0/-1 or raises 
             Exception if an invalid EPSG code is sent
      @todo: update bbox and geom with actual extent of projection, bbox in 
             decimal degrees
      """
      currTime = mx.DateTime.utc().mjd
      success = self.executeModifyFunction('lm_updateProjectionInfo', 
                                           prj.getId(), prj.status, currTime, 
                                           prj.priority, prj.getDLocation(), prj.gdalType, 
                                           prj.getCSVExtentString(), prj.epsgcode, 
                                           prj.getWkt(), jobId, computeId)
      return success

# ...............................................
   def rollbackProjection(self, prj):
      """
      @summary Method to rollback a model's projection metadata 
              (clearing jobId and computeResourceId) in the MAL.
      @param prj: The projection to update
      @return: True for success, False for failure
      """
      success = self.updateProjection(prj, None, None)
      return success
   
# ...............................................
   def deleteJob(self, job):
      """
      @summary Deletes a _Job record
      @param job: job (or id of job) to delete 
      @return True on success; False on failure
      """
      jsuccess = False
      if isinstance(job, _Job):
         jsuccess = self.executeModifyFunction('lm_deleteJob', job.getId())
      else:
         # Just in case we only have JobId
         from types import IntType
         if isinstance(job, IntType):
            jsuccess = self.executeModifyFunction('lm_deleteJob', job)
      return jsuccess
      

# ...............................................
   def updateSDMJob(self, job, errorStat, incrementRetry):
      """
      @note: Operates on a complete _Job object. 
      @summary Updates or deletes an _Job record, then updates a 
               SDM Model or Projection with the current stage, 
               status and modification times, then moves all dependent jobs
               (Project or Notify) 
      @param job: job to update
      @param errorStat: minimum error status; above this, dependent jobs fail
      @param incrementRetry: If this job has just been pulled by a 
                             ComputeResource, increment the number of tries.  
      @return True on success; False on failure
      """
      # Just in case we only have JobId
      from types import IntType
      if isinstance(job, IntType):
         job = self.getJob(job)
      jsuccess = osuccess = False
      # Update or delete job
      if job.status == JobStatus.COMPLETE or job.status >= errorStat:
         jsuccess = self.deleteJob(job)
      else:
         jsuccess = self.executeModifyFunction('lm_updateJob', job.getId(), 
                                               job.computeResourceId, 
                                               job.status, 
                                               job.statusModTime,
                                               incrementRetry)
      # If job update was unsuccessful, job could be missing
      if jsuccess:
         # Update object 
         if job.outputObjType == ReferenceType.SDMProjection:
            osuccess = self.updateProjection(job.outputObj, job.getId(), 
                                             job.computeResourceId)
         elif job.outputObjType == ReferenceType.SDMModel:
            osuccess = self.updateModel(job.outputObj, job.getId(), 
                                        job.computeResourceId, errorStat)
         elif job.outputObjType == ReferenceType.OccurrenceSet:
            if job.status == JobStatus.COMPLETE:
#                job.outputObj.setRawDLocation(None, job.statusModTime)
               job.outputObj.updateStatus(job.status, modTime=job.statusModTime, 
                                          queryCount=job.outputObj.count)
            osuccess = self.updateOccurrenceSetMetadataAndStatus(job.outputObj)
         
      return osuccess, jsuccess
 
# ...............................................
   def moveDependentJobs(self, job, completeStat, errorStat, notReadyStat, 
                         readyStat, currtime):
      success = True
      if job.status == completeStat or job.status >= errorStat:            
         currtime = mx.DateTime.gmt().mjd
         depErrorStat = JobStatus.MODEL_ERROR        
         
         if job.outputObjType == ReferenceType.OccurrenceSet:
            success = self.executeModifyFunction('lm_updateOccurrenceDependentJobs',
                                                 job.outputObj.getId(), 
                                                 completeStat, errorStat,
                                                 notReadyStat, readyStat, 
                                                 currtime)
         elif job.outputObjType == ReferenceType.SDMModel:
            success = self.executeModifyFunction('lm_updateModelDependentJobs',
                                                 job.outputObj.getId(), 
                                                 completeStat, errorStat,
                                                 notReadyStat, readyStat, 
                                                 depErrorStat, currtime)
         elif job.outputObjType == ReferenceType.SDMProjection:
            success = self.executeModifyFunction('lm_updateExpDependentJobs',
                                                 job.outputObj.getId(), 
                                                 completeStat, errorStat,
                                                 notReadyStat, readyStat,
                                                 currtime)
      return success

# ...............................................
   def moveAllDependentJobs(self, completeStat, errorStat, notReadyStat, 
                            readyStat, currtime):      
      depErrorStat = JobStatus.MODEL_ERROR        
      count0 = self.executeModifyReturnValue('lm_updateAllOccsetDependentJobs',
                                            completeStat, errorStat,
                                            notReadyStat, readyStat, currtime)      
      count1 = self.executeModifyReturnValue('lm_updateAllModelDependentJobs',
                                            completeStat, errorStat,
                                            notReadyStat, readyStat, 
                                            depErrorStat, currtime)
      count2 = self.executeModifyReturnValue('lm_updateAllExpDependentJobs',
                                             completeStat, notReadyStat, 
                                             readyStat, currtime)
      return count0+count1+count2
   
# ...............................................
   def insertAlgorithm(self, alg):
      """
      @summary Inserts an Algorithm into the database
      @param alg: The algorithm to add
      @note: lm_insertAlgorithm(varchar, varchar, double) returns an int
      """
      alg.modTime = mx.DateTime.utc().mjd
      algid = self.executeInsertFunction('lm_insertAlgorithm', alg.code, alg.name, 
                                      alg.modTime)
      return algid
   
# ...............................................
   def findProblemObjects(self, cutofftime, startStat, endStat, count, ignoreUser):
      """
      @summary: Method to notify developers of a problem in the pipeline or a 
                user experiment.
      """
      modelids = []
      models = []
      projs = []
      rows, idxs = self.executeSelectManyFunction('lm_findProblemModels',   
                                                  cutofftime,
                                                  startStat, endStat,
                                                  ignoreUser, count)
      if rows:
         for r in rows:
            mdl = self._createModel(r, idxs, doFillScenario=False)
            modelids.append(mdl.getId())

      rows, idxs = self.executeSelectManyFunction('lm_findProblemProjections', 
                                                  cutofftime,
                                                  startStat, endStat,
                                                  ignoreUser, count)
      if rows:
         for r in rows:
            proj = self._createProjection(r, idxs, doFillScenario=False)
            
            if proj.getModel().getId() not in modelids:
               projs.append(proj)
               
      return models, projs

# ...............................................
   def findExistingJobs(self, occId=None, mdlId=None, prjId=None, status=None):
      """
      @note: Returns records ordered by highest to lowest job status, 
             oldest to newest modtime 
      """
      if occId is not None:
         rows, idxs = self.executeSelectManyFunction('lm_findOccurrenceJobs', 
                                                     occId, status)
      elif mdlId is not None:
         rows, idxs = self.executeSelectManyFunction('lm_findModelJobs', mdlId, status)
      elif prjId is not None:
         rows, idxs = self.executeSelectManyFunction('lm_findProjectionJobs', 
                                                     prjId, status)
      jobs = []
      for r in rows:
         job = self._createSDMJobNew(r, idxs) 
         jobs.append(job)
      return jobs
   
# ...............................................
   def getJob(self, jobId):
      job = None
      try:
         row, idxs = self.executeSelectOneFunction('lm_getJobType', jobId)
      except:
         raise LMError('Job %s not found' % str(jobId))
      if row is None:
         raise LMError('Job %s not found' % str(jobId))
      objtype = row[0]
      if objtype == ReferenceType.SDMModel:
         fnName = 'lm_getModelJob'
      elif objtype == ReferenceType.SDMProjection:
         fnName = 'lm_getProjectionJob'
      elif objtype == ReferenceType.OccurrenceSet:
         fnName = 'lm_getOccurrenceJob'
      else:
         raise LMError('Unknown Job object type %s' % str(objtype))   
      row, idxs = self.executeSelectOneFunction(fnName, jobId)
      job = self._createSDMJobNew(row, idxs)
      return job

# ...............................................
   def getJobOfType(self, obj, refType=None):
      job = None
      if refType is not None:
         objid = obj
         if refType == ReferenceType.OccurrenceSet:
            fnName = 'lm_getOccurrenceJobForId'
         elif refType == ReferenceType.SDMModel:
            fnName = 'lm_getModelJobForId'
         elif refType == ReferenceType.SDMProjection:
            fnName = 'lm_getProjectionJobForId'
         else:
            raise LMError('Unknown ReferenceType {}'.format(refType))  
          
      else: 
         objid = obj.getId()
         if isinstance(obj, SDMModel): 
            fnName = 'lm_getModelJobForId'
         elif isinstance(obj, SDMProjection):
            fnName = 'lm_getProjectionJobForId'
         elif isinstance(obj, OccurrenceLayer):
            fnName = 'lm_getOccurrenceJobForId'
         else:
            raise LMError('Unknown Job object type %s' % str(type(obj))) 
      
      row, idxs = self.executeSelectOneFunction(fnName, objid)
      job = self._createSDMJobNew(row, idxs)
      return job
   
# # ...............................................
#    def _getPrjDependents(self, prj):
#       prjDeps = []
#       if isinstance(prj, _Job):
#          prjid = prj.dataObj.getId()
#       else:
#          prjid = prj.getId()
#          
#       rows, idxs = self.executeSelectManyFunction('lm_getIntersectJobsForProjection', 
#                                                     prjid)
#       for r in rows:
#          job = self._createSDMJobNew(r, idxs)
#          prjDeps.append(job)
#       
#       return prjDeps

# ...............................................
   def _getMdlDependents(self, modelid, startStat, endStat, currtime, crid):
      """
      @todo: Add PAV (Intersect) jobs to Projection dependents
      """
      mdlDeps = []
      rows, idxs = self.executeSelectManyFunction('lm_getProjectionJobsForModel', 
                                    modelid, startStat, endStat, currtime, crid)
      for r in rows:
         job = self._createSDMJobNew(r, idxs)
         mdlDeps.append(job)
      
      return mdlDeps
        
# ...............................................
   def _getCompute(self, computeIP, msk):
      try:
         row, idxs = self.executeSelectOneFunction('lm_getComputeRec', 
                                                     computeIP, msk)
         cr = self._createComputeResource(row, idxs)
      except Exception, e:
         return None
      else:
         return cr
            
# ...............................................
   def _getOccDependents(self, occid, startStat, endStat, currtime, crid):
      occDeps = []
      rows, midxs = self.executeSelectManyFunction('lm_pullModelJobsForOcc', 
                           occid, startStat, endStat, currtime, crid)
      for mr in rows:
         mdlDeps = []
         mdljob = self._createSDMJobNew(mr, midxs)
         mdlDependents = self._getMdlDependents(mdljob.dataObj.getId())
            
         occDeps.append((mdljob, mdlDeps))
            
      rows, midxs = self.executeSelectManyFunction('lm_getCompletedModelsForOcc', occid)
      for mr in rows:
         mdlDeps = []
         mdl = self._createModel(mr, midxs, doFillScenario=False)
         mdlDependents = self._getMdlDependents(mdl.getId(), startStat, endStat, 
                                                currtime, crid)

         if len(mdlDependents) > 0:
            occDeps.append((mdl, mdlDeps))
            
      return occDeps

# ...............................................
   def pullTopDownJobChain(self, occ, startStat, endStat, currtime, computeIP, msk=None):
      """
      @return: a nested tuple of dependent jobs and objects as:
         (occObj, [(mdlObj, [(prjObj, [(pavJob, None)]), (prjJob, None)]), 
                   (mdlJob, [(prjJob, [(pavJob, None), (pavJob, None)])]) ])
      """
      top = None
      cr = self._getCompute(computeIP, msk)
      if cr:
         if occ.status == JobStatus.INITIALIZE:
            
            row, idxs = self.executeSelectOneFunction('lm_pullOccurrenceJobForId', 
                     occ.getId(), startStat, endStat, currtime, cr.getId())
            top = self._createSDMJobNew(row, idxs)
         elif occ.status == JobStatus.COMPLETE:
            top = occ   
            
         occDeps = self._getOccDependents(occ.getId(), startStat, endStat, 
                                          currtime, cr.getId())
      else:
         raise LMError('Compute Resource {} / {} is not registered'.format(computeIP, msk))
      return (top, occDeps)
      
# ...............................................
   def pullJobs(self, count, processType, startStat, endStat, usr, 
                inputType, computeIP):
      jobs = []
      if count == 0: 
         return jobs
      currtime = mx.DateTime.gmt().mjd
      if processType == ProcessType.ATT_MODEL or processType == ProcessType.OM_MODEL:
         rows, idxs = self.executeSelectAndModifyManyFunction('lm_pullModelJobs',
                                                              count,
                                                              processType,
                                                              startStat,
                                                              endStat,
                                                              usr, inputType, 
                                                              currtime,
                                                              computeIP)                 
      elif processType == ProcessType.ATT_PROJECT or processType == ProcessType.OM_PROJECT:
         rows, idxs = self.executeSelectAndModifyManyFunction('lm_pullProjectionJobs',
                                                              count,
                                                              processType,
                                                              startStat,
                                                              endStat,
                                                              usr, inputType, 
                                                              currtime,
                                                              computeIP)
      elif processType in [ProcessType.BISON_TAXA_OCCURRENCE, 
                           ProcessType.GBIF_TAXA_OCCURRENCE, 
                           ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                           ProcessType.USER_TAXA_OCCURRENCE]:
         rows, idxs = self.executeSelectAndModifyManyFunction('lm_pullOccurrenceJobs',
                                                              count,
                                                              processType,
                                                              startStat,
                                                              endStat,
                                                              usr, inputType, 
                                                              currtime,
                                                              computeIP)                     
      elif processType == ProcessType.SMTP:
         rows, idxs = self.executeSelectAndModifyManyFunction('lm_pullMessageJobs',
                                                              count,
                                                              processType,
                                                              startStat,
                                                              endStat,
                                                              usr, 
                                                              currtime,
                                                              computeIP)
      for r in rows:
         try:
            j = self._createSDMJobNew(r, idxs)
         except Exception, e:
            self.log.error('Failed to create SDMJob (%s)' % str(e))
            self._updateErrorJob(r, idxs, JobStatus.LM_JOB_NOT_READY)
         else:
            if j is not None:
               jobs.append(j)                  

      return jobs

# ...............................................
   def _updateErrorJob(self, row, idxs, errstat):
      jobid = self._getColumnValue(row, idxs, ['lmjobid'])
      success = self.updateJobAndObjectStatus(jobid, None, errstat, 0, True)

# ...............................................
   def rollbackLifelessJobs(self, giveupTime, pulledStat, initStat, completeStat):
      """
      @note: this rolls back objects that are in-process, not completed, so 
             completed model/projection files should not need to be deleted 
      """
      currtime = mx.DateTime.gmt().mjd
      count = self.executeModifyReturnValue('lm_resetLifelessJobs', giveupTime, 
                                            currtime, pulledStat, initStat, 
                                            completeStat)
      return count
   
# # ...............................................
#    def moveModelJobs(self, count, startStat, endStat, algorithmCodes, userids):
#       """
#       @summary Gets ModelJobs from the MAL with status = startStat, updates 
#                them in the database to endStat, and returns them
#       @param count: The number of model jobs to retrieve.  If count is None, 
#                     move and retrieve all available.
#       @param startstat: beginning status of models to be moved
#       @param endstat: new status of moved models
#       @param algorithmCodes: List of acceptable algorithmCodes for jobs to move;
#                              this is a proxy for the input data required on
#                              the compute node.
#       @param userids: Optional filter of a list of userids of jobs to move
#       @return A list of ModelJob objects, sorted by priority and age
#       @note: lm_moveSomeModelJobs
#                   1) changes the status of models from startstat to endstat
#                   2) returns (changed) setof lm_fullmodel
#       """
#       modtime = mx.DateTime.gmt().mjd
#       currCount = count
#       modelJobs = []
#       if not userids:
#          userids = [ARCHIVE_USER]
#       if currCount > 0:
#          for userid in userids:
#             for algcode in algorithmCodes:
#                rows, idxs = self.executeSelectAndModifyManyFunction('lm_moveSomeModelJobs', 
#                                                                  currCount, 
#                                                                  startStat, 
#                                                                  endStat, 
#                                                                  modtime,
#                                                                  algcode, 
#                                                                  userid)
#                if rows:
#                   for r in rows:
#                      jobid = r[idxs['modelid']]
#                      m = self._createModel(r, idxs)
#                      if m is not None:
#                         modelJobs.append(OmModelJob(m, pid=SDM_ID, jid=jobid))
#                currCount = count - len(modelJobs)
#                if currCount == 0:
#                   break
#                
#       return modelJobs

# ...............................................
   def getOccurrenceSetNamesWithProj(self, qryNamestring, maxCount):
      """
      @param qryNamestring: a partial string to match
      @param maxCount: number of results to return
      @return: a sorted (ascending) list of occurrenceset names 
      @note: Used in website queries
      @note: lm_getOccurrenceSetNamesWithProj(varchar, int, int) returns setof varchar 
      """
      names = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetNamesWithProj', 
                                                  qryNamestring, maxCount, JobStatus.COMPLETE)
      if rows:
         for r in rows:
            names.append(r[0])
      return names
   
# ...............................................
   def getOccurrenceSetsWithPoints(self, qryNamestring, minPoints, maxCount):
      """
      @param qryNamestring: a partial string to match
      @param minPoints: minimum number of occurrencepoints required to be selected
      @param maxCount: maximum number of occurrencesets to return 
      @note: Used in website queries
      @note: lm_getOccurrenceSetNamesWithPoints(varchar, int, int) returns setof varchar
      """
      names = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetNamesWithPoints',
                                                  qryNamestring, minPoints, maxCount)
      if rows:
         for r in rows:
            names.append(r[0])
      return names
   
# ...............................................
   def getOccurrenceSetsForScientificName(self, sciNameId, userid):
      """
      @param sciNameId: ScientificName ID for searching 
      @param userid: Userid for filtering results 
      @return: list of occurrenceSet objects
      """
      occSets = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetsForScinameUser', 
                                                  sciNameId, userid)
      if rows:
         for row in rows:
            occSets.append(self._createOccurrenceSet(row, idxs))
      return occSets
   
# ...............................................
   def getOccurrenceSetsForName(self, taxName, userid, defaultUserid):
      """
      @param taxName: Occurrenceset displayname for searching 
      @param userid: Userid for filtering results 
      @return: list of occurrenceSet objects
      """
      occSets = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetsForName', 
                                                  taxName, userid, defaultUserid)
      if rows:
         for row in rows:
            occSets.append(self._createOccurrenceSet(row, idxs))
      return occSets
   
   
# ...............................................
   def getOccurrenceSetsForGenus(self, genusname):
      """
      @param genusname: OccurrenceLayer displayname for searching 
      @return: list of OccurrenceLayer objects
      @note: lm_getOccurrenceSetsForGenus(varchar, varchar) 
             returns setof occurrenceset
      """
      occSets = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetsForGenus', 
                                                  genusname, ARCHIVE_USER)
      if rows:
         for row in rows:
            occSets.append(self._createOccurrenceSet(row, idxs))
      return occSets
   
# ...............................................
   def getOccurrenceSetsForUser(self, userid, epsgcode):
      """
      @param userid: OccurrenceLayer owner userid 
      @param epsgcode: Filter occurrencesets by this EPSG code
      @return: list of occurrenceSet objects
      @note: lm_getOccurrenceSetsForUser(varchar, varchar) 
             returns setof occurrenceset
      """
      occSets = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetsForUser', 
                                                  userid, epsgcode)
      if rows:
         for row in rows:
            occSets.append(self._createOccurrenceSet(row, idxs))
      return occSets

# ...............................................
#    def getGBIFOccurrenceSetsForName(self, taxName, usrid=ARCHIVE_USER):
   def getOccurrenceSetsForNameAndUser(self, taxName, usrid):
      """
      @summary: Return OccurrenceLayers for the given name
      @param taxName: taxonomic name to match exactly
      @return: list of OccurrenceLayer objects owned by usrid
      """
      occSets = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetsForNameAndUser', 
                                                  taxName, usrid)
      if rows:
         for row in rows:
            occSets.append(self._createOccurrenceSet(row, idxs))
      return occSets

# ...............................................
#    def getGBIFOccurrenceSetsLikeName(self, taxName, usrid=ARCHIVE_USER):
   def getOccurrenceSetsLikeNameAndUser(self, taxName, usrid):
      """
      @summary: Return OccurrenceLayers starting with the given name
      @param taxName: taxonomic name to match 
      @return: list of OccurrenceLayer objects owned by usrid
      """
      occSets = []
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceSetsLikeNameAndUser', 
                                                  taxName, usrid)
      if rows:
         for row in rows:
            occSets.append(self._createOccurrenceSet(row, idxs))
      return occSets
   
   # ...............................................
   def _createRawModel(self, mdl):
      rawMdl = None
      if os.path.exists(mdl.ruleset):
         try:
            f = open(mdl.ruleset)
            rawMdl = ''.join(f.readlines())
            f.close()
         except:
            pass
      return rawMdl
            
# ...............................................
   def insertJob(self, job):
      """
      @param job: SDMJob
      """
      doNotify = (job.email is not None)
      # If job with same referenceType, referenceId, and stage exists, 
      # this function will delete the existing one first
      jobid  = self.executeInsertFunction('lm_insertJob', job.jobFamily,
                                          job.processType,
                                          job.outputObjType, 
                                          job.outputObj.getId(),
                                          # Compute resource not yet chosen
                                          None,
                                          doNotify,
                                          job.priority,
                                          job.status, 
                                          job.stage, 
                                          job.createTime,
                                          JobStatus.COMPLETE,
                                          0)
      if jobid > 0:
         job.setId(jobid)
      else:
         raise LMError(currargs='Unable to insertJob')
      return job
               
# ...............................................
   def getProgress(self, reftype, starttime=None, endtime=None, usrid=None):
      '''
      @summary: returns a dictionary of {status: count}
      @note: uses the db view lm_progress
      '''
      statusDict = {}
      rows, idxs = self.executeSelectManyFunction('lm_measureProgress', reftype,
                                                 starttime, endtime, usrid)
      for r in rows:
         statusDict[r[0]] = r[1]
      return statusDict
   
# ...............................................
   def getEmail(self, usrid):
      row, idxs = self.executeSelectOneFunction('lm_getEmail', usrid)
      return row[0]
   
# ...............................................
   def updateJobAndObjectStatus(self, jobid, computeIP, status, progress,
                                incrementRetry):
      """
      @summary Updates the status on a job and its corresponding object
      @param jobid: The job record to update
      @param computeName: The name of the ComputeResource computing the job
      @param status: The JobStatus
      @param progress: Percent complete
      @note: This updates compute info: compute resource, progress, retryCount,  
                                        status, modtime, lastheartbeat.
      """
      success = self.executeModifyFunction('lm_updateJobAndObjLite', jobid, 
                                           computeIP, status, progress, 
                                           incrementRetry, mx.DateTime.gmt().mjd)
      return success

# ...............................................
   def getProjectionById(self, projId):
      """
      @summary Gets a projection based on the id
      @param projId: The id of the projection to be returned
      @return A Projection object with the specified id
      @exception LMError: Thrown when the number of projections found != 1
      @note: lm_getProjection(int) returns a lm_fullProjection or
             an Exception if exactly one record is not found
      """
      row, idxs = self.executeSelectOneFunction('lm_getProjection', projId)
      proj = self._createProjection(row, idxs)
      return proj
   
# ...............................................
   def getOccurrenceStats(self):
      """
      @summary: Get occurrenceSet information for occurrenceSets with > 0 points.
      @return: list of tuples in the form: 
               (occurrenceSetId, displayname, totalpoints, totalmodels)
      @note: Used just for lucene index creation, so return info in tuples.
      @note: lm_getOccurrenceNumbers(varchar, int) returns setof lm_occStats
      """
      rows, idxs = self.executeSelectManyFunction('lm_getOccurrenceWOModelNumbers',
                                                  ARCHIVE_USER)
      rows2, idxs = self.executeSelectManyFunction('lm_getModelNumbers',
                                                  ARCHIVE_USER, 
                                                  JobStatus.COMPLETE)
      rows.extend(rows2)
      return rows
   
# ...............................................
   def getRandomModel(self, userid, status):
      """
      @summary: Return a random model for a particular status and user
      @param userid: Userid for filtering models
      @param status: Status for filtering models
      @return: list of tuples in the form: 
               (occurrenceSetId, displayname, totalpoints, totalmodels)
      @note: Used just for lucene index creation, so return info in tuples.
      @note: lm_getRandomModel(varchar, int) returns a lm_fullmodel
      """
      row, idxs = self.executeSelectOneFunction("lm_getRandomModel", userid, 
                                                status)
      model = self._createModel(row, idxs, doFillScenario=False)
      return model

# ...............................................
   def getProjectionsForModel(self, mdlId, status):
      """
      @summary Gets the projections associated with a name (any grouping
               of organisms)
      @param mdlId: The modelId of the model to get related projections for
      @param status: status of projections to retrieve.  If status is 
               None, retrieve all.
      @return A list of Projection objects that use the specified model
      @note: lm_getProjectionsForModel(int, int) returns setof lm_fullprojection
      """
      rows, idxs = self.executeSelectManyFunction('lm_getProjectionsForModel', 
                                                  mdlId, status)
      projs = []
      if rows:
         for r in rows:
            p = self._createProjection(r, idxs)
            projs.append(p)
      return projs

# ...............................................
   def getProjectionsNeedingJobs(self, count, userid, readyStat, completeStat):
      projs = []
      rows, idxs = self.executeSelectManyFunction('lm_getProjectionsNeedingJobs', 
                                                  count, userid, 
                                                  readyStat, completeStat)
      for r in rows:
         projs.append(self._createProjection(r, idxs, doFillScenario=False))
      return projs


# ...............................................
   def getProjectionsForOcc(self, occId):
      """
      @summary Gets the projections associated with a name (any grouping
               of organisms)
      @param occId: The occurrenceSetId of the occurrenceSet to get 
                    projections for
      @return A list of Projection objects that use the specified occurrenceSet
      @note: lm_getProjectionsByOccurrenceSet(int, int) 
             returns setof lm_fullprojection  
      """
      # The MAL initializes the Scenario with its code and url only
      # Environmental layer info is filled in by the SDL
      # lm_getModelByName returns a mini-model
      rows, idxs = self.executeSelectManyFunction('lm_getProjectionsByOccurrenceSet', 
                                                  occId, JobStatus.COMPLETE)
      projs = []
      if rows:
         for r in rows:
            p = self._createProjection(r, idxs)
            projs.append(p)
      return projs

# .............................................................................
   def getModelsForOcc(self, occsetid, userid, status):
      """
      @summary Gets the models associated with a name (any grouping
               of organisms)
      @param occsetid: The id of the occurrenceSet to get models for
      @param userid: Filter by userid. Nonsense, since model/occsetid has only 
                     one user. TODO: Remove
      @param status: Filter by status. None to get all. 
      @return A list of Model objects that use the specified OccurrenceSet
      @note: lm_getModelsByOccurrenceLayerUserAndStatus(int, int, int) 
             returns setof lm_fullmodel (contains OccurrenceLayer)
      """
      rows, idxs = self.executeSelectManyFunction(
          'lm_getModelsByOccurrenceSetUserAndStatus', occsetid, userid, status)
      models = []
      for r in rows:
         models.append(self._createModel(r, idxs))
      return models

# .............................................................................
   def getModelsForUser(self, userid, status):
      """
      @summary Gets the models associated with a name (any grouping
               of organisms)
      @param userid: The userid to get models for
      @param status: Filter by status. None to get all. 
      @return A list of Model objects owned by the specified LMUser
      @note: lm_getModelsByUserAndStatus(int, int) 
             returns setof lm_fullmodel (contains OccurrenceLayer)
      """
      rows, idxs = self.executeSelectManyFunction('lm_getModelsByUserAndStatus', 
                                                  userid, status)
      models = []
      for r in rows:
         models.append(self._createModel(r, idxs))
      return models

# .............................................................................
   def getModelById(self, modelId, doFillScenario=True):
      """
      @summary Gets a model based on the id
      @param modelId: The model id of the model to be returned
      @param doFillScenario: true to fill in scenario layers on the model
      @return: Model object with the specified id
      @exception LMError: Thrown when the number of models found != 1
      @note: lm_getModel(int) returns an lm_fullmodel
      """
      row, idxs = self.executeSelectOneFunction('lm_getModel', modelId)
      model = self._createModel(row, idxs, doFillScenario=doFillScenario)
      return model
     
# .............................................................................
   def getModelByProjection(self, projId, doFillScenario=True):
      """
      @summary Gets a model associated with a projection id
      @param projId: The id of a projection 
      @param doFillScenario: true to fill in scenario layers on the model
      @return A Model object for the given projection id
      @exception LMError: Thrown when the number of models found != 1
      @note: lm_getModelForProjection(int) returns an lm_fullmodel
      """
      # The MAL initializes the Scenario with its code and url only
      # Environmental layer info is filled in by the SDL
      row, idxs = self.executeSelectOneFunction('lm_getModelForProjection',projId)
      model = self._createModel(row, idxs, doFillScenario=doFillScenario)
      return model

# .............................................................................
   def deleteModel(self, modelId):
      """
      @summary Deletes a model from the database as well as any projections 
               associated with it
      @param modelId: The id of the Model to be deleted
      @return: True on success, False on failure
      @note: lm_deleteModel(int) returns 0/-1
      """
      success = self.executeModifyFunction('lm_deleteModel', modelId)
      return success
   
# .............................................................................
   def deleteProjection(self, prjId):
      """
      @summary Deletes a projection from the database 
      @param prjId: The id of the Projection to be deleted
      @return: True on success, False on failure
      @note: lm_deleteProjection(int) returns 0/-1
      """
      success = self.executeModifyFunction('lm_deleteProjection', prjId)
      return success
   
# .............................................................................
   def deleteOccurrenceSet(self, occId):
      """
      @summary Deletes an occurrenceset from the database 
      @param occId: The id of the OccurrenceLayer to be deleted
      @return: True on success, False on failure
      @note: lm_deleteOccurrenceSet(int) returns 0/-1
      """
      success = self.executeModifyFunction('lm_deleteOccurrenceSet', occId)
      return success
   
# .............................................................................
   def deleteOccAndDependentObjects(self, occId, usr):
      """
      @summary Deletes an occurrenceset and all objects depending on it 
               (jobs, projections, models) from the database 
      @param occId: The id of the OccurrenceLayer to be deleted
      @param usr: The userid of the OccurrenceLayer and objects to be deleted
      @return: True on success, False on failure
      @note: lm_deleteOccAndDependentObjects(int, varchar) returns 0/-1
      """
      success = self.executeModifyFunction('lm_deleteOccAndDependentObjects', 
                                           occId, usr)
      return success

# .............................................................................
   def setSpeciesEnvironment(self, occId, primaryEnvCode):
      success = self.executeModifyFunction('lm_setOccurrenceSetPrimaryEnv', 
                                           occId, primaryEnvCode)
      return success
   

# .............................................................................
   def resetObjectsJobsFromStatus(self, reftype, oldstat, newstat, usr):
      """
      @summary Reset objects and any dependent jobs from one status to another.
      @param reftype: LmServer.common.lmconstants.ReferenceType
      @param oldstat: target status to change
      @param newstat: desired status
      @param usr: optional filter by userId
      @note: lm_resetObjectsJobsAtStatus(int, int, int, double, varchar) 
      @return: number of modified jobs
      """
      cnt = self.executeModifyReturnValue('lm_resetObjectsJobsAtStatus', 
                        reftype, oldstat, newstat, mx.DateTime.utc().mjd, usr)
      return cnt

# .............................................................................
   def resetObjectAndJobs(self, reftype, objid, newstat):
      """
      @summary Reset object and any related jobs to new status.
      @param reftype: LmServer.common.lmconstants.ReferenceType
      @param newstat: desired status
      @note: lm_resetObject(int, int, int, double, varchar) changes status 
             and returns the number of modified jobs
      """
      cnt = self.executeModifyReturnValue('lm_resetObjectAndJob', reftype, objid,
                                          newstat, mx.DateTime.utc().mjd)
      return cnt

# .............................................................................
   def resetSDMChain(self, top, oldstat, startstat, depstat, usr):
      '''
      @summary: Reset a chain of jobs, starting with a 'top' level dependency, 
                then all objects/jobs dependent on completion of that object 
      @note: If an object does not have a job, it will not create one
      @param oldstat: target status to change
      @param startstat: desired status for top level job
      @param depstat: desired status for dependent jobs
      @param top: LmServer.common.lmconstants.ReferenceType, could start at 
                  OccurrenceSet, SDMModel, or SDMProjection.  
      @param usr: optional filter by userId
      @todo: put dependencies in object classes
      '''
      objids = []
      rows, idxs = self.executeSelectManyFunction('lm_getJobObjIds', top, 
                                                  oldstat, usr)
      for r in rows:
         objids.append(r['objid'])
      
      # Reset top level   
      total = self.resetSDMJobs(top, oldstat, startstat, usr)
      
      # Reset dependent objects
      depObjs = {ReferenceType.SDMModel: [], ReferenceType.SDMProjection: []}
      if top == ReferenceType.OccurrenceSet:
         for occid in objids:
            mdldeps = self.getModelsForOcc(occid, None, None)
            depObjs[ReferenceType.SDMModel].extend([mdl.getId() for mdl in mdldeps]) 
            
            prjdeps = self.getProjectionsForOcc(occid)
            depObjs[ReferenceType.SDMProjection].extend([prj.getId() for prj in prjdeps])
            
      elif top == ReferenceType.SDMModel:
         for mdlid in objids:
            prjdeps = self.getProjectionsForModel(mdlid, None)
            depObjs[ReferenceType.SDMProjection].extend([prj.getId() for prj in prjdeps])
      
      for reftype, refids in depObjs.iteritems():
         for oid in refids:
            count = self.resetObjectAndJob(reftype, oid, oldstat, depstat)
            total += count
            
      return count
            
# .............................................................................
   def rollbackSDMJobs(self, queuedStatus):
      """
      @summary Reset models (JobStatus.xxx_QUEUED) that were not 
      completed for whatever reason.  Set models to previously-completed-stage.
      @param queuedStatus: status within a Pipeline.Worker queue
      @note: lm_resetSDMJobsToReadyAndWaiting(double, int, int) changes status and returns int
      """
      cnt = self.executeModifyReturnValue('lm_resetSDMJobsToReadyAndWaiting', 
                                          mx.DateTime.utc().mjd, 
                                          queuedStatus, JobStatus.INITIALIZE, 
                                          JobStatus.GENERAL)
      return cnt

# .............................................................................
   def resetExperimentsForOccSet(self, setid, status, statusModTime, priority):
      """
      @summary Reset all experiments (models and their associated projections) 
               associated with an occurrenceset.
      @param setid: Id of occurrenceset for which to reset all experiments
      @param status: new status for reset experiments
      @param statusModTime: modification time for reset experiments
      @param priority: priority for reset experiments
      @return: the number of experiments reset
      @note: lm_resetExperimentsForOccurrenceSet(int, int, double, int) 
             changes status and returns int
      """
      count = self.executeModifyReturnValue('lm_resetExperimentsForOccurrenceSet',
                                            setid, status, statusModTime, priority)
      return count

# .............................................................................
   def clearOccSet(self, setid, modTime):
      """
      @param setid: Id of the occurrenceset for which to set the count to -1
      @param modTime: time of modification
      @return: True on success, False on failure
      @note: lm_clearOccurrenceSet(int, double) returns 0/-1
      """
      success = self.executeModifyFunction('lm_clearOccurrenceSet', setid, modTime)
      return success

# .............................................................................
   def getUserMapservice(self, mapname, usr, epsg):
      """
      @summary: Create a mapservice from user environmental data.  
      @todo: Include occurrence data and its projections
      @todo: Include user-uploaded vector layers
      @param userid: Unique string for user record.
      @return: a MapLayerSet object containing all relevant layers.
      """
      lyrs = []
      mapSvc = None
      # returns layer
      rows, idxs = self.executeSelectManyFunction('lm_getAncillaryLayersForUser', 
                                                  usr, epsg)
      for r in rows:
         lyr = self._createLayer(r, idxs)
         lyrs.append(lyr)
         
      # returns lm_envlayer
      rows, idxs = self.executeSelectManyFunction('lm_getEnvLayersForUser', 
                                                  usr, epsg)
      for r in rows:
         if r[idxs['name']] == 'testLayer18':
            pass
         lyr = self._createEnvironmentalLayer(r, idxs)
         lyrs.append(lyr)
      
         
      # OccurrenceLayers (vector layers)
      occs = self.getOccurrenceSetsForUser(usr, epsg)
      for occ in occs: 
         # SDMProjections (raster layers)
         prjlyrs = self.getProjectionsForOcc(occ.getId())
         lyrs.append(occ)
         lyrs.extend(prjlyrs)
      if len(lyrs) > 0:
         mapSvc = MapLayerSet(mapname, title='%s Data' % usr, epsgcode=epsg, 
                              layers=lyrs, userId=usr)
      return mapSvc

# .............................................................................
   def getOccMapservice(self, occsetid, userid=ARCHIVE_USER):
      """
      @summary: Create a mapservice from an OccurrenceLayer and all Projections
                associated with it.
      @todo: Handle user occurrence data and its projections differently?
      @param occsetid: Unique id for OccurrenceLayer record.
      @return: a MapLayerSet object containing all relevant layers.
      """
      occ = self.getOccurrenceSet(occsetid)
      if occ is not None:
         if occ.queryCount > 0:
            allLyrs = self.getLayersForOcc(occ)
         else:
            raise LMError(currargs='OccurrenceLayer %d has no points' % occsetid,
                          logger=self.log)
      else:
         raise LMError(currargs='OccurrenceLayer %d does not exist' % occsetid,
                       logger=self.log)
      
      # TODO: this assumes all layers are owned by the same user.  True?
      mapSvc = MapLayerSet(occ.mapName, title='OccurrenceLayer %d Data' % occsetid, 
                           layers=allLyrs, userId=userid, dbId=occsetid, 
                           moduleType=LMServiceModule.SDM)
      mapSvc.setLocalMapFilename(mapfname=occ.mapFilename)
      mapSvc.setMapPrefix(mapprefix=occ.mapPrefix)
      return mapSvc

# .............................................................................
   def getScenarioCodes(self):
      """
      @summary Returns a list of available scenario codes
      @return A list of scenario codes
      @exception LMError: Thrown when the scenarioCode does not match a known
                          scenario
      @note lm_getScenarioCodes() returns setof varchar
      """
      rows, idxs = self.executeSelectManyFunction('lm_getScenarioCodes')
      scenariocodes = []
      if rows:
         scenariocodes = [r[0] for r in rows]
      return scenariocodes
   
# .............................................................................
   def getScenariosByKeyword(self, keyword):
      """
      @summary: Returns a list of available scenarios which are linked to the 
               given keyword.  Does *not* fill layers.
      @param keyword: A keyword for which to return all associated scenarios
      @return: A list of scenario objects
      @note: lm_getScenariosByKeyword(varchar) 
             returns setof lm_scenarioAndKeywords
      """
      rows, idxs = self.executeSelectManyFunction('lm_getScenariosByKeyword', keyword)
      scenarios = []
      for r in rows:
         scenarios.append(self._createScenario(r, idxs))
      return scenarios
   
# .............................................................................
   def getMatchingScenarios(self, scenarioid):
      """
      @summary: Get scenarios with layers matching those of the  
             given scenarioid.  Do not fill layers.  Also returns the scenario
             to be matched.
      @param scenarioid: Id of the scenario for which to find others with 
             matching layertypes.
      @return: List of scenario objects NOT filled with layers; NOT filled with 
             keywords.
      @note: lm_getMatchingScenariosNoKeywords(int) returns setof scenario
      """
      rows, idxs = self.executeSelectManyFunction('lm_getMatchingScenariosNoKeywords', 
                                                  scenarioid)
      scenarios = []
      for r in rows:
         scenarios.append(self._createScenario(r, idxs))
      return scenarios

# .............................................................................
   def getScenarioByCode(self, code, matchingLayers):
      """
      @summary: Return a scenario by its code, filling its layers.  
      @param id: Code for the scenario to be fetched.
      @param matchingLayers: Layers for new scenario to match.  
      """
      row, idxs = self.executeSelectOneFunction('lm_getScenarioByCode', code)
      scen = self._createScenario(row, idxs)
      self._fillScenarioLayers(scen, matchingLayers)
      return scen
         
# .............................................................................
   def getScenarioById(self, scenid, matchingLayers):
      """
      @summary: Return a scenario by its id, filling its layers.  If 
                matchScenarioId is present, make sure that the newly created 
                scenario contains only the same types as the match scenario, and 
                has them in the same order.
      @param id: Database key for the scenario to be fetched.
      @param matchScenarioId: Database key for a scenario to match.  
      """
      row, idxs = self.executeSelectOneFunction('lm_getScenarioById', scenid)
      scen = self._createScenario(row, idxs)
      self._fillScenarioLayers(scen, matchingLayers)
      return scen

# .............................................................................
   def getScenariosForLayer(self, lyrId):
      """
      @summary: Return all scenarios containing layer with lyrid; do not fill
                in layers
      @param id: Database key for the layer for which to fetch scenarios.
      """
      scens = []
      rows, idxs = self.executeSelectManyFunction('lm_getScenariosForLayer', lyrId)
      for r in rows:
         scens.append(self._createScenario(r, idxs))
      return scens
   
# ...............................................
   def insertScenario(self, scen):
      """
      @summary Inserts all scenario layers into the database
      @param scen: The scenario to insert
      """
      currtime = mx.DateTime.utc().mjd
      mUrlWithPlaceholder = scen.metadataUrl
      scenid = self.executeInsertFunction('lm_insertScenario', scen.name, 
                                      scen.title, scen.author, scen.description,
                                      mUrlWithPlaceholder, 
                                      scen.startDate, scen.endDate, 
                                      scen.units, scen.resolution, 
                                      scen.epsgcode,
                                      scen.getCSVExtentString(), 
                                      scen.getWkt(), 
                                      currtime, scen.getUserId())
      scen.setId(scenid)
      for kw in scen.keywords:
         successCode = self.executeInsertFunction('lm_insertScenarioKeyword',
                                              scenid, kw)
         if successCode != 0:
            self.log.error('Failed to insert keyword %s for scenario %d' % 
                           (kw, scenid))
      return scenid
   
# ...............................................
   def getEnvironmentalType(self, typecode, usrid):
      try:
         row, idxs = self.executeSelectOneFunction('lm_getLayerType', 
                                                   usrid, typecode)
      except:
         envType = None
      else:
         envType = self._createLayerType(row, idxs)
      return envType

# ...............................................
   def getEnvironmentalTypeById(self, typeid):
      try:
         row, idxs = self.executeSelectOneFunction('lm_getLayerType', typeid)
      except:
         envType = None
      else:
         envType = self._createLayerType(row, idxs)
      return envType
      
# ...............................................
   def insertEnvironmentalType(self, envtype):
      """
      @summary: Insert _EnvironmentalType values. Return the updated (or found) record.
      @param envtype: An EnvironmentalType or EnvironmentalLayer object
      @note: Method returns a new object in case one or more records (layer or
             parameter values) are present in the database for this user.
      """
      if envtype.parametersModTime is None:
         envtype.parametersModTime = mx.DateTime.utc().mjd
      etid = self.executeInsertFunction('lm_insertLayerType',
                                         envtype.getParametersUserId(),
                                         envtype.typeCode,
                                         envtype.typeTitle,
                                         envtype.typeDescription,
                                         envtype.parametersModTime)
      envtype.setParametersId(etid)
      for kw in envtype.typeKeywords:
         # exactly the same as etid above
         etid = self.executeInsertFunction('lm_insertLayerTypeKeyword', None,
                                             envtype.getParametersId(), kw)
      return etid
                             
# ...............................................
   def _insertEnvironmentalTypeFromLayer(self, envlyr):
      """
      @summary: Insert (or find) _EnvironmentalType values. Update the 
                EnvironmentalLayer object and return the EnvironmentalType id.
      @note: Method returns a new object in case one or more records (layer or
             parameter values) are present in the database for this user.
      """
      etid = envlyr.getParametersId()
      if etid is None:
         envType = self.getEnvironmentalType(envlyr.typeCode, envlyr.getUserId())
         if envType is not None:
            envlyr.setLayerParam(envType)
            etid = envType.getParametersId()
         else:
            etid = self.insertEnvironmentalType(envlyr)
      return etid
                             
# ...............................................
   def insertEnvLayer(self, lyr, scenarioId=None):
      """
      @summary Insert or find a layer's metadata in the MAL. 
      @param envLayer: layer to update
      @return: the updated or found EnvironmentalLayer
      @note: layer title and layertype title are the same
      @note: Layer should already have name, filename, and url populated.
      @note: We are setting the layername to the layertype to ensure that they 
             will be unique within a scenario
      @note: lm_insertEnvLayer(...) returns int
      @note lm_insertLayerTypeKeyword(...) returns int
      """
      lyr.modTime = mx.DateTime.utc().mjd
      envTypeId = self._insertEnvironmentalTypeFromLayer(lyr)
      if lyr.getParametersId() is None:
         lyr.setParametersId(envTypeId)

      lyrid = self.executeInsertFunction('lm_insertEnvLayer', 
                                         lyr.verify,
                                         lyr.metadataUrl,
                                         envTypeId,
                                         lyr.title,
                                         lyr.name,
                                         lyr.minVal,
                                         lyr.maxVal, 
                                         lyr.nodataVal,
                                         lyr.valUnits,
                                         lyr.isCategorical,
                                         lyr.getDLocation(),
                                         lyr.getMetaLocation(),
                                         lyr.dataFormat,
                                         lyr.gdalType,
                                         lyr.startDate,
                                         lyr.endDate,
                                         lyr.mapUnits,
                                         lyr.resolution,
                                         lyr.epsgcode,
                                         lyr.getUserId(),
                                         lyr.description,
                                         lyr.modTime,
                                         lyr.getCSVExtentString(), 
                                         lyr.getWkt())
      lyr.setId(lyrid)
      if scenarioId is not None:
         success = self.executeModifyFunction('lm_joinScenarioLayer', 
                                              scenarioId, lyrid)
         if not success:
            raise LMError(currargs='Failure joining layer %s to scenario %s' %
                          (str(lyrid), str(scenarioId)), 
                          logger=self.log)
      return lyr

# ...............................................
   def insertVectorLayer(self, lyr):
      """
      @summary: Insert an Vector into the Layer table in the MAL 
                database.  Currently only used for ChangeThinking display layers.
      @param lyr: a Vector to insert
      @return: new layer id on success, -1 for failure
      @note: Layer should already have name, filename, and mapPrefix
             populated.
      """
      lyr.modTime = mx.DateTime.utc().mjd
      wkt = None
      if lyr.epsgcode == DEFAULT_EPSG:
         wkt = lyr.getFeaturesWkt()
      # This updates the metadataUrl and dlocation to new values (incl layerid)
      lyrid = self.executeInsertFunction('lm_insertAncillaryLayer', 
                                         lyr.verify,
                                         lyr.squid,
                                         lyr.getUserId(),
                                         lyr.name,
                                         lyr.title,
                                         lyr.description,
                                         lyr.getDLocation(),
                                         lyr.metadataUrl,
                                         None,
                                         lyr.ogrType,
                                         lyr.dataFormat,
                                         lyr.epsgcode,
                                         lyr.mapUnits,
                                         lyr.resolution,
                                         lyr.getValAttribute(),
                                         lyr.startDate,
                                         lyr.endDate,
                                         lyr.modTime,
                                         # Raster: minVal, maxVal, nodataVal, valUnits
                                         None, None, None, None,
                                         # only for epsg:4326
                                         lyr.getCSVExtentString(), 
                                         wkt)
      # Set the new layerId back on the object
      lyr.setId(lyrid)
      return lyrid

# ...............................................
   def findVectorLayer(self, usr, lyrname):
      """
      @summary: Find a Vector layer with this name for this userid in the  
                VLayer table in the MAL database.  Currently only used for 
                ChangeThinking display layers.
      @param usr: LMUser id
      @param lyrname: layer name
      @return: existing layer id if found, -1 if not found
      """
      # This updates the metadataUrl and dlocation to new values (incl layerid)
      row, idxs = self.executeSelectOneFunction('lm_findVectorLayer', 
                                                usr, lyrname)
      vlyr = self._createVLayer(row, idxs)
      return vlyr

# ...............................................
   def updateEnvLayer(self, eLyr):
      """
      @summary Method to update a layer's metadata in the MAL.
      @param envLayer: layer to update
      @return: True for success, False for failure
      @note: lm_updateLayer(int, varchar, varchar, varchar, varchar, double,
             double, double, varchar, varchar, int, double, double, varchar,
             double, int, varchar, double, varchar, varchar) returns 0/-1
      """
      eLyr.modTime = mx.DateTime.utc().mjd
      success = self.executeModifyFunction('lm_updateLayer', eLyr.getId(), 
                                           eLyr.typeCode, eLyr.metadataUrl,
                                           eLyr.title, eLyr.name, 
                                           eLyr.minVal, eLyr.maxVal,
                                           eLyr.nodataVal, eLyr.valUnits,
                                           eLyr.getDLocation(), eLyr.gdalType,
                                           eLyr.startDate, eLyr.endDate,
                                           eLyr.mapUnits, 
                                           eLyr.resolution,
                                           eLyr.epsgcode,
                                           eLyr.description, 
                                           eLyr.getUserId(),
                                           eLyr.modTime, eLyr.getCSVExtentString(),
                                           eLyr.getWkt())
      return success
   
# ...............................................
   def updateLayerTypeKeyword(self, layerid, typeid, typekeyword):
      """
      @note: Don't allow updates after initial LayerType creation.  This method
               only exists for creating/linking keywords after linking 
               layers and keywords in database modification. 
      @summary Insert keywords associated with a layerType and its attributes into the database
      @param layerid: Primary key of the layer
      @param typekeyword: One keyword for the layertype
      @return: True on success, False on failure
      @note: lm_insertLayerTypeKeyword(int, int, varchar) returns 0/-1
      """
      successcode = self.executeInsertFunction('lm_insertLayerTypeKeyword',
                                               layerid, typeid, typekeyword)
      return successcode == 0


# ...............................................
   def deleteScenario(self, scenarioid):
      """
      @summary Deletes a scenario from the database
      @param scenarioid: The id of the scenario to delete
      @return: True on success, False on failure
      @note lm_deleteScenario(int) returns 0/-1
      """
      success = self.executeModifyFunction('lm_deleteScenario',scenarioid)
      return success
      
# ...............................................
   def deleteEnvLayer(self, sdmlyrid):
      """
      @summary Deletes an EnvironmentalLayer from the database, if it is not connected 
               to any scenarios.
      @param sdmlyrid: The id of the EnvironmentalLayer to delete
      @return: True on success, False on failure
      @note lm_deleteEnvLayer(int) returns 0/-1
      """
      success = self.executeModifyFunction('lm_deleteEnvLayer',sdmlyrid)
      return success
   
# ...............................................
   def deleteLayerType(self, typeid):
      """
      @summary Deletes an LayerType from the database, if it is not connected 
               to any layers.  Also deletes orphaned keywords
      @param typeid: The id of the LayerType to delete
      @return: True on success, False on failure
      @note lm_deleteLayerType(int) returns 0/-1
      """
      success = self.executeModifyFunction('lm_deleteLayerType',typeid)
      return success
   
# ...............................................
   def getUserIdForObjId(self, codeOrId, isScenario=False, isOccurrence=False):
      """
      """
      usr = None
      if isScenario:
         qry = 'scenario where scenarioid = %s' % codeOrId
      elif isOccurrence:
         qry = 'occurrenceset where occurrencesetid = %d' % codeOrId
      else:
         raise LMError('getUserIdForObjId implemented only for Scenarios and OccurrenceSets')
      row, idxs = self.executeQueryOneFunction(qry) 
      if row is not None:
         usr = self._getColumnValue(row, idxs, ['userid'])
      return usr

# ...............................................
   def getUserIds(self):
      """
      """
      usrids = []
      rows, idxs = self.executeQueryFunction('userid', 'lm3.lmuser')
      for r in rows:
         usrid = self._getColumnValue(r, idxs, ['userid'])
         if usrid is not None:
            usrids.append(usrid)
      return usrids

# ...............................................
   def insertUser(self, usr):
      """
      @summary: Insert a user of the Lifemapper system.  Allows 
            on-demand-modeling with user-submitted point data.
      @param usr: LMUser object to insert
      @return: True on success, False on failure (i.e. userid is not unique)
      @note: Identical to RAD function
      """
      usr.modTime = mx.DateTime.utc().mjd
      successcode = self.executeInsertFunction('lm_insertUser', usr.userid, 
                                           usr.firstName, usr.lastName, usr.institution, 
                                           usr.address1, usr.address2, 
                                           usr.address3, usr.phone, 
                                           usr.email, usr.modTime, usr.getPassword())
      if successcode == 0:
         return usr.userid
      else:
         self.log.error('userid %s already present in database' % usr.userid)
         return None
      
# ...............................................
   def updateUser(self, usr):
      """
      @summary: Update a user of the Lifemapper system.  All fields are updated,
             even if null, so fill all of them appropriately before calling this 
             method.
      @param usr: LMUser object to insert
      @return: True on success, False on failure
      @note: Identical to RAD function
      """
      usr.modTime = mx.DateTime.utc().mjd
      success = self.executeModifyFunction('lm_updateUser', usr.userid, 
                                           usr.firstName, usr.lastName, usr.institution, 
                                           usr.address1, usr.address2, 
                                           usr.address3, usr.phone, 
                                           usr.email, usr.modTime, usr.getPassword())
      return success
   
# ...............................................
   def deleteUser(self, usr):
      """
      @summary: Delete a user of the Lifemapper system.
      @param usr: LMUser object to delete
      @return: True on success, False on failure
      @note: Identical to RAD function
      """
      success = self.executeModifyFunction('lm_deleteUser', usr.userid)
      return success

   # ...............................................
   def getUser(self, usrid):
      """
      @summary: get a user, including the encrypted password
      @param usrid: the database primary key of the LMUser in the MAL
      @return: a LMUser object
      @note: Identical to RAD function
      """
      row, idxs = self.executeSelectOneFunction('lm_getUser', usrid)
      usr = self._createUser(row, idxs)
      return usr
      
   # ...............................................
   def getUsers(self):
      """
      @summary: get all users, including the encrypted password
      @return: a list of LMUser objects
      """
      users = []
      rows, idxs = self.executeSelectManyFunction('lm_getUsers')
      for r in rows:
         usr = self._createUser(r, idxs)
         users.append(usr)
      return users
      
   # ...............................................
   def findUser(self, usrid, email):
      """
      @summary: find a user with either a matching userId or email address
      @param usrid: the database primary key of the LMUser in the MAL
      @param email: the email address of the LMUser in the MAL
      @return: a LMUser object
      @note: Identical to RAD function
      """
      row, idxs = self.executeSelectOneFunction('lm_findUser', usrid, email)
      usr = self._createUser(row, idxs)
      return usr

# ...............................................
   def getComputeResourceByIP(self, ipAddr, ipMask=None):
      """
      """
      cr = None
      row, idxs = self.executeSelectOneFunction('lm_getCompute', ipAddr, ipMask)
      if row is not None:
         cr = self._createComputeResource(row, idxs)
      return cr

# ...............................................
   def getAllComputeResources(self):
      """
      """
      comps = []
      rows, idxs = self.executeSelectManyFunction('lm_getAllComputes')
      for r in rows:
         cr = self._createComputeResource(r, idxs)
         comps.append(cr)
      return comps

# ...............................................
   def insertComputeResource(self, compResource):
      """
      @summary: Insert a compute resource of this Lifemapper system.  
      @param usr: LMComputeResource object to insert
      @return: True on success, False on failure (i.e. IPAddress is not unique)
      """
      currtime = mx.DateTime.utc().mjd
      crid = self.executeInsertFunction('lm_insertCompute', compResource.name, 
                        compResource.ipAddress, compResource.ipMask, compResource.FQDN, 
                        compResource.getUserId(), currtime)
      
      compResource.setId(crid)
      return compResource

# ...............................................
   def insertOccurrenceSetMetadata(self, occ):
      """
      @summary Inserts an occurrences object into the database.  The actual
               number of points present on the object is saved as featureCount.
      @param occ: The occurrences to insert
      @note: if point features are present, uses actual count, otherwise uses 
             queryCount, set upon GBIF cache query and/or shapefile write. 
      @note: lm_insertOccurrenceSet(varchar, boolean, varchar, varchar, int, 
                double, int, varchar, varchar, varchar) returns int
      """
      occ.modTime = mx.DateTime.utc().mjd
      if occ.getFeatures():
         pointtotal = occ.featureCount
         bbstr = occ.getCSVExtentString()
         polywkt = occ.getConvexHullWkt()
         pointswkt = occ.getWkt()
      else:
         pointtotal = occ.queryCount
         bbstr = None
         polywkt = None
         pointswkt = None
      occid = self.executeInsertFunction('lm_insertOccurrenceSet', 
                                      occ.verify,
                                      occ.squid,
                                      occ.getUserId(),
                                      occ.fromGbif,
                                      occ.displayName, occ.getDLocation(), 
                                      pointtotal, occ.modTime, occ.epsgcode, 
                                      bbstr, polywkt, pointswkt, occ.metadataUrl,
                                      occ.primaryEnv, occ.getRawDLocation(),
                                      occ.status, occ.statusModTime, 
                                      occ.getScientificNameId())
      occ.setId(occid)
      return occid
   
# ...............................................
   def touchOccurrenceSet(self, occ):
      """
      @summary Method to update an occurrenceSet object in the MAL database with 
               the time last checked.
      @param occ the occurrences object to update
      """
      occ.setTouchTime(mx.DateTime.utc().mjd)
      # occ.queryCount should be populated correctly
      success = self.executeModifyFunction('lm_touchOccurrenceSet', 
                                          occ.getId(), occ.getTouchTime())
      return success
   
# ...............................................
   def existOccurrenceSet(self, displayName):
      """
      @summary Inserts an occurrences object into the database
      @param occ: The occurrences to insert
      """
      row, idxs = self.executeSelectOneFunction('lm_existOccurrenceSet', displayName)
      if row:
         if row[idxs['success']] == -1:
            return False
         else:
            return True
      else:
         return False
      
# ...............................................
   def getLayersForOcc(self, occset):
      """
      @summary Create a list of Layer objects for an OccurrenceLayer vector layer 
               and Projection raster layers to a mapservice prior to exposing 
               it via OGC services.  
      @note: Uses lm_projectionLayer.
      @param occset: Occurrenceset for which to add all projections.
      @return the new layers
      """
      lyrs = [occset]
      rows, idxs = self.executeSelectManyFunction('lm_getProjectionsByOccurrenceSetAndUser', 
                                                  occset.getId(),
                                                  occset.getUserId(), 
                                                  JobStatus.COMPLETE)
      for r in rows:
         prjlyr = self._createProjection(r, idxs)
         lyrs.append(prjlyr)
      return lyrs
# ...............................................
   def getProjectionLayer(self, layerid):
      """
      @summary Add a projection raster layer to a mapservice prior to exposing 
               it via OGC services.  Uses lm_projectionLayer.
      @param layerid: projectionId of the layer to add.
      @return the Raster layer 
      @exception LMError: Thrown when the number of layer rows != 1
      @todo Store the projection SRS?
      """
      row, idxs = self.executeSelectOneFunction('lm_getProjection', layerid)
      lyr = self._createProjection(row, idxs)
      return lyr

# ...............................................
   def getPointLayer(self, occSetId):
      """
      @summary Create a vector Point layer (for occurrenceSet or mapservice) 
      @param occSetId: occurrenceSetId of the point set to add.
      @return: OccurrenceLayer without points
      """
      row, idxs = self.executeSelectOneFunction('lm_getPointMaplayer', occSetId)
      occ = self._createOccurrenceSet(row, idxs)
      # Read to make sure the datasource is present and valid
      if not occ.isValidDataset():
         raise LMError(currargs='Error reading shapefile %s' 
                       % str(occ.getDLocation()),
                       logger=self.log)
      return occ
   
# .............................................................................
   def listModels(self, firstRecNum, maxNum, usrid, displayName, 
                  beforetime, aftertime, epsg, status, completeStat, occsetid, 
                  algcode, atom):
      if displayName is not None:
         displayName = displayName.strip() + '%'
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listModels', 
                                                     firstRecNum, 
                                                     maxNum, usrid, displayName, 
                                                     beforetime, aftertime, epsg,
                                                     status, completeStat, occsetid, 
                                                     algcode)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listModelObjects', 
                                                     firstRecNum, 
                                                     maxNum, usrid, displayName, 
                                                     beforetime, aftertime, epsg,
                                                     status, completeStat, occsetid, 
                                                     algcode)
         for r in rows:
            objs.append(self._createModel(r, idxs, doFillScenario=False))
      return objs
   
# .............................................................................
   def getOutdatedModels(self, count, usr, completeStat):
      models = []
      rows, idxs = self.executeSelectManyFunction('lm_getModelsToRollback',
                                                  count, usr, completeStat)
      for r in rows:
         models.append(self._createModel(r, idxs, doFillScenario=False))
      return models
   
# .............................................................................
   def countModels(self, usrid, displayName, beforetime, aftertime, epsg, 
                   status, completeStat, occsetid, algcode):
      if displayName is not None:
         displayName = displayName.strip() + '%'
      row, idxs = self.executeSelectOneFunction('lm_countModels', usrid, displayName, 
                                                beforetime, aftertime, epsg,
                                                status, completeStat, occsetid, 
                                                algcode)
      return self._getCount(row)

# .............................................................................
   def countModeledSpecies(self, status, userid):
      row, idxs = self.executeSelectOneFunction('lm_countModeledSpecies', status, userid)
      return self._getCount(row)
      
# # .............................................................................
#    def countJobs(self, status):
#       row, idxs = self.executeSelectOneFunction('lm_countJobs', status)
#       return self._getCount(row)

# .............................................................................
   def getLatestModelTime(self, status):
      row, idxs = self.executeSelectOneFunction('lm_getLatestModelTime', status)
      if row:
         return row[0]
      else:
         self.log.warning('%s failed to return a model for status %d' % 
                          (self.getLocation(), status))
         return None

# .............................................................................
   def getLatestProjectionTime(self, status):
      row, idxs = self.executeSelectOneFunction('lm_getLatestProjectionTime', status)
      if row:
         return row[0]
      else:
         self.log.warning('%s failed to return a projection for status %d' % 
                          (self.getLocation(), status))
         return None

# .............................................................................
   def returnStatisticValue(self, key):
      row, idxs = self.executeSelectOneFunction('lm_getStatistic', key)
      if row:
         return row[idxs['value']]
      else:
         raise LMError(currargs='Failed to return a value',
                       location=self.getLocation(), 
                       logger=self.log)
      
# .............................................................................
   def returnStatisticQuery(self, key):
      row, idxs = self.executeSelectOneFunction('lm_getStatistic', key)
      if row:
         return row[idxs['query']]
      else:
         raise LMError(currargs='MAL.returnStatisticQuery failed to return a query',
                       location=self.getLocation(), 
                       logger=self.log)
   
# .............................................................................
   def getStatisticsQueries(self):
      rows, idxs = self.executeSelectManyFunction('lm_getAllStatistics')      
      stats = {}
      for r in rows:
         stats[r[idxs['key']]] = r[idxs['query']]
      return stats

# .............................................................................
   def updateStatistic(self, key, qry):
      currtime = mx.DateTime.utc().mjd
      qrow, qidxs = self.executeQueryOneFunction(qry)
      if qrow:
         val = qrow[0]
         success = self.executeModifyFunction('lm_updateStatistic', 
                                              key, val, currtime)
         if success:
            self.log.debug('MAL.updateStatistics key: %s; val: %s'
                            % (key, str(val)))
         else:
            self.log.error('MAL.updateStatistics %s failed to update to %s'
                            % (key, str(val)))
      else:
         self.log.error('MAL.updateStatistics (%s) failed to execute query'  % qry)
            
# .............................................................................
   def _runQuery(self, cols, fromClause, whereEtcClause):
      qrows, qidxs = self.executeQueryFunction(cols, fromClause, whereEtcClause)
      return qrows, qidxs

# .............................................................................
   def listProjections(self, firstRecNum, maxCount, userId, displayName, 
                       beforeTime, afterTime, epsg, status, completeStat, 
                       occSetId, mdlId, algCode, scenarioId, atom):
      if displayName is not None:
         displayName = displayName.strip() + '%'
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listProjections', 
                                                     firstRecNum, maxCount, 
                                                     userId, displayName, 
                                                     beforeTime, afterTime, 
                                                     epsg, status, completeStat, 
                                                     occSetId, mdlId, algCode, 
                                                     scenarioId)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listProjectionObjects', 
                                                     firstRecNum, maxCount, 
                                                     userId, displayName, 
                                                     beforeTime, afterTime, 
                                                     epsg, status, completeStat, 
                                                     occSetId, mdlId, algCode, 
                                                     scenarioId)
         for r in rows:
            objs.append(self._createProjection(r, idxs, doFillScenario=False))
      return objs
   
# .............................................................................
   def countProjections(self, userId, displayName, beforeTime, afterTime, 
                        epsg, status, completeStat, occSetId, mdlId, algCode, 
                        scenarioId):
      """
      @summary: Return the number of projections fitting the given filter conditions
      @param userId: include projections with this userid
      @param beforeTime: include projections modified at or before this time
      @param afterTime: include projections modified at or after this time
      @param status: include projections with this status
      @param occSetId: include projections associated with this occurrenceset
      @param mdlId: include projections associated with this model
      @param algCode: include projections associated with this algorithm
      @param scenarioId: include projections associated with this scenario
      @return: number of projections fitting the given filter conditions
      @note: lm_countProjections(varchar, double, double, int, int, int, int, 
                                 varchar, int) returns int
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      row, idxs = self.executeSelectOneFunction('lm_countProjections', 
                                                userId, displayName, beforeTime, 
                                                afterTime, epsg, status, 
                                                completeStat, occSetId, mdlId, 
                                                algCode, scenarioId)
      return self._getCount(row)
      
# .............................................................................
   def countJobsOld(self, reftype, status, userId):      
      """
      @summary: Return the number of jobs fitting the given filter conditions
      @param userId: include only jobs with this userid
      @param status: include only jobs with this status
      @return: number of jobs fitting the given filter conditions
      """
      if reftype == ReferenceType.OccurrenceSet:
         fnname = 'lm_countOccJobs'
      elif reftype == ReferenceType.SDMModel:
         fnname = 'lm_countMdlJobs'
      elif reftype == ReferenceType.SDMProjection:
         fnname = 'lm_countPrjJobs'
      elif reftype == ReferenceType.SDMExperiment:
         fnname = 'lm_countMsgJobs'
      else:
         raise LMError('Unknown ReferenceType %s' % str(reftype))
      
      row, idxs = self.executeSelectOneFunction(fnname, userId, status)
      return self._getCount(row)

# .............................................................................
   def countJobs(self, proctype, status, userId):      
      """
      @summary: Return the number of jobs fitting the given filter conditions
      @param proctype: include only jobs with this 
                       LmCommon.common.lmconstants.ProcessType  
      @param userId: include only jobs with this userid
      @param status: include only jobs with this status
      @return: number of jobs fitting the given filter conditions
      @todo: use LmCommon.common.lmconstants.ProcessType here and in functions 
      """
      if proctype in (ProcessType.GBIF_TAXA_OCCURRENCE, 
                ProcessType.BISON_TAXA_OCCURRENCE, 
                ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                ProcessType.USER_TAXA_OCCURRENCE):
         fnname = 'lm_countOccJobs'
      elif proctype in (ProcessType.ATT_MODEL, ProcessType.OM_MODEL):
         fnname = 'lm_countMdlJobs'
      elif proctype in (ProcessType.ATT_PROJECT, ProcessType.OM_PROJECT):
         fnname = 'lm_countPrjJobs'
      elif proctype == ProcessType.SMTP:
         fnname = 'lm_countMsgJobs'
      else:
         return 0
      row, idxs = self.executeSelectOneFunction(fnname, userId, status)
      return self._getCount(row)

# .............................................................................
   def countLayers(self, userId, typecode, beforeTime, afterTime, epsg, 
                   isCategorical, scenarioId):
      """
      @summary: Return the number of layers fitting the given filter conditions
      @param userId: LMUser for whom to count EnvironmentalLayers
      @param beforeTime: include layers modified at or before this time
      @param afterTime: include layers modified at or after this time
      @param scenarioId: include layers associated with this scenario
      @return: number of layers fitting the given filter conditions
      @note: lm_countLayers(double, double, int) returns int
      """
      row, idxs = self.executeSelectOneFunction('lm_countLayers', userId, 
                                                typecode, beforeTime, afterTime, 
                                                epsg, isCategorical, scenarioId)
      return self._getCount(row)

# .............................................................................
   def countLayerTypeCodes(self, userId, beforeTime, afterTime):
      """
      @summary: Return the number of layers fitting the given filter conditions
      @param userId: LMUser for whom to count EnvironmentalTypes
      @param beforeTime: include layers modified at or before this time
      @param afterTime: include layers modified at or after this time
      @return: number of EnvironmentalTypes fitting the given filter conditions
      @note: lm_countLayers(double, double, int) returns int
      """
      row, idxs = self.executeSelectOneFunction('lm_countTypeCodes', userId, 
                                                beforeTime, afterTime)
      return self._getCount(row)
   
# .............................................................................
   def listLayerTypeCodes(self, firstRecNum, maxCount, userId, 
                          beforeTime, afterTime, atom):
      """
      @param userId: LMUser for whom to return EnvironmentalTypes
      @param beforeTime: include layers modified at or before this time
      @param afterTime: include layers modified at or after this time
      @return: EnvironmentalTypes fitting the given filter conditions
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listTypeCodes', 
                                                     firstRecNum, maxCount, 
                                                     userId, beforeTime, afterTime)
         objs = self._getAtoms(rows, idxs)
      else:
         rows, idxs = self.executeSelectManyFunction('lm_listTypeCodeObjects', 
                                                     firstRecNum, maxCount, 
                                                     userId, beforeTime, afterTime)
         objs = []
         for r in rows:
            objs.append(self._createLayerType(r, idxs))
      return objs

# .............................................................................
   def countScenarios(self, userId, beforeTime, afterTime, epsg, matchingId, 
                      kywds):
      """
      @summary: Return the number of scenarios fitting the given filter conditions
      @param userId: LMUser for whom to return scenarios
      @param beforeTime: include scenarios modified at or before this time
      @param afterTime: include scenarios modified at or after this time
      @param matchingId: include scenarios matching (with exactly the same 
                         layertypes) this scenarioid
      @param kywds: include scenarios associated with all of the keywords in 
                    this comma delimited string of keywords
      @return: number of scenarios fitting the given filter conditions
      @note: lm_countScenarios(double, double, int, varchar) returns int
      """
      row, idxs = self.executeSelectOneFunction('lm_countScenarios', userId, 
                                                beforeTime, afterTime, epsg,
                                                matchingId, kywds)
      return self._getCount(row)
      
# .............................................................................
   def listScenarios(self, firstRecNum, maxNum, userId, beforetime, 
                     aftertime, epsg, matchingId, kywds, atom):
      """
      @summary: Return scenario Atoms fitting the given filter conditions
      @param firstRecNum: start at this record
      @param maxNum: maximum number of records to return
      @param userId: LMUser for whom to return scenarios
      @param beforeTime: include scenarios modified at or before this time
      @param afterTime: include scenarios modified at or after this time
      @param epsg:
      @param matchingId: include scenarios matching (with exactly the same 
                         layertypes) this scenarioid
      @param kywds: include scenarios associated with all of the keywords in 
                    this comma delimited string of keywords
      @param atom: True if return objects will be Atoms, False if full Scenario
                   objects
      @return: number of scenarios fitting the given filter conditions
      @note: lm_listScenarios(int, int, double, double, int, varchar) returns int
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listScenarios', 
                                                     firstRecNum, maxNum, userId, 
                                                     beforetime, aftertime, epsg,
                                                     matchingId, kywds)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listScenarioObjects', 
                                                     firstRecNum, maxNum, userId, 
                                                     beforetime, aftertime, epsg,
                                                     matchingId, kywds)
         for r in rows:
            objs.append(self._createScenario(r, idxs))
      return objs

# .............................................................................
   def countAlgorithms(self):
      """
      @summary: Return the count of all Algorithms.
      @note: lm_countAlgorithms() returns int 
      """
      row, idxs = self.executeSelectOneFunction('lm_countAlgorithms')
      return self._getCount(row)

# .............................................................................
   def listAlgorithms(self, firstRecNum, maxNum, atom):
      """
      @summary: Return all Algorithm Atoms.
      @param firstRecNum: start at this record
      @param maxNum: maximum number of records to return
      @param atom: True if return objects will be Atoms, False if full 
                   Algorithm objects
      @note: lm_listAlgorithms(int, int) returns setof lm_atom 
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listAlgorithms', 
                                                     firstRecNum, maxNum, atom)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
      return objs
   
# .............................................................................
   def listLayers(self, firstRecNum, maxNum, userId, typecode, beforetime, 
                     aftertime, epsg, isCategorical, scenarioid, atom):
      """
      @summary: Return the number of layers fitting the given filter conditions
      @param firstRecNum: start at this record
      @param maxNum: maximum number of records to return
      @param userId: LMUser for whom to count EnvironmentalLayers
      @param beforeTime: include layers modified at or before this time
      @param afterTime: include layers modified at or after this time
      @param epsg: Filter layers by this EPSG code
      @param scenarioId: include layers associated with this scenario
      @param atom: True if return objects will be Atoms, False if full 
                   EnvironmentalLayer objects
      @return: number of layers fitting the given filter conditions
      @note: lm_countLayers(double, double, int) returns int
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listLayers', 
                                                     firstRecNum, maxNum, 
                                                     userId, typecode, beforetime, 
                                                     aftertime, epsg, isCategorical, 
                                                     scenarioid)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listLayerObjects', 
                                                     firstRecNum, maxNum, 
                                                     userId, typecode, beforetime, 
                                                     aftertime, epsg, isCategorical, 
                                                     scenarioid)
         for r in rows:
            objs.append(self._createEnvironmentalLayer(r, idxs))
      return objs
   
# .............................................................................
   def getLayer(self, lyrid):
      """
      @summary: Return an EnvironmentalLayer for the given id
      @param id: id of the desired EnvironmentalLayer
      @return: an EnvironmentalLayer object
      @note: lm_getLayer(int) returns an lm_envlayer
      """
      row, idxs = self.executeSelectOneFunction('lm_getLayer', lyrid)
      lyr = self._createEnvironmentalLayer(row, idxs)         
      return lyr
   
# .............................................................................
   def getEnvLayersByNameUserEpsg(self, name, user, epsg):
      """
      @summary: Return an EnvironmentalLayer for the given name, user, epsg (a unique combo)
      @param name: Layer name 
      @param user: User id for this layer
      @param epsg: EPSG code for this layer
      @return: an EnvironmentalLayer object
      @note: lm_getLayer(varchar, varchar, int) returns an lm_envlayer
      """
      lyrs = []
      rows, idxs = self.executeSelectManyFunction('lm_getEnvLayersByNameUserEpsg', 
                                                name, user, epsg)
      for r in rows:
         lyrs.append(self._createEnvironmentalLayer(r, idxs))         
      return lyrs

# .............................................................................
   def getEnvLayersByNameUser(self, name, user, epsg):
      """
      @summary: Return an EnvironmentalLayer for the given name, user, epsg (a unique combo)
      @param name: Layer name 
      @param user: User id for this layer
      @param epsg: EPSG code for this layer
      @return: an EnvironmentalLayer object
      @note: lm_getLayer(varchar, varchar, int) returns an lm_envlayer
      """
      lyrs = []
      rows, idxs = self.executeSelectOneFunction('lm_getEnvLayersByNameUser', 
                                                name, user, epsg)
      for r in rows:
         lyrs.append(self._createEnvironmentalLayer(r, idxs))         
      return lyrs


# .............................................................................
   def getLayerByMapPrefix(self, metadataUrl):
      """
      @summary: Return an EnvironmentalLayer for the given id
      @param metadataUrl: metadataUrl (WMS endpoint) of the desired EnvironmentalLayer
      @return: an EnvironmentalLayer object
      @note: lm_getLayer(int) returns an lm_envlayer
      """
      row, idxs = self.executeSelectOneFunction('lm_getLayerByUrl', metadataUrl)
      lyr = self._createEnvironmentalLayer(row, idxs)
      return lyr

# .............................................................................
   def countOccurrenceSets(self, minOccurrenceCount, hasProjections, userid, 
                           displayName, beforetime, aftertime, epgs, stat, 
                           completestat):
      """
      @summary: Count all OccurrenceSets matching the filter conditions 
      @param minOccurrenceCount: minimum number of occurrences present in set.
             Defaults to 0 (but no occurrencesets should be empty).
      @param hasProjections: True if occurrenceset must have completed 
             SDMProjections associated with it; False if no condition
      @param userId: User (owner) for which to return occurrencesets.  
      @param displayName: Display name for which to return occurrencesets.
      @param beforeTime: filter by occurrencesets modified at or before this time
      @param afterTime: filter by occurrencesets modified at or after this time
      @param epsg: filter occurrencesets by this EPSG code
      @return: a list of OccurrenceSet atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      row, idxs = self.executeSelectOneFunction('lm_countOccurrenceSets', 
                                                minOccurrenceCount,
                                                hasProjections,
                                                userid, displayName,
                                                beforetime, aftertime, epgs,
                                                stat, completestat)
      return self._getCount(row)

# .............................................................................
   def listOccurrenceSets(self, firstRecNum, maxNum, 
                          minOccurrenceCount, hasProjections, userid, 
                          displayName, beforetime, aftertime, epsg,
                          stat, completestat, atom):
      """
      @summary: Return all OccurrenceSets matching filter conditions and 
                starting at the firstRecNum limited to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param minOccurrenceCount: minimum number of occurrences present in set.
             Defaults to 0 (but no occurrencesets should be empty).
      @param hasProjections: True if occurrenceset must have completed 
             SDMProjections associated with it; False if no condition
      @param userId: User (owner) for which to return occurrencesets.  
      @param displayName: Display name for which to return occurrencesets.
      @param beforeTime: filter by occurrencesets modified at or before this time
      @param afterTime: filter by occurrencesets modified at or after this time
      @param epsg: filter occurrencesets by this EPSG code
      @param atom: True if return objects will be Atoms, False if full 
                   OccurrenceLayer objects
      @return: a list of OccurrenceSet atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listOccurrenceSets', 
                                                     firstRecNum, maxNum, 
                                                     minOccurrenceCount,
                                                     hasProjections, userid, 
                                                     displayName, beforetime, 
                                                     aftertime, epsg, stat,
                                                     completestat)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listOccurrenceSetObjects', 
                                                     firstRecNum, maxNum, 
                                                     minOccurrenceCount,
                                                     hasProjections, userid, 
                                                     displayName, beforetime, 
                                                     aftertime, epsg, stat,
                                                     completestat)
         for r in rows:
            objs.append(self._createOccurrenceSet(r, idxs))
      return objs
   
# .............................................................................
   def getOccurrenceSet(self, id):
      row, idxs = self.executeSelectOneFunction('lm_getOccurrenceSet', id)
      occset = self._createOccurrenceSet(row, idxs)
      cnt = row[idxs['querycount']]
      if occset.queryCount != cnt:
         self.log.info('WTF is going on!?!')
         occset.queryCount = cnt
      return occset
   
# .............................................................................
   def insertStatisticRow(self, key, query, description):
      statid = self.executeInsertFunction('lm_insertStatistic',key, query, description)
      return statid
      
# .............................................................................
# Private functions
# .............................................................................

# ...............................................
   def _createProjection(self, row, idxs, doFillScenario=True):
      """
      @summary Creates a Projection object from a lm_fullProjection or 
               a projection table record in the MAL database.  
      @param row: The database row with projection data
      @param idxs: Indexes for the row
      @return A Projection
      """
      prj = None 
      if row is not None:
         try:
            # Create Model object
            model = self._createModel(row, idxs, False) 
            
            isDiscrete = None
            alg = self._getColumnValue(row, idxs, ['algorithmcode'])
            if alg is not None:
               isDiscrete = ALGORITHM_DATA[alg]['isDiscreteOutput']
               
            dlocation = self._getColumnValue(row, idxs, ['prjdlocation', 'dlocation'])
            stat = self._getColumnValue(row, idxs, ['prjstatus', 'status'])
            statTime = self._getColumnValue(row, idxs, ['prjstatusmodtime', 'statusmodtime'])
            createTime = self._getColumnValue(row, idxs, ['prjcreatetime', 'createtime'])
            priority = self._getColumnValue(row, idxs, ['prjpriority', 'priority'])
            murl = self._getColumnValue(row, idxs, ['prjmetadataurl', 'metadataurl'])
            bbox = self._getColumnValue(row, idxs, ['prjbbox', 'bbox'])
            usr = self._getColumnValue(row, idxs, ['mdluserid'])
            epsg = self._getColumnValue(row, idxs, ['prjepsgcode', 'epsgcode'])
            scenid = self._getColumnValue(row, idxs, ['prjscenarioid', 'scenarioid'])
            scencode = self._getColumnValue(row, idxs, ['prjscenariocode', 'scenariocode'])
            mskid = self._getColumnValue(row, idxs, ['prjmaskid', 'maskid'])
            gtype = self._getColumnValue(row, idxs, ['datatype'])
            projid = self._getColumnValue(row, idxs, ['projectionid'])
            verify = self._getColumnValue(row, idxs, ['prjverify', 'verify'])
            squid = self._getColumnValue(row, idxs, ['prjsquid', 'squid'])
               
            if doFillScenario:
               prjscenario = self.getScenarioById(scenid, None)
               masklyr = self.getLayer(mskid) 
            else:
               prjscenario = Scenario(scencode, scenarioid=scenid)
               masklyr = None
               
         except Exception, e:
            lmerr = LMError(currargs='Failed on prj %d' % row[idxs['projectionid']], 
                            prevargs=e.args,
                            location=self.getLocation(), 
                            logger=self.log)
            raise lmerr

         # CJG - 03/23/2013 
         # Put this in because projections created with the ATT version of 
         #    MaxEnt were failing to write due to a mismatched GDAL format.  
         #    This note is a reminder to show Aimee and talk about how to solve
         #    this.  It should at least be handled with constants.
         gdalFormat = DEFAULT_PROJECTION_FORMAT

         try:
            prj = SDMProjection(model, 
                             prjscenario, 
                             verify=verify, 
                             squid=squid,
                             mask=masklyr,
                             priority=priority,
                             metadataUrl=murl,
                             dlocation=dlocation,
                             status=stat, 
                             statusModTime=statTime,
                             bbox=bbox,
                             epsgcode=epsg,
                             gdalType=gtype,
                             gdalFormat=gdalFormat,
                             isDiscreteData=isDiscrete,
                             projectionId=projid, 
                             userId=usr,
                             createTime=createTime)
         except Exception, e:
            lmerr = LMError(currargs='Failed on prj %d' % row[idxs['projectionid']], 
                            prevargs=e.args,
                            location=self.getLocation(), 
                            logger=self.log)
            raise lmerr
            
      return prj

# ...............................................
   def _createUser(self, row, idxs):
      usr = None
      if row is not None:
         usr = LMUser(row[idxs['userid']], row[idxs['email']], 
                      row[idxs['password']], isEncrypted=True, 
                      firstName=row[idxs['firstname']], lastName=row[idxs['lastname']], 
                      institution=row[idxs['institution']], 
                      addr1=row[idxs['address1']], addr2=row[idxs['address2']], 
                      addr3=row[idxs['address3']], phone=row[idxs['phone']], 
                      modTime=row[idxs['datelastmodified']])
      return usr
   
# ...............................................
   def _createComputeResource(self, row, idxs):
      cr = None 
      if row is not None:
         cr = LMComputeResource(self._getColumnValue(row, idxs, ['name']), 
                                self._getColumnValue(row, idxs, ['ipaddress']), 
                                self._getColumnValue(row, idxs, ['userid']), 
                                ipMask=self._getColumnValue(row, idxs, ['ipmask']), 
                                FQDN=self._getColumnValue(row, idxs, ['fqdn']), 
                                dbId=self._getColumnValue(row, idxs, ['computeresourceid']), 
                                createTime=self._getColumnValue(row, idxs, ['datecreated']), 
                                modTime=self._getColumnValue(row, idxs, ['datelastmodified']), 
                                hbTime=self._getColumnValue(row, idxs, ['lastheartbeat']))
      return cr

# ...............................................
   def _createScenario(self, row, idxs):
      scen = None
      if row is not None:
         scen = Scenario(self._getColumnValue(row, idxs, ['scenariocode']), 
                         title=self._getColumnValue(row, idxs, ['title']), 
                         author=self._getColumnValue(row, idxs, ['author']), 
                         description=self._getColumnValue(row, idxs, ['description']),
                         metadataUrl=self._getColumnValue(row, idxs, ['metadataurl']), 
                         dlocation=self._getColumnValue(row, idxs, ['dlocation']),
                         startdt=self._getColumnValue(row, idxs, ['startdate']), 
                         enddt=self._getColumnValue(row, idxs, ['enddate']),
                         units=self._getColumnValue(row, idxs, ['units']), 
                         res=self._getColumnValue(row, idxs, ['resolution']), 
                         bbox=self._getColumnValue(row, idxs, ['bbox']), 
                         modTime=self._getColumnValue(row, idxs, ['datelastmodified']),
                         epsgcode=self._getColumnValue(row, idxs, ['epsgcode']),
                         scenarioid=self._getColumnValue(row, idxs, ['scenarioid']))
         keystr = self._getColumnValue(row, idxs, ['keywords'])
         if keystr is not None:
            scen.keywords = keystr.split(',')
      return scen

# ...............................................
   def _fillScenarioLayers(self, scen, matchingLayers=None):
      """
      @summary: Fill a scenario object with its EnvironmentalLayers 
      @param scen: Scenario object to be filled
      @postcondition: Scenario object contains all of its EnvironmentalLayers
      @note: lm_getLayersByScenarioId(int) returns a setof lm_envlayer
      """
      if scen is not None:
         rows, idxs = self.executeSelectManyFunction('lm_getLayersByScenarioId', 
                                                     scen.getId())
         lyrDict = {}
         for r in rows:
            lyr = self._createEnvironmentalLayer(r, idxs)
            if matchingLayers is None:
               scen.addLayer(lyr)
            else:
               #  Save in a dictionary for reordering like matchingLayers
               lyrDict[lyr.typeCode] = lyr
            
         if matchingLayers is not None:
            mtchTypes = set(lyr.getParametersId() for lyr in matchingLayers)
            nonMatchingLayers = mtchTypes.symmetric_difference(lyrDict.keys())
            
            if len(nonMatchingLayers) == 0:
               for mtchLyr in matchingLayers:
                  scen.addLayer(lyrDict[mtchLyr.typeCode])
            else:
               raise LMError(currargs='Scenario %d; nonMatchingLayers %s' 
                             % (scen.getId(), str(nonMatchingLayers)),
                             location=self.getLocation(), 
                             logger=self.log)
         
# ...............................................
   def _createLayerType(self, row, idxs):
      """
      Create an _EnvironmentalType from a LayerType, lm_envlayer,
      lm_envlayerAndKeywords or lm_layerTypeAndKeywords record in the MAL
      """
      lyrType = None
      keywordLst = []
      if row is not None:
         keystr = self._getColumnValue(row, idxs, ['keywords'])
         if keystr is not None and len(keystr) > 0:
            keywordLst = keystr.split(',')
         code = self._getColumnValue(row, idxs, ['typecode', 'code'])
         title = self._getColumnValue(row, idxs, ['typetitle', 'title'])
         desc = self._getColumnValue(row, idxs, ['typedescription', 'description'])
         modtime = self._getColumnValue(row, idxs, ['typemodtime', 'datelastmodified'])
         usr = self._getColumnValue(row, idxs, ['userid'])
         ltid = self._getColumnValue(row, idxs, ['layertypeid'])
                                                
         lyrType = EnvironmentalType(code, title, desc, usr,
                                     keywords=keywordLst,
                                     modTime=modtime, 
                                     environmentalTypeId=ltid)
      return lyrType
# ...............................................
   def _createEnvironmentalLayer(self, row, idxs):
      """
      Create an EnvironmentalLayer from a lm_envlayer record in the MAL
      """
      envRst = None
      if row is not None:
         rst = self._createLayer(row, idxs)
         if rst is not None:
            etype = self._createLayerType(row, idxs)
            envRst = EnvironmentalLayer.initFromParts(rst, etype)
      return envRst

# ...............................................
   def _createLayer(self, row, idxs):
      """
      Create Raster or Vector layer from a Layer record in the MAL
      """
      lyr = None
      if row is not None:
         verify = self._getColumnValue(row, idxs, ['verify'])
         squid = self._getColumnValue(row, idxs, ['squid'])
         name = self._getColumnValue(row, idxs, ['name'])
         title = self._getColumnValue(row, idxs, ['title'])
         author = self._getColumnValue(row, idxs, ['author'])
         desc = self._getColumnValue(row, idxs, ['description'])
         dlocation = self._getColumnValue(row, idxs, ['dlocation'])
         mlocation = self._getColumnValue(row, idxs, ['metalocation'])
         vtype = self._getColumnValue(row, idxs, ['ogrtype'])
         rtype = self._getColumnValue(row, idxs, ['gdaltype'])
         iscat = self._getColumnValue(row, idxs, ['iscategorical'])
         fformat = self._getColumnValue(row, idxs, ['dataformat'])
         epsg = self._getColumnValue(row, idxs, ['epsgcode'])
         munits = self._getColumnValue(row, idxs, ['mapunits'])
         res = self._getColumnValue(row, idxs, ['resolution'])
         vattr = self._getColumnValue(row, idxs, ['valattribute'])
         sDate = self._getColumnValue(row, idxs, ['startdate'])
         eDate = self._getColumnValue(row, idxs, ['enddate'])
         bbox = self._getColumnValue(row, idxs, ['bbox'])
         thumb = self._getColumnValue(row, idxs, ['thumbnail'])
         nodata = self._getColumnValue(row, idxs, ['nodataval'])
         minval = self._getColumnValue(row, idxs, ['minval'])
         maxval = self._getColumnValue(row, idxs, ['maxval'])
         vunits = self._getColumnValue(row, idxs, ['valunits'])
         # For OccurrenceLayer, SDMProjection, EnvironmentalLayer
         # layerId == svcObjId
         dbid = self._getColumnValue(row, idxs, 
                  ['projectionid', 'occurrencesetid', 'layerid'])
         usr = self._getColumnValue(row, idxs, ['lyruserid', 'userid'])
         murl = self._getColumnValue(row, idxs, 
                  ['prjmetadataurl', 'occmetadataurl', 'metadataurl'])
         dtcreate = self._getColumnValue(row, idxs, ['datecreated', 'createtime'])
         dtmod = self._getColumnValue(row, idxs, 
                  ['prjstatusmodtime', 'occstatusmodtime', 'datelastmodified', 
                   'statusmodtime'])
                     
         if vtype is not None:
            lyr = Vector(name=name, title=title, bbox=bbox, startDate=sDate, 
                         verify=verify, squid=squid,
                         endDate=eDate, mapunits=munits, resolution=res, 
                         epsgcode=epsg, dlocation=dlocation, 
                         metalocation=mlocation, valAttribute=vattr, 
                         valUnits=vunits, isCategorical=iscat, 
                         ogrType=vtype, ogrFormat=fformat, 
                         author=author, description=desc, 
                         svcObjId=dbid, lyrId=dbid, lyrUserId=usr, 
                         createTime=dtcreate, modTime=dtmod,
                         metadataUrl=murl, moduleType=LMServiceModule.SDM) 
         elif rtype is not None:
            lyr = Raster(name=name, title=title, bbox=bbox, startDate=sDate, 
                         verify=verify, squid=squid,
                         endDate=eDate, mapunits=munits, resolution=res, 
                         epsgcode=epsg, dlocation=dlocation, 
                         metalocation=mlocation, minVal=minval, maxVal=maxval, 
                         nodataVal=nodata, valUnits=vunits, isCategorical=iscat,
                         gdalType=rtype, gdalFormat=fformat, author=author, 
                         description=desc, svcObjId=dbid, lyrId=dbid, lyrUserId=usr, 
                         createTime=dtcreate, modTime=dtmod, metadataUrl=murl,
                         moduleType=LMServiceModule.SDM)
      return lyr
   
# ...............................................
   def _createSDMJobNew(self, row, idxs, newStatus=None):
      job = None
      if row is not None:
         jobid = self._getColumnValue(row, idxs, ['lmjobid'])
         crid = self._getColumnValue(row, idxs, ['jbcomputeresourceid'])
         createtime = self._getColumnValue(row, idxs, ['jbdatecreated'])
         if newStatus is not None:
            stat = newStatus
            stattime = mx.DateTime.gmt().mjd
         else:
            stat = self._getColumnValue(row, idxs, ['jbstatus'])
            stattime= self._getColumnValue(row, idxs, ['jbstatusmodtime'])
         priority= self._getColumnValue(row, idxs, ['priority'])
         hbtime = self._getColumnValue(row, idxs, ['lastheartbeat'])
         retries = self._getColumnValue(row, idxs, ['retrycount'])
         processtype = self._getColumnValue(row, idxs, ['reqsoftware'])
         stage = self._getColumnValue(row, idxs, ['jbstage'])
         reftype = self._getColumnValue(row, idxs, ['referencetype'])
         
         if reftype == ReferenceType.OccurrenceSet:
            occ = self._createOccurrenceSet(row, idxs)
            job = SDMOccurrenceJob(occ, processType=processtype, computeId=crid,  
                                   status=stat, statusModTime=stattime, 
                                   priority=priority, lastHeartbeat=hbtime, 
                                   createTime=createtime, jid=jobid, 
                                   retryCount=retries)
         else:
            mdl = self._createModel(row, idxs)
            if reftype == ReferenceType.SDMExperiment:
               job = NotifyJob(obj=mdl, objType=ReferenceType.SDMExperiment, 
                               jobFamily=JobFamily.SDM, computeId=crid, 
                               email=row[idxs['email']], 
                               status=stat, 
                               statusModTime=stattime, 
                               priority=priority,
                               lastHeartbeat=hbtime, createTime=createtime, 
                               jid=jobid, retryCount=retries)
            elif reftype == ReferenceType.SDMProjection:
               prj = self._createProjection(row, idxs)
               job = SDMProjectionJob(prj, processType=processtype, computeId=crid,  
                                        lastHeartbeat=hbtime, createTime=createtime, 
                                        jid=jobid, retryCount=retries)
            elif reftype == ReferenceType.SDMModel:
               job = SDMModelJob(mdl, processType=processtype, computeId=crid,  
                                lastHeartbeat=hbtime, createTime=createtime, 
                                jid=jobid, retryCount=retries)
            else:
               raise LMError('Unknown referenceType %s for job %d' 
                             % (str(reftype), jobid))
      return job

      
# ...............................................
   def _createModel(self, row, idxs, doFillScenario=True):
      """
      @summary Returns a Model object from a lm_fullmodel or lm_fullprojection
                  record from the MAL database.  The 'lm_fullmodel' record type 
                  also populates algorithm parameters. Scenario is filled only 
                  with the layer urls, not all layer metadata.
      @param row: A row of lm_fullmodel or lm_fullprojection data
      @param idxs: Indexes for the row of data
      @return A model generated from the information in the row
      @todo: check this!  We know what fields are returned, 
             are we checking for value data in 'if idxs.keys().count ...'?
      @todo: check returned values of lm_fullmodel against object members
             (originally documented for lm_minimodel)
      """
      model = None
      if row is not None:
         occurrences = self._createOccurrenceSet(row, idxs)
         algorithm = self._createAlgorithm(row, idxs)

         mdlid = self._getColumnValue(row, idxs, ['modelid'])
         dlocation = self._getColumnValue(row, idxs, ['mdldlocation', 'dlocation'])
         name = self._getColumnValue(row, idxs, ['mdlname', 'name'])
         desc = self._getColumnValue(row, idxs, ['mdldescription', 'description'])
         scenid = self._getColumnValue(row, idxs, ['mdlscenarioid', 'scenarioid'])
         scencode = self._getColumnValue(row, idxs, ['mdlscenariocode', 'scenariocode'])
         prty = self._getColumnValue(row, idxs, ['mdlpriority', 'priority'])
         stat = self._getColumnValue(row, idxs, ['mdlstatus', 'status'])
         stattime = self._getColumnValue(row, idxs, ['mdlstatusmodtime', 'statusmodtime'])
         ctime = self._getColumnValue(row, idxs, ['mdlcreatetime', 'createtime'])
         usrid = self._getColumnValue(row, idxs, ['mdluserid', 'userid'])
         qc = self._getColumnValue(row, idxs, ['qc']) 
         email = self._getColumnValue(row, idxs, ['email'])
         if doFillScenario:
            scenario = self.getScenarioById(scenid, None)
            mdlMask = self.getLayer(self._getColumnValue(row, idxs, ['mdlmaskid', 'maskid']))
         else:
            scenario = Scenario(scencode, scenarioid=scenid)
            mdlMask = None
               
         model = SDMModel(prty, occurrences, scenario, algorithm, 
                          name=name, description=desc, mask=mdlMask,
                          createTime=ctime, status=stat, 
                          statusModTime=stattime, ruleset=dlocation, 
                          qc=qc, email=email, userId=usrid, modelId=mdlid)
      return model

# ...............................................
   def _createScientificName(self, row, idxs):
      """
      @summary Returns an ScientificName object from:
                - an ScientificName row
                - an lm_fullScientificName
      @param row: A row of ScientificName data
      @param idxs: Indexes for the row of data
      @return A ScientificName object generated from the information in the row
      """
      sciname = None
      if row is not None:
         name = self._getColumnValue(row, idxs, ['sciname'])
         
         if name is not None:
            kingdom = self._getColumnValue(row, idxs, ['kingdom'])
            phylum = self._getColumnValue(row, idxs, ['phylum']) 
            txClass = self._getColumnValue(row, idxs, ['tx_class'])
            txOrder = self._getColumnValue(row, idxs, ['tx_order'])
            family = self._getColumnValue(row, idxs, ['family'])
            genus = self._getColumnValue(row, idxs, ['genus'])
            lcnt = self._getColumnValue(row, idxs, ['lastcount'])
            createtime = self._getColumnValue(row, idxs, 
                                              ['scidatecreated', 'datecreated'])
            modtime = self._getColumnValue(row, idxs, 
                                           ['scidatelastmodified', 'datelastmodified'])
            taxonomySourceId = self._getColumnValue(row, idxs, ['taxonomysourceid']) 
            srckey = self._getColumnValue(row, idxs, ['taxonomykey'])
            genkey = self._getColumnValue(row, idxs, ['genuskey'])
            spkey = self._getColumnValue(row, idxs, ['specieskey'])
            hier = self._getColumnValue(row, idxs, ['keyhierarchy'])
            id = self._getColumnValue(row, idxs, ['scientificnameid'])
            sciname = ScientificName(name, kingdom=kingdom, phylum=phylum,  
                                     txClass=txClass, txOrder=txOrder, 
                                     family=family, genus=genus, 
                                     lastOccurrenceCount=lcnt, 
                                     createTime=createtime, modTime=modtime, 
                                     taxonomySourceId=taxonomySourceId, 
                                     taxonomySourceKey=srckey, 
                                     taxonomySourceGenusKey=genkey, 
                                     taxonomySourceSpeciesKey=spkey, 
                                     taxonomySourceKeyHierarchy=hier,
                                     scientificNameId=id)
      return sciname
         
# ...............................................
   def _createOccurrenceSet(self, row, idxs):
      """
      @summary Returns an OccurrenceLayer object from:
                - an occurrences row, lm_fullOccurrenceset, lm_occJob
                - an lm_fullmodel, lm_fullprojection, lm_mdlJob, lm_prjJob, lm_msgJob
                - a model row  in the MAL database
      @param row: A row of occurrences data
      @param idxs: Indexes for the row of data
      @return A OccurrenceLayer object generated from the information in the row
      """
      occ = None
      if row is not None:
         sciname = self._createScientificName(row, idxs)
         occsetId = self._getColumnValue(row, idxs, ['occurrencesetid'])
         verify = self._getColumnValue(row, idxs, ['occverify', 'verify'])
         squid = self._getColumnValue(row, idxs, ['occsquid', 'squid'])
         if not(idxs.has_key('displayname')):
            return OccurrenceLayer(None, occId=occsetId)
         
         usr = self._getColumnValue(row, idxs, ['occuserid', 'userid'])
         stat = self._getColumnValue(row, idxs, ['occstatus', 'status'])
         stattime = self._getColumnValue(row, idxs, 
                                         ['occstatusmodtime', 'statusmodtime'])
         murl = self._getColumnValue(row, idxs, ['occmetadataurl', 'metadataurl'])
         dloc = self._getColumnValue(row, idxs, ['occdlocation', 'dlocation'])
         bbox = self._getColumnValue(row, idxs, ['occbbox', 'bbox'])
         geom = self._getColumnValue(row, idxs, ['occgeom', 'geom'])
         epsg = self._getColumnValue(row, idxs, ['occepsgcode', 'epsgcode'])
         fromgbif = self._getColumnValue(row, idxs, ['fromgbif'])
         dname = self._getColumnValue(row, idxs, ['displayname'])
         sciid = self._getColumnValue(row, idxs, ['scientificnameid'])
         qcount = self._getColumnValue(row, idxs, ['querycount'])
         modtime = self._getColumnValue(row, idxs, ['datelastmodified'])
         touchtime = self._getColumnValue(row, idxs, ['datelastchecked'])
         primaryEnv = self._getColumnValue(row, idxs, ['primaryenv'])
         rdloc = self._getColumnValue(row, idxs, ['rawdlocation'])
         if bbox == '':
            bbox = None
   
         # Calculate and populate name in constructor
         # TODO: Require/Accept name from non-Archive User
         occ = OccurrenceLayer(dname, fromGbif=fromgbif, dlocation=dloc, 
                               verify=verify, squid=squid,
                               metadataUrl=murl, queryCount=qcount,
                               modTime=modtime, touchTime=touchtime,
                               epsgcode=epsg, bbox=bbox, 
                               primaryEnv=primaryEnv, userId=usr,
                               occId=occsetId, rawDLocation=rdloc, 
                               sciName=sciname, 
                               status=stat, statusModTime=stattime)
      return occ

# ...............................................
   def _selectUnProjectedModels(self, count):
      """
      @summary Method to get species which have not yet been projected.  
      @param count: Number of species to return
      @return List of OccurrenceLayer objects
      """
      # lm_getModelsNeverProjected returns a lm_minimodel
      rows, idxs = self.executeSelectManyFunction('lm_getModelsNeverProjected', 
                                                  count, JobStatus.COMPLETE)
      models = []
      if rows:
         for r in rows:
            models.append(self._createModel(r, idxs))
      return models  

# ...............................................
   def findModel(self, mdl):
      """
      Find models matching the parameters of the given model
      @param mdl: model with parameters for which to find a match
      @note: parameters to match include:
             user,
             occurrenceSet
             scenario
             algorithm (with exact parameter set)
      @note: lm_findModel(varchar, int, int, int) returns lm_fullmodel
      @note: NOT currently used
      """
      row, idxs = self.executeSelectOneFunction('lm_findModel', 
                                                mdl.getUserId(), 
                                                mdl.occurrenceSet.getId(),
                                                mdl.getScenario().getId(),
                                                mdl.getAlgorithm().code)
      model = self._createModel(row, idxs, doFillScenario=False)
      return model

# ...............................................
   def _createAlgorithm(self, row, idxs):
      """
      Created only from a model, lm_fullModel, or lm_fullProjection 
      """
      code = self._getColumnValue(row, idxs, ['algorithmcode'])
      params = self._getColumnValue(row, idxs, ['algorithmparams'])
      try:
         alg = Algorithm(code, parameters=params)
      except:
         alg = None
      return alg
         
# ...............................................
   def _attemptBadStoredProcedure(self):
      success = self.executeSelectOneFunction('lm_nonExistentStoredProcedure')
      return success

# ...............................................
   def getComplexNamedArchiveOccurrenceSets(self, count, userid, beforeTime):
      occsets = []
      rows, idxs = self.executeSelectManyFunction('lm_listComplexOccurrenceSetObjectsByTime',
                                                 count, userid, beforeTime)
      for r in rows:
         occ = self._createOccurrenceSet(r, idxs)
         occsets.append(occ)
      return occsets
   
# ...............................................
   def insertTaxonSource(self, taxonSourceName, taxonSourceUrl, currtime):
      taxSourceId = self.executeInsertFunction('lm_insertTaxonSource', 
                                               taxonSourceName, 
                                               taxonSourceUrl, currtime)
      return taxSourceId

# ...............................................
   def findTaxonSource(self, taxonSourceName):
      txSourceId = url = createdate = moddate = None
      if taxonSourceName is not None:
         try:
            row, idxs = self.executeSelectOneFunction('lm_findTaxonSource', 
                                                      taxonSourceName)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
         if row is not None:
            txSourceId = row[idxs['taxonomysourceid']]
            url = row[idxs['url']]
            createdate = row[idxs['datecreated']]
            moddate =  row[idxs['datelastmodified']]
      return txSourceId, url, createdate, moddate

# ...............................................
   def findTaxon(self, taxonSourceId, taxonkey):
      try:
         row, idxs = self.executeSelectOneFunction('lm_findTaxon', 
                                                   taxonSourceId, taxonkey)
      except Exception, e:
         raise e
      sciname = self._createScientificName(row, idxs)
      return sciname

# ...............................................
   def findOrUpdateTaxonRec(self, taxonSourceId, taxonkey, kingdomStr, phylumStr, 
                            classStr, orderStr, familyStr, genusStr, scinameStr, 
                            genuskey, specieskey, keyhierarchy, count):
      updated = False
      scinameId = None
      currtime = mx.DateTime.gmt().mjd
      try:
         row, idxs = self.executeSelectOneFunction('lm_findOrUpdateTaxon', taxonSourceId,
                                                   taxonkey, kingdomStr, phylumStr, 
                                                   classStr, orderStr, familyStr, 
                                                   genusStr, scinameStr, genuskey, 
                                                   specieskey, keyhierarchy, 
                                                   count, currtime)
      except Exception, e:
         raise e
      if row is not None:
         scinameId = row[idxs['scientificnameid']]
         modtime = row[idxs['datelastmodified']]
         if modtime == currtime:
            updated = True
            self.log.debug('Updated %s' % scinameStr)
         else:
            self.log.debug('Static %s' % scinameStr)
      return scinameId, updated

# ...............................................
   def insertTaxonRec(self, taxonSourceId, taxonkey, kingdomStr, phylumStr, 
                      classStr, orderStr, familyStr, genusStr, scinameStr, 
                      genuskey, specieskey, keyhierarchy, count):
      inserted = False
      scinameId, updated = self.findOrUpdateTaxonRec(taxonSourceId, taxonkey, 
                                                     kingdomStr, phylumStr, 
                                                     classStr, orderStr, 
                                                     familyStr, genusStr, 
                                                     scinameStr, genuskey, 
                                                     specieskey, keyhierarchy, 
                                                     count)
      if scinameId is None:
         try:
            scinameId = self.executeInsertFunction('lm_insertTaxon',taxonSourceId, 
                                                 taxonkey, kingdomStr, phylumStr, 
                                                 classStr, orderStr, familyStr, 
                                                 genusStr, scinameStr, genuskey, 
                                                 specieskey, keyhierarchy, count, 
                                                 mx.DateTime.gmt().mjd)
         except Exception, e:
            raise e
         inserted = True
         self.log.debug('Inserted taxon %d, %s' % (scinameId, scinameStr))

      return scinameId, updated, inserted
   
# ...............................................
   def insertTaxon(self, sciName):
      inserted = False
      currtime = mx.DateTime.gmt().mjd
      try:
         scinameId = self.executeInsertFunction('lm_insertTaxon', 
                                                sciName._sourceId,
                                                sciName._sourceSpeciesKey,
                                                sciName.kingdom, sciName.phylum,
                                                sciName.txClass, sciName.txOrder,
                                                sciName.family, sciName.genus,
                                                sciName.scientificName,
                                                sciName._sourceGenusKey,
                                                sciName._sourceSpeciesKey,
                                                sciName._sourceKeyHierarchy,
                                                sciName.lastOccurrenceCount,
                                                currtime)
      except Exception, e:
         raise e
      else:
         sciName.setId(scinameId)
      self.log.debug('Inserted taxon %d, %s' % (scinameId, sciName.scientificName))

# ...............................................
   def deleteTaxon(self, sciName):
      jsuccess = self.executeModifyFunction('lm_deleteTaxon', sciName.getId())
      return jsuccess
      

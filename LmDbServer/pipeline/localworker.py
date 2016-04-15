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
import csv
import mx.DateTime as dt
import os
from osgeo.ogr import wkbPoint
import sys

from LmBackend.common.occparse import OccDataParser

from LmCommon.common.apiquery import BisonAPI, GbifAPI, IdigbioAPI
from LmCommon.common.lmconstants import (BISON_OCC_FILTERS, BISON_HIERARCHY_KEY,
            BISON_MIN_POINT_COUNT, Instances, ProcessType, DEFAULT_EPSG, 
            DEFAULT_POST_USER, JobStatus, ONE_DAY, ONE_HOUR, ONE_MIN,
            IDIGBIO_GBIFID_FIELD)

from LmDbServer.common.localconstants import WORKER_JOB_LIMIT
from LmDbServer.pipeline.pipeline import _Worker

from LmServer.base.lmobj import LMError, LmHTTPError
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import (JobFamily, Priority, 
                                         PrimaryEnvironment, LOG_PATH)
from LmServer.common.localconstants import (ARCHIVE_USER, POINT_COUNT_MIN, 
                                            APP_PATH, DATASOURCE)
from LmServer.db.scribe import Scribe
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.omJob import OmProjectionJob, OmModelJob
from LmServer.sdm.meJob import MeProjectionJob, MeModelJob
from LmServer.sdm.sdmJob import SDMOccurrenceJob

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR
GBIF_SERVICE_INTERVAL = 3 * ONE_MIN            

# .............................................................................
class _LMWorker(_Worker):
   def __init__(self, lock, threadSpeed, pipelineName, updateInterval, 
                startStatus=None, queueStatus=None, endStatus=None, 
                threadSuffix=None):
      
      _Worker.__init__(self, lock, pipelineName, threadSuffix=threadSuffix)
      self.threadSpeed = threadSpeed
      self.startStatus = startStatus
      self.queueStatus = queueStatus
      self.updateInterval = updateInterval
      self.updateTime = None
      self._gbifQueryTime = None
      self.simpleChecked = set()

      try:
         self._scribe = Scribe(self.log)
         success = self._scribe.openConnections()

      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         self._failGracefully(e)

      if success:
         self.log.info('%s opened databases' % (self.name))
      else:
         raise LMError('%s failed to open databases' % self.name)
   
# ...............................................
   def _findStart(self):
      linenum = 0
      if os.path.exists(self.startFile):
         with open(self.startFile, 'r') as f:
            line = f.read()
            try:
               linenum = int(line)
            except:
               print 'Failed to interpret %s' % str(line)
            else:
               self.log.debug('Start location = %d' % linenum)
         if linenum > 0:
            os.remove(self.startFile)
      return linenum
                  
# ...............................................
   def _writeNextStart(self, linenum):
      try:
         f = open(self.startFile, 'w')
         f.write(str(linenum))
         f.close()
      except Exception, e:
         self.log.error('Failed to write %d to chainer start file %s' 
                        % (linenum, self.startFile))
   
# ...............................................
   def _rollbackQueuedJobs(self):
      if self.startStatus < self.queueStatus:
         try:
            self._getLock()
            count = self._scribe.rollbackIncompleteRADJobs(self.queueStatus)
            self.updateTime = dt.gmt().mjd
            self.log.debug('Reset %d queued (%d) jobs to completed status (%d)' 
                           % (count, self.queueStatus, self.startStatus))
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self.log.debug(str(e))
               
            self.log.error('Failed in _rollbackQueuedJobs %s' % str(e))
            raise
         finally:
            self._freeLock()
            
# ...............................................
   def _timeToRollback(self):
      if (self.updateTime is None 
          or dt.gmt().mjd - self.updateTime > TROUBLESHOOT_UPDATE_INTERVAL): 
         return True
      else:
         return False
                  
# ...............................................
   def _identifyEnvironment(self, canonicalName):
      """
      Sets the primary environment and deletes any models for occSets with these 
      names.
      """
      primaryEnv = PrimaryEnvironment.TERRESTRIAL
      if canonicalName in self._excludeList:
         primaryEnv = PrimaryEnvironment.MARINE
      return primaryEnv
   
# ...............................................
   def _getLocalOccurrencesets(self, name):
      occSets = []
      try:
         # For genus-only: exact match, for genus-species: all occurrencesets 
         # with display name starting with namestring, case-insensitive 
         # Convert to string in case of numeric 'names'
         occSets = self._scribe.getGBIFOccurrenceSetsForName(str(name))
      except Exception, e:
         self.log.error('Failed to get LM Occsets starting with %s (%s)'
                        % (name, str(e)))
      return occSets
   
# ...............................................
   def _notifyComplete(self, job):
      # Should already have lock
      success = True
      msg = None
      if job.email is not None:
         # Notify on Model only if fail
         if isinstance(job, (MeModelJob, OmModelJob)):
            if job.status >= JobStatus.NOT_FOUND:
               subject = 'Lifemapper SDM Model %d Error' % job.jid
               msg = ''.join(('The Lifemapper Species Distribution Modeling '
                              '(LmSDM) job that you requested has failed with code %d.  '
                              % job.status,
                              'You may visit %s for error information' 
                              % job.metadataUrl))
         # Notify on Projection only if it's the last one
         elif isinstance(job, (MeProjectionJob, OmProjectionJob)):
            model = job.projection.getModel()
            try:
               self._getLock()
               unfinishedCount = self._scribe.countProjections(job.userId, 
                                                               inProcess=True, 
                                                               mdlId=model.getId())
            except Exception, e:
               if not isinstance(e, LMError):
                  e = LMError(currargs=e.args, lineno=self.getLineno(), 
                              location=self.getLocation())
               raise e
            
            finally:
               self._freeLock()
   
            if unfinishedCount == 0:
               if job.status == JobStatus.COMPLETE:
                  subject = 'Lifemapper SDM Experiment %d Completed' % model.getId()
                  msg = ''.join(('The Lifemapper Species Distribution Modeling '
                                 '(LmSDM) job that you requested is now complete.  ',
                                 'You may visit %s to retrieve your data' 
                                 % job.metadataUrl)) 
               else:
                  subject = 'Lifemapper SDM Experiment %d Error' % model.getId()
                  msg = ''.join(('The Lifemapper Species Distribution Modeling '
                                 '(LmSDM) job that you requested has failed with code %d.  '
                                 % job.status,
                                 'You may visit %s for error information' 
                                 % job.metadataUrl))  
            else:
               self.log.info('Notify: %d unfinished projections for model %d, user %s'
                             % (unfinishedCount, model.getId(), job.email))
         if msg is not None:
            try:
               self._notifyPeople(subject, msg, recipients=[job.email])
               self.log.info('Notified: %s, user %s'
                             % (subject, job.email))
            except LMError, e:
               self.log.info('Failed to notify user %s of %s (%s)' % 
                             (job.email, subject, str(e)))
               success = False
      return success         

# ...............................................
   def _findSingleOccset(self, taxonName, originalOcc):
      """
      Create, update, or delete Name
      """
      deletedOriginal = False
      matchingOcc = None
      occSets = self._getLocalOccurrencesets(taxonName)

      for occ in occSets:
         if occ.displayName == taxonName:
            matchingOcc = occ
         else:
            try:
               deleted = self._scribe.completelyRemoveOccurrenceSet(occ)
            except Exception, e:
               self.log.error('Failed to completely remove similar occurrenceSet %s/%s (%s)'
                              % (str(occ.getId()), occ.displayName, str(e)))
            else:
               self.log.debug('   removed similar occurrenceset %s/%s in MAL' 
                              % (str(occ.getId()), occ.displayName))

      if originalOcc is not None: 
         if (originalOcc.displayName != taxonName or 
             (matchingOcc is not None and originalOcc.getId() != matchingOcc.getId())):
            try:
               deletedOriginal = self._scribe.completelyRemoveOccurrenceSet(originalOcc)
            except Exception, e:
               self.log.error('Failed to completely remove occurrenceSet %s with synonym %s (%s)'
                              % (str(originalOcc.getId()), originalOcc.displayName, str(e)))
            else:
               self.log.debug('   removed occurrenceset %s/ with synonym %s in MAL' 
                              % (str(originalOcc.getId()), originalOcc.displayName))

      return matchingOcc, deletedOriginal

   # ...............................................
   def _deleteOccurrenceSet(self, occSet):
      try:
         deleted = self._scribe.completelyRemoveOccurrenceSet(occSet)
      except Exception, e:
         self.log.error('Failed to completely remove occurrenceSet %s (%s)'
                        % (str(occSet.getId()), str(e)))
      else:
         self.log.debug('   removed occurrenceset %s/%s in MAL' 
                        % (str(occSet.getId()), occSet.displayName))
      return deleted
         
# ...............................................
   def _getInsertSciNameForGBIFSpeciesKey(self, speciesKey):
      """
      Returns an existing or newly inserted ScientificName
      """
      try:
         self._getLock()           
         sciName = self._scribe.findTaxon(self._taxonSourceId, 
                                              speciesKey)
         if sciName is None:
            # Use API to get and insert species name 
            try:
               (kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr,
                speciesStr, genuskey, retSpecieskey) = GbifAPI.getTaxonomy(speciesKey)
            except LmHTTPError, e:
               self.log.info('Failed lookup for key {}, ({})'.format(
                                                      speciesKey, e.msg))
            if retSpecieskey == speciesKey:
               currtime = dt.gmt().mjd
               sciName = ScientificName(speciesStr, 
                               kingdom=kingdomStr, phylum=phylumStr, 
                               txClass=None, txOrder=orderStr, 
                               family=familyStr, genus=genusStr, 
                               createTime=currtime, modTime=currtime, 
                               taxonomySourceId=self._taxonSourceId, 
                               taxonomySourceKey=speciesKey, 
                               taxonomySourceGenusKey=genuskey, 
                               taxonomySourceSpeciesKey=speciesKey)
               self._scribe.insertTaxon(sciName)
      except LMError, e:
         raise e
      except Exception, e:
         raise LMError(currargs=e.args, lineno=self.getLineno())
      finally:
         self._freeLock()
         
      return sciName
         
# ...............................................
   def _processSDMChainForDynamicQuery(self, sciname, taxonSourceKeyVal, dataCount):
      currtime = dt.gmt().mjd
      occ = None
      jobs = []
      try:
         occs = self._scribe.getOccurrenceSetsForScientificName(sciname, 
                                                             ARCHIVE_USER)
         if not occs:
            occ = OccurrenceLayer(sciname.scientificName, 
                  name=sciname.scientificName, fromGbif=False, 
                  queryCount=dataCount, epsgcode=DEFAULT_EPSG, 
                  ogrType=wkbPoint, userId=ARCHIVE_USER,
                  primaryEnv=PrimaryEnvironment.TERRESTRIAL, createTime=currtime, 
                  status=JobStatus.INITIALIZE, statusModTime=currtime, 
                  sciName=sciname)
            occid = self._scribe.insertOccurrenceSet(occ)
         elif len(occs) == 1:
            if occs[0].statusModTime > 0 and occs[0].statusModTime < self._obsoleteTime:
               occ = occs[0]
            else:
               self.log.debug('Occurrenceset %d (%s) is up to date' 
                              % (occs[0].getId(), sciname.scientificName))
         else:
            raise LMError('Too many (%d) occurrenceLayers for %s'
                          % (len(occs), sciname.scientificName))
            
         if occ is not None:
            url = self._getQueryUrl(taxonSourceKeyVal)
            occ.setRawDLocation(url, currtime)
            # Create jobs for Archive Chain: occurrence population, 
            # model, projection, and (later) intersect computation
            jobs = self._scribe.initSDMChain(ARCHIVE_USER, occ, self.algs, 
                                      self.modelScenario, 
                                      self.projScenarios, 
                                      occJobProcessType=ProcessType.BISON_TAXA_OCCURRENCE, 
                                      priority=Priority.NORMAL, 
                                      intersectGrid=self.intersectGrid,
                                      minPointCount=BISON_MIN_POINT_COUNT)
            self.log.debug('Created %d jobs for occurrenceset %d' 
                           % (len(jobs), occ.getId()))
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e



# .............................................................................
class Troubleshooter(_LMWorker):
   def __init__(self, lock, pipelineName, updateInterval,
                archiveDataDeleteTime=None):
      threadspeed = WORKER_JOB_LIMIT/2
      self.archiveDataDeleteTime = archiveDataDeleteTime
      _LMWorker.__init__(self, lock, threadspeed, pipelineName, updateInterval)
      
# ...............................................
   def run(self):
      while not(self._existKillFile()):
         currtime = dt.gmt().mjd
         if (self.updateTime is None or
             currtime - self.updateTime > TROUBLESHOOT_UPDATE_INTERVAL): 
            oldtime = currtime - TROUBLESHOOT_UPDATE_INTERVAL
            self.updateTime = currtime
            try:
               # each method below: LOCKED then UNLOCKED
               self._moveAllDependentJobs()
#                self._createJobsForReadyObjects()
               self._deleteOldAnonymous(self.threadSpeed)
#               # All done - subspecies and old-style binomials 
#                count = self._deleteSubspeciesOccurrenceSets(self.threadSpeed)
#               # All done - subspecies and old-style binomials 
#                if self.archiveDataDeleteTime is not None:
#                   count = self._deleteObsoleteOccurrenceSets(self.archiveDataDeleteTime, 
#                                                              self.threadSpeed*4)
#                # TODO: Revisit these
#                count = self._rollbackLifelessJobs()
#                self._notifyOfStalledExperiments(oldtime, JobStatus.COMPLETE)
#                self._notifyOfLimboExperiments(oldtime, JobStatus.COMPLETE, 
#                                               JobStatus.GENERAL_ERROR)
            except Exception, e:
               if not isinstance(e, LMError):
                  e = LMError(currargs=e.args, lineno=self.getLineno())
               self._failGracefully(e)
               break

      if self._existKillFile():
         self._failGracefully(None)
            
# ...............................................
   def _rollbackLifelessJobs(self):
      stotal = rtotal = 0
      sdmLimit = dt.gmt().mjd - ONE_DAY
      radLimit = dt.gmt().mjd - (ONE_DAY * 2)
      try:
         self._getLock()
         stotal = self._scribe.rollbackLifelessJobs(sdmLimit, jobFamily=JobFamily.SDM)
         self.log.debug('Rolled back %d SDM jobs' % stotal)
#          rtotal = self._scribe.rollbackLifelessJobs(radLimit, jobFamily=JobFamily.RAD)
#          self.log.debug('Rolled back %d RAD jobs' % rtotal)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      finally:
         self._freeLock()
         
      return stotal + rtotal

# ...............................................
   def _deleteOldAnonymous(self, count):
      tooOldTime = dt.gmt().mjd - (ONE_DAY * 14)
      try:
         self._getLock()
         atoms = self._scribe.listModels(0, count, userId=DEFAULT_POST_USER, 
                                   beforeTime=tooOldTime)
      
         self.log.info('Deleting %d experiments for %s' % (len(atoms), DEFAULT_POST_USER))
         for atm in atoms:
            model = self._scribe.getModel(atm.getId())
            if model is not None:
               success = self._scribe.deleteExperiment(model)
               self.log.info ('   Success %s deleting experiment %d' 
                              % (str(success), atm.getId()))
            else:
               self.log.warning('  Unable to get model %d' % atm.getId())
               
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

      finally:
         self._freeLock()
         
   # ...............................................
   def _deleteObsoleteOccurrenceSets(self, cutofftime, count):
      total = 0
      try:
         self._getLock() 
         # Cleanup old GBIF occurrenceSets and associated experiments
         oldOccs = self._scribe.listOccurrenceSets(0, count, userId=ARCHIVE_USER, 
                                             beforeTime=cutofftime, atom=False)
         self.log.debug('Found %d obsolete Occs to delete' % len(oldOccs))
         for occ in oldOccs:
            try:
               success = self._scribe.completelyRemoveOccurrenceSet(occ)
            except Exception, e:
               self.log.error(str(e))
            else:
               if success:
                  total += 1
               else:
                  self.log.error('Failed to remove occ %d' % occ.getId())
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      finally:
         self._freeLock()

      return total
   
   # ...............................................
   def _deleteSubspeciesOccurrenceSets(self, count):
      total = 0
      try:
         self._getLock()
         # Cleanup old GBIF occurrenceSets and associated experiments
         occs = self._scribe.getOccurrenceSetsSubSpecies(count, ARCHIVE_USER)
         self.log.debug('Found %d sub-species Occs to delete' % len(occs))
      
         for occ in occs: 
            if occ._scientificName is None or occ._scientificName.rank is None:
               try:
                  success = self._scribe.completelyRemoveOccurrenceSet(occ)
                  if success:
                     total += 1
                     if occ._scientificName is not None:
                        success = self._scribe.deleteTaxon(occ._scientificName)
               except Exception, e:
                  self.log.error(str(e))
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      finally:
         self._freeLock()

      return total

# ...............................................
   def _moveAllDependentJobs(self):
      try:
         self._getLock()
         sdmCount, radCount = self._scribe.moveAllDependentJobs()
         self.log.info('Moved %d dependent SDM jobs; %d dependent RAD jobs' 
                       % (sdmCount, radCount))             
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

      finally:
         self._freeLock()
               
# ...............................................
   def _notifyOfStalledExperiments(self, oldtime, pipelineCompleteStatus):
      try:
         self._getLock()
         models, projs = self._scribe.findProblemObjects(oldtime, 
                                                endStat=pipelineCompleteStatus, 
                                                ignoreUser=ARCHIVE_USER)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

      finally:
         self._freeLock()
         
      probs = self._organizeProblemObjects(models, 'Model')
      pprobs = self._organizeProblemObjects(projs, 'Projection')
      for usr in pprobs.keys():
         if probs.has_key(usr):
            probs[usr]['Projection'] = pprobs[usr]['Projection']
         
      if probs.keys():
         msg = ('Stalled SDM User Data started before %s (mjd=%d)' 
                % (dt.DateTimeFromMJD(oldtime).localtime().Format()))
         for usr in probs.keys():
            msg += '%s\n' % usr
            msg += '  ModelId  Status\n'
            for m in probs[usr]['Model']:
               msg += '  %s     %s\n' % (m.getId(), m.status)
         self.log.debug(msg)
         self._notifyPeople('Stalled user experiments', msg)
      
# ...............................................
   def _notifyOfLimboExperiments(self, oldtime, pipelineCompleteStatus,
                                 errorStartStatus):
      try:
         self._getLock()
         models, projs = self._scribe.findProblemObjects(oldtime, 
                                                startStat=pipelineCompleteStatus,
                                                endStat=errorStartStatus,
                                                ignoreUser=ARCHIVE_USER)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

      finally:
         self._freeLock()
         
      probs = self._organizeProblemObjects(models, 'Model')
      pprobs = self._organizeProblemObjects(projs, 'Projection')
      for usr in pprobs.keys():
         if probs.has_key(usr):
            probs[usr]['Projection'] = pprobs[usr]['Projection']
         
      if probs.keys():
         msg = ('Limbo SDM User Data started before %s (mjd=%d)' 
                % (dt.DateTimeFromMJD(oldtime).localtime().Format()))
         for usr in probs.keys():
            msg += '%s\n' % usr
            msg += '  ModelId  Status\n'
            for m in probs[usr]['Model']:
               msg += '  %s     %s\n' % (m.getId(), m.status)
         self.log.debug(msg)
         self._notifyPeople('Limbo user experiments', msg)
   
# ...............................................
   def _organizeProblemObjects(self, objects, objname):
      problems = {}
      for o in objects:
         usr = o.getUserId()
         if not (problems.has_key(usr)):
            problems[usr] = {objname: set([])}
         problems[usr][objname].add(o)
      return problems
   
# .............................................................................
class Infiller(_LMWorker):
   """
   @summary: Creates jobs and objects for SDM processes ready to run.
                1. Creates new experiments for occurrencesets not yet modeled, 
                   based on default algorithms, model and projection scenarios - 
                   this is only executed if we modify the default algorithms or 
                   scenarios.
                2. Creates new projections for models not yet projected onto  
                   default scenarios - this is only executed if we modify 
                   scenarios to the default set.
                3. If an occurrenceset has data newer than its experiments, 
                   re-initialize existing models and projections so they are 
                   re-executed.
   """
   def __init__(self, lock, pipelineName, updateInterval, algLst, 
                mdlScen, prjScenLst, mdlMask=None, prjMask=None,
                intersectGrid=None):
      threadspeed = WORKER_JOB_LIMIT * 2
      _LMWorker.__init__(self, lock, threadspeed, pipelineName, updateInterval, 
                         startStatus=JobStatus.GENERAL, 
                         queueStatus=JobStatus.GENERAL, 
                         endStatus=JobStatus.INITIALIZE)
      self.algs = algLst
      self.modelScenario = mdlScen
      self.projScenarios = prjScenLst
      self.modelMask = mdlMask
      self.projMask = prjMask      
      self.intersectGrid = intersectGrid
      self._sleeptime = self._sleeptime * 10
      
# ...............................................
   def run(self):
      infillTotal = 0
      # Gets and frees lock for each set of Absent/Incomplete Experiments
      while not(self._existKillFile()):
         # Rollback obsolete
         try:
            infillTotal = self._rollbackExperimentsWithNewData()
            self.log.info('Rollback/delete %d experiments with new data' % infillTotal)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self._failGracefully(e)
            break
         
         # Create missing jobs
         try:
            occTotal = self._createJobsForReadyOccs()
            infillTotal += occTotal
#             mdlTotal = self._createJobsForReadyModels()
#             prjTotal = self._createJobsForReadyProjs()
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self._failGracefully(e)
            break

         # Fill in archive algorithm/scenario/species gaps
         for alg in self.algs:
            try:
               # Up-to-date occurrence sets not modeled with this algorithm and 
               # scenario combo; LOCKED then UNLOCKED
               mdlCount = self._initAbsentExperiments(alg, ARCHIVE_USER, 
                                                      Priority.NORMAL)
               infillTotal += mdlCount
#                # Initialize missing projections for models matching request
#                prjCount = self._initIncompleteExperiments(alg, Priority.NORMAL)
               if self._existKillFile():
                  break
                
            except Exception, e:
               if not isinstance(e, LMError):
                  e = LMError(currargs=e.args, lineno=self.getLineno())
               self._failGracefully(e)
               break
         
         if infillTotal == 0:
            self.log.info('Nothing to infill; catnapping for 12 hours')
            self._wait(360*2)

      if self._existKillFile():
         self._failGracefully(None)

# ...............................................
   def _createJobsForReadyModels(self):
      try:
         self._getLock()
         models = self._scribe.getReadyModelsWithoutJobs(self.threadSpeed, 
                                                         JobStatus.INITIALIZE)
         for mdl in models:
            mdlJob, notJob = self._scribe.reinitSDMModel(mdl, Priority.OBSOLETE,
                                                         modtime=dt.gmt().mjd, 
                                                         doRollback=False)
         self.log.debug('Created %d jobs for jobless models' % len(models))
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      finally:
         self._freeLock()
      return len(models)
                     
# ...............................................
   def _createJobsForReadyProjs(self):   
      try:
         self._getLock()
         projs = self._scribe.getReadyProjectionsWithoutJobs(self.threadSpeed, 
                                                             JobStatus.INITIALIZE,
                                                             JobStatus.COMPLETE)
         for prj in projs:
            job = self._scribe.reinitSDMProjection(prj, Priority.OBSOLETE, 
                                                   doRollback=False)
         self.log.debug('Created %d jobs for jobless projections' % len(projs))
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      finally:
         self._freeLock()
      return len(projs)

# ...............................................
   def _createJobsForReadyOccs(self):
      total = 0
      try:
         self._getLock()
         # This finds occurrencesets < JobStatus.COMPLETE without a job
         occSets = self._scribe.findUnfinishedJoblessOccurrenceSets(self.threadSpeed)
         for occ in occSets:
            thistotal = self._scribe.reinitSDMExperiment(occ)
#             # make sure raw input data is valid
#             processtype = None
#             if occ.getRawDLocation() is not None:
#                if DATASOURCE == Instances.BISON:
#                   processtype = ProcessType.BISON_TAXA_OCCURRENCE
#                elif DATASOURCE == Instances.GBIF and os.path.exists(occ.getRawDLocation()):
#                   processtype = ProcessType.GBIF_TAXA_OCCURRENCE
#                elif DATASOURCE == Instances.IDIGBIO and os.path.exists(occ.getRawDLocation()):
#                   processtype = ProcessType.IDIGBIO_TAXA_OCCURRENCE
#                else:
#                   raise LMError('Unknown DATASOURCE %s' % str(DATASOURCE))
#             # inits jobs for occset, model and projections
#             if processtype is None:
#                occ.updateStatus(JobStatus.LM_RAW_POINT_DATA_ERROR, dt.gmt().mjd)
#                self._scribe.updateOccset(occ)
#             else:
#                jobs = self._scribe.initSDMChain(ARCHIVE_USER, occ, self.algs, 
#                                          self.modelScenario, 
#                                          self.projScenarios, 
#                                          occJobProcessType=processtype,
#                                          priority=Priority.OBSOLETE, 
#                                          intersectGrid=None,
#                                          minPointCount=POINT_COUNT_MIN)
            total += thistotal
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      finally:
         self._freeLock()
      self.log.debug('Created %d chained jobs for jobless Occsets' % total)
      return total

# ...............................................
   def _initAbsentExperiments(self, alg, userid, priority):
      jobCount = 0
      exptime = dt.gmt().mjd - self.updateInterval
      try:
         self._getLock()
         # This only finds occurrencesets < JobStatus.GeneralError
         occSets = self._scribe.findUnmodeledOccurrenceSets(ARCHIVE_USER,
                        self.threadSpeed, PrimaryEnvironment.TERRESTRIAL, 
                        POINT_COUNT_MIN, alg, self.modelScenario, 
                        JobStatus.GENERAL_ERROR)
         self._freeLock()
         
         for occ in occSets:
            # inits jobs for model and projections
            self._getLock()
            if occ.status == JobStatus.COMPLETE:
               processtype = None
            elif occ.status == JobStatus.INITIALIZE:
               # default is arbitrary CSV data with metadata
               processtype = ProcessType.USER_TAXA_OCCURRENCE
               
               if DATASOURCE == Instances.BISON:
                  processtype = ProcessType.BISON_TAXA_OCCURRENCE
               elif DATASOURCE == Instances.IDIGBIO:
                  processtype = ProcessType.IDIGBIO_TAXA_OCCURRENCE
            else:
               # Can't re-init GBIF OccurrenceJobs, chainer gets data serially
               break
            
            jobs = self._scribe.initSDMChain(ARCHIVE_USER, occ, [alg], 
                                      self.modelScenario, 
                                      self.projScenarios, 
                                      occJobProcessType=processtype,
                                      priority=Priority.NORMAL, 
                                      intersectGrid=None,
                                      minPointCount=POINT_COUNT_MIN)
            jobCount += len(jobs)
            self._freeLock()
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

      finally:
         self._freeLock()
      self.log.debug('Processed %d unmodeled occurrencesets; created %d jobs' 
                     % (len(occSets), jobCount))
      return jobCount
   
# ...............................................
   def _initIncompleteExperiments(self, alg, priority):
      count = 0
      try:
         for prjScen in self.projScenarios:
            self._getLock()
            models = self._scribe.getUnprojectedArchiveModels(self.threadSpeed, 
                                             alg, self.modelScenario, prjScen)
            self._freeLock()
            
            for mdl in models:
               # init job for projection
               self._getLock()
               pjob = self._scribe.initSDMProjectionJob(mdl, prjScen, priority,
                                                    prjMask=self.projMask)
               self._freeLock()
               if pjob is not None:
                  count += 1

      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      
      finally:
         self._freeLock()
      
      return count
               
# ...............................................
   def _rollbackExperimentsWithNewData(self):
      expids = []
      try:
         self._getLock()
         sdmExps = self._scribe.getOutdatedSDMArchiveExperiments(self.threadSpeed)
         for exp in sdmExps:
            self._scribe.rollbackOrDeleteExperiment(exp, Priority.OBSOLETE)
            expids.append(exp.getId())
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      
      finally:
         self._freeLock()
      
      return len(sdmExps)

"""
http://bisonapi.usgs.ornl.gov/solr/occurrences/select/?
q=ITISscientificName:/[A-Za-z]*[%20]{1,1}[A-Za-z]*/%20
  AND%20decimalLatitude:[-15%20TO%2072]
&rows=0
&facet=true
&facet.field=TSNs
&facet.limit=-1
&facet.mincount=20
&wt=json

http://bisonapi.usgs.ornl.gov/solr/occurrences/select/?
q=hierarchy_homonym_string:*%5C-202422%5C-*%20
  AND%20decimalLatitude:%5b0%20TO%2090%5d%20
  NOT%20basisOfRecord:living%20
  NOT%20basisOfRecord:fossil
&rows=0
&facet=true
&facet.field=ITISscientificName
&facet.limit=-1
&facet.mincount=20
&facet.sort=count
&wt=json
"""
# ..............................................................................
class BisonChainer(_LMWorker):
   """
   @summary: Initializes the job chainer for BISON.
   """
   def __init__(self, lock, pipelineName, updateInterval, algLst, mdlScen, 
                prjScenLst, tsnfilename, expDate, taxonSource=None, 
                mdlMask=None, prjMask=None, intersectGrid=None):
      threadspeed = WORKER_JOB_LIMIT
      _LMWorker.__init__(self, lock, threadspeed, pipelineName, None,
                         updateInterval)
      
      if taxonSource is None:
         self._failGracefully('Missing taxonomic source')
      else:
         self._taxonSourceId = taxonSource
      self.startFile = os.path.join(APP_PATH, LOG_PATH, 'start.%s.txt' % pipelineName)
      self.algs = algLst
      self.modelScenario = mdlScen
      self.projScenarios = prjScenLst
      self.modelMask = mdlMask
      self.projMask = prjMask
      self.intersectGrid = intersectGrid
      self._recnum = 0
      self._tsnfile = open(tsnfilename, 'r')
      self._currRec = self._getTsnRec()
      self._obsoleteTime = expDate
      # Start mid-file if necessary 
      self._skipAhead()
      
# ...............................................
   @property
   def nextStart(self):
      return self._recnum

# ...............................................
   def _getItisTsn(self):
      if self._currRec is not None:
         return self._currRec[0]
      else:
         return None
   
# ...............................................
   def _getTsnCount(self):
      if self._currRec is not None:
         return self._currRec[1]
      else:
         return None
      
# ...............................................
   def _getTsnRec(self):
      eof = success = False
      tsnRec = None
      while not eof and not success:
         line = self._tsnfile.readline()
         if line == '':
            eof = True
         else:
            try:               
               first, second = line.split(',')
               # Returns TSN, TSNCount
               tsnRec = (int(first), int(second))
               self._recnum += 1
               success = True
            except Exception, e:
               self.log.debug('Exception reading line %d (%s)' % (self._recnum, str(e)))
      return tsnRec

# ...............................................
   def run(self):
      allFail = True
      # Gets and frees lock for each name checked
      while (not(self._existKillFile())):
         try:
            while self._currRec is not None:
               occ = None
               tsn = self._getItisTsn()
               try:
                  self._getLock()           
                  self.processInputItisTSN(tsn, self._getTsnCount())
               finally:
                  self._freeLock()
               self._currRec = self._getTsnRec()
               
               # Check for stop signal
               if self._existKillFile():
                  self.log.info('LAST CHECKED line %d (stopped with killfile)' 
                                % (self._recnum))
                  self._failGracefully(None)
                  break

            # Check for end of data
            if self._currRec is None:
               allFail = False
               self.signalKill(allFail=allFail)
               self.log.info('Chainer complete, last rec num = %d (next -9999)' 
                             % self._recnum)
               self._recnum = -9999
               break

         except Exception, e:
            self.log.info('LAST CHECKED line %d (exception)' % (self._recnum))
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self._failGracefully(e)
            break

      if self._existKillFile():
         self._failGracefully(None, allFail=allFail)

# ...............................................
   def _failGracefully(self, lmerr, linenum=None, allFail=True):
      if linenum is None:
         linenum = self.nextStart
      self._writeNextStart(linenum)
      _LMWorker._failGracefully(self, lmerr, allFail=allFail)
      
# ...............................................
   def _getQueryUrl(self, speciesTsn):
      occAPI = BisonAPI(qFilters={BISON_HIERARCHY_KEY: '*-%d-*' % speciesTsn}, 
                        otherFilters=BISON_OCC_FILTERS)
      # TODO: Remove this when we can properly parse XML file with full URL
      occAPI.clearOtherFilters()
      return occAPI.url

# ...............................................
   def _processInputItisTSN(self, speciesTsn, dataCount):
      try:
         sciName = self._getInsertSciNameForItisTSN(speciesTsn, dataCount)
         self._processSDMChainForDynamicQuery(sciName, speciesTsn, dataCount, 
                                              ProcessType.BISON_TAXA_OCCURRENCE,
                                              BISON_MIN_POINT_COUNT)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

# ...............................................
   def _skipAhead(self):
      linenum = self._findStart()  
      if linenum < 0:
         self._recnum = linenum
         self._currRec = None
      else:
         self._currRec = self._getTsnRec()      
         while self._currRec is not None and self._recnum < linenum:
            self._currRec = self._getTsnRec()
         
# ...............................................
# ...............................................
   def  _getInsertSciNameForItisTSN(self, itisTsn, tsnCount):
      if itisTsn is None:
         return None
      sciname = self._scribe.findTaxon(self._taxonSourceId, itisTsn)
         
      if sciname is None:
         try:
            (itisname, king, tsnHier) = BisonAPI.getItisTSNValues(itisTsn)
            self._wait(20)
         except Exception, e:
            self.log.error('Failed to get results for ITIS TSN %s (%s)' % (str(itisTsn), str(e)))
         else:
            if itisname is not None and itisname != '':
               sciname = ScientificName(itisname, kingdom=king,
                                     lastOccurrenceCount=tsnCount, 
                                     taxonomySourceId=self._taxonSourceId, 
                                     taxonomySourceKey=itisTsn, 
                                     taxonomySourceSpeciesKey=itisTsn,
                                     taxonomySourceKeyHierarchy=tsnHier)
               self._scribe.insertTaxon(sciname)
      return sciname

# ..............................................................................
class UserChainer(_LMWorker):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, lock, pipelineName, updateInterval, 
                algLst, mdlScen, prjScenLst, occDataFname, occMetaFname, expDate,
                mdlMask=None, prjMask=None, intersectGrid=None):
      threadspeed = WORKER_JOB_LIMIT
      _LMWorker.__init__(self, lock, threadspeed, pipelineName, updateInterval)
      self.startFile = os.path.join(APP_PATH, LOG_PATH, 'start.%s.txt' % pipelineName)
      
      try:
         self.occParser = OccDataParser(self.log, occDataFname, occMetaFname)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         self._failGracefully(e)

      self.algs = algLst
      self.modelScenario = mdlScen
      self.projScenarios = prjScenLst
      self.modelMask = mdlMask
      self.projMask = prjMask
      self.intersectGrid = intersectGrid
      self._obsoleteTime = expDate
      # Start mid-file? Assumes first line is header
      linenum = self._findStart()
      if linenum > 2:
         self.occParser.skipToRecord(linenum)
      
# ...............................................
   @property
   def nextStart(self):
      try:
         num = self.occParser.keyFirstRec
      except:
         num = 0
      return num

# ...............................................
   def run(self):
      allFail = True
      killMeNow = False
      # Gets and frees lock for each name checked
      while (not(self._existKillFile())):
         try:
            while not(self.occParser.eof()):
               occ = None
               chunk = self.occParser.pullCurrentChunk()
               self._processInputSpecies(chunk)
                           
               if self._existKillFile():
                  break
                           
            if self.occParser.eof():
               killMeNow = True
            self.occParser.close()
               
            if killMeNow:
               self.signalKill(allFail=False)
               self.log.info('Chainer complete')
               break

         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self._failGracefully(e)
            break

      if self._existKillFile():
         self.log.info('LAST CHECKED line {} (killfile)'.format(self.nextStart))
         self._failGracefully(None, allFail=allFail)

# ...............................................
   def _simplifyName(self, longname):
      front = longname.split('(')[0]
      newfront = front.split(',')
      finalfront = front.strip()
      return finalfront
   
# ...............................................
   def _processInputSpecies(self, dataChunk):
      currtime = dt.gmt().mjd
      occ = None
      spname = self._simplifyName(self.occParser.nameValue)
      try:
         self._getLock()           
         occs = self._scribe.getOccurrenceSetsForName(spname, userid=ARCHIVE_USER)
         if not occs:
            # Populate metadataUrl with metadata filename for computation
            occ = OccurrenceLayer(spname, name=spname, 
                  queryCount=len(dataChunk), epsgcode=DEFAULT_EPSG, 
                  ogrType=wkbPoint, ogrFormat='CSV', 
                  primaryEnv=PrimaryEnvironment.TERRESTRIAL,
                  userId=ARCHIVE_USER, createTime=currtime, 
                  status=JobStatus.INITIALIZE, statusModTime=currtime)
            occid = self._scribe.insertOccurrenceSet(occ)
         elif len(occs) == 1:
            if (occs[0].status > JobStatus.COMPLETE or 
                occs[0].statusModTime < self._obsoleteTime):
               occ = occs[0]
            else:
               self.log.debug('OccurrenceSet %d is up to date (%.2f)' 
                              % (occs[0].getId(), occs[0].statusModTime))
         else:
            raise LMError('Too many (%d) occurrenceLayers for %s'
                          % (len(occs), spname))
            
         if occ is not None:
            # Write new raw data
            processtype = ProcessType.USER_TAXA_OCCURRENCE
            rdloc = occ.createLocalDLocation(raw=True)
            success = occ.writeCSV(dataChunk, dlocation=rdloc, overwrite=True,
                                   header=self.occParser.header)
            if not success:
               self.log.debug('Unable to write CSV file %s' % rdloc)
            else:
               occ.setRawDLocation(rdloc, currtime)
               # Create jobs for Archive Chain: occurrence population, 
               # model, projection, and (later) intersect computation
               jobs = self._scribe.initSDMChain(ARCHIVE_USER, occ, self.algs, 
                                         self.modelScenario, 
                                         self.projScenarios, 
                                         occJobProcessType=processtype,
                                         priority=Priority.NORMAL, 
                                         intersectGrid=None,
                                         minPointCount=POINT_COUNT_MIN)
               self.log.debug('Init {} jobs for {} ({} points, occid {})'.format(
                              len(jobs), spname, len(dataChunk), occ.getId()))
      except LMError, e:
         raise e
      except Exception, e:
         raise LMError(currargs=e.args, lineno=self.getLineno())
      finally:
         self._freeLock()


# ...............................................
   def _failGracefully(self, lmerr, linenum=None, allFail=True):
      if linenum is None: 
         try:
            linenum = self.nextStart
         except:
            pass
      if linenum:
         self._writeNextStart(linenum)
      _LMWorker._failGracefully(self, lmerr, allFail=allFail)

# ..............................................................................
class GBIFChainer(_LMWorker):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, lock, pipelineName, updateInterval, 
                algLst, mdlScen, prjScenLst, occfilename, expDate,
                fieldnames, keyColname, taxonSource=None, 
                providerKeyFile=None, providerKeyColname=None,
                mdlMask=None, prjMask=None, intersectGrid=None):
      threadspeed = WORKER_JOB_LIMIT
      _LMWorker.__init__(self, lock, threadspeed, pipelineName, updateInterval)
      self.startFile = os.path.join(APP_PATH, LOG_PATH, 'start.%s.txt' % pipelineName)
      if taxonSource is None:
         self._failGracefully('Missing taxonomic source')
      else:
         self._taxonSourceId = taxonSource

      self.algs = algLst
      self.modelScenario = mdlScen
      self.projScenarios = prjScenLst
      self.modelMask = mdlMask
      self.projMask = prjMask
      self.intersectGrid = intersectGrid

      self._fieldnames = fieldnames
      self._providers, self._provCol = self._readProviderKeys(providerKeyFile, 
                                                              providerKeyColname)
      self._dumpfile = open(occfilename, 'r')
      csv.field_size_limit(sys.maxsize)
      self._csvreader = csv.reader(self._dumpfile, delimiter='\t')
      self._keyCol = fieldnames.index(keyColname)
      self._recnum = 0
      self._currRec, self._currSpeciesKey = self._getCSVRecord()
      self._currKeyFirstRecnum = self._recnum
      self._obsoleteTime = expDate
      # Start mid-file 
      self._skipAhead()
      
# ...............................................
   @property
   def nextStart(self):
      return self._currKeyFirstRecnum
   
# ...............................................
   def _readProviderKeys(self, providerKeyFile, providerKeyColname):
      providers = {}
      
      try:
         provKeyCol = self._fieldnames.index(providerKeyColname)
      except Exception, e:
         self.log.error('Unable to find %s in fieldnames' % providerKeyColname)
         provKeyCol = None
         
      if providerKeyFile is not None and providerKeyColname is not None: 
         import os
         if not os.path.exists(providerKeyFile):
            self.log.error('Missing provider file %s' % providerKeyFile)
         else:
            dumpfile = open(providerKeyFile, 'r')
            csv.field_size_limit(sys.maxsize)
            csvreader = csv.reader(dumpfile, delimiter=';')
            for line in csvreader:
               try:
                  key, name = line
                  if key != 'key':
                     providers[key] = name
               except:
                  pass
            dumpfile.close()
      return providers, provKeyCol
         
# ...............................................
   def run(self):
      allFail = True
      # Gets and frees lock for each name checked
      while (not(self._existKillFile())):
         try:
            while self._currRec is not None:
               occ = None
               speciesKey, dataCount, dataChunk = self._getOccurrenceChunk()
#                speciesName = self._scribe.findTaxon(self._taxonSourceId, 
#                                                     speciesKey)
#                if speciesName is None:
#                   try:
#                      speciesName = self._getInsertSciNameForGBIFSpeciesKey(speciesKey)
#                   except LmHTTPError, e:
#                      self.log.info('Failed lookup for key {}, ({})'.format(
#                                                             speciesKey, e.msg))
               self._processInputSpeciesKey(speciesKey, dataCount, dataChunk)
#                else:
#                   self.log.info('Unknown taxa for key {}'.format(speciesKey))

               if self._existKillFile():
                  break
                        
            self._dumpfile.close()
            if self._currRec is None:
               self._currKeyFirstRecnum = -9999
               allFail = False
               self.signalKill(allFail=allFail)
               self.log.info('Chainer complete, last first rec = %d' 
                             % self._currKeyFirstRecnum)
               break
         
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self._failGracefully(e)
            break

      if self._existKillFile():
         self.log.info('LAST CHECKED line %d (stopped with killfile)' 
                       % (self._currKeyFirstRecnum))
         self._failGracefully(None, allFail=allFail)

# ...............................................
   def _failGracefully(self, lmerr, linenum=None, allFail=True):
      if linenum is None: 
         try:
            linenum = self.nextStart
         except:
            pass
      if linenum:
         self._writeNextStart(linenum)
      _LMWorker._failGracefully(self, lmerr, allFail=allFail)
            
# ...............................................
   def _skipAhead(self):
      linenum = self._findStart()         
      if linenum < 0:
         self._currKeyFirstRecnum = linenum
         self._currRec = None
      else:
         self._currRec, tmp = self._getCSVRecord(parse=False)
         while self._currRec is not None and self._recnum < linenum:
            self._currRec, tmp = self._getCSVRecord(parse=False)
            
# # ...............................................
#    def _processInputTaxa(self, speciesKey):
#       # This gets the record for the accepted species key, so the record 
#       # scientific name = accepted name (i.e. full, not canonical, species name)
#       speciesName = None
#       try:
#          self._getLock()           
#          # Use API to get and insert species name 
#          (kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr,
#           speciesStr, genuskey, retSpecieskey) = GbifAPI.getTaxonomy(speciesKey)
#          if retSpecieskey == speciesKey:
#             currtime = dt.gmt().mjd
#             speciesName = ScientificName(speciesStr, 
#                             kingdom=kingdomStr, phylum=phylumStr, 
#                             txClass=None, txOrder=orderStr, 
#                             family=familyStr, genus=genusStr, 
#                             createTime=currtime, modTime=currtime, 
#                             taxonomySourceId=self._taxonSourceId, 
#                             taxonomySourceKey=speciesKey, 
#                             taxonomySourceGenusKey=genuskey, 
#                             taxonomySourceSpeciesKey=speciesKey)
#             self._scribe.insertTaxon(speciesName)
#             return speciesName
#       except LMError, e:
#          raise e
#       except Exception, e:
#          raise LMError(currargs=e.args, lineno=self.getLineno())
#       finally:
#          self._freeLock()
         
# ...............................................
   def _processInputSpeciesKey(self, speciesKey, dataCount, dataChunk):
      # This gets the record for the species key, so the record 
      # scientific name = species name
      currtime = dt.gmt().mjd
      occ = None
      try:
         self._getLock()
         sciName = self._getInsertSciNameForGBIFSpeciesKey(speciesKey)
         
         occs = self._scribe.getOccurrenceSetsForScientificName(sciName, 
                                                       ARCHIVE_USER)
         if not occs:
            occ = OccurrenceLayer(sciName.scientificName, 
                  name=sciName.scientificName, fromGbif=True, 
                  queryCount=dataCount, epsgcode=DEFAULT_EPSG, 
                  ogrType=wkbPoint, ogrFormat='CSV', 
                  primaryEnv=PrimaryEnvironment.TERRESTRIAL,
                  userId=ARCHIVE_USER, createTime=currtime, 
                  status=JobStatus.INITIALIZE, 
                  statusModTime=currtime, sciName=sciName)
            occid = self._scribe.insertOccurrenceSet(occ)
         elif len(occs) == 1:
            if (occs[0].status != JobStatus.COMPLETE or 
                occs[0].statusModTime < self._obsoleteTime):
               occ = occs[0]
            else:
               self.log.debug('OccurrenceSet %d is up to date (%.2f)' 
                              % (occs[0].getId(), occs[0].statusModTime))
         else:
            raise LMError('Too many (%d) occurrenceLayers for %s'
                          % (len(occs), sciName.scientificName))
            
         if occ is not None:
            # Write new raw data
            processtype = ProcessType.GBIF_TAXA_OCCURRENCE
            rdloc = occ.createLocalDLocation(raw=True)
            success = occ.writeCSV(dataChunk, dlocation=rdloc, overwrite=True)
            if not success:
               self.log.debug('Unable to write CSV file %s' % rdloc)
            else:
               occ.setRawDLocation(rdloc, currtime)
               # Create jobs for Archive Chain: occurrence population, 
               # model, projection, and (later) intersect computation
               jobs = self._scribe.initSDMChain(ARCHIVE_USER, occ, self.algs, 
                                         self.modelScenario, 
                                         self.projScenarios, 
                                         occJobProcessType=processtype,
                                         priority=Priority.NORMAL, 
                                         intersectGrid=None,
                                         minPointCount=POINT_COUNT_MIN)
               self.log.debug('Initialized %d jobs for occ %d' % (len(jobs), occ.getId()))
      except LMError, e:
         raise e
      except Exception, e:
         raise LMError(currargs=e.args, lineno=self.getLineno())
      finally:
         self._freeLock()

# ...............................................
   def _insertNewDeleteErrorJobs(self, occ, currtime):
      """
      @note: Thread already has lock when this is called
      """
      found = False
      jobs = self._scribe.findExistingJobs(occId=occ.getId())
      for j in jobs:
         if j.status >= JobStatus.GENERAL_ERROR:
            # This deletes error jobs
            self._scribe.updateJob(j)
         elif j.status == JobStatus.INITIALIZE:
            found = True
      # If an existing job has been started, create a new one??
      if not found:
         occ.updateStatus(JobStatus.INITIALIZE, modTime=currtime)
         occJob = SDMOccurrenceJob(occ, status=JobStatus.INITIALIZE, 
                                   statusModTime=currtime, createTime=currtime,
                                   priority=Priority.NORMAL)
         try:
            success = self._scribe.updateOccState(occ)
            updatedOccJob = self._scribe.insertJob(occJob)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self.log.error('   Failed to insert occurrenceSet Job for %s (%s)'
                           % (str(occ.getId()), str(e)))
         else:
            self.log.debug('   inserted job to write points for occurrenceSet %s in MAL' 
                           % (str(occ.getId()))) 

# ...............................................
   def _getCSVRecord(self, parse=True):
      success = False
      line = specieskey = None
      while not success:
         try:
            line = self._csvreader.next()
            self._recnum += 1
            success = True
         except OverflowError, e:
            self._recnum += 1
            self.log.debug( 'OverflowError on %d (%s), moving on' % (self._recnum, str(e)))
         except Exception, e:
            self.log.debug('Exception reading line %d (%s)' % (self._recnum, str(e)))
            success = True
         if parse:
            line, specieskey = self._parseCSVRecord(line)
      return line, specieskey

# ...............................................
   def _parseCSVRecord(self, line):
      specieskey = provkey = None
      if line is not None and len(line) >= 16:
         try:
            specieskey = int(line[self._keyCol])
         except Exception, e:
            line = None
            self.log.debug('Skipping line; failed to convert specieskey on record %d (%s)' 
                   % (self._recnum, str(line)))
            
         if self._provCol is not None:
            try:
               provkey = line[self._provCol]
            except Exception, e:
               self.log.debug('Failed to find providerKey on record %d (%s)' 
                      % (self._recnum, str(line)))
            else:
               provname = provkey
               try:
                  provname = self._providers[provkey]
               except:
                  try:
                     provname = GbifAPI.getPublishingOrg(provkey)
                     self._providers[provkey] = provname
                  except:
                     self.log.debug('Failed to find providerKey %s in providers or GBIF API' 
                                 % (provkey))

               line[self._provCol] = provname
            
      return line, specieskey

# ...............................................
   def _getOccurrenceChunk(self):
      """
      """
      completeChunk = False
      currKey = None
      currCount = 0
      currChunk = []
      while not completeChunk:
#          line, specieskey = self._parseCSVRecord(self._currRec)
         if currKey is None:
            currKey = self._currSpeciesKey
            self._currKeyFirstRecnum = self._recnum
         
         if self._currSpeciesKey == currKey:
            currCount += 1
            currChunk.append(self._currRec)
         else:
            completeChunk = True
                  
         if not completeChunk:
            self._currRec, self._currSpeciesKey = self._getCSVRecord()
            if self._currRec is None:
               completeChunk = True
               self.log.debug('Ended on line %d (chunk started on %d)' 
                      % (self._recnum, self._currKeyFirstRecnum))
      self.log.debug('Returning %d records for %d (starting on line %d)' 
            % (currCount, currKey, self._currKeyFirstRecnum))
      return currKey, currCount, currChunk
         
# ..............................................................................
class iDigBioChainer(_LMWorker):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, lock, pipelineName, updateInterval, 
                algLst, mdlScen, prjScenLst, idigFname, expDate,
                taxonSource=None, mdlMask=None, prjMask=None, intersectGrid=None):
      threadspeed = WORKER_JOB_LIMIT
      _LMWorker.__init__(self, lock, threadspeed, pipelineName, updateInterval)
      self.startFile = os.path.join(APP_PATH, LOG_PATH, 'start.%s.txt' % pipelineName)

      if taxonSource is None:
         self._failGracefully('Missing taxonomic source')
      else:
         self._taxonSourceId = taxonSource

      self.algs = algLst
      self.modelScenario = mdlScen
      self.projScenarios = prjScenLst
      self.modelMask = mdlMask
      self.projMask = prjMask
      self.intersectGrid = intersectGrid
      self._obsoleteTime = expDate
      self._linenum = 0
      self._dumpDir, ext = os.path.splitext(idigFname)
      try:
         self._idigFile = open(idigFname, 'r')
      except Exception, e:
         raise LMError('Invalid file %s (%s)' % (str(idigFname), str(e)))
      self._currBinomial = None
      self._currGbifTaxonId = None
      self._currReportedCount = None
         
# ...............................................
   def run(self):
      allFail = True
      nextStart = None
      # Start mid-file 
      taxonId, taxonCount, taxonName = self._skipAhead()

      # Gets and frees lock for each name checked
      while (not(self._existKillFile())):
         try:
            while taxonId is not None:
               occ = None
               self._processInputSpecies(taxonName, taxonId, taxonCount)
               if self._existKillFile():
                  break
               else:
                  taxonId, taxonCount, taxonName = self._getCurrTaxon()
                        
            self._idigFile.close()
            if self._currBinomial is None:
               nextStart = -9999
               allFail = False
               self.signalKill(allFail=allFail)
               self.log.info('Chainer complete, last line num = %d' 
                             % self._linenum)
               break
         
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self._failGracefully(e)
            break

      if self._existKillFile():
         self.log.info('LAST CHECKED line %d (stopped with killfile)' 
                       % (self._linenum))
         self._failGracefully(None, linenum=nextStart, allFail=allFail)

# ...............................................
   def _failGracefully(self, lmerr, linenum=None, allFail=True):
      if linenum is None: 
         linenum = self._linenum
      self._writeNextStart(linenum)
      _LMWorker._failGracefully(self, lmerr, allFail=allFail)
      
# ...............................................
   def _getCurrTaxon(self):
      """
      @summary: Returns currBinomial, currGbifTaxonId, currReportedCount 
      """
      currGbifTaxonId = None
      currReportedCount = None
      currBinomial = None
      success = False
      while not success:
         try:
            line = self._idigFile.readline()
         except Exception, e:
            self._linenum += 1
            if isinstance(e, OverflowError):
               self.log.debug( 'OverflowError on {} ({}), moving on'.format(
                                                self._linenum, str(e)))
            else:
               self.log.debug('Exception reading line %d (%s)'.format(
                                                self._linenum, str(e)))
               success = True
         else:
            self._linenum += 1
            success = True
            if line == '':
               line = None
               self._linenum = -9999
         
      if line is not None:
         tempvals = line.strip().split()
         if len(tempvals) < 3:
            print('Missing data in line {}'.format(line))
         else:
            try:
               currGbifTaxonId = int(tempvals[0])
            except:
               pass
            try:
               currReportedCount = int(tempvals[1])
            except:
               pass
            currBinomial = tempvals[2]
            try:
               currBinomial = ' '.join([currBinomial, tempvals[2]])
            except:
               pass
      return currGbifTaxonId, currReportedCount, currBinomial

# ...............................................
   def _skipAhead(self):
      taxonId = taxonCount = taxonName = None
      startline = self._findStart()         
      if startline < 0:
         self._linenum = 0
      else:
         taxonId, taxonCount, taxonName = self._getCurrTaxon()
         while taxonName is not None and self._linenum < startline:
            taxonId, taxonCount, taxonName = self._getCurrTaxon()
      return  taxonId, taxonCount, taxonName
         
# ...............................................
   def _countRecords(self, rawfname):
      pointcount = 0
      try:
         f  = open(rawfname, 'r')
         blob = f.read()
      except Exception, e:
         self.log.debug('Failed to read %s' % rawfname)
      else:
         pointcount = len(blob.split('\n')) - 1
      finally:
         try:
            f.close()
         except:
            pass
         
      return pointcount
      
# ...............................................
   def _getQueryUrl(self, taxonId):
      occAPI = IdigbioAPI(qFilters={IDIGBIO_GBIFID_FIELD: taxonId})
      # TODO: Remove this when we can properly parse XML file with full URL
      occAPI.clearOtherFilters()
      return occAPI.url
      
# ...............................................
   def _processInputGBIFTaxonId(self, taxonName, taxonId, taxonCount):
      try:
         self._getLock()
         sciName = self._getInsertSciNameForGBIFSpeciesKey(taxonId)
         self._processSDMChainForDynamicQuery(sciName, taxonId, taxonCount,
                                              ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                                              POINT_COUNT_MIN)
      except LMError, e:
         raise e
      except Exception, e:
         raise LMError(currargs=e.args, lineno=self.getLineno())
      finally:
         self._freeLock()
         
# .............................................................................
class ProcessRunner(_LMWorker):
   def __init__(self, lock, pipelineName, updateInterval,
                processTypes=[ProcessType.RAD_INTERSECT, 
                              ProcessType.RAD_COMPRESS, 
                              ProcessType.RAD_SWAP, ProcessType.RAD_SPLOTCH, 
                              ProcessType.RAD_CALCULATE, ProcessType.SMTP],
                threadSuffix=None):
      # Slow down this thread so it doesn't overwhelm other functions
      threadspeed = 4
      startStatus = JobStatus.INITIALIZE
      queueStatus = JobStatus.PULL_REQUESTED
      endStatus = JobStatus.COMPLETE
      import socket
      self.ipaddress = None
      (self.ipaddress, network, cidr, iface) = self._getNetworkInfo()
      
      _LMWorker.__init__(self, lock, threadspeed, pipelineName, updateInterval, 
                         startStatus, queueStatus, endStatus, 
                         threadSuffix=threadSuffix)
      self.processTypes = processTypes
      try:
         # Rolls back and moves dependent jobs for SDM and RAD
         count = self._scribe.rollbackIncompleteJobs(self.queueStatus)
         
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         self._failGracefully(e)
      
# ...............................................
   def _pullJobs(self):
      """
      beginIntersect - moveRADJobs
       insert into lm3.computeresource (name, requestprefix, userid, datecreated, datelastmodified, lastheartbeat, ipaddress)
       values ('badenov', 'badenov.nhm.ku.edu', 'lm2', 56573, 56573, 56573,'129.237.201.119'); 
      """
      jobs = []
      try:
         self._getLock()
         
         jobs = self._scribe.pullJobs(self.threadSpeed, self.ipaddress,  
                                      processTypes=self.processTypes)
         self.log.debug('Pulled %d local Jobs' % (len(jobs)))
         
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         self._failGracefully(e)

      finally:
         self._freeLock()
      
      # If no jobs are ready, give someone else a chance
      if len(jobs) == 0:
         self._wait(30)
         
      return jobs
   
# ...............................................
   def _updateJobAttributes(self, job, status):
      success = False
      try:
         self._getLock()
         # updates status on job and outputObj
         job.update(status=status)
         success = self._scribe.updateJob(job, JobStatus.GENERAL_ERROR)
         self.log.debug('Updated job %d status' % (job.getId()))
         
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno(),
                        location=self.getLocation())
         raise e
      
      finally:
         self._freeLock()
      
      return success

# ...............................................
   def _moveDependentJobs(self, job):
      success = False
      try:
         self._getLock()
         success = self._scribe.moveDependentJobs(job, completeStat=JobStatus.COMPLETE, 
                                       errorStat=JobStatus.GENERAL_ERROR)
         self.log.debug('Moved jobs dependent on %d (success=%s)' 
                        % (job.getId(), str(success)))
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno(),
                        location=self.getLocation())
         raise e
       
      finally:
         self._freeLock()
       
      return success

# ...............................................
   def run(self):
      while not(self._existKillFile()):
         # dispatch jobs 
         try:
            jobs = self._pullJobs()
            if len(jobs) == 0:
               self._wait(30)
               
            for j in jobs:
               # Run jobs locally, including writing and cataloging results
               outputs = j.run()
               status = j.write(*outputs)
               # In database, update object, update or delete job
               success = self._updateJobAttributes(j, status)
               # Move all jobs that were waiting on this one
               self._moveDependentJobs(j)
                  
               # do not finish jobs
               if self._existKillFile():
                  break
               
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self._failGracefully(e)
            break   

      if self._existKillFile():
         self._failGracefully(None)
         

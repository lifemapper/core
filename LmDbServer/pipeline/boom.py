"""
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
try:
   import mx.DateTime as dt
except:
   pass

import csv
import os
import sys
from time import sleep
from types import ListType, TupleType, StringType, UnicodeType

from LmBackend.common.occparse import OccDataParser
from LmBackend.makeflow.documentBuilder import LMMakeflowDocument
from LmCommon.common.apiquery import BisonAPI, GbifAPI, IdigbioAPI
from LmCommon.common.lmconstants import (BISON_OCC_FILTERS, BISON_HIERARCHY_KEY,
            BISON_MIN_POINT_COUNT, ProcessType, DEFAULT_EPSG, JobStatus, 
            ONE_HOUR, ONE_MIN, IDIGBIO_GBIFID_FIELD, GBIF_EXPORT_FIELDS,
            GBIF_TAXONKEY_FIELD, GBIF_PROVIDER_FIELD)
from LmServer.base.lmobj import LMError, LMObject
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import (Priority, PrimaryEnvironment, LOG_PATH, 
                                         wkbPoint)
from LmServer.common.localconstants import (POINT_COUNT_MIN, TROUBLESHOOTERS, 
                                            APP_PATH)
from LmServer.common.log import ScriptLogger
from LmServer.db.scribe import Scribe
from LmServer.notifications.email import EmailNotifier
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.omJob import OmProjectionJob, OmModelJob
from LmServer.sdm.meJob import MeProjectionJob, MeModelJob
from LmServer.sdm.sdmJob import SDMOccurrenceJob, SDMModelJob, SDMProjectionJob

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR
GBIF_SERVICE_INTERVAL = 3 * ONE_MIN            

# .............................................................................
class _LMBoomer(LMObject):
   # .............................
   def __init__(self, userid, priority, algLst, mdlScen, prjScenLst, 
                taxonSourceName=None, mdlMask=None, prjMask=None, 
                intersectGrid=None, log=None):
      super(_LMBoomer, self).__init__()
      import socket
      self.hostname = socket.gethostname().lower()
      self.userid = userid
      self.priority = priority
      self.algs = []
      self.modelScenario = None
      self.projScenarios = []
      self.modelMask = None
      self.projMask = None
      self.intersectGrid = None

      if userid is None:
         self.name = self.__class__.__name__.lower()
      else:
         self.name = '{}_{}'.format(self.__class__.__name__.lower(), userid)
      self.startFile = os.path.join(APP_PATH, LOG_PATH, 
                                    'start.{}.txt'.format(self.name))
      if log is None:
         log = ScriptLogger(self.name)
      self.log = log
      self.developers = TROUBLESHOOTERS
      self.startStatus=JobStatus.GENERAL 
      self.queueStatus=JobStatus.GENERAL 
      self.endStatus=JobStatus.INITIALIZE
      self.updateTime = None

      try:
         self._scribe = Scribe(self.log)
         success = self._scribe.openConnections()
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs='Failed to open database', 
                        prevargs=e.args)
         self._failGracefully(lmerr=e)
      else:
         if not success:
            self._failGracefully(lmerr='Failed to open database')
            
      self.log.info('{} opened databases'.format(self.name))
      self._fillDefaultObjects(algLst, mdlScen, prjScenLst, mdlMask, prjMask, 
                               intersectGrid, taxonSourceName)
         
# ...............................................
   def _fillDefaultObjects(self, algCodes, mdlScenarioCode, projScenarioCodes, 
                           mdlMaskId, prjMaskId, intersectGridName,
                           taxonSourceName):
      for acode in algCodes:
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         self.algs.append(alg)

      try:
         txSourceId, x,y,z = self._scribe.findTaxonSource(taxonSourceName)
         self._taxonSourceId = txSourceId
         
         mscen = self._scribe.getScenario(mdlScenarioCode)
         if mscen is not None:
            self.modelScenario = mscen
            if mdlScenarioCode not in projScenarioCodes:
               self.projScenarios.append(self.modelScenario)
            for pcode in projScenarioCodes:
               scen = self._scribe.getScenario(pcode)
               if scen is not None:
                  self.projScenarios.append(scen)
               else:
                  raise LMError('Failed to retrieve scenario {}'.format(pcode))
         else:
            raise LMError('Failed to retrieve scenario {}'.format(mdlScenarioCode))
         
         self.modelMask = self._scribe.getLayer(mdlMaskId)
         self.projMask = self._scribe.getLayer(prjMaskId)
         self.intersectGrid = self._scribe.getShapeGrid(self.userid, 
                                                  shpname=intersectGridName)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

# ...............................................
   def _failGracefully(self, lmerr=None):
      self.saveNextStart()
      if lmerr is not None:
         logmsg = str(lmerr)
         if isinstance(lmerr, LMError):
            logmsg += '\n Traceback: {}'.format(lmerr.getTraceback())
            self._notifyPeople("Lifemapper BOOM has failed", 
                  '{} Lifemapper BOOM has crashed with the message:\n\n{}'
                  .format(self.hostname, logmsg))
         self.log.error(logmsg)

      if self._scribe.isOpen:
         try:
            self._scribe.closeConnections()
         except Exception, e:
            self.log.warning('Error while failing: {}'.format(str(e)))
            
      self.log.debug('Time: {}; gracefully exiting ...'.format(dt.utc().mjd))

# ...............................................
   def _notifyPeople(self, subject, message, recipients=None):
      if recipients is None:
         recipients = self.developers
      elif not (isinstance(recipients, ListType) 
                or isinstance(recipients, TupleType)):
         recipients = [recipients]
      notifier = EmailNotifier()
      try:
         notifier.sendMessage(recipients, subject, message)
      except Exception, e:
         self.log.error('Failed to notify {} about {} (e={})'
                        .format(recipients, subject, e))

# ...............................................
   def _findStart(self):
      linenum = 0
      if os.path.exists(self.startFile):
         with open(self.startFile, 'r') as f:
            line = f.read()
            try:
               linenum = int(line)
            except:
               print 'Failed to interpret {}'.format(line)
         if linenum > 0:
            os.remove(self.startFile)
      return linenum
                  
# ...............................................
   def saveNextStart(self):
      if self.nextStart is not None:
         try:
            f = open(self.startFile, 'w')
            f.write(str(self.nextStart))
            f.close()
         except:
            self.log.error('Failed to write {} to chainer start file {}'
                           .format(self.nextStart, self.startFile))
   
# ...............................................
   def _rollbackQueuedJobs(self):
      if self.startStatus < self.queueStatus:
         count = self._scribe.rollbackIncompleteRADJobs(self.queueStatus)
         self.updateTime = dt.gmt().mjd
         self.log.debug('Reset {} queued ({}) jobs to completed status ({})'
                        .format(count, self.queueStatus, self.startStatus))
   
# ...............................................
   def _notifyComplete(self, job):
      # Should already have lock
      success = True
      msg = None
      if job.email is not None:
         # Notify on Model only if fail
         if isinstance(job, (MeModelJob, OmModelJob)):
            if job.status >= JobStatus.NOT_FOUND:
               subject = 'Lifemapper SDM Model {} Error'.format(job.jid)
               msg = ''.join(('The Lifemapper Species Distribution Modeling '
                              '(LmSDM) job that you requested has failed with code {}.  '
                              .format(job.status),
                              'You may visit {} for error information'
                              .format(job.metadataUrl)))
         # Notify on Projection only if it's the last one
         elif isinstance(job, (MeProjectionJob, OmProjectionJob)):
            model = job.projection.getModel()
            unfinishedCount = self._scribe.countProjections(job.userId, 
                                                            inProcess=True, 
                                                            mdlId=model.getId())   
            if unfinishedCount == 0:
               if job.status == JobStatus.COMPLETE:
                  subject = 'Lifemapper SDM Experiment {} Completed'.format(model.getId())
                  msg = ''.join(('The Lifemapper Species Distribution Modeling '
                                 '(LmSDM) job that you requested is now complete.  ',
                                 'You may visit {} to retrieve your data' 
                                 .format(job.metadataUrl))) 
               else:
                  subject = 'Lifemapper SDM Experiment {} Error'.format(model.getId())
                  msg = ''.join(('The Lifemapper Species Distribution Modeling '
                                 '(LmSDM) job that you requested has failed with code {}.  '
                                 .format(job.status),
                                 'You may visit {} for error information' 
                                 .format(job.metadataUrl)))  
            else:
               self.log.info('Notify: {} unfinished projections for model {}, user {}'
                             .format(unfinishedCount, model.getId(), job.email))
         if msg is not None:
            try:
               self._notifyPeople(subject, msg, recipients=[job.email])
               self.log.info('Notified: {}, user {}'.format(subject, job.email))
            except LMError, e:
               self.log.info('Failed to notify user {} of {} ({})'
                             .format(job.email, subject, str(e)))
               success = False
      return success         

   # ...............................................
   def _deleteOccurrenceSet(self, occSet):
      try:
         deleted = self._scribe.completelyRemoveOccurrenceSet(occSet)
      except Exception, e:
         self.log.error('Failed to completely remove occurrenceSet {} ({})'
                        .format(occSet.getId(), str(e)))
      else:
         self.log.debug('   removed occurrenceset {}/{} in MAL'
                        .format(occSet.getId(), occSet.displayName))
      return deleted
         
# ...............................................
   def _getInsertSciNameForGBIFSpeciesKey(self, taxonKey, taxonCount):
      """
      Returns an existing or newly inserted ScientificName
      """
      sciName = self._scribe.findTaxon(self._taxonSourceId, 
                                           taxonKey)
      if sciName is not None:
         self.log.info('Found sciname for taxonKey {}, {}, with {} points'
                       .format(taxonKey, sciName.scientificName, taxonCount))
      else:
         # Use API to get and insert species name 
         try:
            (rankStr, scinameStr, canonicalStr, acceptedKey, acceptedStr, 
             nubKey, taxStatus, kingdomStr, phylumStr, classStr, orderStr, 
             familyStr, genusStr, speciesStr, genusKey, speciesKey, 
             loglines) = GbifAPI.getTaxonomy(taxonKey)
         except Exception, e:
            self.log.info('Failed lookup for key {}, ({})'.format(
                                                   taxonKey, e))
         else:
            # if no species key, this is not a species
            if taxStatus == 'ACCEPTED':
#             if taxonKey in (retSpecieskey, acceptedkey, genuskey):
               currtime = dt.gmt().mjd
               sciName = ScientificName(scinameStr, 
                               rank=rankStr, 
                               canonicalName=canonicalStr,
                               lastOccurrenceCount=taxonCount,
                               kingdom=kingdomStr, phylum=phylumStr, 
                               txClass=None, txOrder=orderStr, 
                               family=familyStr, genus=genusStr, 
                               createTime=currtime, modTime=currtime, 
                               taxonomySourceId=self._taxonSourceId, 
                               taxonomySourceKey=taxonKey, 
                               taxonomySourceGenusKey=genusKey, 
                               taxonomySourceSpeciesKey=speciesKey)
               try:
                  self._scribe.insertTaxon(sciName)
                  self.log.info('Inserted sciname for taxonKey {}, {}'
                                .format(taxonKey, sciName.scientificName))
               except Exception, e:
                  if not isinstance(e, LMError):
                     e = LMError(currargs='Failed on taxonKey {}, linenum {}'
                                          .format(taxonKey, self._linenum), 
                                 prevargs=e.args, lineno=self.getLineno())
                  raise e
            else:
               self.log.info('taxonKey {} is not an accepted genus or species'
                             .format(taxonKey))
      return sciName
         
# ...............................................
   def _raiseSubclassError(self):
      raise LMError(currargs='Function must be implemented in subclass')

# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      self._raiseSubclassError()
   
# ...............................................
   def _getInsertSciNameForExternalSpeciesKey(self, speciesKey):
      self._raiseSubclassError()
      
# ...............................................
   def _createMakeflow(self, jobs):
      jobchainId = usr = filename = None
      if jobs:
         mfdoc = LMMakeflowDocument()
         for j in jobs:
            if isinstance(j, SDMOccurrenceJob):
               mfdoc.buildOccurrenceSet(j)
            elif isinstance(j, SDMModelJob):
               mfdoc.buildModel(j)
            elif isinstance(j, SDMProjectionJob):
               mfdoc.buildProjection(j)
            if usr is None:
               usr = j.getUserId()
            if filename is None:
               filename = j.makeflowFilename
         self.log.info('Writing makeflow document {} ...'.format(filename))
         success = mfdoc.write(filename)
         if not success:
            self.log.error('Failed to write {}'.format(filename))
         
         try:
            jobchainId = self._scribe.insertJobChain(usr, filename, self.priority)
         except Exception, e:
            raise LMError(currargs='Failed to insert jobChain for {}; ({})'
                          .format(filename, str(e)))
      return jobchainId

# ...............................................
   def chainOne(self):
      self._raiseSubclassError()

# ...............................................
   def close(self):
      self._raiseSubclassError()
      
# ...............................................
   @property
   def complete(self):
      self._raiseSubclassError()
                  
# ...............................................
   @property
   def nextStart(self):
      self._raiseSubclassError()
   
# ...............................................
   def moveToStart(self):
      self._raiseSubclassError()
   
# ...............................................
   @property
   def currRecnum(self):
      if self.complete:
         return 0
      else:
         return self._linenum

# ...............................................
   def _createOrResetOccurrenceset(self, sciname, taxonSourceKeyVal, 
                                   occProcessType, dataCount, data=None):
      """
      @note: Updates to existing occset are not saved until 
      """
      currtime = dt.gmt().mjd
      occ = occs = None
      # Find existing
      try:
         if isinstance(sciname, ScientificName):
            occs = self._scribe.getOccurrenceSetsForScientificName(sciname, 
                                                                self.userid)
            taxonName = sciname.name
         elif isinstance(sciname, StringType) or isinstance(sciname, UnicodeType):
            occs = self._scribe.getOccurrenceSetsForName(sciname, userid=self.userid)
            taxonName = sciname
            sciname = None
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
         
      # Create new
      if not occs:
         ogrFormat = None
         if occProcessType == ProcessType.GBIF_TAXA_OCCURRENCE:
            ogrFormat = 'CSV'
         occ = OccurrenceLayer(taxonName, name=taxonName, fromGbif=False, 
               queryCount=dataCount, epsgcode=DEFAULT_EPSG, 
               ogrType=wkbPoint, ogrFormat=ogrFormat, userId=self.userid,
               primaryEnv=PrimaryEnvironment.TERRESTRIAL, createTime=currtime, 
               status=JobStatus.INITIALIZE, statusModTime=currtime, 
               sciName=sciname)
         try:
            occid = self._scribe.insertOccurrenceSet(occ)
            self.log.info('Inserted occset for taxonname {}'.format(taxonName))
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
            
      # Reset existing if missing data, obsolete, or failed
      elif len(occs) == 1:
         tmpOcc = occs[0]
         # waiting but raw data missing
         if ((JobStatus.waiting(tmpOcc.status) 
              and tmpOcc.getRawDLocation() is None)
             or
             # complete but obsolete
             (tmpOcc.status == JobStatus.COMPLETE 
              and tmpOcc.statusModTime > 0 
              and tmpOcc.statusModTime < self._obsoleteTime)
             or 
             # failed
             JobStatus.failed(tmpOcc.status)):
            # Reset existing 
            occ = tmpOcc
            occ.updateStatus(JobStatus.INITIALIZE, modTime=currtime)
            self.log.info('Updating occset {} ({})'
                          .format(tmpOcc.getId(), taxonName))
         else:
            self.log.debug('Ignoring occset {} ({}) is up to date'
                           .format(tmpOcc.getId(), taxonName))
      else:
         raise LMError(currargs='Too many ({}) occsets for {}'
                       .format(len(occs), taxonName))

      # Set raw data
      if occ:
         rdloc = self._locateRawData(occ, taxonSourceKeyVal=taxonSourceKeyVal, 
                                     data=data)
         if rdloc:
            occ.setRawDLocation(rdloc, currtime)
         else:
            raise LMError(currargs='Unable to set raw data location')
      
      return occ
   
# ...............................................
   def _processSDMChain(self, sciname, taxonSourceKeyVal, occProcessType,
                        dataCount, minPointCount, data=None):
      jobs = []
      if sciname is not None:
         try:
            occ = self._createOrResetOccurrenceset(sciname, taxonSourceKeyVal, 
                              occProcessType, dataCount, data=data)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
   
         if occ:
            # Create jobs for Archive Chain; 'reset' to existing occset will be 
            # saved here
            try:
               jobs = self._scribe.initSDMChain(self.userid, occ, self.algs, 
                                         self.modelScenario, 
                                         self.projScenarios, 
                                         occJobProcessType=occProcessType, 
                                         priority=self.priority, 
                                         intersectGrid=self.intersectGrid,
                                         minPointCount=minPointCount)
               self.log.debug('Created {} jobs for occurrenceset {}'
                              .format(len(jobs), occ.getId()))
            except Exception, e:
               if not isinstance(e, LMError):
                  e = LMError(currargs=e.args, lineno=self.getLineno())
               raise e
      return jobs
#       else:
#          self.log.debug('ScientificName does not exist')

# ..............................................................................
class BisonBoom(_LMBoomer):
   """
   @summary: Initializes the job chainer for BISON.
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, tsnfilename, expDate, 
                priority=Priority.NORMAL, taxonSourceName=None, 
                mdlMask=None, prjMask=None, intersectGrid=None, log=None):
      self._tsnfile = None
      
      if taxonSourceName is None:
         self._failGracefully(lmerr='Missing taxonomic source')
         
      self._updateFile(tsnfilename, expDate)
      try:
         self._tsnfile = open(tsnfilename, 'r')
      except:
         self._failGracefully(lmerr='Unable to open {}'.format(tsnfilename))
      
      self._linenum = 0
      super(BisonBoom, self).__init__(userid, priority, algLst, 
                                      mdlScen, prjScenLst, 
                                      taxonSourceName=taxonSourceName, 
                                      mdlMask=mdlMask, prjMask=prjMask, 
                                      intersectGrid=intersectGrid, log=log)
      self._obsoleteTime = expDate
      
# ...............................................
   def close(self):
      try:
         self._tsnfile.close()
      except:
         self.log.error('Unable to close tsnfile {}'.format(self._tsnfile))
         
# ...............................................
   @property
   def nextStart(self):
      if self.complete:
         return 0
      else:
         return self._linenum + 1

# ...............................................
   @property
   def complete(self):
      try:
         return self._tsnfile.closed
      except:
         return True
      
# ...............................................
   def _updateFile(self, filename, expDate):
      """
      If file does not exist or is older than expDate, create a new file. 
      """
      if filename is None or not os.path.exists(filename):
         self._recreateFile(filename)
      elif expDate is not None:
         ticktime = os.path.getmtime(filename)
         modtime = dt.DateFromTicks(ticktime).mjd
         if modtime < expDate:
            self._recreateFile(filename)

# ...............................................
   def _recreateFile(self, filename):
      """
      Create a new file from BISON TSN query for binomials with > 20 points. 
      """
      tsnList = BisonAPI.getTsnListForBinomials()
      with open(filename, 'w') as f:
         for tsn, tsnCount in tsnList:
            f.write('{}, {}\n'.format(tsn, tsnCount))
      
# ...............................................
   def _getTsnRec(self):
      success = False
      tsn = tsnCount = None
      while not self._tsnfile.closed and not success:
         line = self._tsnfile.readline()
         if line == '':
            self._tsnfile.close()
         else:
            try:               
               first, second = line.split(',')
               # Returns TSN, TSNCount
               tsn, tsnCount = (int(first), int(second))
               self._linenum += 1
               success = True
            except Exception, e:
               self.log.debug('Exception reading line {} ({})'
                              .format(self._linenum, str(e)))
      return tsn, tsnCount

# ...............................................
   def _processTsn(self, tsn, tsnCount):
      jobs = []
      if tsn is not None:
         sciName = self._getInsertSciNameForItisTSN(tsn, tsnCount)
         jobs = self._processSDMChain(sciName, tsn, 
                               ProcessType.BISON_TAXA_OCCURRENCE, 
                               tsnCount, BISON_MIN_POINT_COUNT)
      return jobs
         
# ...............................................
   def chainOne(self):
      tsn, tsnCount = self._getTsnRec()
      if tsn is not None:
         jobs = self._processTsn(tsn, tsnCount)
         self.log.info('Processed tsn {}, with {} points; next start {}'
                       .format(tsn, tsnCount, self.nextStart))
         self._createMakeflow(jobs)

# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      if taxonSourceKeyVal is None:
         raise LMError(currargs='Missing taxonSourceKeyVal for BISON query url')
      occAPI = BisonAPI(qFilters=
                        {BISON_HIERARCHY_KEY: '*-{}-*'.format(taxonSourceKeyVal)}, 
                        otherFilters=BISON_OCC_FILTERS)
      occAPI.clearOtherFilters()
      return occAPI.url

# ...............................................
   def moveToStart(self):
      startline = self._findStart()  
      if startline < 1:
         self._linenum = 0
         self._currRec = None
      else:
         tsn, tsnCount = self._getTsnRec()      
         while tsn is not None and self._linenum < startline-1:
            tsn, tsnCount = self._getTsnRec()

# ...............................................
   def  _getInsertSciNameForItisTSN(self, itisTsn, tsnCount):
      if itisTsn is None:
         return None
      sciname = self._scribe.findTaxon(self._taxonSourceId, itisTsn)
         
      if sciname is None:
         try:
            (itisname, king, tsnHier) = BisonAPI.getItisTSNValues(itisTsn)
         except Exception, e:
            self.log.error('Failed to get results for ITIS TSN {} ({})'
                           .format(itisTsn, str(e)))
         else:
            sleep(5)
            if itisname is not None and itisname != '':
               sciname = ScientificName(itisname, kingdom=king,
                                     lastOccurrenceCount=tsnCount, 
                                     taxonomySourceId=self._taxonSourceId, 
                                     taxonomySourceKey=itisTsn, 
                                     taxonomySourceSpeciesKey=itisTsn,
                                     taxonomySourceKeyHierarchy=tsnHier)
               self._scribe.insertTaxon(sciname)
               self.log.info('Inserted sciname for ITIS tsn {}, {}'
                             .format(itisTsn, sciname.scientificName))
      return sciname

# ..............................................................................
class UserBoom(_LMBoomer):
   """
   @summary: Parses a CSV file (with headers) of Occurrences using a metadata 
             file.  A template for the metadata, with instructions, is at 
             LmDbServer/tools/occurrence.meta.example.  
             The parser writes each new text chunk to a file, updates the 
             Occurrence record and inserts one or more jobs.
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, occDataFname, 
                occMetaFname, expDate, priority=Priority.HIGH,
                mdlMask=None, prjMask=None, intersectGrid=None, log=None):
      super(UserBoom, self).__init__(userid, priority, algLst, mdlScen, prjScenLst, 
                                     taxonSourceName=None, 
                                     mdlMask=mdlMask, prjMask=prjMask, 
                                     intersectGrid=intersectGrid, log=log)
      self.occParser = None
      try:
         self.occParser = OccDataParser(self.log, occDataFname, occMetaFname)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         self._failGracefully(lmerr=e)
         
      if self.occParser is not None:
         self._fieldNames = self.occParser.header
         self._obsoleteTime = expDate
      else:
         raise LMError('Failed to initialize OccDataParser')
      
# ...............................................
   def close(self):
      try:
         self.occParser.close()
      except:
         try:
            dataname = self.occParser.dataFname
         except:
            dataname = None
         self.log.error('Unable to close OccDataParser with file/data {}'
                        .format(dataname))
               
# ...............................................
   @property
   def complete(self):
      if self.occParser is not None:
         return self.occParser.closed
      else:
         return True
       
# ...............................................
   @property
   def nextStart(self):
      if self.complete:
         return 0
      else:
         try:
            return self.occParser.keyFirstRec
         except:
            return 0

# ...............................................
   @property
   def currRecnum(self):
      if self.complete:
         return 0
      else:
         try:
            return self.occParser.currRecnum
         except:
            return 0
         
# ...............................................
   def moveToStart(self):
      startline = self._findStart()
      # Assumes first line is header
      if startline > 2:
         self.occParser.skipToRecord(startline)
      elif startline < 0:
         self._currRec = None

# ...............................................
   def _getChunk(self):
      chunk = self.occParser.pullCurrentChunk()
      taxonName = self.occParser.nameValue
      return chunk, len(chunk), taxonName
      
# ...............................................
   def chainOne(self):
      dataChunk, dataCount, taxonName  = self._getChunk()
      if dataChunk:
         jobs = self._processInputSpecies(dataChunk, dataCount, taxonName)
         self.log.info('Processed name {}, with {} records; next start {}'
                       .format(taxonName, len(dataChunk), self.nextStart))
         self._createMakeflow(jobs)

# ...............................................
   def _simplifyName(self, longname):
      front = longname.split('(')[0]
      newfront = front.split(',')[0]
      finalfront = newfront.strip()
      return finalfront
   
# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      rdloc = occ.createLocalDLocation(raw=True)
      success = occ.writeCSV(data, dlocation=rdloc, overwrite=True,
                             header=self._fieldNames)
      if not success:
         rdloc = None
         self.log.debug('Unable to write CSV file {}'.format(rdloc))
      return rdloc

# ...............................................
   def _processInputSpecies(self, dataChunk, dataCount, taxonName):
      jobs = []
      if dataChunk:
         occ = self._createOrResetOccurrenceset(taxonName, None, 
                                          ProcessType.USER_TAXA_OCCURRENCE,
                                          dataCount, data=dataChunk)
   
         # Create jobs for Archive Chain: occurrence population, 
         # model, projection, and (later) intersect computation
         if occ is not None:
            jobs = self._scribe.initSDMChain(self.userid, occ, self.algs, 
                                 self.modelScenario, self.projScenarios, 
                                 occJobProcessType=ProcessType.USER_TAXA_OCCURRENCE,
                                 priority=self.priority, 
                                 intersectGrid=None,
                                 minPointCount=POINT_COUNT_MIN)
            self.log.debug('Init {} jobs for {} ({} points, occid {})'.format(
                           len(jobs), taxonName, len(dataChunk), occ.getId()))
      else:
         self.log.debug('No data in chunk')
      return jobs

# ..............................................................................
class GBIFBoom(_LMBoomer):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, occfilename, expDate,
                priority=Priority.NORMAL, taxonSourceName=None, providerListFile=None,
                mdlMask=None, prjMask=None, intersectGrid=None, log=None):
      self._dumpfile = None
      try:
         self._dumpfile = open(occfilename, 'r')
      except:
         self._failGracefully(lmerr='Failed to open {}'.format(occfilename))

      csv.field_size_limit(sys.maxsize)
      try:
         self._csvreader = csv.reader(self._dumpfile, delimiter='\t')
      except:
         self._failGracefully(lmerr='Failed to init CSV reader with {}'.format(occfilename))

      self._linenum = 0
      super(GBIFBoom, self).__init__(userid, priority, algLst, mdlScen, prjScenLst, 
                                      taxonSourceName=taxonSourceName, 
                                      mdlMask=mdlMask, prjMask=prjMask, 
                                      intersectGrid=intersectGrid, log=log)               
      gbifFldNames = []
      idxs = GBIF_EXPORT_FIELDS.keys()
      idxs.sort()
      for idx in idxs:
         gbifFldNames.append(GBIF_EXPORT_FIELDS[idx][0])
      self._fieldNames = gbifFldNames
      
      self._providers, self._provCol = self._readProviderKeys(providerListFile, 
                                                         GBIF_PROVIDER_FIELD)
      self._keyCol = self._fieldNames.index(GBIF_TAXONKEY_FIELD)
      self._obsoleteTime = expDate
      self._currKeyFirstRecnum = None
      self._currRec = None
      self._currSpeciesKey = None

# ...............................................
   def close(self):
      try:
         self._dumpfile.close()
      except:
         self.log.error('Unable to close {}'.format(self._dumpfile))
         
# ...............................................
   @property
   def complete(self):
      try:
         return self._dumpfile.closed
      except:
         return True
         
# ...............................................
   @property
   def nextStart(self):
      if self.complete:
         return 0
      else:
         return self._currKeyFirstRecnum
   
# ...............................................
   def _readProviderKeys(self, providerKeyFile, providerKeyColname):
      providers = {}
      try:
         provKeyCol = self._fieldNames.index(providerKeyColname)
      except:
         self.log.error('Unable to find {} in fieldnames'
                        .format(providerKeyColname))
         provKeyCol = None
         
      if providerKeyFile is not None and providerKeyColname is not None: 
         if not os.path.exists(providerKeyFile):
            self.log.error('Missing provider file {}'.format(providerKeyFile))
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
   def _processChunk(self, speciesKey, dataCount, dataChunk):
      jobs = []
      sciName = self._getInsertSciNameForGBIFSpeciesKey(speciesKey, dataCount)
      if sciName:       
         jobs = self._processSDMChain(sciName, speciesKey, 
                            ProcessType.GBIF_TAXA_OCCURRENCE, 
                            dataCount, POINT_COUNT_MIN, data=dataChunk)
      return jobs
   
# ...............................................
   def chainOne(self):
      speciesKey, dataCount, dataChunk = self._getOccurrenceChunk()
      if speciesKey:
         jobs = self._processChunk(speciesKey, dataCount, dataChunk)
         self.log.info('Processed gbif key {} with {} records; next start {}'
                       .format(speciesKey, len(dataChunk), self.nextStart))
         self._createMakeflow(jobs)

# ...............................................
   def moveToStart(self):
      startline = self._findStart()         
      if startline < 0:
         self._currKeyFirstRecnum = startline
         self._currRec = self._currSpeciesKey = None
      else:
         line, specieskey = self._getCSVRecord(parse=True)
         # If not there yet, power through lines
         while line is not None and self._linenum < startline-1:
            line, specieskey = self._getCSVRecord(parse=False)
      
# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      rdloc = occ.createLocalDLocation(raw=True)
      success = occ.writeCSV(data, dlocation=rdloc, overwrite=True)
      if not success:
         rdloc = None
         self.log.debug('Unable to write CSV file {}}'.format(rdloc))
      return rdloc
         
# ...............................................
   def _getCSVRecord(self, parse=True):
      success = False
      line = specieskey = None
      while not self._dumpfile.closed and not success:
         try:
            line = self._csvreader.next()
            self._linenum += 1
            success = True
            
         except StopIteration, e:
            self.log.debug('Finished file {} on line {}'
                           .format(self._dumpfile.name, self._linenum))
            self._dumpfile.close()
            success = True
            
         except OverflowError, e:
            self.log.debug( 'OverflowError on {} ({}), moving on'
                            .format(self._linenum, e))
            self._linenum += 1
            
         except Exception, e:
            self.log.debug('Exception reading line {} ({}), moving on'
                           .format(self._linenum, e))
            self._linenum += 1
         
         if line and parse:
            # Post-parse, line is a dictionary
            line, specieskey = self._parseCSVRecord(line)
      return line, specieskey

# ...............................................
   def _parseCSVRecord(self, line):
      specieskey = provkey = None
      if line is not None and len(line) >= 16:
         try:
            specieskey = int(line[self._keyCol])
         except:
            line = None
            self.log.debug('Skipping line; failed to convert specieskey on record {} ({})' 
                  .format(self._linenum, line))
            
         if self._provCol is not None:
            try:
               provkey = line[self._provCol]
            except:
               self.log.debug('Failed to find providerKey on record {} ({})' 
                     .format(self._linenum, str(line)))
            else:
               provname = provkey
               try:
                  provname = self._providers[provkey]
               except:
                  try:
                     provname = GbifAPI.getPublishingOrg(provkey)
                     self._providers[provkey] = provname
                  except:
                     self.log.debug('Failed to find providerKey {} in providers or GBIF API' 
                                .format(provkey))

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
      # if we're at the beginning, pull a record
      if self._currSpeciesKey is None:
         self._currRec, self._currSpeciesKey = self._getCSVRecord()
         if self._currRec is None:
            completeChunk = True
         
      while not completeChunk:
         # If first record of chunk
         if currKey is None:
            currKey = self._currSpeciesKey
            self._currKeyFirstRecnum = self._linenum
         # If record of this chunk
         if self._currSpeciesKey == currKey:
            currCount += 1
            currChunk.append(self._currRec)
         else:
            completeChunk = True
         # Get another record
         if not completeChunk:
            self._currRec, self._currSpeciesKey = self._getCSVRecord()
            if self._currRec is None:
               completeChunk = True
      self.log.debug('Returning {} records for {} (starting on line {})' 
                     .format(currCount, currKey, self._currKeyFirstRecnum))
      return currKey, currCount, currChunk
         
# ..............................................................................
class iDigBioBoom(_LMBoomer):
   """
   @summary: Parses an iDigBio provided file of GBIF Taxon ID, count, binomial, 
             creating a chain of SDM jobs for each, unless the species is 
             up-to-date. 
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, idigFname, expDate,
                priority=Priority.NORMAL, taxonSourceName=None, 
                mdlMask=None, prjMask=None, intersectGrid=None, log=None):
      if taxonSourceName is None:
         self._failGracefully(lmerr='Missing taxonomic source')
         
      try:
         self._idigFile = open(idigFname, 'r')
         self._linenum = 0
      except:
         raise LMError(currargs='Unable to open {}'.format(idigFname))

      super(iDigBioBoom, self).__init__(userid, priority, algLst, mdlScen, prjScenLst, 
                                        taxonSourceName=taxonSourceName, 
                                        mdlMask=mdlMask, prjMask=prjMask, 
                                        intersectGrid=intersectGrid, log=log)
      self._obsoleteTime = expDate      
      self._currBinomial = None
      self._currGbifTaxonId = None
      self._currReportedCount = None
         
# ...............................................
   def chainOne(self):
      taxonKey, taxonCount, taxonName = self._getCurrTaxon()
      if taxonKey:
         jobs = self._processInputGBIFTaxonId(taxonName, taxonKey, taxonCount)
         self._createMakeflow(jobs)

# ...............................................
   def close(self):
      try:
         self._idigFile.close()
      except:
         self.log.error('Unable to close idigFile {}'.format(self._idigFile))
      
# ...............................................
   @property
   def complete(self):
      return self._idigFile.closed
                  
# ...............................................
   @property
   def nextStart(self):
      if self.complete:
         return 0
      else:
         return self._linenum+1

# ...............................................
   def _getCurrTaxon(self):
      """
      @summary: Returns currBinomial, currGbifTaxonId, currReportedCount 
      """
      currGbifTaxonId = None
      currReportedCount = None
      currName = None
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
               self.log.debug('Exception reading line {} ({})'.format(
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
            tempvals = tempvals[1:]
            tempvals = tempvals[1:]
            try:
               currName = ' '.join(tempvals)
            except:
               pass
      return currGbifTaxonId, currReportedCount, currName

# ...............................................
   def moveToStart(self):
      startline = self._findStart()         
      if startline < 1:
         self._linenum = 0
      else:
         taxonKey, taxonCount, taxonName = self._getCurrTaxon()
         while taxonName is not None and self._linenum < startline-1:
            taxonKey, taxonCount, taxonName = self._getCurrTaxon()
         
# ...............................................
   def _countRecords(self, rawfname):
      pointcount = 0
      try:
         f  = open(rawfname, 'r')
         blob = f.read()
      except Exception, e:
         self.log.debug('Failed to read {}'.format(rawfname))
      else:
         pointcount = len(blob.split('\n')) - 1
      finally:
         try:
            f.close()
         except:
            pass
         
      return pointcount
    
# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      if taxonSourceKeyVal is None:
         raise LMError(currargs='Missing taxonSourceKeyVal for iDigBio query url')
      return str(taxonSourceKeyVal)
         
# ...............................................
   def _processInputGBIFTaxonId(self, taxonName, taxonKey, taxonCount):
      jobs = []
      if taxonKey is not None:
         sciName = self._getInsertSciNameForGBIFSpeciesKey(taxonKey, taxonCount)
         jobs = self._processSDMChain(sciName, taxonKey, 
                               ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                               taxonCount, POINT_COUNT_MIN)
      return jobs

# .............................................................................
# .............................................................................
if __name__ == "__main__":
   from LmDbServer.common.lmconstants import IDIGBIO_FILE
   from LmDbServer.common.localconstants import (DEFAULT_ALGORITHMS, 
            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS)
   from LmServer.common.localconstants import ARCHIVE_USER
   
   expdate = dt.DateTime(2016, 1, 1)
   try:
      boomer = iDigBioBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                         DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                         IDIGBIO_FILE, expdate.mjd, taxonSource=1,
                         mdlMask=None, prjMask=None, intersectGrid=None)
   except Exception, e:
      raise LMError(prevargs=e.args)
   else:
      print 'iDigBioBoom is fine'
      
   boomer.chainOne()


"""
import mx.DateTime as dt
import os, sys
import time
from LmCommon.common.apiquery import BisonAPI, GbifAPI, IdigbioAPI
from LmBackend.common.daemon import Daemon
from LmCommon.common.log import DaemonLogger
from LmCommon.common.lmconstants import ProcessType
from LmDbServer.common.lmconstants import (BOOM_PID_FILE, BISON_TSN_FILE, 
         GBIF_DUMP_FILE, IDIGBIO_FILE, TAXONOMIC_SOURCE, PROVIDER_DUMP_FILE,
         USER_OCCURRENCE_CSV, USER_OCCURRENCE_META)
from LmDbServer.common.localconstants import (DEFAULT_ALGORITHMS, 
         DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, DEFAULT_GRID_NAME, 
         SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, SPECIES_EXP_DAY)
from LmDbServer.pipeline.boom import BisonBoom, GBIFBoom, iDigBioBoom, UserBoom
from LmServer.base.taxon import ScientificName
from LmServer.common.localconstants import ARCHIVE_USER, DATASOURCE
expdate = dt.DateTime(2016, 1, 1)
taxname = TAXONOMIC_SOURCE[DATASOURCE]['name']


# ...............................................
boomer = iDigBioBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                         DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                         IDIGBIO_FILE, expdate.mjd, taxonSourceName=taxname,
                         mdlMask=None, prjMask=None, 
                         intersectGrid=DEFAULT_GRID_NAME)
taxonKey, taxonCount, taxonName = boomer._getCurrTaxon()

# ...............................................
boomer = BisonBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            BISON_TSN_FILE, expdate, 
                            taxonSourceName=taxname, mdlMask=None, prjMask=None, 
                            intersectGrid=DEFAULT_GRID_NAME)


# ...............................................
boomer = GBIFBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            GBIF_DUMP_FILE, expdate.mjd, taxonSourceName=taxname,
                            providerListFile=PROVIDER_DUMP_FILE,
                            mdlMask=None, prjMask=None, 
                            intersectGrid=DEFAULT_GRID_NAME)
speciesKey, dataCount, dataChunk = boomer._getOccurrenceChunk()


# ...............................................
boomer = UserBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            USER_OCCURRENCE_CSV, USER_OCCURRENCE_META, expdate, 
                            mdlMask=None, prjMask=None, 
                            intersectGrid=DEFAULT_GRID_NAME)
dataChunk, dataCount, taxonName  = boomer._getChunk()
occ = boomer._createOrResetOccurrenceset(taxonName, None, 
                                       ProcessType.USER_TAXA_OCCURRENCE,
                                       dataCount, data=dataChunk)

jobs = boomer._scribe.initSDMChain(boomer.userid, occ, boomer.algs, 
                          boomer.modelScenario, 
                          boomer.projScenarios, 
                          occJobProcessType=ProcessType.USER_TAXA_OCCURRENCE,
                          priority=Priority.NORMAL, 
                          intersectGrid=None,
                          minPointCount=POINT_COUNT_MIN)

# ...............................................
(rankStr, acceptedkey, kingdomStr, phylumStr, classStr, orderStr, 
             familyStr, genusStr, speciesStr, genuskey, 
             retSpecieskey) = GbifAPI.getTaxonomy(taxonKey)
print('orig={}, accepted={}, species={}, genus={}'.format(taxonKey, acceptedkey, 
      retSpecieskey,  genuskey)) 
             
"""
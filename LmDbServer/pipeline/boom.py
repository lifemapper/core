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
try:
   import mx.DateTime as dt
except:
   pass
try:
   from osgeo.ogr import wkbPoint
except:
   wkbPoint = 1

import csv
import os
import sys
from types import ListType, TupleType

from LmBackend.common.occparse import OccDataParser
from LmCommon.common.apiquery import BisonAPI, GbifAPI, IdigbioAPI
from LmCommon.common.lmconstants import (BISON_OCC_FILTERS, BISON_HIERARCHY_KEY,
            BISON_MIN_POINT_COUNT, ProcessType, DEFAULT_EPSG, JobStatus, 
            ONE_HOUR, ONE_MIN, IDIGBIO_GBIFID_FIELD)
from LmDbServer.common.localconstants import WORKER_JOB_LIMIT
from LmServer.base.lmobj import LMError, LmHTTPError
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import (Priority, PrimaryEnvironment, LOG_PATH)
from LmServer.common.localconstants import (POINT_COUNT_MIN, 
                                    TROUBLESHOOTERS, APP_PATH)
from LmServer.common.log import ScriptLogger
from LmServer.db.scribe import Scribe
from LmServer.notifications.email import EmailNotifier
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.omJob import OmProjectionJob, OmModelJob
from LmServer.sdm.meJob import MeProjectionJob, MeModelJob
from LmServer.sdm.sdmJob import SDMOccurrenceJob

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR
GBIF_SERVICE_INTERVAL = 3 * ONE_MIN            

# .............................................................................
class _LMBoomer(object):
   # .............................
   def __init__(self, userid, algLst, mdlScen, prjScenLst, intersectGrid=None):
      self.userid = userid
      self.algs = algLst
      self.modelScenario = mdlScen
      self.projScenarios = prjScenLst
      self.intersectGrid = intersectGrid

      if userid is None:
         self.name = self.__class__.__name__.lower()
      else:
         self.name = '{}_{}'.format(self.__class__.__name__.lower(), userid)
      self.startFile = os.path.join(APP_PATH, LOG_PATH, 
                                    'start.{}.txt'.format(self.name))
      self._linenum = None
      self.log = ScriptLogger(self.name)
      self.developers = TROUBLESHOOTERS
      self.startStatus=JobStatus.GENERAL 
      self.queueStatus=JobStatus.GENERAL 
      self.endStatus=JobStatus.INITIALIZE
      import socket
      self.hostname = socket.gethostname().lower()
      self.updateTime = None

      try:
         self._scribe = Scribe(self.log)
         success = self._scribe.openConnections()

      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         self._failGracefully(e)
   
      except Exception, e:
         raise LMError(prevargs=e.args)
      else:
         if success:
            self.log.info('%s opened databases' % (self.name))
         else:
            raise LMError('%s failed to open databases' % self.name)
         
# ...............................................
   def _failGracefully(self, lmerr=None):
      self._writeNextStart()
      if lmerr is not None:
         logmsg = str(lmerr)
         logmsg += '\n Traceback: %s' % lmerr.getTraceback()
         self.log.error(logmsg)
         self._notifyPeople("Lifemapper BOOM has failed", 
               '{} Lifemapper BOOM has crashed with the message:\n\n{}'
               .format(self.hostname, logmsg))

      if self._scribe.isOpen:
         try:
            self._scribe.closeConnections()
         except Exception, e:
            self.log.warning('Error while failing: %s' % str(e))
            
      self.log.debug('Time: {}; gracefully exiting ...'.format(dt.utc().mjd))

# ...............................................
   @property
   def nextStart(self):
      return None

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
         self.log.error('Failed to notify %s about %s' 
                        % (str(recipients), subject))

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
   def _writeNextStart(self, linenum=None):
      if self.nextStart is not None:
         try:
            f = open(self.startFile, 'w')
            f.write(str(self.nextStart))
            f.close()
         except Exception, e:
            self.log.error('Failed to write {} to chainer start file {}'
                           .format(self.nextStart, self.startFile))
   
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
   def _getInsertSciNameForExternalSpeciesKey(self, speciesKey):
      raise LMError(currargs='Function must be implemented in subclass')

# ...............................................
   def _getInsertSciNameForGBIFSpeciesKey(self, taxonKey, taxonCount):
      """
      Returns an existing or newly inserted ScientificName
      """
      try:
         sciName = self._scribe.findTaxon(self._taxonSourceId, 
                                              taxonKey)
         if sciName is None:
            # Use API to get and insert species name 
            try:
               (kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr,
                speciesStr, genuskey, retSpecieskey) = GbifAPI.getTaxonomy(taxonKey)
            except LmHTTPError, e:
               self.log.info('Failed lookup for key {}, ({})'.format(
                                                      taxonKey, e.msg))
            if retSpecieskey == taxonKey:
               currtime = dt.gmt().mjd
               sciName = ScientificName(speciesStr, 
                               lastOccurrenceCount=taxonCount,
                               kingdom=kingdomStr, phylum=phylumStr, 
                               txClass=None, txOrder=orderStr, 
                               family=familyStr, genus=genusStr, 
                               createTime=currtime, modTime=currtime, 
                               taxonomySourceId=self._taxonSourceId, 
                               taxonomySourceKey=taxonKey, 
                               taxonomySourceGenusKey=genuskey, 
                               taxonomySourceSpeciesKey=taxonKey)
               self._scribe.insertTaxon(sciName)
      except LMError, e:
         raise e
      except Exception, e:
         raise LMError(currargs=e.args, lineno=self.getLineno())
         
      return sciName
         
# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      raise LMError(currargs='Function must be implemented in subclass')
   
# ...............................................
   def _createOrResetOccurrenceset(self, sciname, taxonSourceKeyVal, 
                                   occProcessType, dataCount, data=None):
      """
      @note: Updates to existing occset are not saved until 
      """
      currtime = dt.gmt().mjd
      occ = None
      try:
         if isinstance(sciname, ScientificName):
            occs = self._scribe.getOccurrenceSetsForScientificName(sciname, 
                                                                self.userid)
            taxonName = sciname.scientificName
         else:
            occs = self._scribe.getOccurrenceSetsForName(sciname, userid=self.userid)
            taxonName = sciname
            sciname = None
            
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
            occid = self._scribe.insertOccurrenceSet(occ)
         elif len(occs) == 1:
            if (occs[0].statusModTime > 0 and 
                occs[0].statusModTime < self._obsoleteTime):
               occ = occs[0]
               occ.updateStatus(JobStatus.INITIALIZE, modTime=currtime)
            else:
               self.log.debug('Occurrenceset {} ({}) is up to date'
                              .format(occs[0].getId(), taxonName))
         else:
            raise LMError('Too many ({}) occurrenceLayers for {}'
                          .format(len(occs), taxonName))
         if occ:
            rdloc = self._locateRawData(occ, taxonSourceKeyVal=taxonSourceKeyVal, 
                                        data=data)
            if rdloc:
               occ.setRawDLocation(rdloc, currtime)
            else:
               raise LMError(currargs='Unable to set raw data location')
            
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      
      return occ
   
# ...............................................
   def _processSDMChain(self, sciname, taxonSourceKeyVal, occProcessType,
                        dataCount, minPointCount, data=None):
      try:
         occ = self._createOrResetOccurrenceset(sciname, taxonSourceKeyVal, 
                           occProcessType, dataCount, minPointCount, data=data)
         if occ:
            # Create jobs for Archive Chain; 'reset' to existing occset will be 
            # saved here
            jobs = self._scribe.initSDMChain(self.userid, occ, self.algs, 
                                      self.modelScenario, 
                                      self.projScenarios, 
                                      occJobProcessType=occProcessType, 
                                      priority=Priority.NORMAL, 
                                      intersectGrid=self.intersectGrid,
                                      minPointCount=minPointCount)
            self.log.debug('Created {} jobs for occurrenceset {}'
                           .format(len(jobs), occ.getId()))
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

# ..............................................................................
class BisonBoom(_LMBoomer):
   """
   @summary: Initializes the job chainer for BISON.
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, tsnfilename, expDate, 
                taxonSource=None, mdlMask=None, prjMask=None, intersectGrid=None):
      super(_LMBoomer, self).__init__(userid, algLst, mdlScen, prjScenLst, 
                                      intersectGrid=intersectGrid)
      if taxonSource is None:
         self._failGracefully('Missing taxonomic source')
      else:
         self._taxonSourceId = taxonSource

      self.modelMask = mdlMask
      self.projMask = prjMask
      self._linenum = 0
      self._tsnfile = open(tsnfilename, 'r')
      self._obsoleteTime = expDate
      self._currTsn, self._currCount = self._skipAhead()
      
# ...............................................
   @property
   def nextStart(self):
      return self._linenum

# ...............................................
   def _getTsnRec(self):
      eof = success = False
      tsn = tsnCount = None
      while not eof and not success:
         line = self._tsnfile.readline()
         if line == '':
            eof = True
         else:
            try:               
               first, second = line.split(',')
               # Returns TSN, TSNCount
               tsn, tsnCount = (int(first), int(second))
               self._linenum += 1
               success = True
            except Exception, e:
               self.log.debug('Exception reading line %d (%s)' % (self._linenum, str(e)))
      return tsn, tsnCount

# ...............................................
   def _processTsn(self, tsn, tsnCount):
      if tsn is not None:
         try:
            sciName = self._getInsertSciNameForItisTSN(tsn, tsnCount)
            self._processSDMChain(sciName, tsn, 
                                  ProcessType.BISON_TAXA_OCCURRENCE, 
                                  tsnCount, BISON_MIN_POINT_COUNT)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e

# ...............................................
   def chainOne(self):
      tsn, tsnCount = self._skipAhead()
      self._processTsn(tsn, tsnCount)

# ...............................................
   def chainAll(self):
      tsn, tsnCount = self._skipAhead()
      while tsn is not None:
         self._processTsn(tsn, tsnCount)
         tsn, tsnCount = self._getTsnRec()
      
# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, dataChunk=None):
      if taxonSourceKeyVal is None:
         raise LMError('Missing taxonSourceKeyVal for BISON query url')
      occAPI = BisonAPI(qFilters=
                        {BISON_HIERARCHY_KEY: '*-%d-*'.format(taxonSourceKeyVal)}, 
                        otherFilters=BISON_OCC_FILTERS)
      occAPI.clearOtherFilters()
      return occAPI.url
   
# ...............................................
   def _processInputItisTSN(self, tsn, tsnCount):
      try:
         sciName = self._getInsertSciNameForItisTSN(tsn, tsnCount)
         self._processSDMChain(sciName, tsn, 
                               ProcessType.BISON_TAXA_OCCURRENCE, 
                               tsnCount, BISON_MIN_POINT_COUNT)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

# ...............................................
   def _skipAhead(self):
      startline = self._findStart()  
      if startline < 0:
         self._linenum = startline
         self._currRec = None
      else:
         tsn, tsnCount = self._getTsnRec()      
         while tsn is not None and self._linenum < startline:
            tsn, tsnCount = self._getTsnRec()
      return tsn, tsnCount
         
# ...............................................
# # ...............................................
#    def _getQueryUrl(self, speciesTsn):
#       occAPI = BisonAPI(qFilters={BISON_HIERARCHY_KEY: '*-%d-*' % speciesTsn}, 
#                         otherFilters=BISON_OCC_FILTERS)
#       occAPI.clearOtherFilters()
#       return occAPI.url

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
class UserBoom(_LMBoomer):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, occDataFname, 
                occMetaFname, expDate, 
                mdlMask=None, prjMask=None, intersectGrid=None):
      super(_LMBoomer, self).__init__(userid, algLst, mdlScen, prjScenLst, 
                                      intersectGrid=intersectGrid)      
      try:
         self.occParser = OccDataParser(self.log, occDataFname, occMetaFname)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         self._failGracefully(e)
         
      self._fieldNames = self.occParser.header
      self.modelMask = mdlMask
      self.projMask = prjMask
      self._obsoleteTime = expDate
      
# ...............................................
   @property
   def nextStart(self):
      try:
         num = self.occParser.keyFirstRec
      except:
         num = 0
      return num

# ...............................................
   def _skipAhead(self):
      startline = self._findStart()
      # Start mid-file? Assumes first line is header
      if startline > 2:
         self.occParser.skipToRecord(startline)
      elif startline < 0:
         self._linenum = startline
         self._currRec = None

# ...............................................
   def _getChunk(self):
      chunk = self.occParser.pullCurrentChunk()
      taxonName = self.occParser.nameValue
      return chunk, len(chunk), taxonName
      
# ...............................................
   def chainOne(self):
      self._skipAhead()
      dataChunk, dataCount, taxonName  = self._getChunk()
      if dataChunk:
         self._processInputSpecies(dataChunk, dataCount, taxonName)

# ...............................................
   def chainAll(self):
      self._skipAhead()
      dataChunk, dataCount, taxonName  = self._getChunk()
      while dataChunk:
         self._processInputSpecies(dataChunk, dataCount, taxonName)

# ...............................................
   def run(self):
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
               self.log.info('Boom complete')
               break

         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            self._failGracefully(e)
            break

      if self._existKillFile():
         self.log.info('LAST CHECKED line {} (killfile)'.format(self.nextStart))
         self._failGracefully(None)

# ...............................................
   def _simplifyName(self, longname):
      front = longname.split('(')[0]
      newfront = front.split(',')
      finalfront = front.strip()
      return finalfront
   
# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      rdloc = occ.createLocalDLocation(raw=True)
      success = occ.writeCSV(data, dlocation=rdloc, overwrite=True,
                             header=self.fieldnames)
      if not success:
         rdloc = None
         self.log.debug('Unable to write CSV file %s' % rdloc)
      return rdloc

# ...............................................
   def _processInputSpecies(self, dataChunk, dataCount, taxonName):
      occ = self._createOrResetOccurrenceset(taxonName, None, 
                                       ProcessType.USER_TAXA_OCCURRENCE,
                                       dataCount, data=dataChunk)

      # Create jobs for Archive Chain: occurrence population, 
      # model, projection, and (later) intersect computation
      jobs = self._scribe.initSDMChain(self.userid, occ, self.algs, 
                                self.modelScenario, 
                                self.projScenarios, 
                                occJobProcessType=ProcessType.USER_TAXA_OCCURRENCE,
                                priority=Priority.NORMAL, 
                                intersectGrid=None,
                                minPointCount=POINT_COUNT_MIN)
      self.log.debug('Init {} jobs for {} ({} points, occid {})'.format(
                     len(jobs), taxonName, len(dataChunk), occ.getId()))


# ..............................................................................
class GBIFBoom(_LMBoomer):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, occfilename, expDate,
                fieldnames, keyColname, taxonSource=None, 
                providerKeyFile=None, providerKeyColname=None,
                mdlMask=None, prjMask=None, intersectGrid=None):
      super(_LMBoomer, self).__init__(userid, algLst, mdlScen, prjScenLst, 
                                      intersectGrid=intersectGrid)
      if taxonSource is None:
         self._failGracefully('Missing taxonomic source')
      else:
         self._taxonSourceId = taxonSource

      self.modelMask = mdlMask
      self.projMask = prjMask

      self._fieldnames = fieldnames
      self._providers, self._provCol = self._readProviderKeys(providerKeyFile, 
                                                              providerKeyColname)
      self._dumpfile = open(occfilename, 'r')
      csv.field_size_limit(sys.maxsize)
      self._csvreader = csv.reader(self._dumpfile, delimiter='\t')
      self._keyCol = fieldnames.index(keyColname)
      self._linenum = 0
      self._obsoleteTime = expDate
#       # Populate self._currKeyFirstRecnum
#       self._currRec, self._currSpeciesKey = self._skipAhead()
      
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
   def _processChunk(self, speciesKey, dataCount, dataChunk):
      try:
         sciName = self._getInsertSciNameForGBIFSpeciesKey(speciesKey, dataCount)         
         self._processSDMChain(sciName, speciesKey, 
                               ProcessType.GBIF_TAXA_OCCURRENCE, 
                               dataCount, POINT_COUNT_MIN, data=dataChunk)
      except LMError, e:
         raise e
      except Exception, e:
         raise LMError(currargs=e.args, lineno=self.getLineno())

# ...............................................
   def chainOne(self):
      # Populate self._currKeyFirstRecnum
      self._currRec, self._currSpeciesKey = self._skipAhead()
      speciesKey, dataCount, dataChunk = self._getOccurrenceChunk()
      self._processChunk(speciesKey, dataCount, dataChunk)

# ...............................................
   def chainAll(self):
      # Populate self._currKeyFirstRecnum
      self._currRec, self._currSpeciesKey = self._skipAhead()
      while self._currRec is not None:
         # _getOccurrenceChunk advances self._currRec
         speciesKey, dataCount, dataChunk = self._getOccurrenceChunk()
         sciName = self._getInsertSciNameForGBIFSpeciesKey(speciesKey, dataCount)         
         self._processSDMChain(sciName, speciesKey, 
                               ProcessType.GBIF_TAXA_OCCURRENCE, 
                               dataCount, POINT_COUNT_MIN, data=dataChunk)

# ...............................................
   def _failGracefully(self, lmerr, linenum=None):
      if linenum is None: 
         try:
            linenum = self.nextStart
         except:
            pass
      if linenum:
         self._writeNextStart(linenum)
      _LMBoomer._failGracefully(self, lmerr)
            
# ...............................................
   def _skipAhead(self):
      linenum = self._findStart()         
      if linenum < 0:
         self._currKeyFirstRecnum = linenum
         self._currRec = self._currSpeciesKey = None
      else:
         line, specieskey = self._getCSVRecord(parse=True)
         # If not there yet, power through lines
         while line is not None and self._linenum < linenum:
            line, specieskey = self._getCSVRecord(parse=False)
         # Finish by parsing and saving the record to be processed next
         self._currKeyFirstRecnum = self._linenum
         line, specieskey = self._parseCSVRecord(line)
         return line, specieskey
      
# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      rdloc = occ.createLocalDLocation(raw=True)
      success = occ.writeCSV(data, dlocation=rdloc, overwrite=True)
      if not success:
         rdloc = None
         self.log.debug('Unable to write CSV file %s' % rdloc)
      return rdloc
         
# ...............................................
   def _getCSVRecord(self, parse=True):
      success = False
      line = specieskey = None
      while not success:
         try:
            line = self._csvreader.next()
            self._linenum += 1
            success = True
         except OverflowError, e:
            self._linenum += 1
            self.log.debug( 'OverflowError on %d (%s), moving on' % (self._linenum, str(e)))
         except Exception, e:
            self.log.debug('Exception reading line %d (%s)' % (self._linenum, str(e)))
            success = True
         if parse:
            # Post-parse, line is a dictionary
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
                   % (self._linenum, str(line)))
            
         if self._provCol is not None:
            try:
               provkey = line[self._provCol]
            except Exception, e:
               self.log.debug('Failed to find providerKey on record %d (%s)' 
                      % (self._linenum, str(line)))
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
            self._currKeyFirstRecnum = self._linenum
         
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
                      % (self._linenum, self._currKeyFirstRecnum))
      self.log.debug('Returning %d records for %d (starting on line %d)' 
            % (currCount, currKey, self._currKeyFirstRecnum))
      return currKey, currCount, currChunk
         
# ..............................................................................
class iDigBioBoom(_LMBoomer):
   """
   @summary: Parses an iDigBio provided file of GBIF Taxon ID, count, binomial, 
             creating a chain of SDM jobs for each, unless the species is 
             up-to-date. 
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, idigFname, expDate,
                taxonSource=None, mdlMask=None, prjMask=None, 
                intersectGrid=None):
      super(_LMBoomer, self).__init__(userid)
      self.modelMask = mdlMask
      self.projMask = prjMask
      self._obsoleteTime = expDate
      self._linenum = 0
      self._currBinomial = None
      self._currGbifTaxonId = None
      self._currReportedCount = None
      if taxonSource is None:
         self._failGracefully('Missing taxonomic source')
      else:
         self._taxonSourceId = taxonSource      
         
# ...............................................
   def chainOne(self):
      taxonId, taxonCount, taxonName = self._skipAhead()
      self._processInputSpecies(taxonName, taxonId, taxonCount)
      self._writeNextStart(self._linenum)

# ...............................................
   def chainAll(self):
      taxonId, taxonCount, taxonName = self._skipAhead()
      while taxonId is not None:
         self._processInputSpecies(taxonName, taxonId, taxonCount)
         taxonId, taxonCount, taxonName = self._getCurrTaxon()
      
# ...............................................
   def _failGracefully(self, lmerr, linenum=None):
      if linenum is None: 
         linenum = self._linenum
      self._writeNextStart(linenum)
      _LMBoomer._failGracefully(self, lmerr)
      
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
      startline = self._findStart()         
      taxonId = taxonCount = taxonName = None
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
   def _locateRawData(self, occ, taxonSourceKeyVal=None, dataChunk=None):
      if taxonSourceKeyVal is None:
         raise LMError('Missing taxonSourceKeyVal for iDigBio query url')
      occAPI = IdigbioAPI(qFilters={IDIGBIO_GBIFID_FIELD: taxonSourceKeyVal})
      occAPI.clearOtherFilters()
      return occAPI.url
         
# ...............................................
   def _processInputGBIFTaxonId(self, taxonName, taxonId, taxonCount):
      try:
         self._getLock()
         sciName = self._getInsertSciNameForGBIFSpeciesKey(taxonId, taxonCount)
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
# .............................................................................
if __name__ == "__main__":
   pass
#    if os.path.exists(JOB_MEDIATOR_PID_FILE):
#       pid = open(JOB_MEDIATOR_PID_FILE).read().strip()
#    else:
#       pid = os.getpid()
#    
#    jobMediator = JobMediator(JOB_MEDIATOR_PID_FILE, log=MediatorLogger(pid))
#    
#    if len(sys.argv) == 2:
#       if sys.argv[1].lower() == 'start':
#          jobMediator.start()
#       elif sys.argv[1].lower() == 'stop':
#          jobMediator.stop()
#       #elif sys.argv[1].lower() == 'update':
#       #   jobMediator.update()
#       elif sys.argv[1].lower() == 'restart':
#          jobMediator.restart()
#       else:
#          print("Unknown command: %s" % sys.argv[1].lower())
#          sys.exit(2)
#    else:
#       print("usage: %s start|stop|update" % sys.argv[0])
#       sys.exit(2)
"""      
# .............................................................................         
if __name__ == '__main__':
   
   from LmCommon.common.lmconstants import ONE_MONTH
   from LmDbServer.common.lmconstants import IDIGBIO_FILE
   from LmDbServer.common.localconstants import (DEFAULT_ALGORITHMS, 
            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, SPECIES_EXP_YEAR, 
            SPECIES_EXP_MONTH, SPECIES_EXP_DAY)

   expdate = dt.DateTime(SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, 
                                  SPECIES_EXP_DAY)
   pname = DATASOURCE.lower()
   txSourceId = 1
   try:
      w = iDigBioBoom(None, pname, ONE_MONTH, DEFAULT_ALGORITHMS, 
                      DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                      IDIGBIO_FILE, expDate=expdate.mjd, taxonSource=txSourceId,
                      mdlMask=None, prjMask=None, intersectGrid=None)
   except Exception, e:
      raise LMError(e)
   else:
      print 'iDigBioBoom is fine'
      
"""

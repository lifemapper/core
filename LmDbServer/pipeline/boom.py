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
from LmServer.base.lmobj import LMError, LmHTTPError, LMObject
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import (Priority, PrimaryEnvironment, LOG_PATH)
from LmServer.common.localconstants import (POINT_COUNT_MIN, TROUBLESHOOTERS, 
                                            APP_PATH)
from LmServer.common.log import ScriptLogger
from LmServer.db.scribe import Scribe
from LmServer.notifications.email import EmailNotifier
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.omJob import OmProjectionJob, OmModelJob
from LmServer.sdm.meJob import MeProjectionJob, MeModelJob

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR
GBIF_SERVICE_INTERVAL = 3 * ONE_MIN            

# .............................................................................
class _LMBoomer(LMObject):
   # .............................
   def __init__(self, userid, algLst, mdlScen, prjScenLst, intersectGrid=None):
      super(_LMBoomer, self).__init__()
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
            self.log.info('{} opened databases'.format(self.name))
         else:
            raise LMError(currargs='{} failed to open databases'.format(self.name))
         
# ...............................................
   def _failGracefully(self, lmerr=None):
      self.saveNextStart()
      if lmerr is not None:
         logmsg = str(lmerr)
         logmsg += '\n Traceback: {}'.format(lmerr.getTraceback())
         self.log.error(logmsg)
         self._notifyPeople("Lifemapper BOOM has failed", 
               '{} Lifemapper BOOM has crashed with the message:\n\n{}'
               .format(self.hostname, logmsg))

      if self._scribe.isOpen:
         try:
            self._scribe.closeConnections()
         except Exception, e:
            self.log.warning('Error while failing: {}'.format(str(e)))
            
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
         self.log.error('Failed to notify {} about {}'.format(recipients, subject))

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
            else:
               self.log.debug('Start location = {}'.format(linenum))
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
         except Exception, e:
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
               occ = tmpOcc
               occ.updateStatus(JobStatus.INITIALIZE, modTime=currtime)
            else:
               self.log.debug('Occurrenceset {} ({}) is up to date'
                              .format(occs[0].getId(), taxonName))
         else:
            raise LMError(currargs='Too many ({}) occurrenceLayers for {}'
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
                           occProcessType, dataCount, data=data)
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
                taxonSourceName=None, mdlMask=None, prjMask=None, intersectGrid=None):
      super(BisonBoom, self).__init__(userid, algLst, mdlScen, prjScenLst, 
                                      intersectGrid=intersectGrid)
      if taxonSourceName is None:
         self._failGracefully('Missing taxonomic source')
      else:
         txSourceId, x,y,z = self._scribe.findTaxonSource(taxonSourceName)
         self._taxonSourceId = txSourceId

      self.modelMask = mdlMask
      self.projMask = prjMask
      self._linenum = 0
      try:
         self._tsnfile = open(tsnfilename, 'r')
      except Exception, e:
         raise LMError(currargs='Unable to open {}'.format(tsnfilename))
      self._obsoleteTime = expDate
      self._currTsn, self._currCount = self.moveToStart()
      
# ...............................................
   @property
   def nextStart(self):
      return self._linenum + 1

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
               self.log.debug('Exception reading line {} ({})'
                              .format(self._linenum, str(e)))
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
      tsn, tsnCount = self._getTsnRec()
      self._processTsn(tsn, tsnCount)
      self.log.info('Processed tsn {}, with {} points; next start {}'
                    .format(tsn, tsnCount, self.nextStart))

# ...............................................
   def chainAll(self):
      tsn, tsnCount = self.moveToStart()
      while tsn is not None:
         self._processTsn(tsn, tsnCount)
         tsn, tsnCount = self._getTsnRec()
      
# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, dataChunk=None):
      if taxonSourceKeyVal is None:
         raise LMError(currargs='Missing taxonSourceKeyVal for BISON query url')
      occAPI = BisonAPI(qFilters=
                        {BISON_HIERARCHY_KEY: '*-{}-*'.format(taxonSourceKeyVal)}, 
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
            self._wait(20)
         except Exception, e:
            self.log.error('Failed to get results for ITIS TSN {} ({})'
                           .format(itisTsn, str(e)))
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
      super(UserBoom, self).__init__(userid, algLst, mdlScen, prjScenLst, 
                                      intersectGrid=intersectGrid)
      self._taxonSourceId = None
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
   def moveToStart(self):
      startline = self._findStart()
      # Assumes first line is header
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
      self.moveToStart()
      dataChunk, dataCount, taxonName  = self._getChunk()
      self._processInputSpecies(dataChunk, dataCount, taxonName)
      self.saveNextStart()
      self.log.info('Processed name {}, with {} records; next start {}'
                    .format(taxonName, len(dataChunk), self.nextStart))

# ...............................................
   def chainAll(self):
      self.moveToStart()
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
         self.log.debug('Unable to write CSV file {}'.format(rdloc))
      return rdloc

# ...............................................
   def _processInputSpecies(self, dataChunk, dataCount, taxonName):
      if dataChunk:
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
      else:
         self.log.debug('No data in chunk')



# ..............................................................................
class GBIFBoom(_LMBoomer):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, userid, algLst, mdlScen, prjScenLst, occfilename, expDate,
                fieldnames, keyColname, taxonSourceName=None, 
                providerKeyFile=None, providerKeyColname=None,
                mdlMask=None, prjMask=None, intersectGrid=None):
      super(GBIFBoom, self).__init__(userid, algLst, mdlScen, prjScenLst, 
                                      intersectGrid=intersectGrid)
      if taxonSourceName is None:
         self._failGracefully('Missing taxonomic source')
      else:
         txSourceId, x,y,z = self._scribe.findTaxonSource(taxonSourceName)
         self._taxonSourceId = txSourceId

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
      self._currKeyFirstRecnum = None
      
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
         self.log.error('Unable to find {} in fieldnames'
                        .format(providerKeyColname))
         provKeyCol = None
         
      if providerKeyFile is not None and providerKeyColname is not None: 
         import os
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
      speciesKey, dataCount, dataChunk = self._getOccurrenceChunk()
      self._processChunk(speciesKey, dataCount, dataChunk)
      self.log.info('Processed gbif key {} with {} records; next start {}'
                    .format(speciesKey, len(dataChunk), self.nextStart))

# ...............................................
   def chainAll(self):
      self.moveToStart()
      while self._currRec is not None:
         # _getOccurrenceChunk advances self._currRec
         speciesKey, dataCount, dataChunk = self._getOccurrenceChunk()
         self._processChunk(speciesKey, dataCount, dataChunk)

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
      while not success:
         try:
            line = self._csvreader.next()
            self._linenum += 1
            success = True
         except OverflowError, e:
            self._linenum += 1
            self.log.debug( 'OverflowError on {} ({}), moving on'
                            .format(self._linenum, e))
         except Exception, e:
            self.log.debug('Exception reading line {} ({})'
                           .format(self._linenum, e))
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
            self.log.debug('Skipping line; failed to convert specieskey on record {} ({})' 
                  .format(self._linenum, str(line)))
            
         if self._provCol is not None:
            try:
               provkey = line[self._provCol]
            except Exception, e:
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
               self.log.debug('Ended on line {} (chunk started on {})' 
                              .format(self._linenum, self._currKeyFirstRecnum))
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
                taxonSourceName=None, mdlMask=None, prjMask=None, 
                intersectGrid=None):
      super(iDigBioBoom, self).__init__(userid, algLst, mdlScen, prjScenLst, 
                                      intersectGrid=intersectGrid)
      if taxonSourceName is None:
         self._failGracefully('Missing taxonomic source')
      else:
         txSourceId, x,y,z = self._scribe.findTaxonSource(taxonSourceName)
         self._taxonSourceId = txSourceId

      self.modelMask = mdlMask
      self.projMask = prjMask
      self._obsoleteTime = expDate
      try:
         self._idigFile = open(idigFname, 'r')
      except Exception, e:
         raise LMError(currargs='Unable to open {}'.format(idigFname))
      self._linenum = 0
      self._currBinomial = None
      self._currGbifTaxonId = None
      self._currReportedCount = None
         
# ...............................................
   def chainOne(self):
      taxonId, taxonCount, taxonName = self._getCurrTaxon()
      self._processInputGBIFTaxonId(taxonName, taxonId, taxonCount)
      self.log.info('Processed taxonId {}, {}, with {} points; next start {}'
                    .format(taxonId, taxonName, taxonCount, self.nextStart))

# ...............................................
   def chainAll(self):
      self.moveToStart()
      taxonId, taxonCount, taxonName = self._getCurrTaxon()
      while taxonId is not None:
         self._processInputGBIFTaxonId(taxonName, taxonId, taxonCount)
         taxonId, taxonCount, taxonName = self._getCurrTaxon()
      
# ...............................................
   @property
   def nextStart(self):
      return self._linenum+1

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
            currBinomial = tempvals[2]
            try:
               currBinomial = ' '.join([currBinomial, tempvals[2]])
            except:
               pass
      return currGbifTaxonId, currReportedCount, currBinomial

# ...............................................
   def moveToStart(self):
      startline = self._findStart()         
      if startline < 1:
         self._linenum = 0
      else:
         taxonId, taxonCount, taxonName = self._getCurrTaxon()
         while taxonName is not None and self._linenum < startline-1:
            taxonId, taxonCount, taxonName = self._getCurrTaxon()
         
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
      occAPI = IdigbioAPI(qFilters={IDIGBIO_GBIFID_FIELD: taxonSourceKeyVal})
      occAPI.clearOtherFilters()
      return occAPI.url
         
# ...............................................
   def _processInputGBIFTaxonId(self, taxonName, taxonId, taxonCount):
      if taxonId is not None:
         try:
            sciName = self._getInsertSciNameForGBIFSpeciesKey(taxonId, taxonCount)
            self._processSDMChain(sciName, taxonId, 
                                  ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                                  taxonCount, POINT_COUNT_MIN)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
         

# .............................................................................
# .............................................................................
if __name__ == "__main__":
   from LmDbServer.common.lmconstants import IDIGBIO_FILE
   from LmDbServer.common.localconstants import (DEFAULT_ALGORITHMS, 
            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS)
   from LmServer.common.localconstants import ARCHIVE_USER
   
   expdate = dt.DateTime(2016, 1, 1)
   try:
      idig = iDigBioBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                         DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                         IDIGBIO_FILE, expdate.mjd, taxonSource=1,
                         mdlMask=None, prjMask=None, intersectGrid=None)
   except Exception, e:
      raise LMError(prevargs=e.args)
   else:
      print 'iDigBioBoom is fine'
      
   idig.chainOne()

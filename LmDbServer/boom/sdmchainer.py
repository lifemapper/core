"""
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
from osgeo.ogr import wkbPoint
import os
import sys
from time import sleep
from types import ListType, TupleType

from LmBackend.common.occparse import OccDataParser
from LmCommon.common.apiquery import BisonAPI, GbifAPI
from LmCommon.common.lmconstants import (GBIF, GBIF_QUERY, BISON, BISON_QUERY, 
                                    ProcessType, JobStatus, ONE_HOUR, OutputFormat) 
from LmDbServer.common.lmconstants import (SpeciesDatasource)
from LmServer.base.lmobj import LMError, LMObject
from LmServer.base.taxon import ScientificName
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import Priority, LOG_PATH, LMFileType
from LmServer.common.localconstants import TROUBLESHOOTERS
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.processchain import MFChain



from LmServer.notifications.email import EmailNotifier

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR

# .............................................................................
class _LMChainer(LMObject):
   # .............................
   def __init__(self, archiveName, userid, epsg, priority, algLst, 
                mdlScen, prjScenLst, 
                taxonSourceName=None, mdlMask=None, prjMask=None, 
                minPointCount=None, intersectGrid=None, intersectParams=None, 
                log=None):
      super(_LMChainer, self).__init__()
      import socket
      self.hostname = socket.gethostname().lower()
      self.userid = userid
      self.priority = priority
      self.minPointCount = minPointCount
      self.algs = []
      self.epsg = epsg
      self.modelScenario = None
      self.projScenarios = []
      self.modelMask = None
      self.projMask = None
      self.intersectGrid = None
      self.intersectParams = intersectParams
      self.globalPAM = None

      if userid is None:
         self.name = self.__class__.__name__.lower()
      else:
         self.name = '{}_{}'.format(self.__class__.__name__.lower(), userid)
      self.startFile = os.path.join(LOG_PATH, 'start.{}.txt'.format(self.name))
      if log is None:
         log = ScriptLogger(self.name)
      self.log = log
      self.developers = TROUBLESHOOTERS
      self.updateTime = None

      try:
         self._scribe = BorgScribe(self.log)
         success = self._scribe.openConnections()
      except Exception, e:
         raise LMError(currargs='Exception opening database', prevargs=e.args)
      else:
         if not success:
            raise LMError(currargs='Failed to open database')
         
      self.log.info('{} opened databases'.format(self.name))
      self._fillDefaultObjects(archiveName, algLst, mdlScen, prjScenLst, 
                               mdlMask, prjMask, intersectGrid, taxonSourceName)
         
# ...............................................
   def _fillConfiguredObjects(self, usr, archiveName, algCodes, mdlScenarioCode, 
                              projScenarioCodes, mdlMaskId, prjMaskId, 
                              intersectGridName, taxonSourceName):
      (userId, archiveName, datasource, algorithms, minPoints, mdlScen, prjScens, 
       epsg, gridname, userOccCSV, userOccDelimiter, userOccMeta, 
       bisonTsnFile, idigTaxonidsFile, gbifTaxFile, gbifOccFile, gbifProvFile, 
       speciesExpYear, speciesExpMonth, speciesExpDay,
       intersectParams) = self.getArchiveSpecificConfig(usr, archiveName)
       
      algs = prjscens = []
      mscen = None
      
      for acode in algorithms:
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         algs.append(alg)

      try:
         txSourceId, url, moddate = self._scribe.findTaxonSource(taxonSourceName)
         
         mscen = self._scribe.getScenario(mdlScenarioCode, user=self.userid, 
                                          fillLayers=True)
         if mscen is not None:
            if mdlScenarioCode not in projScenarioCodes:
               prjscens.append(mscen)
            for pcode in projScenarioCodes:
               scen = self._scribe.getScenario(pcode, user=self.userid, 
                                               fillLayers=True)
               if scen is not None:
                  prjscens.append(scen)
               else:
                  raise LMError('Failed to retrieve scenario {}'.format(pcode))
         else:
            raise LMError('Failed to retrieve scenario {}'.format(mdlScenarioCode))
         
         modelMask = self._scribe.getLayer(mdlMaskId)
         projMask = self._scribe.getLayer(prjMaskId)
         intersectGrid = self._scribe.getShapeGrid(userId=self.userid, 
                                             lyrName=intersectGridName,
                                             epsg=self.epsg)
         # Get gridset for Archive "Global PAM"
         newGridset = Gridset(name=archiveName, shapeGrid=self.intersectGrid, 
                        epsgcode=self.epsg, userId=self.userid)
         boomGridset = self._scribe.getGridset(newGridset, fillMatrices=True)
         if boomGridset is None or boomGridset.pam is None:
            raise LMError('Failed to retrieve Gridset or Global PAM')

      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      
      return (userId, archiveName, algs, txSourceId, mscen, prjscens, modelMask, 
              projMask, intersectGrid, intersectParams, boomGridset)

# ...............................................
   def _fillDefaultObjects(self, archiveName, algCodes, mdlScenarioCode, 
                           projScenarioCodes, mdlMaskId, prjMaskId, 
                           intersectGridName, taxonSourceName):
      # TODO: get config values here, don't pass
#       (userId, archiveName, datasource, algorithms, minPoints, mdlScen, prjScens, 
#        epsg, gridname, userOccCSV, userOccDelimiter, userOccMeta, 
#        bisonTsnFile, idigTaxonidsFile, gbifTaxFile, gbifOccFile, gbifProvFile, 
#        speciesExpYear, speciesExpMonth, speciesExpDay,
#        intersectParams) = self.getArchiveSpecificConfig(self.userId, self.archiveName)
      for acode in algCodes:
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         self.algs.append(alg)

      try:
         txSourceId, url, moddate = self._scribe.findTaxonSource(taxonSourceName)
         self._taxonSourceId = txSourceId
         
         mscen = self._scribe.getScenario(mdlScenarioCode, user=self.userid, 
                                          fillLayers=True)
         if mscen is not None:
            self.modelScenario = mscen
            if mdlScenarioCode not in projScenarioCodes:
               self.projScenarios.append(self.modelScenario)
            for pcode in projScenarioCodes:
               scen = self._scribe.getScenario(pcode, user=self.userid, 
                                               fillLayers=True)
               if scen is not None:
                  self.projScenarios.append(scen)
               else:
                  raise LMError('Failed to retrieve scenario {}'.format(pcode))
         else:
            raise LMError('Failed to retrieve scenario {}'.format(mdlScenarioCode))
         
         self.modelMask = self._scribe.getLayer(mdlMaskId)
         self.projMask = self._scribe.getLayer(prjMaskId)
         self.intersectGrid = self._scribe.getShapeGrid(userId=self.userid, 
                                             lyrName=intersectGridName,
                                             epsg=self.epsg)
         # Get gridset for Archive "Global PAM"
         boomGridset = Gridset(name=archiveName, shapeGrid=self.intersectGrid, 
                        epsgcode=self.epsg, userId=self.userid)
         self.boomGridset = self._scribe.getGridset(boomGridset, fillMatrices=True)
         if self.boomGridset is None or self.boomGridset.pam is None:
            raise LMError('Failed to retrieve Gridset or Global PAM')

      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e

   # .............................
   def getArchiveSpecificConfig(self, userId=None, archiveName=None):
      from LmCommon.common.config import Config
      from LmServer.common.lmconstants import SPECIES_DATA_PATH
      
      fileList = []
      earl = EarlJr()
      pth = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
      archiveConfigFile = os.path.join(pth, 
                                 '{}{}'.format(archiveName, OutputFormat.CONFIG))
      print 'Config file at {}'.format(archiveConfigFile)
      if os.path.exists(archiveConfigFile):
         fileList.append(archiveConfigFile)
      cfg = Config(fns=fileList)
      
      _ENV_CONFIG_HEADING = "LmServer - environment"
      _PIPELINE_CONFIG_HEADING = "LmServer - pipeline"
   
      userCfg = cfg.get(_ENV_CONFIG_HEADING, 'ARCHIVE_USER')
      archiveNameCfg = cfg.get(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_NAME')
      if userId is None:
         userId = userCfg
      if archiveName is None:
         archiveName = archiveNameCfg

      try:
         datasource = cfg.get(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_DATASOURCE')
      except:
         datasource = cfg.get(_ENV_CONFIG_HEADING, 'DATASOURCE')

      algorithms = cfg.getlist(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_ALGORITHMS')
      mdlScen = cfg.get(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_MODEL_SCENARIO')
      prjScens = cfg.getlist(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_PROJECTION_SCENARIOS')
      epsg = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_EPSG')
      gridname = cfg.get(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_GRID_NAME')
      minPoints = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_POINT_COUNT_MIN')
      
      intersectParams = {
         MatrixColumn.INTERSECT_PARAM_FILTER_STRING: 
            Config().get(_PIPELINE_CONFIG_HEADING, 'INTERSECT_FILTERSTRING'),
         MatrixColumn.INTERSECT_PARAM_VAL_NAME: 
            Config().get(_PIPELINE_CONFIG_HEADING, 'INTERSECT_VALNAME'),
         MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: 
            Config().getint(_PIPELINE_CONFIG_HEADING, 'INTERSECT_MINPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: 
            Config().getint(_PIPELINE_CONFIG_HEADING, 'INTERSECT_MAXPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: 
            Config().getint(_PIPELINE_CONFIG_HEADING, 'INTERSECT_MINPERCENT')}

      # Expiration date for retrieved species data 
      speciesExpYear = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_SPECIES_EXP_YEAR')
      speciesExpMonth = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_SPECIES_EXP_MONTH')
      speciesExpDay = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_SPECIES_EXP_DAY')
   
      # User data  
      userOccCSV = userOccMeta = userOccDelimiter = None 
      if SpeciesDatasource.isUser(datasource):
         userOccData = cfg.get(_PIPELINE_CONFIG_HEADING, 
                               'ARCHIVE_USER_OCCURRENCE_DATA')
         userOccDelimiter = cfg.get(_PIPELINE_CONFIG_HEADING, 
                               'ARCHIVE_USER_OCCURRENCE_DATA_DELIMITER')
         userOccCSV = os.path.join(pth, userOccData + OutputFormat.CSV)
         userOccMeta = os.path.join(pth, userOccData + OutputFormat.METADATA)
      
      # Bison data
      bisonTsn = Config().get(_PIPELINE_CONFIG_HEADING, 'BISON_TSN_FILENAME')
      bisonTsnFile = os.path.join(SPECIES_DATA_PATH, bisonTsn)
         
      # iDigBio data
      idigTaxonids = Config().get(_PIPELINE_CONFIG_HEADING, 'IDIG_FILENAME')
      idigTaxonidsFile = os.path.join(SPECIES_DATA_PATH, idigTaxonids)
      
      # GBIF data
      gbifTax = cfg.get(_PIPELINE_CONFIG_HEADING, 'GBIF_TAXONOMY_FILENAME')
      gbifTaxFile = os.path.join(SPECIES_DATA_PATH, gbifTax)
      gbifOcc = cfg.get(_PIPELINE_CONFIG_HEADING, 'GBIF_OCCURRENCE_FILENAME')
      gbifOccFile = os.path.join(SPECIES_DATA_PATH, gbifOcc)
      gbifProv = cfg.get(_PIPELINE_CONFIG_HEADING, 'GBIF_PROVIDER_FILENAME')
      gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
         
      return (userId, archiveName, datasource, algorithms, minPoints, 
              mdlScen, prjScens, epsg, gridname, 
              userOccCSV, userOccDelimiter, userOccMeta, 
              bisonTsnFile, idigTaxonidsFile, 
              gbifTaxFile, gbifOccFile, gbifProvFile, 
              speciesExpYear, speciesExpMonth, speciesExpDay, intersectParams)  


# ...............................................
   def _failGracefully(self, lmerr=None):
      self.saveNextStart(fail=True)
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
            self.log.warning('Error while failing: {}'.format(e))
            
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
   def _getNextLine(self, infile, csvreader=None):
      success = False
      line = None
      while not infile.closed and not success:
         try:
            if csvreader is not None:
               line = csvreader.next()
            else:
               line = infile.next()
         except StopIteration, e:
            self.log.debug('Finished file {} on line {}'
                           .format(infile.name, self._linenum))
            infile.close()
            self._linenum = -9999
            success = True
         except OverflowError, e:
            self._linenum += 1
            self.log.debug( 'OverflowError on {} ({}), moving on'
                            .format(self._linenum, e))
         except Exception, e:
            self._linenum += 1
            self.log.debug('Exception reading line {} ({}), moving on'
                           .format(self._linenum, e))
         else:
            self._linenum += 1
            success = True
            if line == '':
               line = None
      return line

# ...............................................
   def saveNextStart(self, fail=False):
      if fail:
         lineNum = self.thisStart
      else:
         lineNum = self.nextStart
      if lineNum is not None:
         try:
            f = open(self.startFile, 'w')
            f.write(str(lineNum))
            f.close()
         except:
            self.log.error('Failed to write next starting line {} to file {}'
                           .format(lineNum, self.startFile))

   # ...............................................
   def _deleteOccurrenceSet(self, occSet):
      try:
         deleted = self._scribe.completelyRemoveOccurrenceSet(occSet)
      except Exception, e:
         self.log.error('Failed to completely remove occurrenceSet {} ({})'
                        .format(occSet.getId(), e))
      else:
         self.log.debug('   removed occurrenceset {}/{} in MAL'
                        .format(occSet.getId(), occSet.displayName))
      return deleted
         
# ...............................................
   def _getInsertSciNameForGBIFSpeciesKey(self, taxonKey, taxonCount):
      """
      Returns an existing or newly inserted ScientificName
      """
      sciName = self._scribe.findOrInsertTaxon(taxonSourceId=self._taxonSourceId, 
                                               taxonKey=taxonKey)
      if sciName is not None:
         self.log.info('Found sciName for taxonKey {}, {}, with {} points'
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
               sname = ScientificName(scinameStr, 
                               rank=rankStr, 
                               canonicalName=canonicalStr,
                               userId=self.userid, squid=None,
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
                  sciName = self._scribe.findOrInsertTaxon(sciName=sname)
                  self.log.info('Inserted sciName for taxonKey {}, {}'
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
   def _createMakeflow(self, objs):
      updatedMFChain = None
      if objs:
         meta = {MFChain.META_CREATED_BY: os.path.basename(__file__)}
         newMFC = MFChain(self.userid, priority=self.priority, 
                           metadata=meta, status=JobStatus.INITIALIZE, 
                           statusModTime=dt.gmt().mjd)
         updatedMFChain = self._scribe.insertMFChain(newMFC)

         for o in objs:
            try:
               rules = o.computeMe()
            except Exception, e:
               self.log.info('Failed on object.compute {}, ({})'.format(type(o), 
                                                                        str(e)))
            else:
               updatedMFChain.addCommands(rules)
         
         updatedMFChain.write()
         self._scribe.updateObject(updatedMFChain)
         
      return updatedMFChain

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
   @property
   def thisStart(self):
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
   def _createOrResetOccurrenceset(self, sciName, taxonSourceKeyVal, 
                                   occProcessType, dataCount, data=None):
      """
      @param sciName: ScientificName object
      @param taxonSourceKeyVal: unique identifier for this name in source 
             taxonomy database
      @param occProcessType: Type of input data to be converted to shapefile
      @param dataCount: reported number of points for taxon in input dataset
      @param data: raw point data
      @note: Updates to existing occset are not saved until 
      """
      currtime = dt.gmt().mjd
      occ = None
      ignore = False
      # Find existing
      try:
         occ = self._scribe.getOccurrenceSet(squid=sciName.squid, 
                                             userId=self.userid, epsg=self.epsg)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      # Reset existing if:
      if occ is not None:
         # failed
         if (JobStatus.failed(occ.status)
              or
              # waiting with missing data
             (JobStatus.waiting(occ.status) and occ.getRawDLocation() is None)
              or 
              # out-of-date
             (occ.status == JobStatus.COMPLETE and 
              occ.statusModTime > 0 and occ.statusModTime < self._obsoleteTime)):
            # Reset verify hash, name, count, status 
            occ.clearVerify()
            occ.displayName = sciName.scientificName
            occ.queryCount = dataCount
            occ.updateStatus(JobStatus.INITIALIZE, modTime=currtime)
            self.log.info('Updating occset {} ({})'
                          .format(occ.getId(), sciName.scientificName))
         else:
            ignore = True
            self.log.debug('Ignoring occset {} ({}) is up to date'
                           .format(occ.getId(), sciName.scientificName))
      # Create new
      else:
         occ = OccurrenceLayer(sciName.scientificName, self.userid, self.epsg, 
               dataCount, squid=sciName.squid, ogrType=wkbPoint, 
               processType=occProcessType, status=JobStatus.INITIALIZE, 
               statusModTime=currtime, sciName=sciName, rawMetaDLocation=self.metaFname)
         try:
            occ = self._scribe.findOrInsertOccurrenceSet(occ)
            self.log.info('Inserted occset for taxonname {}'.format(sciName.scientificName))
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
      # Set raw data and update status
      if occ and not ignore:
         # Set processType and metadata location (from config, not saved in DB)
         occ.processType = occProcessType
         occ.rawMetaDLocation = self.metaFname
         # Set raw data location
         rdloc = self._locateRawData(occ, taxonSourceKeyVal=taxonSourceKeyVal, 
                                     data=data)
         if not rdloc:
            raise LMError(currargs='Unable to set raw data location')
         occ.setRawDLocation(rdloc, currtime)
         success = self._scribe.updateOccset(occ, polyWkt=None, pointsWkt=None)
      # No need to return up-to-date occ
      if ignore:
         occ = None
      return occ
   
# ...............................................
   def _processSDMChain(self, sciName, taxonSourceKeyVal, occProcessType,
                        dataCount, data=None):
      objs = []
      if sciName is not None:
         try:
            occ = self._createOrResetOccurrenceset(sciName, taxonSourceKeyVal, 
                              occProcessType, dataCount, data=data)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
   
         if occ:
            # Create objs for Archive Chain; 'reset' to existing occset will be 
            # saved here
            try:
               objs = self._scribe.initOrRollbackSDMChain(occ, self.algs, 
                              self.modelScenario, self.projScenarios, 
                              mdlMask=self.modelMask, projMask=self.projMask,
                              gridset=self.boomGridset, 
                              intersectParams=self.intersectParams,
                              minPointCount=self.minPointCount)
               self.log.debug('Created {} objects for occurrenceset {}'
                              .format(len(objs), occ.getId()))
            except Exception, e:
               if not isinstance(e, LMError):
                  e = LMError(currargs=e.args, lineno=self.getLineno())
               raise e
      return objs

# ..............................................................................
class BisonChainer(_LMChainer):
   """
   @summary: Initializes the job chainer for BISON.
   """
   def __init__(self, archiveName, userid, epsg, algLst, mdlScen, prjScenLst, 
                tsnfilename, expDate, 
                priority=Priority.NORMAL, taxonSourceName=None, 
                mdlMask=None, prjMask=None, minPointCount=None,
                intersectGrid=None, intersectParams=None, log=None):
      super(BisonChainer, self).__init__(archiveName, userid, epsg, priority, 
                                      algLst, mdlScen, prjScenLst, 
                                      taxonSourceName=taxonSourceName, 
                                      mdlMask=mdlMask, prjMask=prjMask, 
                                      minPointCount=minPointCount,
                                      intersectGrid=intersectGrid, 
                                      intersectParams=intersectParams, log=log)
      self._tsnfile = None      
      if taxonSourceName is None:
         raise LMError(currargs='Missing taxonomic source')
         
      self._updateFile(tsnfilename, expDate)
      try:
         self._tsnfile = open(tsnfilename, 'r')
      except:
         raise LMError(currargs='Unable to open {}'.format(tsnfilename))
      
      self._linenum = 0
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
   def thisStart(self):
      if self.complete:
         return 0
      else:
         return self._linenum

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
      tsn = tsnCount = None
      line = self._getNextLine(self._tsnfile)
      if line is not None:
         try:               
            first, second = line.split(',')
            # Returns TSN, TSNCount
            tsn, tsnCount = (int(first), int(second))
         except Exception, e:
            self.log.debug('Exception reading line {} ({})'
                           .format(self._linenum, e))
      return tsn, tsnCount

# ...............................................
   def _processTsn(self, tsn, tsnCount):
      objs = []
      if tsn is not None:
         sciName = self._getInsertSciNameForItisTSN(tsn, tsnCount)
         objs = self._processSDMChain(sciName, tsn, 
                               ProcessType.BISON_TAXA_OCCURRENCE, 
                               tsnCount)
      return objs
         
# ...............................................
   def chainOne(self):
      tsn, tsnCount = self._getTsnRec()
      if tsn is not None:
         objs = self._processTsn(tsn, tsnCount)
         self._createMakeflow(objs)
         self.log.info('Processed tsn {}, with {} points; next start {}'
                       .format(tsn, tsnCount, self.nextStart))

# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
      if taxonSourceKeyVal is None:
         raise LMError(currargs='Missing taxonSourceKeyVal for BISON query url')
      occAPI = BisonAPI(qFilters=
                        {BISON.HIERARCHY_KEY: '*-{}-*'.format(taxonSourceKeyVal)}, 
                        otherFilters=BISON_QUERY.OCC_FILTERS)
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
      sciName = self._scribe.findTaxon(self._taxonSourceId, itisTsn)
         
      if sciName is None:
         try:
            (itisname, king, tsnHier) = BisonAPI.getItisTSNValues(itisTsn)
         except Exception, e:
            self.log.error('Failed to get results for ITIS TSN {} ({})'
                           .format(itisTsn, e))
         else:
            sleep(5)
            if itisname is not None and itisname != '':
               sciName = ScientificName(itisname, kingdom=king,
                                     lastOccurrenceCount=tsnCount, 
                                     taxonomySourceId=self._taxonSourceId, 
                                     taxonomySourceKey=itisTsn, 
                                     taxonomySourceSpeciesKey=itisTsn,
                                     taxonomySourceKeyHierarchy=tsnHier)
               self._scribe.insertTaxon(sciName)
               self.log.info('Inserted sciName for ITIS tsn {}, {}'
                             .format(itisTsn, sciName.scientificName))
      return sciName

# ..............................................................................
class UserChainer(_LMChainer):
   """
   @summary: Parses a CSV file (with headers) of Occurrences using a metadata 
             file.  A template for the metadata, with instructions, is at 
             LmDbServer/tools/occurrence.meta.example.  
             The parser writes each new text chunk to a file, inserts or updates  
             the Occurrence record and inserts any dependent objects.
   """
   def __init__(self, archiveName, userid, epsg, algLst, mdlScen, prjScenLst, 
                userOccCSV, userOccMeta, expDate, userOccDelimiter=',',
                priority=Priority.HIGH, minPointCount=None,
                mdlMask=None, prjMask=None, intersectGrid=None, 
                intersectParams=None, log=None):
      super(UserChainer, self).__init__(archiveName, userid, epsg, priority, 
                                     algLst, mdlScen, prjScenLst, 
                                     taxonSourceName=None, 
                                     minPointCount=minPointCount,
                                     mdlMask=mdlMask, prjMask=prjMask, 
                                     intersectGrid=intersectGrid, 
                                     intersectParams=intersectParams, log=log)
      self.metaFname = userOccMeta
      self.occParser = None
      try:
         self.occParser = OccDataParser(self.log, userOccCSV, userOccMeta, 
                                        delimiter=userOccDelimiter) 
      except Exception, e:
         raise LMError(currargs=e.args)
         
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
   def thisStart(self):
      if self.complete:
         return 0
      else:
         try:
            return self.occParser.keyFirstRec
         except:
            return 0

# ...............................................
   @property
   def nextStart(self):
      if self.complete:
         return 0
      else:
         try:
            return self.occParser.currRecnum
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
         objs = self._processUserChunk(dataChunk, dataCount, taxonName)
         self._createMakeflow(objs)
         self.log.info('Processed name {}, with {} records; next start {}'
                       .format(taxonName, len(dataChunk), self.nextStart))

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
   def _getInsertSciNameForUser(self, taxonName):
      bbsciName = ScientificName(taxonName, userId=self.userid)
      sciName = self._scribe.findOrInsertTaxon(sciName=bbsciName)
      return sciName

# ...............................................
   def _processUserChunk(self, dataChunk, dataCount, taxonName):
      objs = []
      sciName = self._getInsertSciNameForUser(taxonName)
      if sciName:       
         objs = self._processSDMChain(sciName, None, 
                            ProcessType.USER_TAXA_OCCURRENCE, 
                            dataCount, data=dataChunk)
      return objs

# ..............................................................................
class GBIFChainer(_LMChainer):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, archiveName, userid, epsg, algLst, mdlScen, prjScenLst, 
                occfilename, expDate,
                priority=Priority.NORMAL, taxonSourceName=None, 
                providerListFile=None, mdlMask=None, prjMask=None, 
                minPointCount=None, intersectGrid=None, intersectParams=None, 
                log=None):
      super(GBIFChainer, self).__init__(archiveName, userid, epsg, priority, 
                                     algLst, mdlScen, prjScenLst, 
                                     taxonSourceName=taxonSourceName, 
                                     mdlMask=mdlMask, prjMask=prjMask, 
                                     minPointCount=minPointCount,
                                     intersectGrid=intersectGrid, 
                                     intersectParams=intersectParams, log=log)               
      self._dumpfile = None
      csv.field_size_limit(sys.maxsize)
      try:
         self._dumpfile = open(occfilename, 'r')
      except:
         raise LMError(currargs='Failed to open {}'.format(occfilename))
      try:
         self._csvreader = csv.reader(self._dumpfile, delimiter='\t')
      except:
         raise LMError(currargs='Failed to init CSV reader with {}'.format(occfilename))

      self._linenum = 0
      gbifFldNames = []
      idxs = GBIF_QUERY.EXPORT_FIELDS.keys()
      idxs.sort()
      for idx in idxs:
         gbifFldNames.append(GBIF_QUERY.EXPORT_FIELDS[idx][0])
      self._fieldNames = gbifFldNames
      
      self._providers, self._provCol = self._readProviderKeys(providerListFile, 
                                                         GBIF.PROVIDER_FIELD)
      self._keyCol = self._fieldNames.index(GBIF.TAXONKEY_FIELD)
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
         return self._linenum

# ...............................................
   @property
   def thisStart(self):
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
   def _processGBIFChunk(self, speciesKey, dataCount, dataChunk):
      objs = []
      sciName = self._getInsertSciNameForGBIFSpeciesKey(speciesKey, dataCount)
      if sciName:       
         objs = self._processSDMChain(sciName, speciesKey, 
                            ProcessType.GBIF_TAXA_OCCURRENCE, 
                            dataCount, data=dataChunk)
      return objs
   
# ...............................................
   def chainOne(self):
      speciesKey, dataCount, dataChunk = self._getOccurrenceChunk()
      if speciesKey:
         objs = self._processGBIFChunk(speciesKey, dataCount, dataChunk)
#          self._createMakeflow(objs)
         self.log.info('Processed gbif key {} with {} records; next start {}'
                       .format(speciesKey, len(dataChunk), self.nextStart))

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
      specieskey = None
      line = self._getNextLine(self._dumpfile, csvreader=self._csvreader)
         
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
                     .format(self._linenum, line))
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
class iDigBioChainer(_LMChainer):
   """
   @summary: Parses an iDigBio provided file of GBIF Taxon ID, count, binomial, 
             creating a chain of SDM objs for each, unless the species is 
             up-to-date. 
   """
   def __init__(self, archiveName, userid, epsg, algLst, mdlScen, prjScenLst, 
                idigFname, expDate,
                priority=Priority.NORMAL, taxonSourceName=None, 
                mdlMask=None, prjMask=None, minPointCount=None, 
                intersectGrid=None, intersectParams=None, log=None):
      super(iDigBioChainer, self).__init__(archiveName, userid, epsg, priority, 
                                        algLst, mdlScen, prjScenLst, 
                                        taxonSourceName=taxonSourceName, 
                                        mdlMask=mdlMask, prjMask=prjMask, 
                                        minPointCount=minPointCount,
                                        intersectGrid=intersectGrid, 
                                        intersectParams=intersectParams, log=log)
      if taxonSourceName is None:
         raise LMError(currargs='Missing taxonomic source')
         
      try:
         self._idigFile = open(idigFname, 'r')
         self._linenum = 0
      except:
         raise LMError(currargs='Unable to open {}'.format(idigFname))

      self._obsoleteTime = expDate
      self._currBinomial = None
      self._currGbifTaxonId = None
      self._currReportedCount = None
         
# ...............................................
   def chainOne(self):
      taxonKey, taxonCount, taxonName = self._getCurrTaxon()
      if taxonKey:
         objs = self._processInputGBIFTaxonId(taxonKey, taxonCount)
         self._createMakeflow(objs)
         self.log.info('Processed key/name {}/{}, with {} records; next start {}'
                       .format(taxonKey, taxonName, taxonCount, self.nextStart))

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
   @property
   def thisStart(self):
      if self.complete:
         return 0
      else:
         return self._linenum

# ...............................................
   def _getCurrTaxon(self):
      """
      @summary: Returns currGbifTaxonId, currReportedCount, currBinomial 
      """
      currGbifTaxonId = currReportedCount = currName = None
      line = self._getNextLine(self._idigFile)
         
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
   def _processInputGBIFTaxonId(self, taxonKey, taxonCount):
      objs = []
      if taxonKey is not None:
         sciName = self._getInsertSciNameForGBIFSpeciesKey(taxonKey, taxonCount)
         objs = self._processSDMChain(sciName, taxonKey, 
                               ProcessType.IDIGBIO_TAXA_OCCURRENCE,
                               taxonCount, self.minPointCount)
      return objs

# .............................................................................


"""
import mx.DateTime as dt
import os, sys
import time

from LmCommon.common.apiquery import BisonAPI, GbifAPI, IdigbioAPI
from LmServer.common.log import ScriptLogger
from LmCommon.common.lmconstants import ProcessType, MatrixType
from LmServer.base.taxon import ScientificName
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.sdmproj import SDMProjection
from LmDbServer.boom.sdmchainer import *
from LmDbServer.boom.boom import Archivist
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.db.borgscribe import BorgScribe
from LmCommon.common.verify import computeHash

from LmDbServer.boom.boom import Archivist
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE

tstUserId='ryan'
tstArchiveName='Heuchera_archive'

(user, archiveName, datasource, algorithms, minPoints, mdlScen, prjScens,  
 epsg, gridname, userOccCSV, userOccDelimiter, userOccMeta, 
 bisonTsnFile, idigTaxonidsFile, gbifTaxFile, gbifOccFile, gbifProvFile, 
 speciesExpYear, speciesExpMonth, 
 speciesExpDay, intersectParams) = Archivist.getArchiveSpecificConfig(userId=tstUserId, 
                                                     archiveName=tstArchiveName)

currtime = dt.gmt().mjd
taxonSourceKeyVal = None
occProcessType = ProcessType.USER_TAXA_OCCURRENCE
expdate = dt.DateTime(speciesExpYear, speciesExpMonth, speciesExpDay)
log = ScriptLogger('testboomborg')

boomer = UserChainer(archiveName, user, epsg, algorithms, mdlScen, prjScens, 
                      userOccCSV, userOccMeta, expdate, 
                      mdlMask=None, prjMask=None, 
                      minPointCount=minPoints, 
                      intersectGrid=gridname, intersectParams=intersectParams,
                      log=log)
                      
# ..............................................................................
# Do this repeatedly to find a new taxa
# ..............................................................................
count = 0
for i in range(63):
   boomer.chainOne()
   
   
   
   
#    dataChunk, dataCount, taxonName  = boomer._getChunk()
#    objs = boomer._processUserChunk(dataChunk, dataCount, taxonName)
#    count = len(objs)


updatedMFChain = boomer._createMakeflow(objs)
print updatedMFChain.getDLocation()





# ...............................................
tstUserId='ryan'
tstArchiveName='Heuchera_archive'
(user, archiveName, datasource, algorithms, minPoints, mdlScen, prjScens,  
 epsg, gridname, userOccCSV, userOccDelimiter, userOccMeta, 
 bisonTsnFile, idigTaxonidsFile, gbifTaxFile, gbifOccFile, gbifProvFile, 
 speciesExpYear, speciesExpMonth, 
 speciesExpDay) = Archivist.getArchiveSpecificConfig(userId=tstUserId, 
                                                     archiveName=tstArchiveName)
taxname = TAXONOMIC_SOURCE[datasource]['name']
boomer = GBIFChainer(archiveName, user, epsg, algorithms, mdlScen, prjScens,
                   gbifOccFile, expdate, taxonSourceName=taxname,
                   providerListFile=gbifProvFile,
                   mdlMask=None, prjMask=None, 
                   minPointCount=minPoints,  
                   intersectGrid=gridname, log=log)

   
   
sciName = boomer._getInsertSciNameForGBIFSpeciesKey(speciesKey, dataCount)
taxonSourceKeyVal = speciesKey
occProcessType = ProcessType.GBIF_TAXA_OCCURRENCE
data = dataChunk

occ = OccurrenceLayer(sciName.scientificName, user, epsg, dataCount, 
               squid=sciName.squid, ogrType=wkbPoint, processType=occProcessType,
               status=JobStatus.INITIALIZE, statusModTime=currtime, 
               sciName=sciName)
occ = boomer._scribe.findOrInsertOccurrenceSet(occ)
prjs = boomer._scribe.initOrRollbackSDMProjects(occ, boomer.modelScenario, 
               boomer.projScenarios, boomer.algs[0], mdlMask=None, projMask=None, 
               modtime=currtime)
prj = prjs[0]

mtxcol = boomer._scribe.initOrRollbackIntersect(prj, gpam, currtime)



objs = boomer._processGBIFChunk(speciesKey, dataCount, dataChunk)
self._createMakeflow(objs)


dataChunk, dataCount, taxonName  = boomer._getChunk()
occ = boomer._createOrResetOccurrenceset(taxonName, None, 
                                       ProcessType.USER_TAXA_OCCURRENCE,
                                       dataCount, data=dataChunk)

objs = boomer._scribe.initOrRollbackSDMChain(occ, boomer.algs, 
                          boomer.modelScenario, 
                          boomer.projScenarios, 
                          intersectGrid=None,
                          minPointCount=POINT_COUNT_MIN)

# ...............................................
(rankStr, acceptedkey, kingdomStr, phylumStr, classStr, orderStr, 
             familyStr, genusStr, speciesStr, genuskey, 
             retSpecieskey) = GbifAPI.getTaxonomy(taxonKey)
print('orig={}, accepted={}, species={}, genus={}'.format(taxonKey, acceptedkey, 
      retSpecieskey,  genuskey)) 
             
"""
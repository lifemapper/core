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
class _SpeciesWeaponOfChoice(LMObject):
   # .............................
   def __init__(self, scribe, user, archiveName, epsg, expDate, minPoints, 
                taxonSourceName=None, logger=None):
      super(_SpeciesWeaponOfChoice, self).__init__()
      # Set name for this WoC
      self.name = '{}_{}_{}'.format(user, self.__class__.__name__.lower(), 
                                    archiveName)
      # Optionally use parent process logger
      if logger is None:
         logger = ScriptLogger(self.name)
      self.log = logger
      self._scribe = scribe
      self.userId = user
      self.epsg = epsg
      self._obsoleteTime = expDate
      self._minPoints = minPoints
      # Taxon Source for known taxonomy
      self._taxonSourceId = None
      if taxonSourceName is not None:
         txSourceId, x, x = self._scribe.findTaxonSource(taxonSourceName)
         self._taxonSourceId = txSourceId
      # Beginning of iteration
      self.startFile = os.path.join(LOG_PATH, 'start.{}.txt'.format(self.name))
      self._linenum = 0

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
   def _doReset(self, status, statusModTime, rawDataLocation):
      doReset = False
      if (JobStatus.failed(status) or
           # waiting with missing data
          (JobStatus.waiting(status) and rawDataLocation is None) or 
           # out-of-date
          (status == JobStatus.COMPLETE and 
           statusModTime > 0 and 
           statusModTime < self._obsoleteTime)):
         doReset = True
      return doReset

# ...............................................
   def _createOrResetOccurrenceset(self, sciName, dataCount, 
                                   taxonSourceKey=None, data=None):
      """
      @param sciName: ScientificName object
      @param dataCount: reported number of points for taxon in input dataset
      @param taxonSourceKey: unique identifier for this name in source 
             taxonomy database
      @param data: raw point data
      @note: Updates to existing occset are not saved until 
      """
      currtime = dt.gmt().mjd
      occ = None
      # Find existing
      try:
         occ = self._scribe.getOccurrenceSet(squid=sciName.squid, 
                                             userId=self.userId, epsg=self.epsg)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      
      # Reset existing
      if occ is not None:
         if self._doReset(occ.status, occ.statusModTime, occ.getRawDLocation()):
            # Reset verify hash, name, count, status 
            occ.clearVerify()
            occ.displayName = sciName.scientificName
            occ.queryCount = dataCount
            occ.updateStatus(JobStatus.INITIALIZE, modTime=currtime)
            self.log.info('Found and reseting occset {} ({})'
                          .format(occ.getId(), sciName.scientificName))
         else:
            occ = None
            self.log.debug('Ignoring occset {} ({}) is up to date'
                           .format(occ.getId(), sciName.scientificName))
      # or create new
      else:
         occ = OccurrenceLayer(sciName.scientificName, self.userId, self.epsg, 
               dataCount, squid=sciName.squid, ogrType=wkbPoint, 
               processType=self.processType, status=JobStatus.INITIALIZE, 
               statusModTime=currtime, sciName=sciName, 
               rawMetaDLocation=self.metaFname)
         try:
            occ = self._scribe.findOrInsertOccurrenceSet(occ)
            self.log.info('Inserted occset for taxonname {}'.format(sciName.scientificName))
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
         
      # Update new or reset object with raw data
      if occ is not None:
         rdloc = self._locateRawData(occ, taxonSourceKeyVal=taxonSourceKey, 
                                     data=data)
         if not rdloc:
            raise LMError(currargs='Unable to set raw data location')
         occ.setRawDLocation(rdloc, currtime)
         # Set processType and metadata location (from config, not saved in DB)
         occ.processType = self.occProcessType
         occ.rawMetaDLocation = self.metaFname
         success = self._scribe.updateOccset(occ, polyWkt=None, pointsWkt=None)

      return occ
# ...............................................
   def _raiseSubclassError(self):
      raise LMError(currargs='Function must be implemented in subclass')

# ...............................................
   def _locateRawData(self, occ, taxonSourceKeyVal=None, data=None):
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


# ..............................................................................
class BisonWoC(_SpeciesWeaponOfChoice):
   """
   @summary: Initializes the species WoC for BISON.
   """
   def __init__(self, scribe, user, archiveName, epsg, expDate, minPoints, 
                tsnFname, taxonSourceName=None, logger=None):
#       # Set name for this WoC
#       self.name = '{}_{}_{}'.format(user, self.__class__.__name__.lower(), 
#                                     archiveName)
      super(BisonWoC, self).__init__(scribe, user, archiveName, epsg, expDate, 
                     minPoints, taxonSourceName=taxonSourceName, logger=logger)
      # Bison-specific attributes
      self.processType = ProcessType.BISON_TAXA_OCCURRENCE
      self._tsnfile = None      
      if taxonSourceName is None:
         raise LMError(currargs='Missing taxonomic source')
         
      self._updateFile(tsnFname, expDate)
      try:
         self._tsnfile = open(tsnFname, 'r')
      except:
         raise LMError(currargs='Unable to open {}'.format(tsnFname))
      
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
   def getOne(self):
      occ = None
      tsn, tsnCount = self._getTsnRec()
      if tsn is not None:
         sciName = self._getInsertSciNameForItisTSN(tsn, tsnCount)
         if sciName is not None:
            occ = self._createOrResetOccurrenceset(sciName, tsn, tsnCount)
         self.log.info('Processed tsn {}, with {} points; next start {}'
                       .format(tsn, tsnCount, self.nextStart))
      return occ

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

# ..............................................................................
class UserWoC(_SpeciesWeaponOfChoice):
   """
   @summary: Parses a CSV file (with headers) of Occurrences using a metadata 
             file.  A template for the metadata, with instructions, is at 
             LmDbServer/tools/occurrence.meta.example.  
             The parser writes each new text chunk to a file, inserts or updates  
             the Occurrence record and inserts any dependent objects.
   """
   def __init__(self, scribe, user, archiveName, epsg, expDate, minPoints, 
                userOccCSV, userOccMeta, userOccDelimiter, logger=None):
      super(UserWoC, self).__init__(scribe, user, archiveName, epsg, expDate, 
                                    minPoints, logger=logger)
      # User-specific attributes
      self.processType = ProcessType.USER_TAXA_OCCURRENCE
      self.metaFname = userOccMeta
      self.dataFname = userOccCSV
      self.occParser = None
      try:
         self.occParser = OccDataParser(logger, userOccCSV, userOccMeta, 
                                        delimiter=userOccDelimiter) 
      except Exception, e:
         raise LMError(currargs=e.args)
         
      if self.occParser is not None:
         self._fieldNames = self.occParser.header
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
   def getOne(self):
      occ = None
      dataChunk, dataCount, taxonName  = self._getChunk()
      if dataChunk:
         # Get or insert ScientificName (squid)
         bbsciName = ScientificName(taxonName, userId=self.userId)
         sciName = self._scribe.findOrInsertTaxon(sciName=bbsciName)
         if sciName is not None:
            occ = self._createOrResetOccurrenceset(sciName, None, dataCount, data=dataChunk)
         self.log.info('Processed name {}, with {} records; next start {}'
                       .format(taxonName, len(dataChunk), self.nextStart))
      return occ

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


# ..............................................................................
class GBIFWoC(_SpeciesWeaponOfChoice):
   """
   @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
             text chunk to a file, then creates an OccurrenceJob for it and 
             updates the Occurrence record and inserts a job.
   """
   def __init__(self, scribe, user, archiveName, epsg, expDate, minPoints, 
                occFname, providerFname=None, taxonSourceName=None, logger=None):
      super(GBIFWoC, self).__init__(scribe, user, archiveName, epsg, expDate, 
                     minPoints, taxonSourceName=taxonSourceName, logger=logger)
      # GBIF-specific sorted CSV data
      self.processType = ProcessType.GBIF_TAXA_OCCURRENCE
      self._dumpfile = None
      csv.field_size_limit(sys.maxsize)
      try:
         self._dumpfile = open(occFname, 'r')
      except:
         raise LMError(currargs='Failed to open {}'.format(occFname))
      try:
         self._csvreader = csv.reader(self._dumpfile, delimiter='\t')
      except:
         raise LMError(currargs='Failed to init CSV reader with {}'.format(occFname))

      # GBIF fieldnames/column indices           
      self._keyCol = self._fieldNames.index(GBIF.TAXONKEY_FIELD)
      gbifFldNames = []
      idxs = GBIF_QUERY.EXPORT_FIELDS.keys()
      idxs.sort()
      for idx in idxs:
         gbifFldNames.append(GBIF_QUERY.EXPORT_FIELDS[idx][0])
      self._fieldNames = gbifFldNames
      
      # Save known GBIF provider/IDs for lookup if available
      try:
         self._providers, self._provCol = self._readProviderKeys(providerFname, 
                                                            GBIF.PROVIDER_FIELD)
      except:
         self._providers = []
         self._provCol = -1
         
      # Record start
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
   def getOne(self):
      occ = None
      speciesKey, dataCount, dataChunk = self._getOccurrenceChunk()
      if speciesKey:
         sciName = self._getInsertSciNameForGBIFSpeciesKey(speciesKey, dataCount)
         if sciName is not None:
            occ = self._createOrResetOccurrenceset(sciName, speciesKey, 
                                                   dataCount, data=dataChunk)
         self.log.info('Processed gbif key {} with {} records; next start {}'
                       .format(speciesKey, len(dataChunk), self.nextStart))
      return occ 
   
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
class iDigBioWoC(_SpeciesWeaponOfChoice):
   """
   @summary: Parses an iDigBio provided file of GBIF Taxon ID, count, binomial, 
             creating a chain of SDM objs for each, unless the species is 
             up-to-date. 
   """
   def __init__(self, scribe, user, archiveName, epsg, expDate, minPoints, 
                idigFname, taxonSourceName=None, logger=None):
      super(iDigBioWoC, self).__init__(scribe, user, archiveName, epsg, expDate, 
                     minPoints, taxonSourceName=taxonSourceName, logger=logger)
      self.processType = ProcessType.IDIGBIO_TAXA_OCCURRENCE
      # iDigBio-specific file of (GBIF) accepted taxon-ids
      try:
         self._idigFile = open(idigFname, 'r')
      except:
         raise LMError(currargs='Unable to open {}'.format(idigFname))

      self._currBinomial = None
      self._currGbifTaxonId = None
      self._currReportedCount = None
        
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
   def getOne(self):
      occ = None
      taxonKey, taxonCount, taxonName = self._getCurrTaxon()
      if taxonKey is not None:
         sciName = self._getInsertSciNameForGBIFSpeciesKey(taxonKey, taxonCount)
         if sciName is not None:
            occ = self._createOrResetOccurrenceset(sciName, taxonKey, taxonCount)         
         self.log.info('Processed key/name {}/{}, with {} records; next start {}'
                       .format(taxonKey, taxonName, taxonCount, self.nextStart))
      return occ
   
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

# .............................................................................
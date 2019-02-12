"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
import shutil
try:
    import mx.DateTime as dt
except:
    pass

import csv
import json
import os
import sys

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.apiquery import GbifAPI
from LmCommon.common.unicode import fromUnicode, toUnicode
from LmCommon.common.lmconstants import (GBIF, ProcessType, 
                                         JobStatus, ONE_HOUR, LMFormat) 
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import readyFilename

from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import LOG_PATH
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.legion.occlayer import OccurrenceLayer

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR

# .............................................................................
class _SpeciesWeaponOfChoice(LMObject):
    # .............................
    def __init__(self, scribe, user, archiveName, epsg, expDate, inputFname,  
                     metaFname=None, taxonSourceName=None, logger=None):
        """
        @param scribe: An open LmServer.db.borgscribe.BorgScribe object
        @param user: Userid
        @param archiveName: Name of gridset to be created for these data
        @param epsg: EPSG code for map projection
        @param expDate: Expiration date in Modified Julian Day format
        @param inputFname: Input species data filename or directory containing 
                 multiple files of CSV data
        @param metaFname: Input species metadata filename or dictionary
        @param taxonSourceName: Unique name of entity providing taxonomy data
        @param logger: a logger from LmServer.common.log
        """
        super(_SpeciesWeaponOfChoice, self).__init__()
        self.finishedInput = False
        # Set name for this WoC
        self.name = '{}_{}'.format(user, archiveName)
        # Optionally use parent process logger
        if logger is None:
            logger = ScriptLogger(self.name)
        self.log = logger
        self._scribe = scribe
        self.userId = user
        self.epsg = epsg
        self._obsoleteTime = expDate
        # either a file or directory
        self.inputFilename = inputFname
        # Common metadata description for csv points
        # May be installed with species data in location unavailable to Makeflow
        self.metaFilename = metaFname
        # Taxon Source for known taxonomy
        self._taxonSourceId = None
        if taxonSourceName is not None:
            txSourceId, _, _ = self._scribe.findTaxonSource(taxonSourceName)
            self._taxonSourceId = txSourceId
        # Beginning of iteration
        self.startFile = os.path.join(LOG_PATH, 'start.{}.txt'.format(self.name))
        self._linenum = 0

    # .............................
    def initializeMe(self):
        pass

# ...............................................
    @property
    def expirationDate(self):
        return self._obsoleteTime
    
# ...............................................
    def _findStart(self):
        linenum = 0
        complete = False
        if os.path.exists(self.startFile):
            f = open(self.startFile, 'r')
            for line in f:
                if not complete:
                    self.log.info('Start on line {}'.format(line))
                    try:
                        linenum = int(line)
                        complete = True
                    except Exception, e:
                        # Ignore comment lines
                        pass
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
                self.finishedInput = True
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
                f.write('# Next start line for {} using species data {}\n'
                          .format(self.name, self.inputFilename))
                f.write('{}\n'.format(lineNum))
                f.close()
            except:
                self.log.error('Failed to write next starting line {} to file {}'
                                    .format(lineNum, self.startFile))

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
    def _willCompute(self, status, statusModTime, dlocation, rawDataLocation):
        willCompute = False
        noRawData = (rawDataLocation is None) or not (os.path.exists(rawDataLocation))
        noCompleteData = (dlocation is None) or not (os.path.exists(dlocation))
        obsoleteData = (statusModTime > 0) and (statusModTime < self._obsoleteTime)
        if (JobStatus.incomplete(status) or
             JobStatus.failed(status) or
              # waiting with missing data
             (JobStatus.waiting(status) and noRawData) or 
              # out-of-date
             (status == JobStatus.COMPLETE and noCompleteData or obsoleteData)):
            willCompute = True
        return willCompute

# ...............................................
    def _findOrInsertOccurrenceset(self, sciName, dataCount, data=None, 
                                   metadata={}):
        """
        @param sciName: ScientificName object
        @param dataCount: reported number of points for taxon in input dataset
        @param data: raw point data, stream or filename
        """
        currtime = dt.gmt().mjd
        occ = None
        # Find existing
        tmpocc = OccurrenceLayer(sciName.scientificName, self.userId, self.epsg, 
                dataCount, squid=sciName.squid,
                processType=self.processType, status=JobStatus.INITIALIZE, 
                statusModTime=currtime, sciName=sciName, 
                rawMetaDLocation=self.metaFilename)
        try:
            occ = self._scribe.findOrInsertOccurrenceSet(tmpocc)
            self.log.info('    Found/inserted OccLayer {}'.format(occ.getId()))
        except Exception, e:
            if not isinstance(e, LMError):
                e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e

        if occ is not None:
            # Do reset existing or new Occ?
            willCompute = self._willCompute(occ.status, occ.statusModTime, 
                                            occ.getDLocation(), occ.getRawDLocation())
            if willCompute:
                self.log.info('    Reseting OccLayer raw data')
                # Write raw data and metadata, update in new or reset object
                rdloc, rawmeta_dloc = self._writeRawData(occ, data=data, 
                                                         metadata=metadata)
                if not rdloc:
                    raise LMError(currargs='    Failed to find raw data location')
                occ.setRawDLocation(rdloc, currtime)
                # Set scientificName, not pulled from DB, for alternate iDigBio query
                _ = self._scribe.updateObject(occ)
                # Set processType and metadata location (from config, not saved in DB)
                occ.processType = self.processType
                occ.rawMetaDLocation = self.metaFilename
            else:
                # Return existing, completed, unchanged
                self.log.info('    Returning up-to-date OccLayer')
        return occ
    
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
                (rankStr, scinameStr, canonicalStr, _, _, _, taxStatus, 
                 kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr, 
                 _, genusKey, speciesKey, _) = GbifAPI.getTaxonomy(taxonKey)
            except Exception, e:
                self.log.info('Failed lookup for key {}, ({})'.format(
                                                                    taxonKey, e))
            else:
                # if no species key, this is not a species
                if rankStr in ('SPECIES', 'GENUS') and taxStatus == 'ACCEPTED':
#                 if taxonKey in (retSpecieskey, acceptedkey, genuskey):
                    currtime = dt.gmt().mjd
                    # Do not tie GBIF taxonomy to one userid
                    sname = ScientificName(scinameStr, 
                                         rank=rankStr, 
                                         canonicalName=canonicalStr, squid=None,
                                         lastOccurrenceCount=taxonCount,
                                         kingdom=kingdomStr, phylum=phylumStr, 
                                         txClass=classStr, txOrder=orderStr, 
                                         family=familyStr, genus=genusStr, 
                                         modTime=currtime, 
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
    def _writeRawData(self, occ, data=None, metadata=None):
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
class UserWoC(_SpeciesWeaponOfChoice):
    """
    @summary: Parses a CSV file (with headers) of Occurrences using a metadata 
                 file.  A template for the metadata, with instructions, is at 
                 LmDbServer/tools/occurrence.meta.example.  
                 The parser writes each new text chunk to a file, inserts or updates  
                 the Occurrence record and inserts any dependent objects.
    @note: If useGBIFTaxonomy is true, the 'GroupBy' field in the metadata
                 should name the field containing the GBIF TaxonID for the accepted 
                 Taxon of each record in the group. 
    """
    def __init__(self, scribe, user, archiveName, epsg, expDate, 
                     userOccCSV, userOccMeta, userOccDelimiter, 
                     logger=None, processType=ProcessType.USER_TAXA_OCCURRENCE, 
                     providerFname=None, useGBIFTaxonomy=False, 
                     taxonSourceName=None):
        super(UserWoC, self).__init__(scribe, user, archiveName, epsg, expDate, 
                                                userOccCSV, metaFname=userOccMeta, 
                                                taxonSourceName=taxonSourceName, 
                                                logger=logger)
        # Save known GBIF provider/IDs for lookup if available
        self._providers = []
        self._provCol = None
        if providerFname is not None and os.path.exists(providerFname):
            try:
                self._providers, self._provCol = self._readProviderKeys(
                    providerFname, GBIF.PROVIDER_FIELD)
            except:
                pass
        # User-specific attributes
        self.processType = processType
        self.useGBIFTaxonomy = useGBIFTaxonomy
        self._userOccCSV = userOccCSV
        self._userOccMeta = userOccMeta
        self._delimiter = userOccDelimiter
        self.occParser = None
            
        
    # .............................
    def initializeMe(self):
        """
        @summary: Creates objects (ChristopherWalken for walking the species
                     and MFChain objects for workflow computation requests.
        """
        try:
            self.occParser = OccDataParser(self.log, self._userOccCSV, 
                                                     self._userOccMeta, 
                                                     delimiter=self._delimiter,
                                                     pullChunks=True) 
        except Exception, e:
            raise LMError('Failed to construct OccDataParser, {}'.format(e))
        
        self._fieldNames = self.occParser.header
        self.occParser.initializeMe()         

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
        try:
            return self.occParser.closed
        except:
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
    def _replaceLookupKeys(self, dataChunk):
        chunk = []
        for line in dataChunk:
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
                chunk.append(line)
        return chunk        

# ...............................................
    def getOne(self):
        """
        @summary: Create and return an OccurrenceLayer from a chunk of CSV 
                  records grouped by a GroupBy value indicating species,  
                  possibly a GBIF `taxonKey`
        @note: If useGBIFTaxonomy is true, 
                 - the `taxonKey` will contain the GBIF TaxonID for the accepted 
                    Taxon of each record in the chunk, and a taxon record will be 
                    retrieved (if already present) or queried from GBIF and inserted
                 - the OccurrenceLayer.displayname will use the resolved GBIF 
                    canonical name
        @note: If taxonName is missing, and useGBIFTaxonomy is False, 
                 the OccurrenceLayer.displayname will use the GroupBy value
        """
        occ = None
        dataChunk, taxonKey, taxonName = self.occParser.pullCurrentChunk()
        if dataChunk:
            # If data is from GBIF, replace Provider key with name
            if self._provCol is not None:
                dataChunk = self._replaceLookupKeys(dataChunk)
                        
            # Get or insert ScientificName (squid)
            if self.useGBIFTaxonomy:
                sciName = self._getInsertSciNameForGBIFSpeciesKey(taxonKey, None)
                if sciName:
                    # Override the given taxonName with the resolved GBIF canonical name
                    taxonName = sciName.canonicalName
            else:
                if not taxonName:
                    taxonName = taxonKey
                bbsciName = ScientificName(taxonName, userId=self.userId)
                sciName = self._scribe.findOrInsertTaxon(sciName=bbsciName)

            if sciName is not None:
                occ = self._findOrInsertOccurrenceset(sciName, len(dataChunk), 
                                data=dataChunk, metadata=self.occParser.columnMeta)
                if occ is not None:
                    self.log.info('WOC processed occset {}, name {}, with {} records; next start {}'
                                      .format(occ.getId(), taxonName, len(dataChunk), 
                                                 self.nextStart))
        return occ
    
# ...............................................
    def _writeRawData(self, occ, data=None, metadata=None):
        rdloc = occ.createLocalDLocation(raw=True)
        success = occ.writeCSV(data, dlocation=rdloc, overwrite=True,
                               header=self._fieldNames)
        if not success:
            rdloc = None
            self.log.debug('Unable to write CSV file {}'.format(rdloc))
        else:
            # Write interpreted metadata along with raw CSV
            rawmeta_dloc = rdloc + LMFormat.JSON.ext
            readyFilename(rawmeta_dloc, overwrite=True)
            with open(rawmeta_dloc, 'w') as f:
                json.dump(metadata, f)
        return rdloc, rawmeta_dloc
         
# ..............................................................................
class TinyBubblesWoC(_SpeciesWeaponOfChoice):
    """
    @summary: Moves multiple csv occurrence files (pre-parsed by taxa, with or  
              without headers).  A template for the metadata, with instructions,  
              is at LmDbServer/tools/occurrence.meta.example.  
              The WOC renames and moves each csv file to the correct location,  
              inserts or updates the Occurrence record and inserts any dependent 
              objects.
    @note: If useGBIFTaxonomy is true, the 'GroupBy' field in the metadata
           should name the field containing the GBIF TaxonID for the accepted 
           Taxon of each record in the group. 
    """
    def __init__(self, scribe, user, archiveName, epsg, expDate, 
                     occCSVDir, occMeta, occDelimiter, dirContentsFname,
                     logger=None, processType=ProcessType.USER_TAXA_OCCURRENCE, 
                     useGBIFTaxonomy=False, taxonSourceName=None):
        super(TinyBubblesWoC, self).__init__(scribe, user, archiveName, epsg, expDate, 
                                             occCSVDir, metaFname=occMeta, 
                                             taxonSourceName=taxonSourceName, 
                                             logger=logger)
        # specific attributes
        self.processType = processType
        self._occCSVDir = occCSVDir
        self._occMeta = occMeta
        self._delimiter = occDelimiter
        self._dirContentsFile = None
        self._updateFile(dirContentsFname, expDate)
        try:
            self._dirContentsFile = open(dirContentsFname, 'r')
        except:
            raise LMError(currargs='Unable to open {}'.format(dirContentsFname))
        self.useGBIFTaxonomy = useGBIFTaxonomy

# ...............................................
    def  _parseBubble(self, bubbleFname):
        """
        @todo: This method should either get OpenTree ID from filename or 
               some taxon ID (GBIF) from record/s.
        @return species_name: If it can be parsed from the filename, 
                Binomial, genus + species, otherwise it is simply the filename
        @return openTreeId: If it can be parsed from the filename, an integer 
                key indicating the Open Tree of Life unique identifier for the 
                phylogenetic record represented by these data. 
        @return recordCount: number of records in the file (lines not including 
                the header).
        """
        binomial = opentreeId = None
        if bubbleFname is not None:
            _, fname = os.path.split(bubbleFname)
            basename, _ = os.path.splitext(fname)
            parts = basename.split('_')
            if len(parts) >= 2:
                genus =  parts[0]
                species = parts[1]
                try:
                    idstr = parts[2]
                    opentreeId = int(idstr)
                except:
                    self.log.error('Unable to extract integer openTreeId from filename {}'
                                        .format(basename))
                binomial = ' '.join((genus, species))
            else:
                self.log.error('Unable to parse filename {} into binomial and opentreeId'
                                    .format(basename))
        try:
            _ = fromUnicode(toUnicode(binomial))
        except Exception, _:
            self.log.error('Failed to convert binomial to and from unicode')
            binomial = None            
                    
        with open(bubbleFname) as f:
            for idx, line in enumerate(f):
                pass
        recordCount = idx

        return binomial, opentreeId, recordCount
 
# ...............................................
    def  _getInsertSciNameForTinyBubble(self, binomial, opentreeId, recordCount):
        if binomial is not None:
            if opentreeId is not None:
                sciName = ScientificName(binomial,
                                         lastOccurrenceCount=recordCount,
                                         taxonomySourceId=self._taxonSourceId, 
                                         taxonomySourceKey=opentreeId, 
                                         taxonomySourceSpeciesKey=opentreeId)
            else:
                sciName = ScientificName(binomial, userId=self.userId,
                                         lastOccurrenceCount=recordCount)
            self._scribe.findOrInsertTaxon(sciName=sciName)
            self.log.info('Inserted sciName for OpenTree UID {}, {}'
                                  .format(opentreeId, binomial))

        return sciName
    
# ...............................................
    def close(self):
        try:
            self._dirContentsFile.close()
        except:
            self.log.error('Unable to close dirContentsFile {}'.format(self._dirContentsFile))
            
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
            return self._dirContentsFile.closed
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
    def _recreateFile(self, dirContentsFname):
        """
        Create a new file from BISON TSN query for binomials with > 20 points. 
        """
        self.readyFilename(dirContentsFname, overwrite=True)
        with open(dirContentsFname, 'w') as f:
            for root, dirs, files in os.walk(self._occCSVDir):
                for fname in files:
                    if fname.endswith(LMFormat.CSV.ext):
                        fullFname = os.path.join(root, fname)
                        f.write('{}\n'.format(fullFname))

# ...............................................
    def _getNextFilename(self):
        fullOccFname = None
        line = self._getNextLine(self._dirContentsFile)
        if line is not None:
            try:              
                fullOccFname = line.strip()
            except Exception, e:
                self.log.debug('Exception reading line {} ({})'
                                    .format(self._linenum, e))
        return fullOccFname

# ...............................................
    def getOne(self):
        occ = None
        bubbleFname = self._getNextFilename()
        binomial, opentreeId, recordCount = self._parseBubble(bubbleFname)
        if binomial is not None and opentreeId is not None:
            sciName = self._getInsertSciNameForTinyBubble(binomial, opentreeId, 
                                                          recordCount)
            if sciName is not None:
                occ = self._findOrInsertOccurrenceset(sciName, recordCount,
                                                      data=bubbleFname)
            if occ:
                self.log.info('WOC processed occset {}, opentreeId {}, with {} points; next start {}'
                                  .format(occ.getId(), opentreeId, recordCount, self.nextStart))
        return occ

# ...............................................
    def _writeRawData(self, occ, data=None, metadata=None):
        if data is None:
            raise LMError(currargs='Missing data file for occurrenceSet')
        rdloc = occ.createLocalDLocation(raw=True)
        occ.readyFilename(rdloc, overwrite=True)
        shutil.copyfile(data, rdloc)
        
        if metadata is not None:
            rawmeta_dloc = rdloc + LMFormat.JSON.ext
            readyFilename(rawmeta_dloc, overwrite=True)
            with open(rawmeta_dloc, 'w') as f:
                json.dump(metadata, f)
        return rdloc, rawmeta_dloc

# ...............................................
    def moveToStart(self):
        startline = self._findStart()  
        if startline < 1:
            self._linenum = 0
            self._currRec = None
        else:
            fullOccFname = self._getNextFilename()
            while fullOccFname is not None and self._linenum < startline-1:
                fullOccFname = self._getNextFilename()
  
# ..............................................................................
class ExistingWoC(_SpeciesWeaponOfChoice):
    """
    @summary: Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the 
                 text chunk to a file, then creates an OccurrenceJob for it and 
                 updates the Occurrence record and inserts a job.
    """
    def __init__(self, scribe, user, archiveName, epsg, expDate, occIdFname, 
                     logger=None):
        super(ExistingWoC, self).__init__(scribe, user, archiveName, epsg, expDate, 
                                                  occIdFname, logger=logger)
        # Copy the occurrencesets 
        self.processType = None
        try:
            self._idfile = open(occIdFname, 'r')
        except:
            raise LMError(currargs='Failed to open {}'.format(occIdFname))


# ...............................................
    def close(self):
        try:
            self._idfile.close()
        except:
            self.log.error('Unable to close {}'.format(self._dumpfile))
            
# ...............................................
    @property
    def complete(self):
        try:
            return self._idfile.closed
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
    def moveToStart(self):
        startline = self._findStart()  
        if startline > 1:
            while self._linenum < startline-1:
                _ = self._getNextLine(self._idfile)

# ...............................................
    def _getOcc(self):
        occ = None
        line = self._getNextLine(self._idfile)
        while line is not None and not self.complete:
            try:
                tmp = line.strip()
            except Exception, e:
                self._scribe.log.info('Error reading line {} ({}), skipping'
                                     .format(self._linenum, str(e)))
            else:
                try:
                    occid = int(tmp)
                except Exception, e:
                    self._scribe.log.info('Unable to get Id from data {} on line {}'
                                         .format(tmp, self._linenum))
                else:
                    occ = self._scribe.getOccurrenceSet(occId=occid)
                    if occ is None:                            
                        self._scribe.log.info('Unable to get Occset for Id {} on line {}'
                                                    .format(tmp, self._linenum))
                    else:
                        if occ.status != JobStatus.COMPLETE: 
                            self._scribe.log.info('Incomplete or failed occSet for id {} on line {}'
                                                        .format(occid, self._linenum))
            line = None
            if occ is None and not self.complete:
                line = self._getNextLine(self._idfile)
        return occ

# ...............................................
    def getOne(self):
        userOcc = None
        occ = self._getOcc()
        if occ is not None:
            if occ.getUserId() == self.userId:
                userOcc = occ
                self.log.info('Found user occset {}, with {} points; next start {}'
                                  .format(occ.getId(), occ.queryCount, self.nextStart))
            elif occ.getUserId() == PUBLIC_USER:
                tmpOcc = occ.copyForUser(self.userId)
                sciName = self._scribe.getTaxon(squid=occ.squid)
                if sciName is not None:
                    tmpOcc.setScientificName(sciName)
                tmpOcc.readData(dlocation=occ.getDLocation(), 
                                      dataFormat=occ.dataFormat)
                userOcc = self._scribe.findOrInsertOccurrenceSet(tmpOcc)
                # Read the data from the original occurrence set
                userOcc.readData(dlocation=occ.getDLocation(), 
                                      dataFormat=occ.dataFormat, doReadData=True)
                userOcc.writeLayer()
                
                # Copy metadata file
                shutil.copyfile('{}{}'.format(os.path.splitext(occ.getDLocation())[0],
                                                        LMFormat.METADATA.ext),
                                     '{}{}'.format(os.path.splitext(userOcc.getDLocation())[0],
                                                        LMFormat.METADATA.ext))
                
                self._scribe.updateObject(userOcc)
                self.log.info('Copy/insert occset {} to {}, with {} points; next start {}'
                                  .format(occ.getId(), userOcc.getId(), 
                                             userOcc.queryCount, self.nextStart))
            else:
                self._scribe.log.info('Unauthorized user {} for ID {}'
                                            .format(occ.getUserId(), occ.getId()))
        return userOcc
    
"""
import shutil
try:
    import mx.DateTime as dt
except:
    pass

import csv
import glob
import os
import sys
from time import sleep

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.apiquery import BisonAPI, GbifAPI
from LmCommon.common.lmconstants import (GBIF, GBIF_QUERY, BISON, BISON_QUERY, 
                                                ProcessType, JobStatus, ONE_HOUR, LMFormat, IDIG_DUMP) 
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import LOG_PATH
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.tools.occwoc import *
from LmServer.tools.cwalken import *
from LmServer.db.borgscribe import BorgScribe

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR

useGBIFTaxonIds = True
occDelimiter = '\t' 
occData = '/state/partition1/lmserver/data/species/idig'
occMeta = IDIG_DUMP.METADATA

scriptname = 'wocTesting'
logger = ScriptLogger(scriptname)
scribe = BorgScribe(logger)
scribe.openConnections()
userId = PUBLIC_USER
expDate = dt.DateTime(2017,9,20).mjd

if os.path.isfile(occData):
    occCSV = occData
else:
    fnames = glob.glob(os.path.join(occData, 
                             '*{}'.format(LMFormat.CSV.ext)))

if len(fnames) > 0:
    occCSV = fnames[0]
    if len(fnames) > 1:
        moreDataToProcess = True
else:
    occCSV = None
    
occMeta = IDIG_DUMP.METADATA

woc = UserWoC(scribe, userId, 'someArchiveName', 
                             4326, expDate, occCSV, occMeta, 
                             occDelimiter, logger=logger, 
                             useGBIFTaxonomy=useGBIFTaxonIds)
op = woc.occParser            


op = OccDataParser(logger, occCSV, occMeta, pullChunks=True)


f = open(occCSV, 'r')
cr = csv.reader(f, delimiter='\t')
fieldmeta, doMatchHeader = OccDataParser.readMetadata(occMeta)
                    
(fieldNames,
 fieldTypes,
 filters,
 idIdx,
 xIdx,
 yIdx,
 geoIdx,
 groupByIdx, 
 nameIdx) = OccDataParser.getCheckIndexedMetadata(fieldmeta, None)
 
line = cr.next()
goodEnough = True
groupVals = set()

for filterIdx, acceptedVals in filters.iteritems():
    val = line[filterIdx]
    try:
        val = val.lower()
    except:
        pass
    if acceptedVals is not None and val not in acceptedVals:
        goodEnough = False

try:
    gval = line[groupByIdx]
except Exception, e:
    goodEnough = False
else:
    groupVals.add(gval)
    
if idIdx is not None:
    try:
        int(line[idIdx])
    except Exception, e:
        if line[idIdx] == '':
            goodEnough = False
    
x, y = OccDataParser.getXY(line, xIdx, yIdx, geoIdx)
try:
    float(x)
    float(y)
except Exception, e:
    goodEnough = False
else:
    if x == 0 and y == 0:
        goodEnough = False
            
# Dataset name value
if line[nameIdx] == '':
    goodEnough = False
    
value = int(line[groupByIdx])
"""
import csv
import mx.DateTime
import os

from LmBackend.command.server import CatalogTaxonomyCommand
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.lmconstants import GBIF, JobStatus, LMFormat

from LmDbServer.common.lmconstants import GBIF_TAXONOMY_DUMP_FILE, TAXONOMIC_SOURCE

from LmServer.common.lmconstants import NUM_DOCS_PER_POST, Priority
from LmServer.common.localconstants import PUBLIC_USER
import LmServer.common.solr as lm_solr
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.log import ScriptLogger
from LmServer.base.taxon import ScientificName
from LmServer.legion.processchain import MFChain

# .............................................................................
class TaxonFiller(LMObject):
    """
    Class to populates a Lifemapper database with taxonomy for accepted names
    in the GBIF Backbone Taxonomy as read from a text file provided by GBIF.
    @todo: extend this script to add taxonomy for users
    """
# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, taxSrcName, taxonomyFname, taxSuccessFname,
                     taxSrcUrl=None, delimiter='\t', logname=None):
        """
        @summary Constructor for ArchiveFiller class.
        @todo: allow taxSrcName to be a userId, and user taxonomy is allowed
        @todo: define data format for user-provided taxonomy 
        """
        super(TaxonFiller, self).__init__()
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        self.name = scriptname
        try:
            self.scribe = self._getDb(logname)
        except: 
            raise
        self.taxonomyFname = taxonomyFname
        self._successFname = taxSuccessFname
        self._taxonomySourceName = taxSrcName
        self._taxonomySourceUrl = taxSrcUrl
        self._delimiter = delimiter
        self._taxonomySourceId = None
        self._taxonFile = None
        self._csvreader = None
        
    # ...............................................
    def open(self):
        success = self.scribe.openConnections()
        if not success: 
            raise LMError('Failed to open database')

        # ...............................................
    def close(self):
        self._taxonFile.close()
        self.scribe.closeConnections()

    # ...............................................
    def initializeMe(self):
        self._taxonomySourceId = self.scribe.findOrInsertTaxonSource(
                                                                        self._taxonomySourceName, 
                                                                        self._taxonomySourceUrl)
        self._taxonFile = open(self.taxonomyFname, 'r')
        self._csvreader = csv.reader(self._taxonFile, delimiter=delimiter)

    # ...............................................
    @property
    def logFilename(self):
        try:
            fname = self.scribe.log.baseFilename
        except:
            fname = None
        return fname
            
    # ...............................................
    def _getDb(self, logname):
        logger = ScriptLogger(logname)
        scribe = BorgScribe(logger)
        return scribe

    # ...............................................
    def _convertString(self, valStr, isInteger=True):
        val = None
        if isInteger:
            try:
                val = int(valStr)
            except Exception, e:
                pass
        else:
            try:
                val = float(valStr)
            except Exception, e:
                pass
        return val
    
    # ...............................................
    def _getTaxonValues(self, line):
        # aka LmCommon.common.lmconstants GBIF_TAXON_FIELDS
        (taxonkey, kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr, 
         scinameStr, genuskey, specieskey, count) = line
        try:
            txkey = int(taxonkey)
            occcount = int(count)
        except:
            print 'Invalid taxonkey {} or count {} for {}'.format(taxonkey, count, 
                                                                                    scinameStr)
        try:
            genkey = int(genuskey)
        except:
            genkey = None
        try:
            spkey = int(specieskey)
        except:
            spkey = None
        return (txkey, kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr, 
         scinameStr, genkey, spkey, occcount)
        
    # ...............................................
    def writeSuccessFile(self, message):
        self.readyFilename(self._successFname, overwrite=True)
        try:
            f = open(self._successFname, 'w')
            f.write(message)
        except:
            raise
        finally:
            f.close()
        
    # ...............................................
    def readAndInsertTaxonomy(self):
        totalIn = totalOut = totalWrongRank = 0
        
        for line in self._csvreader:
            (taxonkey, kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr, 
             scinameStr, genuskey, specieskey, count) = self._getTaxonValues(line)
             
            sciname_objs = []
            if taxonkey not in (specieskey, genuskey): 
                totalWrongRank += 1
            else:
                if taxonkey == specieskey:
                    rank = GBIF.RESPONSE_SPECIES_KEY
                elif taxonkey == genuskey:
                    rank = GBIF.RESPONSE_GENUS_KEY
                sciName = ScientificName(scinameStr, rank=rank, canonicalName=None, 
                         kingdom=kingdomStr, phylum=phylumStr, txClass=classStr, 
                         txOrder=orderStr, family=familyStr, genus=genusStr, 
                         taxonomySourceId=self._taxonomySourceId, taxonomySourceKey=taxonkey, 
                         taxonomySourceGenusKey=genuskey, 
                         taxonomySourceSpeciesKey=specieskey)
                upSciName = self.scribe._borg.findOrInsertTaxon(
                                                taxonSourceId=self._taxonomySourceId, 
                                                taxonKey=taxonkey, sciName=sciName)
                if upSciName:
                    totalIn += 1
                    self.scribe.log.info('Found or inserted {}'.format(scinameStr))
                    # Add object to list, post to solr if we reach threshold
                    sciname_objs.append(upSciName)
                    if len(sciname_objs) >= NUM_DOCS_PER_POST:
                        lm_solr.add_taxa_to_taxonomy_index(sciname_objs)
                        sciname_objs = []
                else:
                    totalOut += 1
                    self.scribe.log.info('Failed to insert or find {}'.format(scinameStr))
        # Add any leftover taxonomy
        lm_solr.add_taxa_to_taxonomy_index(sciname_objs)
        
        msg = 'Found or inserted {}; failed {}; wrongRank {}'.format(totalIn, 
                                                                totalOut, totalWrongRank)
        self.writeSuccessFile(msg)
        self.scribe.log.info(msg)
    
    # ...............................................
    def createCatalogTaxonomyMF(self):
        """
        @note: Not currently used, MF is created in initWorkflow
        @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                     and configFile written by BOOMFiller.initBoom.
        """
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        meta = {MFChain.META_CREATED_BY: scriptname,
                  MFChain.META_DESCRIPTION: 'Catalog Taxonomy task for source {}'
        .format(PUBLIC_USER, self._taxonomySourceName)}
        newMFC = MFChain(PUBLIC_USER, priority=Priority.HIGH, 
                              metadata=meta, status=JobStatus.GENERAL, 
                              statusModTime=mx.DateTime.gmt().mjd)
        mfChain = self.scribe.insertMFChain(newMFC, None)
    
        # Create a rule from the MF and Arf file creation
        cattaxCmd = CatalogTaxonomyCommand(self._taxonomySourceName, 
                                                      self.taxonomyFname,
                                                      source_url=self._taxonomySourceUrl,
                                                      delimiter=self._delimiter)
        mfChain.addCommands([cattaxCmd.getMakeflowRule(local=True)])
        mfChain.write()
        mfChain.updateStatus(JobStatus.INITIALIZE)
        self.scribe.updateObject(mfChain)
        
# ...............................................
# MAIN
# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
                description=(""""Populate a Lifemapper archive with taxonomic 
                                      data for one or more species, from a CSV file """))
    parser.add_argument('--taxon_source_name', type=str, 
                              default=TAXONOMIC_SOURCE['GBIF']['name'],
                              help=("""Identifier of taxonomy source to populate.  
                                          This must either already exist in the database, 
                                          or it will be added to the database with the 
                                          (now required) optional parameter 
                                          `taxon_source_url`."""))
    parser.add_argument('--taxon_data_filename', type=str,
                              default=None,
                              help=('Filename of CSV taxonomy data.'))
    parser.add_argument('--success_filename', type=str,
                              default=None,
                              help=('Filename to be written on successful completion of script.'))
    parser.add_argument('--taxon_source_url', type=str, default=None,
                              help=("""Optional URL of taxonomy source, required 
                                          for a new source"""))
    parser.add_argument('--delimiter', type=str, default='\t',
                              help=("""Delimiter in CSV taxon_data_filename. Defaults
                                          to tab."""))
    parser.add_argument('--logname', type=str, default=None,
                              help=('Base name of logfile '))
    # Taxonomy ingest Makeflows will generally be written as part of a BOOM job,
    # calling this script, so new species boom data may be connected to taxonomy 
    args = parser.parse_args()
    sourceName = args.taxon_source_name
    taxonFname = args.taxon_data_filename
    successFname = args.success_filename
    logname = args.logname
    sourceUrl = args.taxon_source_url
    delimiter = args.delimiter
    
    if sourceName == TAXONOMIC_SOURCE['GBIF']['name']:
        if taxonFname is None:
            taxonFname = GBIF_TAXONOMY_DUMP_FILE
        if successFname is None:
            taxbasename, _ = os.path.splitext(taxonFname)
            taxonSuccessFname = taxbasename + '.success'

    if logname is None:
        import time
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        dataname, _ = os.path.splitext(taxonFname)
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}.{}'.format(scriptname, dataname, timestamp)
    
    filler = TaxonFiller(sourceName, taxonFname, successFname, 
                                taxSrcUrl=sourceUrl,
                                delimiter=delimiter,
                                logname=logname)
    filler.open()

    filler.initializeMe()
    filler.readAndInsertTaxonomy()
    
    filler.close()


# Total Inserted 744020; updated 3762, Grand total = 747782
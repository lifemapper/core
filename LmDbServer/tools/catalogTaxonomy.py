import csv
import mx.DateTime
import os

from LmCommon.common.lmconstants import GBIF
from LmDbServer.common.lmconstants import GBIF_TAXONOMY_DUMP_FILE, TAXONOMIC_SOURCE
from LmServer.db.borgscribe import BorgScribe
from LmBackend.common.lmobj import LMError, LMObject
from LmServer.common.log import ScriptLogger
from LmServer.base.taxon import ScientificName

# .............................................................................
class TaxonFiller(LMObject):
   """
   Class to populates a Lifemapper database with taxonomy for accepted names
   in the GBIF Backbone Taxonomy as read from a text file provided by GBIF.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, taxonomySourceName, taxonomyFname,
                taxonomySourceUrl=None, delimiter='\t'):
      """
      @summary Constructor for ArchiveFiller class.
      
      """
      super(TaxonFiller, self).__init__()
      try:
         self.scribe = self._getDb()
      except: 
         raise
      self.taxonomyFname = taxonomyFname
      self._taxonomySourceId, x, x = self.scribe.findTaxonSource(taxonomySourceName)
      self._taxonFile = open(taxonomyFname, 'r')
      self._csvreader = csv.reader(self._taxonFile, delimiter=delimiter)
      
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
   @property
   def logFilename(self):
      try:
         fname = self.scribe.log.baseFilename
      except:
         fname = None
      return fname
         
   # ...............................................
   def _getDb(self):
      basefilename = os.path.basename(__file__)
      basename, ext = os.path.splitext(basefilename)
      logger = ScriptLogger(basename)
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
   def readAndInsertTaxonomy(self):
      totalIn = totalOut = totalWrongRank = 0
      
      for line in self._csvreader:
         (taxonkey, kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr, 
          scinameStr, genuskey, specieskey, count) = self._getTaxonValues(line)
          
         if taxonkey not in (specieskey, genuskey): 
            totalWrongRank += 1
         else:
            if taxonkey == specieskey:
               rank = GBIF.RESPONSE_GENUS_KEY
            elif taxonkey == genuskey:
               rank = GBIF.RESPONSE_SPECIES_KEY
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
            else:
               totalOut += 1
               self.scribe.log.info('Failed to insert or find {}'.format(scinameStr))
      self.scribe.log.info('Found or inserted {}; failed {}; wrongRank {}'
            .format(totalIn, totalOut, totalWrongRank))
   
             
# ...............................................
# MAIN
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with taxonomic ' +
                         'data for one or more species, from a CSV file '))
   parser.add_argument('taxon_source_name', type=str,
            help=("""Identifier of taxonomy source to populate.  This must 
                  either already exist in the database, or it will be added to
                  the database with the (now required) optional parameter
                  `taxon_source_url`."""))
   parser.add_argument('taxon_data_filename', type=str,
            help=("""Filename of CSV taxonomy data."""))
   parser.add_argument('logname', type=str,
            help=("""Base name of logfile """))
   parser.add_argument('--taxon_source_url', type=str, default=None,
            help=("""Optional URL of taxonomy source, required for a new source"""))
   parser.add_argument('--delimiter', type=str, default='\t',
            help=("""Delimiter in CSV taxon_data_filename.  Defaults to tab."""))
   args = parser.parse_args()
   sourceName = args.taxon_source_name
   taxonFname = args.taxon_data_filename
   sourceUrl = args.taxon_source_url
   delimiter = args.delimiter
                      
#    sourceName = TAXONOMIC_SOURCE['GBIF']['name']
#    sourceUrl = TAXONOMIC_SOURCE['GBIF']['url']
#    taxonFname = GBIF_TAXONOMY_DUMP_FILE
#    delimiter = '\t'
   
   filler = TaxonFiller(sourceName, taxonFname,
                        sourceUrl=sourceUrl,
                        delimiter=delimiter)
   filler.open()
   filler.readAndInsertTaxonomy()
   filler.close()


# Total Inserted 744020; updated 3762, Grand total = 747782
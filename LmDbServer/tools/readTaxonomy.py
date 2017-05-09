import csv
import mx.DateTime
import os

from LmCommon.common.lmconstants import GBIF
from LmDbServer.common.lmconstants import GBIF_TAXONOMY_DUMP_FILE, TAXONOMIC_SOURCE
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.log import ThreadLogger 
from LmServer.base.taxon import ScientificName

# ...............................................
def _convertString(valStr, isInteger=True):
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
def _getTaxonValues(line):
   # aka LmCommon.common.lmconstants GBIF_TAXON_FIELDS
   (taxonkey, kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr, 
    scinameStr, genuskey, specieskey, count) = line
   try:
      txkey = int(taxonkey)
      occcount = int(count)
   except:
      print 'Invalid taxonkey %s or count %s for %s' % (taxonkey, count, scinameStr)
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
def readTaxonomy(logger, taxonSourceName, taxonFilename):
   totalIn = totalOut = totalWrongRank = 0
   scribe = BorgScribe(logger)
   scribe.openConnections()
   txSourceId, url, moddate = scribe.findTaxonSource(taxonSourceName)
   
   f = open(taxonFilename, 'r')
   csvreader = csv.reader(f, delimiter='\t')
   for line in csvreader:
      (taxonkey, kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr, 
       scinameStr, genuskey, specieskey, count) = _getTaxonValues(line)
       
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
                taxonomySourceId=txSourceId, taxonomySourceKey=taxonkey, 
                taxonomySourceGenusKey=genuskey, 
                taxonomySourceSpeciesKey=specieskey)
         upSciName = scribe._borg.findOrInsertTaxon(taxonSourceId=txSourceId, 
                                             taxonKey=taxonkey, sciName=sciName)
         if upSciName:
            totalIn += 1
            logger.info('Found or inserted {}'.format(scinameStr))
         else:
            totalOut += 1
   logger.info('Found or inserted {}; failed {}; wrongRank {}'
         .format(totalIn, totalOut, totalWrongRank))
   f.close()
   scribe.closeConnections()
   
             
# ...............................................
# MAIN
# ...............................................
if __name__ == '__main__':   
   basefilename = os.path.basename(__file__)
   basename, ext = os.path.splitext(basefilename)
   logger = ThreadLogger(basename)
   readTaxonomy(logger, TAXONOMIC_SOURCE['GBIF']['name'], GBIF_TAXONOMY_DUMP_FILE)




# Total Inserted 744020; updated 3762, Grand total = 747782
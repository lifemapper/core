import csv

from LmDbServer.common.lmconstants import TAXONOMY_DUMP_FILE
from LmDbServer.populate.bioclimMeta import TAXONOMIC_SOURCE 
from LmServer.db.scribe import Scribe
from LmServer.common.log import ThreadLogger 

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
   totalInserted = totalUpdated = totalStatic = totalWrongRank = 0
   scribe = Scribe(logger)
   scribe.openConnections()
   taxonSourceId, url, cdate, mdate = scribe.findTaxonSource(taxonSourceName)
   
   f = open(taxonFilename, 'r')
   csvreader = csv.reader(f, delimiter='\t')
   for line in csvreader:
      (taxonkey, kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr, 
       scinameStr, genuskey, specieskey, count) = _getTaxonValues(line)
       
      if taxonkey not in (specieskey, genuskey): 
         totalWrongRank += 1
      else:
         scinameId, updated, inserted = scribe._mal.insertTaxonRec(taxonSourceId, 
                                                        taxonkey, kingdomStr, 
                                                        phylumStr, classStr, 
                                                        orderStr, familyStr, 
                                                        genusStr, scinameStr, 
                                                        genuskey, specieskey, 
                                                        None, count)
         if updated:
            totalUpdated += 1
         elif inserted:
            totalInserted += 1
         else:
            totalStatic += 1
   print 'Inserted %d; updated %d; wrongRank %d' % (totalInserted, totalUpdated, 
                                                    totalWrongRank)
   f.close()
   scribe.closeConnections()
   
             
# ...............................................
# MAIN
# ...............................................
if __name__ == '__main__':   
   logger = ThreadLogger('readGbif')
   readTaxonomy(logger, TAXONOMIC_SOURCE['GBIF']['name'], TAXONOMY_DUMP_FILE)




# Total Inserted 744020; updated 3762, Grand total = 747782
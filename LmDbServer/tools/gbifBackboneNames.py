
"""
@summary: Runs a job that creates a shapefile from occurrences retrieved 
          from GBIF for a taxonomic name
          Prep:  Start with a listing of all taxonomicStatus=ACCEPTED names
                 in GBIF Backbone Taxonomy for species and genus
          - http://api.gbif.org/v0.9/species?rank=species&taxonomicStatus=ACCEPTED
          - http://api.gbif.org/v0.9/species?rank=genus&taxonomicStatus=ACCEPTED
             
          1. Query for name:  species/match?name=x&rank=species
             -- get acceptedKey, scientificName, tree keys: kingdomKey, phylumKey, class, order, family, genus\
          2. Query for each parent name: using tree keys
             -- get scientific (or canonical) name for each
          3. Query for points.  This will get included names, i.e. below this 
             rank, and synonyms.
             -- occurrence/search?taxonKey=<acceptedKey>&georeferenced=true&spatialIssues=false
"""
URL_ESCAPES = [
               [" ", "%20"]
               ]
OUTPUT_PATH = '/home/astewart'
BASE_FNAME = 'nubCanonicals'
# For querying GBIF REST service for occurrence data
GBIF_REST_URL = 'http://api.gbif.org/v0.9'
GBIF_SPECIES_SERVICE = 'species'
GBIF_SEARCH_COMMAND = 'search'
GBIF_MATCH_COMMAND = 'match'
GBIF_DATASET_SERVICE = 'dataset'
GBIF_DATASET_FILTER = 'dataset_key'
GBIF_DATASET_BACKBONE_VALUE = 'GBIF Backbone Taxonomy'

PAGESIZE = 200

GBIF_SIMPLE_QUERY_KEY = 'q'
GBIF_NAME_QUERY_KEY = 'name'

GBIF_RESULT_KEY = 'results'
GBIF_END_KEY = 'endOfRecords'
GBIF_COUNT_KEY = 'count'

GBIF_IDENTIFIER_KEY = 'key'
GBIF_TAXON_NAMETYPE = 'nameType'
GBIF_TAXON_RANK = 'rank'
GBIF_TAXON_SCIENTIFIC = 'scientificName'
GBIF_TAXON_CANONICAL = 'canonicalName'
GBIF_TAXON_STATUS = 'taxonomicStatus'

# no habitat filter = 2399889, false (not marine) = 17525, true (marine) = 169510

FILTERS = {GBIF_SPECIES_SERVICE: {'rank': 'SPECIES',
                                  'status': 'ACCEPTED',
#                                  'habitat': 'false',
                                  GBIF_DATASET_FILTER: None}
           }
# key = 105958456 , nubKey = 3227657

# ...............................................
def _getGBIFSpeciesMatchQuery(filters, name=None, offset=None, limit=PAGESIZE):
   if offset is not None:
      filters['offset'] = offset
      filters['limit'] = limit

   if name is not None:
      command = GBIF_MATCH_COMMAND
      filters[GBIF_SIMPLE_QUERY_KEY] = name
   else:
      command = GBIF_SEARCH_COMMAND
   url = '%s/%s/%s'% (GBIF_REST_URL, GBIF_SPECIES_SERVICE, command)
   
   if len(filters.values()) > filters.values().count(None):
      separator = '?'
      for key, val in filters.iteritems():
         if val is not None:
            url += '%s%s=%s' % (separator, key, val)
            if separator == '?':
               separator = '&'
            
   for replaceStr, withStr in URL_ESCAPES:
      qry = url.replace(replaceStr, withStr)
   return qry

# ...............................................
def _getGBIFResults(url):
   total = None
   isEnd = False
   resultList = []
   for replaceStr, withStr in URL_ESCAPES:
      qry = url.replace(replaceStr, withStr)
   try:
      output = urllib2.urlopen(qry).read()
   except Exception, e:
      print('Failed on query %s (error: %s)' % (qry, str(e)))
   else:
      outputDict = json.loads(output)
      resultList = outputDict[GBIF_RESULT_KEY]
      isEnd = outputDict[GBIF_END_KEY]
      if outputDict.has_key(GBIF_COUNT_KEY):
         total = outputDict[GBIF_COUNT_KEY]
   return total, isEnd, resultList
      
# ...............................................
def _getGBIFTaxonomyUUID():
   url = '%s/%s?%s=%s'% (GBIF_REST_URL, GBIF_DATASET_SERVICE, 
                         GBIF_SIMPLE_QUERY_KEY, GBIF_DATASET_BACKBONE_VALUE)
   total, isEnd, resultList = _getGBIFResults(url)
   if total == 1: 
      uuid = resultList[0][GBIF_IDENTIFIER_KEY]
      return uuid
   else:
      raise Exception('Unable to find dataset %s' % GBIF_DATASET_BACKBONE_VALUE)
   
# ...............................................
# MAIN
# ...............................................
import urllib2
import json
import os.path

goodNameCount = 0
offset = 0
isEnd = False
fname = os.path.join(OUTPUT_PATH, 
                      BASE_FNAME + '_' + 
                      FILTERS[GBIF_SPECIES_SERVICE]['rank'] + '.txt')
outfile = open(fname, 'w')

nubUUID = _getGBIFTaxonomyUUID()
FILTERS[GBIF_SPECIES_SERVICE][GBIF_DATASET_FILTER] = nubUUID

filters = FILTERS[GBIF_SPECIES_SERVICE].copy()
url = _getGBIFSpeciesMatchQuery(filters, name=None, offset=offset)
fullCount, isEnd, nameList = _getGBIFResults(url)
currTotal = len(nameList)

while not isEnd:
   if currTotal >= fullCount or offset >= fullCount:
      isEnd = True
      break
   print('Offset = %d / %d' % (offset, fullCount))
   for nameInfo in nameList:
      gbifKey = nameInfo[GBIF_IDENTIFIER_KEY]
      rank = nameInfo[GBIF_TAXON_RANK] 
      status = nameInfo[GBIF_TAXON_STATUS]
      if (rank == FILTERS[GBIF_SPECIES_SERVICE]['rank'] and status == 'ACCEPTED'):
         try:
            currname = nameInfo[GBIF_TAXON_CANONICAL].encode('utf-8')
            goodNameCount += 1
            outfile.write('%s\n' % currname)
         except:
            try:
               print ('Failed to get canonical for key: %d, rank: %s, scientific: %s' 
                      % (gbifKey, rank, 
                         nameInfo[GBIF_TAXON_SCIENTIFIC].encode('utf-8')))
            except:
               print ('Failed to get canonical for key: %d, rank: %s, ' 
                   % (gbifKey, rank))
      else:
         print 'Failed rank or status for %d' % (gbifKey)
   if len(nameList) == 0:
      print('No results for offset %d, url: %s' % (offset, url))
   
   offset += len(nameList)
   url = _getGBIFSpeciesMatchQuery(filters, name=None, offset=offset)
   total, isEnd, nameList = _getGBIFResults(url)
   currTotal += len(nameList)
   
outfile.close()
print ('Good canonical names = %d' % goodNameCount)
   
   
   

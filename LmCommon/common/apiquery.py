"""
@summary: Module containing functions for API Queries
@status: beta

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
import json
import re
from types import BooleanType, DictionaryType, FloatType, IntType, ListType, \
                  StringType, TupleType, UnicodeType
import urllib, urllib2
import xml.etree.ElementTree as ET

from LmCommon.common.lmconstants import (BISON_COUNT_KEYS, BISON_FILTERS,
         BISON_HIERARCHY_KEY, BISON_KINGDOM_KEY, BISON_NAME_KEY, BISON_OCC_FILTERS, 
         BISON_QFILTERS, BISON_RECORD_KEYS, BISON_TSN_FILTERS, BISON_TSN_LIST_KEYS,
         BISON_BINOMIAL_REGEX, BISON_OCCURRENCE_URL,
         GBIF_ORGANIZATION_SERVICE, GBIF_REST_URL, GBIF_SPECIES_SERVICE, 
#          IDIGBIO_AGG_SPECIES_GEO_MIN_40, IDIGBIO_BINOMIAL_KEYS, 
#          IDIGBIO_BINOMIAL_REGEX, IDIGBIO_FILTERS, IDIGBIO_URL_PREFIX, 
#          IDIGBIO_OCCURRENCE_POSTFIX, IDIGBIO_SEARCH_POSTFIX, IDIGBIO_QFILTERS, 
#          IDIGBIO_SPECIMENS_BY_BINOMIAL, 
         ITIS_CLASS_KEY, ITIS_DATA_NAMESPACE, 
         ITIS_FAMILY_KEY, ITIS_GENUS_KEY, ITIS_HIERARCHY_TAG, ITIS_KINGDOM_KEY, 
         ITIS_ORDER_KEY, ITIS_PHYLUM_DIVISION_KEY, ITIS_RANK_TAG, 
         ITIS_SPECIES_KEY, ITIS_TAXON_TAG, ITIS_TAXONOMY_HIERARCHY_URL, 
         ITIS_TAXONOMY_KEY, URL_ESCAPES,
         HTTPStatus)

# .............................................................................
class APIQuery(object):
   """
   Class to query APIs and return results
   """
   def __init__(self, baseurl, 
                qFilters={}, otherFilters={}, filterString=None, 
                headers={}):
      """
      @summary Constructor for the APIQuery class
      """
      self.headers = headers
      # No added filters are on url (unless initialized with filters in url)
      self.baseurl = baseurl
      self._qFilters = qFilters
      self._otherFilters = otherFilters
      self.filterString = self._assembleFilterString(filterString=filterString)
      self.output = None
      self.debug = False
      
# ...............................................
   @classmethod
   def initFromUrl(cls, url, headers={}):
      base, filters = url.split('?')
      qry = APIQuery(base, filterString=filters)
      return qry
      
   # .........................................
   @property
   def url(self):
      # All filters added to url
      return '{}?{}'.format(self.baseurl, self.filterString)

# ...............................................
   def addFilters(self, qFilters={}, otherFilters={}):
      """
      @summary: Add new or replace existing filters.  This does not remove 
                existing filters, unless existing keys are sent with new values.
      """
      self.output = None
      for k, v in qFilters.iteritems():
         self._qFilters[k] = v
      for k, v in otherFilters.iteritems():
         self._otherFilters[k] = v
      self.filterString = self._assembleFilterString()
         
# ...............................................
   def clearAll(self, qFilters=True, otherFilters=True):
      """
      @summary: Clear existing qFilters, otherFilters, and output
      """
      self.output = None
      if qFilters:
         self._qFilters = {}
      if otherFilters:
         self._otherFilters = {}
      self.filterString = self._assembleFilterString()

# ...............................................
   def clearOtherFilters(self):
      """
      @summary: Clear existing otherFilters and output
      """
      self.clearAll(otherFilters=True, qFilters=False)

# ...............................................
   def clearQFilters(self):
      """
      @summary: Clear existing qFilters and output
      """
      self.clearAll(otherFilters=False, qFilters=True)

# ...............................................
   def _assembleFilterString(self, filterString=None):
      if filterString is not None:
         for replaceStr, withStr in URL_ESCAPES:
            filterString = filterString.replace(replaceStr, withStr)
      else:
         allFilters = self._otherFilters.copy()
         if self._qFilters:
            qVal = self._assembleQVal(self._qFilters)
            allFilters['q'] = qVal
         filterString = self._assembleKeyValFilters(allFilters)
      return filterString

# ...............................................
   def _assembleKeyValFilters(self, ofDict):
      for k, v in ofDict.iteritems():
         if isinstance(v, BooleanType):
            v = str(v).lower()
         ofDict[k] = unicode(v).encode('utf-8')               
      filterString = urllib.urlencode(ofDict)
      return filterString
      
# ...............................................
   def _interpretQClause(self, key, val):
      cls = None
      if isinstance(val, (FloatType, IntType, StringType, UnicodeType)):
         cls = '{}:{}'.format(key, str(val))
      # Tuple for negated or range value
      elif isinstance(val, TupleType):            
         # negated filter
         if isinstance(val[0], BooleanType) and val[0] is False:
            cls = 'NOT ' + key + ':' + str(val[1])
         # range filter (better be numbers)
         elif ((isinstance(val[0], IntType) or isinstance(val[0], FloatType))
               and (isinstance(val[1], IntType) or isinstance(val[1], FloatType))):
            cls = '%s:[%s TO %s]' % (key, str(val[0]), str(val[1]))
         else:
            print 'Unexpected value type %s' % str(val)
      else:
         print 'Unexpected value type %s' % str(val)
      return cls
   
# ...............................................
   def _assembleQItem(self, key, val):
      itmClauses = []
      # List for multiple values of same key
      if isinstance(val, ListType):
         for v in val:
            itmClauses.append(self._interpretQClause(key, v))
      else:
         itmClauses.append(self._interpretQClause(key, val))
      return itmClauses

# ...............................................
   def _assembleQVal(self, qDict):
      clauses = []
      qval = ''
      # interpret dictionary
      for key, val in qDict.iteritems():
         clauses.extend(self._assembleQItem(key, val))
      # convert to string
      firstClause = None
      for cls in clauses:
         if not firstClause and not cls.startswith('NOT'):
            firstClause = cls
         elif cls.startswith('NOT'):
            qval = ' '.join((qval, cls))
         else:
            qval = ' AND '.join((qval, cls))
      qval = firstClause + qval
      return qval

# ...............................................
   def query(self, outputType='json'):
#       # Also Works
#       response = urllib2.urlopen(fullUrl.decode('utf-8'))
      data = retcode = None
      
      if self._qFilters:
         # All q and other filters are on url
         req = urllib2.Request(self.url, None, self.headers)
      else:
         if self._otherFilters:
            data = urllib.urlencode(self._otherFilters)
         # Any filters are in data
         req = urllib2.Request(self.baseurl, data, self.headers)
      try:
         response = urllib2.urlopen(req)
         retcode = response.getcode()
      except Exception, e:
         if retcode is None:
            retcode = HTTPStatus.INTERNAL_SERVER_ERROR
         raise Exception(retcode, msg='Failed on URL {}; ({})'.format(
                                        self.url, str(e)))
      else:
         try:
            output = response.read()
         except Exception, e:
            raise Exception(HTTPStatus.INTERNAL_SERVER_ERROR, 
              msg='Failed to read output of URL {}; ({})'.format(self.url, str(e)))
         
      try:
         if outputType == 'json':
            self.output = json.loads(output)
         elif outputType == 'xml':
            root = ET.fromstring(output)
            self.output = root
         else:
            print 'Unrecognized output type %s' % str(outputType)
            self.output = None   
      except Exception, e:
         raise Exception(HTTPStatus.INTERNAL_SERVER_ERROR,
                msg='Failed to interpret output of URL {}; ({})'.format(self.url, 
                                                                        str(e)))
      

# .............................................................................
class BisonAPI(APIQuery):
# .............................................................................
   """
   Class to query BISON APIs and return results
   """
# ...............................................
   def __init__(self, qFilters={}, otherFilters={}, filterString=None,
                headers={'Content-Type': 'application/json'}):
      """
      @summary: Constructor for BisonAPI class      
      """
      # Add Q filters for this instance
      for key, val in BISON_QFILTERS.iteritems():
         qFilters[key] = val
      # Add other filters for this instance
      for key, val in BISON_FILTERS.iteritems():
         otherFilters[key] = val
         
      APIQuery.__init__(self, BISON_OCCURRENCE_URL, qFilters=qFilters, 
                        otherFilters=otherFilters, filterString=filterString, 
                        headers=headers)
      
# ...............................................
   @classmethod
   def initFromUrl(cls, url, headers={'Content-Type': 'application/json'}):
      base, filters = url.split('?')
      if base.strip().startswith(BISON_OCCURRENCE_URL):
         qry = BisonAPI(filterString=filters)
      else:
         raise Exception('Bison occurrence API must start with %s' 
                         % BISON_OCCURRENCE_URL)
      return qry
      
# ...............................................
   def _burrow(self, keylst):
      dict = self.output
      if isinstance(dict, DictionaryType):
         try:         
            for key in keylst:
               dict = dict[key]
         except Exception, e:
            raise Exception('Invalid keylist for output ({})'.format(keylst))
      else:
         raise Exception('Invalid output type ({})'.format(type(dict)))
      return dict
         
# ...............................................
   def getBinomialTSNs(self):
      """
      @summary: Returns a list of dictionaries where each dictionary is an 
                occurrence record
      """
      if self.output is None:
         self.query()
      dataCount = self._burrow(BISON_COUNT_KEYS)
      dataList = self._burrow(BISON_TSN_LIST_KEYS)
      print 'Reported count = %d, actual count = %d' % (dataCount, len(dataList))
      return dataList

# ...............................................
   @staticmethod
   def getItisTSNValues(itisTSN):
      """
      @summary: Return ITISScientificName, kingdom, and TSN hierarchy from one 
                occurrence record ending in this TSN (species rank) 
      """
      itisname = king = tsnHier = None
      try:
         occAPI = BisonAPI(qFilters={BISON_HIERARCHY_KEY: '*-%d-' % itisTSN}, 
                           otherFilters={'rows': 1})
         tsnHier = occAPI.getFirstValueFor(BISON_HIERARCHY_KEY)
         itisname = occAPI.getFirstValueFor(BISON_NAME_KEY)
         king = occAPI.getFirstValueFor(BISON_KINGDOM_KEY)
      except Exception, e:
         print str(e)
         raise
      return (itisname, king, tsnHier)
   
# ...............................................
   def getTSNOccurrences(self, asShapefile=False):
      """
      @summary: Returns a list of dictionaries.  Each dictionary is an occurrence record
      """
      if self.output is None:
         self.query()
      dataCount = self._burrow(BISON_COUNT_KEYS)
      dataList = self._burrow(BISON_RECORD_KEYS)
      return dataList

# ...............................................
   def query(self):
      """
      @summary: Queries the API and sets 'output' attribute to a JSON object 
      """
      APIQuery.query(self, outputType='json')
      
# ...............................................
   def getFirstValueFor(self, fieldname):
      """
      @summary: Returns value for given fieldname in the first data record 
                containing a value
      """
      val = None
      records = self.getTSNOccurrences()
      for rec in records:
         try:
            val = records[0][fieldname]
            break
         except:
            print('Missing %s for %s' % (fieldname, self.url))
               
      return val

      
# .............................................................................
class ItisAPI(APIQuery):
# .............................................................................
   """
   Class to query BISON APIs and return results
   """
# ...............................................
   def __init__(self, otherFilters={}):
      """
      @summary: Constructor for ItisAPI class      
      """
      APIQuery.__init__(self, ITIS_TAXONOMY_HIERARCHY_URL, 
                        otherFilters=otherFilters)
   
# ...............................................
   def _findTaxonByRank(self, root, rankKey):
      for tax in root.iter('{%s}%s' % (ITIS_DATA_NAMESPACE, ITIS_HIERARCHY_TAG)):
         rank = tax.find('{%s}%s' % (ITIS_DATA_NAMESPACE, ITIS_RANK_TAG)).text
         if rank == rankKey:
            name = tax.find('{%s}%s' % (ITIS_DATA_NAMESPACE, ITIS_TAXON_TAG)).text
            tsn = tax.find('{%s}%s' % (ITIS_DATA_NAMESPACE, ITIS_TAXONOMY_KEY)).text
         return (tsn, name)
      
# ...............................................
   def _getRankFromPath(self, taxPath, rankKey):
      for rank, tsn, name in taxPath:
         if rank == rankKey:
            return int(tsn), name
      return None, None
         
# ...............................................
   def _returnHierarchy(self):
      """
      @note: for 
      """
      taxPath = []
      for tax in self.output.iter('{%s}%s' % (ITIS_DATA_NAMESPACE, ITIS_HIERARCHY_TAG)):
         rank = tax.find('{%s}%s' % (ITIS_DATA_NAMESPACE, ITIS_RANK_TAG)).text
         name = tax.find('{%s}%s' % (ITIS_DATA_NAMESPACE, ITIS_TAXON_TAG)).text
         tsn = tax.find('{%s}%s' % (ITIS_DATA_NAMESPACE, ITIS_TAXONOMY_KEY)).text
         taxPath.append((rank, tsn, name))
      return taxPath

# ...............................................
   def getTaxonTSNHierarchy(self):
      if self.output is None:
         APIQuery.query(self, outputType='xml')
      taxPath = self._returnHierarchy()
      hierarchy = {}
      for rank in (ITIS_KINGDOM_KEY, ITIS_PHYLUM_DIVISION_KEY, ITIS_CLASS_KEY, 
                   ITIS_ORDER_KEY, ITIS_FAMILY_KEY, ITIS_GENUS_KEY,
                   ITIS_SPECIES_KEY):
         hierarchy[rank] = self._getRankFromPath(taxPath, rank)
      return hierarchy      
   
# ...............................................
   def query(self):
      """
      @summary: Queries the API and sets 'output' attribute to a ElementTree object 
      """
      APIQuery.query(self, outputType='xml')

# .............................................................................
class GbifAPI(APIQuery):
# .............................................................................
   """
   Class to query GBIF APIs and return results
   """
# ...............................................
   def __init__(self, service=GBIF_SPECIES_SERVICE, key=None, otherFilters={}):
      """
      @summary: Constructor for GbifAPI class      
      """
      url = '/'.join((GBIF_REST_URL, service))
      if key is not None:
         url = '/'.join((url, str(key)))
         APIQuery.__init__(self, url)
      else:
         APIQuery.__init__(self, url, otherFilters=otherFilters)


# ...............................................
   @staticmethod
   def _getOutputVal(outDict, name):
      try:
         val = outDict[name]
      except:
         return None
      return val
   
# ...............................................
   @staticmethod
   def getTaxonomy(taxonkey):
      """
      @summary: Return ITISScientificName, kingdom, and TSN hierarchy from one 
                occurrence record ending in this TSN (species rank) 
      """
      taxAPI = GbifAPI(service=GBIF_SPECIES_SERVICE, key=taxonkey)
      try:
         taxAPI.query()
         kingdomStr = taxAPI._getOutputVal(taxAPI.output, 'kingdom')
         phylumStr = taxAPI._getOutputVal(taxAPI.output, 'phylum')
         # Missing class string in GBIF output
#          classStr = taxAPI._getOutputVal(taxAPI.output, '')
         classStr = None
         orderStr = taxAPI._getOutputVal(taxAPI.output, 'order')
         familyStr = taxAPI._getOutputVal(taxAPI.output, 'family')
         genusStr = taxAPI._getOutputVal(taxAPI.output, 'genus')
         speciesStr = taxAPI._getOutputVal(taxAPI.output, 'species') 
         rankStr = taxAPI._getOutputVal(taxAPI.output, 'rank')
         genuskey = taxAPI._getOutputVal(taxAPI.output, 'genusKey')
         specieskey = taxAPI._getOutputVal(taxAPI.output, 'speciesKey')
         acceptedkey = taxAPI._getOutputVal(taxAPI.output, 'acceptedKey')
         if rankStr == 'SPECIES':
            fullSpeciesStr = taxAPI._getOutputVal(taxAPI.output, 'accepted')
            if fullSpeciesStr is not None:
               speciesStr = fullSpeciesStr
      except Exception, e:
         print str(e)
         raise
      return (kingdomStr, phylumStr, classStr, orderStr, familyStr, genusStr,
              speciesStr, genuskey, specieskey)
 
# ...............................................
   @staticmethod
   def getPublishingOrg(puborgkey):
      """
      @summary: Return title from one organization record with this key  
      @param puborgkey: GBIF identifier for this publishing organization
      """
      orgAPI = GbifAPI(service=GBIF_ORGANIZATION_SERVICE, key=puborgkey)
      try:
         orgAPI.query()
         puborgName = orgAPI._getOutputVal(orgAPI.output, 'title')
      except Exception, e:
         print str(e)
         raise
      return puborgName
 
# ...............................................
   def query(self):
      """
      @summary: Queries the API and sets 'output' attribute to a ElementTree object 
      """
      APIQuery.query(self, outputType='json')

# # .............................................................................
# class IdigbioAPI(APIQuery):
# # .............................................................................
#    """
#    Class to query iDigBio APIs and return results
#    """
# # ...............................................
#    def __init__(self, qFilters={}, otherFilters={}, filterString=None,
#                 headers={'Content-Type': 'application/json'}):
#       """
#       @summary: Constructor for IdigbioAPI class      
#       """
#       # Add Q filters for this instance
#       for key, val in IDIGBIO_QFILTERS.iteritems():
#          qFilters[key] = val
#       # Add other filters for this instance
#       for key, val in IDIGBIO_FILTERS.iteritems():
#          otherFilters[key] = val
#           
#       idigSearchUrl = '/'.join((IDIGBIO_OCCURRENCE_URL, IDIGBIO_SEARCH_QUERY_KEY))
#           
#       APIQuery.__init__(self, idigSearchUrl, qFilters=qFilters, 
#                         otherFilters=otherFilters, filterString=filterString, 
#                         headers=headers)
#        
# # ...............................................
#    @classmethod
#    def initFromUrl(cls, url, headers={'Content-Type': 'application/json'}):
#       base, filters = url.split('?')
#       if base == IDIGBIO_OCCURRENCE_URL:
#          qry = IdigbioAPI(filterString=filters)
#       else:
#          raise Exception('iDigBio occurrence API must start with %s' 
#                          % IDIGBIO_OCCURRENCE_URL)
#       return qry
#        
# # ...............................................
#    def _burrow(self, keylst):
#       dict = self.output
#       for key in keylst:
#          dict = dict[key]
#       return dict
#           
# # ...............................................
#    def getBinomial(self):
#       """
#       @summary: Returns a list of dictionaries where each dictionary is an 
#                 occurrence record
#       """
#       if self.debug:
#          print self.url
#       if self.output is None:
#          self.query()
#       dataList = self._burrow(IDIGBIO_BINOMIAL_KEYS)
#       binomialList = []
#       filtered = []
#       print 'Distinct scientific names count = %d' % (len(dataList))
#       for entry in dataList:
#          matches = re.match(IDIGBIO_BINOMIAL_REGEX, entry[IDIGBIO_NAME_KEY])
#          if matches:
#             if not matches.group(2) == IDIGBIO_NAME_POSTFIX_FILTER:
#                binomialList.append(entry)
#             else:
#                if self.debug:
#                   filtered.append(entry[IDIGBIO_NAME_KEY])
#          else:
#             if self.debug:
#                filtered.append(entry[IDIGBIO_NAME_KEY])
#       print 'Distinct binomials count = %d' % (len(binomialList))
#       if self.debug:
#          print 'Filtered:', filtered
#       return binomialList
#   
# # ...............................................
#    def getSpecimensByBinomial(self):
#       """
#       @summary: Returns a list of dictionaries.  Each dictionary is an occurrence record
#       """
#       if self.debug:
#          print self.url
#       if self.output is None:
#          self.query()
#       specimenList = []
#       dataList = self._burrow(IDIGBIO_OCCURRENCE_KEYS)
#       for entry in dataList:
#          specimenList.append(entry[IDIGBIO_OCCURRENCE_CONTENT_KEY])
#       return specimenList
#  
# # ...............................................
#    def getOccurrences(self, asShapefile=False):
#       """
#       @summary: Returns a list of dictionaries.  Each dictionary is an occurrence record
#       """
#       if self.debug:
#          print self.url
#       if self.output is None:
#          self.query()
#       specimenList = []
#       dataList = self._burrow(IDIGBIO_OCCURRENCE_KEYS)
#       for entry in dataList:
#          specimenList.append(entry[IDIGBIO_OCCURRENCE_CONTENT_KEY])
#       return specimenList
#  
# # ...............................................
#    def query(self):
#       """
#       @summary: Queries the API and sets 'output' attribute to a JSON object 
#       """
#       APIQuery.query(self, outputType='json')
      

# .............................................................................
# .............................................................................

if __name__ == '__main__':
#    spnames = ('Regulus calendula', 'Phainopepla nitens', 'Piranga ludoviciana', 
#               'Piranga flava', 'Hippodamia convergens', 'Catharus fuscescens', 
#               'Helichus suturalis', 'Regulus satrapa', 'Myadestes townsendi', 
#               'Sialia mexicana', 'Hylaeus modestus', 'Catharus guttatus', 
#               'Catharus ustulatus', 'Catharus minimus', 'Sialia sialis', 
#               'Polioptila caerulea', 'Polioptila melanura')
   tsnQuery = BisonAPI(qFilters={BISON_NAME_KEY: BISON_BINOMIAL_REGEX}, 
                       otherFilters=BISON_TSN_FILTERS)
   tsnList = tsnQuery.getBinomialTSNs()
   print len(tsnList)
    
   tsnList = [[u'100637', 31], [u'100667', 45], [u'100674', 24]]
   response = {u'facet_counts': 
               {u'facet_ranges': {}, 
                u'facet_fields': {u'TSNs': tsnList}
                }
               }
 
   loopCount = 0
   occAPI = None
   taxAPI = None
    
   for tsnPair in tsnList:
      tsn = int(tsnPair[0])
      count = int(tsnPair[1])
  
#       taxAPI = ItisAPI(otherFilters={ITIS_TAXONOMY_KEY: tsn})
#       taxPath = taxQuery.getTSNHierarchy()
#       for tax in taxPath:
#          print str(tax)
  
      newQ = {BISON_HIERARCHY_KEY: '*-%d-*' % tsn}
      occAPI = BisonAPI(qFilters=newQ, otherFilters=BISON_OCC_FILTERS)
      print occAPI.url
      occList = occAPI.getTSNOccurrences()
      print 'Received %d occurrences for TSN %d' % (len(occList), tsn)
        
      tsnAPI = BisonAPI(qFilters={BISON_HIERARCHY_KEY: '*-%d-' % tsn}, 
                        otherFilters={'rows': 1})
      hier = tsnAPI.getFirstValueFor(BISON_HIERARCHY_KEY)
      name = tsnAPI.getFirstValueFor(BISON_NAME_KEY)
      print name, hier
        
      GbifAPI.getTaxonomy(1000225)
        
      # Only loop twice for debugging
      loopCount += 1
      if loopCount > 2:
         break
# 
#       """
# # .............................................................................
# # Main method to (a) retrieve all scientific names in iDigBio, (b) keep only
# # binomials, (c) for each binomial create a list of other names to be included
# # (usually subspecies and names with authors), and (d) retrieve occurrences
# # for each species.
# # .............................................................................
#       """
# 
#    scinameQuery = IdigbioAPI(filterString="source=" + 
#                              IDIGBIO_AGG_SPECIES_GEO_MIN_40)
#    binomials = scinameQuery.getBinomial()
#    #print binomials
# 
#    for id, binomial in enumerate(binomials):
#       if id > 10:
#          quit()
#       else:
#          specimenQuery = IdigbioAPI(filterString="source=" + 
#                   IDIGBIO_SPECIMENS_BY_BINOMIAL.replace("__BINOMIAL__", 
#                                                         binomial[IDIGBIO_NAME_KEY]))
#          specimens = specimenQuery.getSpecimensByBinomial()
#          print "Retrieved %d specimens as %s" % (len(specimens), binomial[IDIGBIO_NAME_KEY])


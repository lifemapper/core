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
import requests
from types import (BooleanType, DictionaryType, FloatType, IntType, ListType, 
                   StringType, TupleType, UnicodeType)
import urllib
import xml.etree.ElementTree as ET

from LmCommon.common.lmconstants import (BISON, BISON_QUERY, GBIF, ITIS, 
                                         IDIGBIO, IDIGBIO_QUERY, 
                                         URL_ESCAPES, HTTPStatus, DWCNames)

# .............................................................................
class APIQuery(object):
   """
   Class to query APIs and return results
   """
   def __init__(self, baseurl, 
                qKey = None, qFilters={}, otherFilters={}, filterString=None, 
                headers={}):
      """
      @summary Constructor for the APIQuery class
      """
      self._qKey = qKey
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
      if self.filterString and len(self.filterString) > 1:
         return '{}?{}'.format(self.baseurl, self.filterString)
      else:
         return self.baseurl
      
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
            allFilters[self._qKey] = qVal
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
            cls = '{}:[{} TO {}]'.format(key, str(val[0]), str(val[1]))
         else:
            print 'Unexpected value type {}'.format(val)
      else:
         print 'Unexpected value type {}'.format(val)
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
      firstClause = ''
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
   def queryByGet(self, outputType='json'):
      """
      @summary: Queries the API and sets 'output' attribute to a JSON object 
      """
      self.output = None
      data = retcode = None
      try:
         response = requests.get(self.url, headers=self.headers)
      except Exception, e:
         try:
            retcode = response.status_code
            reason = response.reason
         except:
            reason = 'Unknown Error'
         print('Failed on URL {}, code = {}, reason = {} ({})'
               .format(self.url, retcode, reason, str(e)))
       
      if response.status_code == 200:
         try:
            if outputType == 'json':
               self.output = response.json()
            else:
               self.output = response.content
         except Exception, e:
            print('Failed to interpret output of URL {} ({})'
                  .format(self.url, str(e)))
      else:
         print('Failed on URL {}, code = {}, reason = {}'
               .format(self.url, response.status_code, response.reason))

# ...............................................
   def queryByPost(self, outputType='json'):
      self.output = None
      allParams = self._otherFilters.copy()
      allParams[self._qKey] = self._qFilters
      queryAsString = json.dumps(allParams)
      try:
         response = requests.post(self.baseurl, 
                                  data=queryAsString,
                                  headers=self.headers)
      except Exception, e:
         try:
            retcode = response.status_code
            reason = response.reason
         except:
            retcode = HTTPStatus.INTERNAL_SERVER_ERROR
            reason = 'Unknown Error'
         print('Failed on URL {}, code = {}, reason = {} ({})'.format(
                           self.url, retcode, reason, str(e)))
      
      if response.ok:
         try:
            if outputType == 'json':
               self.output = response.json()
            elif outputType == 'xml':
               output = response.text
               self.output = ET.fromstring(output)
            else:
               print('Unrecognized output type {}'.format(outputType))
         except Exception, e:
            print('Failed to interpret output of URL {}, content = {}; ({})'
                  .format(self.baseurl, response.content, str(e)))
      else:
         try:
            retcode = response.status_code
            reason = response.reason
         except:
            retcode = HTTPStatus.INTERNAL_SERVER_ERROR
            reason = 'Unknown Error'
         print('Failed ({}: {}) for baseurl {} and query {}'
               .format(retcode, reason, self.baseurl, queryAsString))

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
      allQFilters = BISON_QUERY.QFILTERS.copy()
      for key, val in qFilters.iteritems():
         allQFilters[key] = val
         
      # Add/replace other filters to defaults for this instance
      allOtherFilters = BISON_QUERY.FILTERS.copy()
      for key, val in otherFilters.iteritems():
         allOtherFilters[key] = val
         
      APIQuery.__init__(self, BISON.OCCURRENCE_URL, qKey='q', qFilters=allQFilters, 
                        otherFilters=allOtherFilters, filterString=filterString, 
                        headers=headers)
      
# ...............................................
   @classmethod
   def initFromUrl(cls, url, headers={'Content-Type': 'application/json'}):
      base, filters = url.split('?')
      if base.strip().startswith(BISON.OCCURRENCE_URL):
         qry = BisonAPI(filterString=filters)
      else:
         raise Exception('Bison occurrence API must start with {}' 
                        .format(BISON.OCCURRENCE_URL))
      return qry

# ...............................................
   def query(self):
      """
      @summary: Queries the API and sets 'output' attribute to a JSON object 
      """
      APIQuery.queryByGet(self, outputType='json')

# ...............................................
   def _burrow(self, keylst):
      thisdict = self.output
      if isinstance(thisdict, DictionaryType):
         try:         
            for key in keylst:
               thisdict = thisdict[key]
         except Exception, e:
            raise Exception('Problem with key {} in output'.format(key))
      else:
         raise Exception('Invalid output type ({})'.format(type(thisdict)))
      return thisdict
         
# ...............................................
   @staticmethod
   def getTsnListForBinomials():
      """
      @summary: Returns a list of sequences containing tsn and tsnCount
      """
      bisonQuery = BisonAPI(qFilters={BISON.NAME_KEY: BISON.BINOMIAL_REGEX}, 
                            otherFilters=BISON_QUERY.TSN_FILTERS)
      tsnList = bisonQuery._getBinomialTSNs()
      return tsnList

# ...............................................
   def _getBinomialTSNs(self):
      dataList = None
      self.query()
      if self.output is not None:
         dataCount = self._burrow(BISON_QUERY.COUNT_KEYS)
         dataList = self._burrow(BISON_QUERY.TSN_LIST_KEYS)
         print 'Reported count = {}, actual count = {}'.format(dataCount, 
                                                               len(dataList))
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
         occAPI = BisonAPI(qFilters={BISON.HIERARCHY_KEY: '*-{}-'.format(itisTSN)}, 
                           otherFilters={'rows': 1})
         tsnHier = occAPI.getFirstValueFor(BISON.HIERARCHY_KEY)
         itisname = occAPI.getFirstValueFor(BISON.NAME_KEY)
         king = occAPI.getFirstValueFor(BISON.KINGDOM_KEY)
      except Exception, e:
         print str(e)
         raise
      return (itisname, king, tsnHier)
   
# ...............................................
   def getTSNOccurrences(self, asShapefile=False):
      """
      @summary: Returns a list of dictionaries.  Each dictionary is an occurrence record
      """
      dataList = []
      if self.output is None:
         self.query()
      if self.output is not None:
         dataCount = self._burrow(BISON_QUERY.COUNT_KEYS)
         dataList = self._burrow(BISON_QUERY.RECORD_KEYS)
      return dataList
      
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
            print('Missing {} for {}'.format(fieldname, self.url))
               
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
      APIQuery.__init__(self, ITIS.TAXONOMY_HIERARCHY_URL, 
                        otherFilters=otherFilters)
   
# ...............................................
   def _findTaxonByRank(self, root, rankKey):
      for tax in root.iter('{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.HIERARCHY_TAG)):
         rank = tax.find('{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.RANK_TAG)).text
         if rank == rankKey:
            name = tax.find('{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.TAXON_TAG)).text
            tsn = tax.find('{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.TAXONOMY_KEY)).text
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
      for tax in self.output.iter('{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.HIERARCHY_TAG)):
         rank = tax.find('{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.RANK_TAG)).text
         name = tax.find('{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.TAXON_TAG)).text
         tsn = tax.find('{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.TAXONOMY_KEY)).text
         taxPath.append((rank, tsn, name))
      return taxPath

# ...............................................
   def getTaxonTSNHierarchy(self):
      if self.output is None:
         APIQuery.query(self, outputType='xml')
      taxPath = self._returnHierarchy()
      hierarchy = {}
      for rank in (ITIS.KINGDOM_KEY, ITIS.PHYLUM_DIVISION_KEY, ITIS.CLASS_KEY, 
                   ITIS.ORDER_KEY, ITIS.FAMILY_KEY, ITIS.GENUS_KEY,
                   ITIS.SPECIES_KEY):
         hierarchy[rank] = self._getRankFromPath(taxPath, rank)
      return hierarchy      
   
# ...............................................
   def query(self):
      """
      @summary: Queries the API and sets 'output' attribute to a ElementTree object 
      """
      APIQuery.queryByGet(self, outputType='xml')

# .............................................................................
class GbifAPI(APIQuery):
# .............................................................................
   """
   Class to query GBIF APIs and return results
   """
# ...............................................
   def __init__(self, service=GBIF.SPECIES_SERVICE, key=None, otherFilters={}):
      """
      @summary: Constructor for GbifAPI class      
      """
      url = '/'.join((GBIF.REST_URL, service))
      if key is not None:
         url = '/'.join((url, str(key)))
         APIQuery.__init__(self, url)
      else:
         APIQuery.__init__(self, url, otherFilters=otherFilters)


# ...............................................
   @staticmethod
   def _getOutputVal(outDict, name):
      try:
         tmp = outDict[name]
         val = unicode(tmp).encode('utf-8')
      except:
         return None
      return val
   
# ...............................................
   @staticmethod
   def getTaxonomy(taxonKey):
      """
      @summary: Return ITISScientificName, kingdom, and TSN hierarchy from one 
                occurrence record ending in this TSN (species rank) 
      """
      taxAPI = GbifAPI(service=GBIF.SPECIES_SERVICE, key=taxonKey)
      try:
         taxAPI.query()
         scinameStr = taxAPI._getOutputVal(taxAPI.output, 'scientificName')
         kingdomStr = taxAPI._getOutputVal(taxAPI.output, 'kingdom')
         phylumStr = taxAPI._getOutputVal(taxAPI.output, 'phylum')
         classStr = taxAPI._getOutputVal(taxAPI.output, 'class')
         orderStr = taxAPI._getOutputVal(taxAPI.output, 'order')
         familyStr = taxAPI._getOutputVal(taxAPI.output, 'family')
         genusStr = taxAPI._getOutputVal(taxAPI.output, 'genus')
         speciesStr = taxAPI._getOutputVal(taxAPI.output, 'species') 
         rankStr = taxAPI._getOutputVal(taxAPI.output, 'rank')
         genusKey = taxAPI._getOutputVal(taxAPI.output, 'genusKey')
         speciesKey = taxAPI._getOutputVal(taxAPI.output, 'speciesKey')
         acceptedKey = taxAPI._getOutputVal(taxAPI.output, 'acceptedKey')
         nubKey = taxAPI._getOutputVal(taxAPI.output, 'nubKey')
         taxStatus = taxAPI._getOutputVal(taxAPI.output, 'taxonomicStatus')
         acceptedStr = taxAPI._getOutputVal(taxAPI.output, 'accepted')
         canonicalStr = taxAPI._getOutputVal(taxAPI.output, 'canonicalName')
         loglines = []
         if taxStatus != 'ACCEPTED':
            try:
               loglines.append(taxAPI.url)
               loglines.append('   genusKey = {}'.format(genusKey))
               loglines.append('   speciesKey = {}'.format(speciesKey))
               loglines.append('   acceptedKey = {}'.format(acceptedKey))
               loglines.append('   acceptedStr = {}'.format(acceptedStr))
               loglines.append('   nubKey = {}'.format(nubKey))
               loglines.append('   taxonomicStatus = {}'.format(taxStatus))
               loglines.append('   accepted = {}'.format(acceptedStr))
               loglines.append('   canonicalName = {}'.format(canonicalStr))
               loglines.append('   rank = {}'.format(rankStr))
            except:
               loglines.append('Failed to format data from {}'.format(taxonKey))
      except Exception, e:
         print str(e)
         raise
      return (rankStr, scinameStr, canonicalStr, acceptedKey, acceptedStr, 
              nubKey, taxStatus, kingdomStr, phylumStr, classStr, orderStr, 
              familyStr, genusStr, speciesStr, genusKey, speciesKey, loglines)
 
# ...............................................
   @staticmethod
   def getPublishingOrg(puborgKey):
      """
      @summary: Return title from one organization record with this key  
      @param puborgKey: GBIF identifier for this publishing organization
      """
      orgAPI = GbifAPI(service=GBIF.ORGANIZATION_SERVICE, key=puborgKey)
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
      APIQuery.queryByGet(self, outputType='json')

# .............................................................................
class IdigbioAPI(APIQuery):
# .............................................................................
   """
   Class to query iDigBio APIs and return results
   """
# ...............................................
   def __init__(self, qFilters={}, otherFilters={}, filterString=None,
                headers={'Content-Type': 'application/json'}):
      """
      @summary: Constructor for IdigbioAPI class      
      """
      idigSearchUrl = '/'.join((IDIGBIO.SEARCH_PREFIX, IDIGBIO.SEARCH_POSTFIX, 
                                IDIGBIO.OCCURRENCE_POSTFIX))

      # Add/replace Q filters to defaults for this instance 
      allQFilters = IDIGBIO_QUERY.QFILTERS.copy()
      for key, val in qFilters.iteritems():
         allQFilters[key] = val
         
      # Add/replace other filters to defaults for this instance
      allOtherFilters = IDIGBIO_QUERY.FILTERS.copy()
      for key, val in otherFilters.iteritems():
         allOtherFilters[key] = val
         
      APIQuery.__init__(self, idigSearchUrl, qKey='rq',
                        qFilters=allQFilters, otherFilters=allOtherFilters, 
                        filterString=filterString, headers=headers)

# ...............................................
   @classmethod
   def initFromUrl(cls, url):
      base, filters = url.split('?')
      if base.strip().startswith(IDIGBIO.SEARCH_PREFIX):
         qry = IdigbioAPI(filterString=filters)
      else:
         raise Exception('iDigBio occurrence API must start with {}' 
                        .format(IDIGBIO.SEARCH_PREFIX))
      return qry

# ...............................................
   def query(self):
      """
      @summary: Queries the API and sets 'output' attribute to a JSON object 
      """
      APIQuery.queryByPost(self, outputType='json')
          
# ...............................................
   @staticmethod
   def getTaxonIdsBinomials():
      pass

# ...............................................
   def queryByGBIFTaxonId(self, taxonKey):
      """
      @summary: Returns a list of dictionaries.  Each dictionary is an occurrence record
      """
      self._qFilters[IDIGBIO.GBIFID_FIELD] = taxonKey
      self.query()
      specimenList = []
      if self.output is not None:
         fullCount = self.output['itemCount']
         for item in self.output[IDIGBIO.OCCURRENCE_ITEMS_KEY]:
            newitem = {}
            for dataFld, dataVal in item[IDIGBIO.RECORD_CONTENT_KEY].iteritems():
               newitem[dataFld] = dataVal
            for idxFld, idxVal in item[IDIGBIO.RECORD_INDEX_KEY].iteritems():
               if idxFld == 'geopoint':
                  newitem[DWCNames.DECIMAL_LONGITUDE['SHORT']] = idxVal['lon']
                  newitem[DWCNames.DECIMAL_LATITUDE['SHORT']] = idxVal['lat']
               else:
                  newitem[idxFld] = idxVal
            specimenList.append(newitem)
      return specimenList

# ...............................................
   def queryBySciname(self, sciname):
      """
      @summary: Returns a list of dictionaries.  Each dictionary is an occurrence record
      """
      self._qFilters['scientificname'] = sciname
      self.query()
      specimenList = []
      if self.output is not None:
         fullCount = self.output['itemCount']
         for item in self.output[IDIGBIO.OCCURRENCE_ITEMS_KEY]:
            newitem = {}
            for dataFld, dataVal in item[IDIGBIO.RECORD_CONTENT_KEY].iteritems():
               newitem[dataFld] = dataVal
            for idxFld, idxVal in item[IDIGBIO.RECORD_INDEX_KEY].iteritems():
               if idxFld == 'geopoint':
                  newitem[DWCNames.DECIMAL_LONGITUDE['SHORT']] = idxVal['lon']
                  newitem[DWCNames.DECIMAL_LATITUDE['SHORT']] = idxVal['lat']
               else:
                  newitem[idxFld] = idxVal
            specimenList.append(newitem)
      return specimenList

# ...............................................
   def getOccurrences(self, asShapefile=False):
      """
      @summary: Returns a list of dictionaries.  Each dictionary is an occurrence record
      """
      if self.output is None:
         self.query()
      specimenList = []
      if self.output is not None:
         for item in self.output[IDIGBIO.OCCURRENCE_ITEMS_KEY]:
            newitem = {}
            for dataFld, dataVal in item[IDIGBIO.RECORD_CONTENT_KEY].iteritems():
               newitem[dataFld] = dataVal
            for idxFld, idxVal in item[IDIGBIO.RECORD_INDEX_KEY].iteritems():
               if idxFld == 'geopoint':
                  newitem[DWCNames.DECIMAL_LONGITUDE['SHORT']] = idxVal['lon']
                  newitem[DWCNames.DECIMAL_LATITUDE['SHORT']] = idxVal['lat']
               else:
                  newitem[idxFld] = idxVal
            specimenList.append(newitem)
      return specimenList

# ...............................................
# ...............................................
def testIdigbioTaxonIds(testcount, infname):
   import os
#    statii = {}
   # Output
#    outfname = '/tmp/idigbio_summary.txt'
#    if os.path.exists(outfname):
#       os.remove(outfname)
   outlist = '/tmp/idigbio_accepted_list.txt'
   if os.path.exists(outlist):
      os.remove(outlist)
   outf = open(outlist, 'w')

   idigList = []
   with open(infname, 'r') as inf:
#          with line in file:
      for i in range(testcount):
         line = inf.readline()
         vals = []
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
               currName = ' '.join(tempvals)
               
            (rankStr, scinameStr, canonicalStr, acceptedKey, acceptedStr, 
             nubKey, taxStatus, kingdomStr, phylumStr, classStr, orderStr, 
             familyStr, genusStr, speciesStr, genusKey, speciesKey, 
             loglines) = GbifAPI.getTaxonomy(currGbifTaxonId)
                         
            if taxStatus == 'ACCEPTED':
               idigList.append([currGbifTaxonId, currReportedCount, currName])
               outf.write(line)
               
   inf.close()
   outf.close()
         
   return idigList

# .............................................................................
# .............................................................................
if __name__ == '__main__':
   idigbio = gbif = bison = False
   idigbio = True
   
   if bison:
      # ******************* BISON ********************************
#       tsnQuery = BisonAPI(qFilters={BISON.NAME_KEY: BISON.BINOMIAL_REGEX}, 
#                           otherFilters=BISON.TSN_FILTERS)
#     
#       qfilters = {'decimalLongitude': (-125, -66), 'decimalLatitude': (24, 50), 
#                   'ITISscientificName': '/[A-Za-z]*[ ]{1,1}[A-Za-z]*/', 
#                   'basisOfRecord': [(False, 'living'), (False, 'fossil')]}
#       otherfilters = {'facet.mincount': 20, 'rows': 0, 'facet.field': 'TSNs', 
#                   'facet': True, 'facet.limit': -1, 'wt': 'json', 'json.nl': 'arrarr'}
#       headers = {'Content-Type': 'application/json'}
#       tsnList = tsnQuery.getBinomialTSNs()
#       print len(tsnList)
          
      tsnList = [[u'100637', 31], [u'100667', 45], [u'100674', 24]]
      response = {u'facet_counts': 
                  {u'facet_ranges': {}, 
                   u'facet_fields': {u'TSNs': tsnList}
                   }
                  }
       
      loopCount = 0
      occAPI = None
      taxAPI = None
          
#       tsnList = BisonAPI.getTsnListForBinomials()
      for tsnPair in tsnList:
         tsn = int(tsnPair[0])
         count = int(tsnPair[1])
                 
         newQ = {BISON.HIERARCHY_KEY: '*-{}-*'.format(tsn)}
         occAPI = BisonAPI(qFilters=newQ, otherFilters=BISON_QUERY.OCC_FILTERS)
         thisurl = occAPI.url
         occList = occAPI.getTSNOccurrences()
         count = None if not occList else len(occList)
         print 'Received {} occurrences for TSN {}'.format(count, tsn)
 
         occAPI2 = BisonAPI.initFromUrl(thisurl)
         occList2 = occAPI2.getTSNOccurrences()
         count = None if not occList2 else len(occList2)
         print 'Received {} occurrences from url init'.format(count)
          
         tsnAPI = BisonAPI(qFilters={BISON.HIERARCHY_KEY: '*-{}-'.format(tsn)}, 
                           otherFilters={'rows': 1})
         hier = tsnAPI.getFirstValueFor(BISON.HIERARCHY_KEY)
         name = tsnAPI.getFirstValueFor(BISON.NAME_KEY)
         print name, hier
   
   if gbif:
      # ******************* GBIF ********************************
      taxonid = 1000225
      output = GbifAPI.getTaxonomy(taxonid)
      print 'GBIF Taxonomy for {} = {}'.format(taxonid, output)
         
   if idigbio:
      infname = '/tank/data/input/idigbio/taxon_ids.txt'
      testcount = 20
#       idigList =  testIdigbioTaxonIds(testcount, infname)

      # ******************* iDigBio ********************************
      idigList = [
#                   (4990907, 65932, 'megascelis subtilis'), 
#                   (5171118, 50533, 'gea argiopides'), 
                  (2437967, 129988, 'peromyscus maniculatus'),
                  (4990907, 65932, 'megascelis subtilis'),
                  (5158206, 63971, 'urana'),
                  (2438635, 0, ''), 
                  (2394563, 0, ''), 
                  (2360481, 0, ''),
                  (5231132, 0, ''),
                  (2350580, 0, ''),
                  (2361357, 0, '')]
      for currGbifTaxonId, currReportedCount, currName in idigList:
         # direct query
         api = IdigbioAPI()
         try:
            occList1 = api.queryByGBIFTaxonId(currGbifTaxonId)
         except:
            print 'Failed on {}'.format(currGbifTaxonId)
         else:
            print("Retrieved {} records for gbif taxonid {}"
                  .format(len(occList1), currGbifTaxonId))
            
         print '   ', api.baseurl
         print '   ', api._otherFilters
         print '   ', api._qFilters
         print
         
"""
from LmCommon.common.apiquery import *
import idigbio


keys = {1967: 'Trichotria pocillum', 1034: 'Antiphonus conatus', 
2350: 'Antheromorpha', 8133: 'Scytonotus piger', 1422: 'Helichus suturalis', 
3799: 'Anacaena debilis', 1393: 'Discoderus papagonis', 653: 'Cicindela repanda', 
896: 'Cicindela purpurea', 1762: 'Cicindela tranquebarica', 
1419: 'Cicindela duodecimguttata', 3641: 'Cicindela punctulata', 
1818: 'Cicindela oregona', 895: 'Cicindela hirticollis', 
1298: 'Cicindela sexguttata', 1507: 'Cicindela ocellata', 
1219: 'Cicindela formosa', 1042: 'Amara obesa', 927: 'Brachinus elongatulus', 
1135: 'Brachinus mexicanus'}

lmapi = IdigbioAPI()
api = idigbio.json()

for taxonKey in keys:
   occList = lmapi.queryByGBIFTaxonId(taxonKey)
   
for key, name in keys.iteritems():
   olist = lmapi.queryByGBIFTaxonId(key)
   countkey = api.count_records(rq={'taxonid':key, 'basisofrecord': 'preservedspecimen'})
   countname = api.count_records(rq={'scientificname':name, 'basisofrecord': 'preservedspecimen'})
   print '{}: lm={}, iTaxonkey={}, iName={}'.format(key, len(olist), countkey, countname) 

"""
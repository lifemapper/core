"""Module containing functions for API Queries
"""
import idigbio
import json
import os
import requests
import sys
from types import (BooleanType, DictionaryType, TupleType, FloatType, IntType, 
                   StringType, UnicodeType, ListType)
import unicodecsv
import urllib

from LmCommon.common.lmconstants import (BISON, BISON_QUERY, GBIF, ITIS, 
                                         IDIGBIO, IDIGBIO_QUERY, 
                                         URL_ESCAPES, HTTPStatus, DWCNames)
from LmCommon.common.lmXml import fromstring, deserialize
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import readyFilename

# .............................................................................
class APIQuery(object):
    """
    Class to query APIs and return results.  
    @note: CSV files are created with tab delimiter
    """
    ENCODING = 'utf-8'
    DELIMITER = '\t'
    GBIF_MISSING_KEY = 'unmatched_gbif_ids'

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
        unicodecsv.field_size_limit(sys.maxsize)
      
# ...............................................
    @classmethod
    def initFromUrl(cls, url, headers={}):
        base, filters = url.split('?')
        qry = APIQuery(base, filterString=filters, headers=headers)
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
    def _getCSVWriter(self, datafile, doAppend=True):
        '''
        @summary: Get a CSV writer that can handle encoding
        '''
        unicodecsv.field_size_limit(sys.maxsize)
        if doAppend:
            mode = 'ab'
        else:
            mode = 'wb'
           
        try:
            readyFilename(datafile)
            f = open(datafile, mode) 
            writer = unicodecsv.writer(f, delimiter=self.DELIMITER, 
                                      encoding=self.ENCODING)
        
        except Exception, e:
            raise Exception('Failed to read or open {}, ({})'
                            .format(datafile, str(e)))
        return writer, f
          
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
        retcode = None
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
            if outputType == 'json':
                try:
                    self.output = response.json()
                except Exception, e:
                    output = response.content
                    self.output = deserialize(fromstring(output))
            elif outputType == 'xml':
                output = response.text
                self.output = deserialize(fromstring(output))
            else:
                print('Unrecognized output type {}'.format(outputType))
        else:
            print('Failed on URL {}, code = {}, reason = {}'
                  .format(self.url, response.status_code, response.reason))

    # ...........    ....................................
    def queryByPost(self, outputType='json', file=None):
        self.output = None
        # Post a file
        if file is not None:
            files = {'files': open(file, 'rb')}
            try:
                response = requests.post(self.baseurl, files=files)
            except Exception, e:
                try:
                    retcode = response.status_code
                    reason = response.reason
                except:
                    retcode = HTTPStatus.INTERNAL_SERVER_ERROR
                    reason = 'Unknown Error'
                print("""Failed on URL {}, posting uploaded file {}, code = {}, 
                        reason = {} ({})""".format(self.url, file, retcode, 
                                                   reason, str(e)))
        # Post parameters
        else:
            allParams = self._otherFilters.copy()
            allParams[self._qKey] = self._qFilters
            queryAsString = json.dumps(allParams)
            try:
                response = requests.post(self.baseurl, data=queryAsString,
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
                    try:
                        self.output = response.json()
                    except Exception, e:
                        output = response.content
                        self.output = deserialize(fromstring(output))
                elif outputType == 'xml':
                    output = response.text
                    self.output = deserialize(fromstring(output))
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
            print('Failed ({}: {}) for baseurl {}'.format(retcode, reason, 
                                                          self.baseurl))

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
            for key in keylst:
                try:         
                    thisdict = thisdict[key]
                except KeyError, e:
                    raise Exception('Missing key {} in output'.format(key))
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
#             dataCount = self._burrow(BISON_QUERY.COUNT_KEYS)
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
                val = rec[fieldname]
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
    NameMatchFieldnames = ['scientificName', 'kingdom', 'phylum', 'class', 
                           'order', 'family', 'genus', 'species', 'rank', 
                           'genusKey', 'speciesKey', 'usageKey', 
                           'canonicalName', 'confidence']
    ACCEPTED_NAME_KEY = 'accepted_name'
    SEARCH_NAME_KEY = 'search_name'
    SPECIES_KEY_KEY = 'speciesKey'
    SPECIES_NAME_KEY = 'species'
    TAXON_ID_KEY = 'taxon_id'

    # ...............................................
    def __init__(self, service=GBIF.SPECIES_SERVICE, key=None, otherFilters={}):
        """
        @summary: Constructor for GbifAPI class      
        """
        url = '/'.join((GBIF.REST_URL, service))
        if key is not None:
            url = '/'.join((url, str(key)))
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
        @summary: Return GBIF backbone taxonomy for this GBIF Taxon ID  
        """
        acceptedKey = acceptedStr = nubKey = None
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
            taxStatus = taxAPI._getOutputVal(taxAPI.output, 'taxonomicStatus')
            canonicalStr = taxAPI._getOutputVal(taxAPI.output, 'canonicalName')
            loglines = []
            if taxStatus != 'ACCEPTED':
                try:
                    # Not present if results are taxonomicStatus=ACCEPTED
                    acceptedKey = taxAPI._getOutputVal(taxAPI.output, 'acceptedKey')
                    acceptedStr = taxAPI._getOutputVal(taxAPI.output, 'accepted')
                    nubKey = taxAPI._getOutputVal(taxAPI.output, 'nubKey')
                    
                    loglines.append(taxAPI.url)
                    loglines.append('   taxonomicStatus = {}'.format(taxStatus))
                    loglines.append('   acceptedKey = {}'.format(acceptedKey))
                    loglines.append('   acceptedStr = {}'.format(acceptedStr))
                    loglines.append('   nubKey = {}'.format(nubKey))
                    loglines.append('   genusKey = {}'.format(genusKey))
                    loglines.append('   speciesKey = {}'.format(speciesKey))
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
    def _getTaiwanRow(self, occAPI, rec):
        row = None
        occKey = occAPI._getOutputVal(rec, 'gbifID')
        lonstr = occAPI._getOutputVal(rec, 'decimalLongitude')
        latstr = occAPI._getOutputVal(rec, 'decimalLatitude')
        try:
            float(lonstr)
        except:
            return row

        try:
            float(latstr)
        except:
            return row

        if (occKey is not None 
            and not latstr.startswith('0.0')
            and not lonstr.startswith('0.0')):
            row = [occKey, lonstr, latstr]
        return row
    
    # ...............................................
    def getTaiwanOccurrences(self, taxonKey, outfname):
        """
        @summary: Return GBIF backbone taxonomy for this GBIF Taxon ID  
        """
        offset = 0
        currcount = 0
        total = 0
        try:
            writer, f = self._getCSVWriter(outfname, doAppend=False)
     
            while offset <= total:
                otherFilters = {'taxonKey': taxonKey, 
                                'offset': offset, 'limit': GBIF.LIMIT, 
                                'country': 'TW'}
                occAPI = GbifAPI(service=GBIF.OCCURRENCE_SERVICE, 
                                 key=GBIF.SEARCH_COMMAND, otherFilters=otherFilters)
                try:
                    occAPI.query()
                except:
                    print 'Failed on {}'.format(taxonKey)
                    currcount = 0
                else:
                    isEnd = occAPI._getOutputVal(occAPI.output, 'endOfRecords').lower()
                    count = occAPI._getOutputVal(occAPI.output, 'count')
                    # Write these records
                    recs = occAPI.output['results']
                    currcount = len(recs)
                    total += currcount
    
                    if offset == 0 and currcount > 0:
                        writer.writerow(['gbifID', 'decimalLongitude', 'decimalLatitude'])
                    
                    for rec in recs:
                        row = self._getTaiwanOcc(rec)
                        if row:
                            writer.writerow(row)
                         
                    print("  Retrieved {} records, starting at {}"
                          .format(currcount, offset))
    
                    offset += GBIF.LIMIT
        except:
            raise 
        finally:
            f.close()
        
    
    # ...............................................
    @staticmethod
    def _getFldVals(bigrec):
        rec = {}
        for fldname in GbifAPI.NameMatchFieldnames:
            try:
                rec[fldname] = bigrec[fldname]
            except:
                pass
        return rec

    # ...............................................
    @staticmethod
    def getAcceptedNames(namestr, kingdom=None):
        """
        @summary: Return closest accepted species in the GBIF backbone taxonomy 
                  for this namestring
                  rank, name, strict, verbose, kingdom, phylum, class, 
                  order, family, genus
        @note: This function uses the Name search API, returning 
        @todo: Rename function to "matchAcceptedName"
        """
        goodnames = []
        nameclean = namestr.strip()
        
        otherFilters={'name': nameclean, 'verbose': 'true'}
        if kingdom:
            otherFilters['kingdom'] = kingdom
        nameAPI = GbifAPI(service=GBIF.SPECIES_SERVICE, key='match', 
                          otherFilters=otherFilters)
        try:
            nameAPI.query()
            output = nameAPI.output
        except Exception, e:
            print ('Failed to get a response for species match on {}, ({})'
                   .format(nameclean, str(e)))
            raise
        
        try:
            status = output['status'].lower()
        except:
            status = None            
        if status in ('accepted', 'synonym'):
            smallrec = nameAPI._getFldVals(output)
            goodnames.append(smallrec)
        else:
            try:
                alternatives = output['alternatives']
                print ('No exact match on {}, returning top alternative of {}'
                       .format(nameclean, len(alternatives)))
                # get first/best synonym
                for alt in alternatives:
                    try:
                        altstatus = alt['status'].lower()
                    except:
                        altstatus = None
                    if altstatus in ('accepted', 'synonym'):
                        smallrec = nameAPI._getFldVals(alt)
                        goodnames.append(smallrec)
                        break
            except:
                print ('No match or alternatives to return for {}'.format(nameclean))
                
        return goodnames
    
# ...............................................
    def _postJsonToParser(self, url, data):
        response = output = None
        try:
            response = requests.post(url, json=data)
        except Exception as e:
            if response is not None:
                retcode = response.status_code
            else:
                print('Failed on URL {} ({})'.format(url, str(e)))
        else:
            if response.ok:
                try:
                    output = response.json()
                except Exception as e:
                    try:
                        output = response.content
                    except Exception:
                        output = response.text
                    else:
                        print('Failed to interpret output of URL {} ({})'
                            .format(url, str(e)))
            else:

                try:
                    retcode = response.status_code        
                    reason = response.reason
                except:
                    print('Failed to find failure reason for URL {} ({})'
                        .format(url, str(e)))
                else:
                    print('Failed on URL {} ({}: {})'
                            .format(url, retcode, reason))
        return output

    
    # ...............................................
    def parseNames(self, filename=None):
        """
        @summary: Return dictionary of given, and clean taxon name for 
                  namestrings in this file
        """
        if os.path.exists(filename):
            names = []
            with open(filename, 'r', encoding='utf-8') as f:
                for line in f:
                    names.append(line.strip())
                    
        cleanNames = {}
        nameAPI = GbifAPI(service=GBIF.PARSER_SERVICE)
        try:
            output = nameAPI._postJsonToParser(names)
        except Exception, e:
            print ('Failed to get a response from GBIF name parser for data in file {}, ({})'
                   .format(filename, str(e)))
            raise
        
        if output:
            for rec in output:
                if rec['parsed'] is True:
                    try:
                        sciname = rec['scientificName']
                        canname = rec['canonicalName']
                    except KeyError as e:
                        print('Missing scientific or canonicalName in output record')
                    except Exception as e:
                        print('Failed, err: {}'.format(str(e)))
                    cleanNames[sciname] = canname
                    
        return cleanNames
    
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
    OCCURRENCE_COUNT_KEY = 'count'
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
    
    # .............................................................................
    def _writeIdigbioMetadata(self, origFldnames, metaFname):
        newMeta = {}
        for colIdx in range(len(origFldnames)):
            fldname = origFldnames[colIdx]
            
            valdict = {'name': fldname , 
                       'type': 'str'}
            if fldname == 'uuid':
                valdict['role'] = OccDataParser.FIELD_ROLE_IDENTIFIER
            elif fldname == 'taxonid':
                valdict['role'] = OccDataParser.FIELD_ROLE_GROUPBY
            elif fldname == 'geopoint':
                valdict['role'] = OccDataParser.FIELD_ROLE_GEOPOINT
            elif fldname == 'canonicalname':            
                valdict['role'] = OccDataParser.FIELD_ROLE_TAXANAME
            elif fldname == 'dec_long':            
                valdict['role'] = OccDataParser.FIELD_ROLE_LONGITUDE
            elif fldname == 'dec_lat':            
                valdict['role'] = OccDataParser.FIELD_ROLE_LATITUDE
            newMeta[str(colIdx)] = valdict
        
        readyFilename(metaFname, overwrite=True)    
        with open(metaFname, 'w') as outf:
            json.dump(newMeta, outf)
        return newMeta
   
    # .............................................................................
    def _getIdigbioFields(self, rec):
        """
        @param gbifTaxonIds: one GBIF TaxonId or a list
        """
        fldnames = rec['indexTerms'].keys()
        # add dec_long and dec_lat to records
        fldnames.extend(['dec_lat', 'dec_long'])
        fldnames.sort()
        return fldnames

    # .............................................................................
    def _countIdigbioRecords(self, gbifTaxonId):
        """
        @param gbifTaxonIds: one GBIF TaxonId or a list
        """
        api = idigbio.json()
        recordQuery = {'taxonid':str(gbifTaxonId), 
                       'geopoint': {'type': 'exists'}}
        try:
            output = api.search_records(rq=recordQuery, limit=1, offset=0)
        except:
            print 'Failed on {}'.format(gbifTaxonId)
            total = 0
        else:
            total = output['itemCount']
        return total
   
    # .............................................................................
    def _getIdigbioRecords(self, gbifTaxonId, fields, writer, meta_output_file):
        """
        @param gbifTaxonIds: one GBIF TaxonId or a list
        """
        api = idigbio.json()
        limit = 100
        offset = 0
        currcount = 0
        total = 0
        recordQuery = {'taxonid':str(gbifTaxonId), 
                       'geopoint': {'type': 'exists'}}
        while offset <= total:
            try:
                output = api.search_records(rq=recordQuery,
                                            limit=limit, offset=offset)
            except:
                print 'Failed on {}'.format(gbifTaxonId)
                total = 0
            else:
                total = output['itemCount']
                
                # First gbifTaxonId where this data retrieval is successful, 
                # get and write header and metadata
                if total > 0 and fields is None:
                    print('Found data, writing fields to data and metadata files')
                    fields = self._getIdigbioFields(output['items'][0])
                    # Write header in datafile
                    writer.writerow(fields)
                    # Write metadata file with column indices 
                    meta = self._writeIdigbioMetadata(fields, meta_output_file)
                    
                # Write these records
                recs = output['items']
                currcount += len(recs)
                print("  Retrieved {} records, {} records starting at {}"
                      .format(len(recs), limit, offset))
                for rec in recs:
                    recdata = rec['indexTerms']
                    vals = []
                    for fldname in fields:
                        # Pull long, lat from geopoint
                        if fldname == 'dec_long':
                            try:
                                vals.append(recdata['geopoint']['lon'])
                            except:
                                vals.append('')
                        elif fldname == 'dec_lat':
                            try:
                                vals.append(recdata['geopoint']['lat'])
                            except:
                                vals.append('')
                        # or just append verbatim
                        else:
                            try:
                                vals.append(recdata[fldname])
                            except:
                                vals.append('')
                    
                    writer.writerow(vals)
                offset += limit
        print('Retrieved {} of {} reported records for {}'.format(currcount, total, gbifTaxonId))
        return currcount, fields

    
    # .............................................................................
    def assembleIdigbioData(self, taxon_ids, point_output_file, meta_output_file, 
                            missing_id_file=None): 
        if not(isinstance(taxon_ids, list)):
            taxon_ids = [taxon_ids]
            
        # Delete old files
        for fname in (point_output_file, meta_output_file):
            if os.path.exists(fname):
                print('Deleting existing file {} ...'.format(fname))
                os.remove(fname)
            
        summary = {self.GBIF_MISSING_KEY: []}
        try:
            writer, f = self._getCSVWriter(point_output_file, doAppend=False)
             
            # get/write data
            fldnames = None
            for gid in taxon_ids:
                # Pull/write fieldnames first time
                ptCount, fldnames = self._getIdigbioRecords(gid, fldnames, 
                                                        writer, meta_output_file)
                summary[gid] = ptCount
                if ptCount == 0:
                    summary[self.GBIF_MISSING_KEY].append(gid)
        except:
            raise 
        finally:
            f.close()
                         
        # get/write missing data
        if missing_id_file is not None and len(summary[self.GBIF_MISSING_KEY]) > 0:
            try: 
                f = open(missing_id_file, 'w')
                for gid in summary[self.GBIF_MISSING_KEY]:
                    f.write('{}\n'.format(gid))
            except Exception, e:
                raise
            finally:
                f.close()

        return summary
    
    # .............................................................................
    def queryIdigbioData(self, taxon_ids): 
        if not(isinstance(taxon_ids, list)):
            taxon_ids = [taxon_ids]
                        
        summary = {self.GBIF_MISSING_KEY: []}
         
        for gid in taxon_ids:
            # Pull/write fieldnames first time
            ptCount = self._countIdigbioRecords(gid)
            if ptCount == 0:
                summary[self.GBIF_MISSING_KEY].append(gid)
            summary[gid] = ptCount

        return summary
    
    # .............................................................................
    def readIdigbioData(self, ptFname, metaFname):
        gbifid_counts = {}
        if not(os.path.exists(ptFname)):
            print ('Point data {} does not exist'.format(ptFname))
        elif not(os.path.exists(metaFname)):
            print ('Metadata {} does not exist'.format(metaFname))
        else:
            occParser = OccDataParser(self.log, ptFname, metaFname, 
                                      delimiter=self.DELIMITER,
                                      pullChunks=True)
            occParser.initializeMe()  
            # returns dict with key = taxonid, val = (name, count)
            summary = occParser.readAllChunks()
            for taxid, (name, count) in summary.iteritems():
                gbifid_counts[taxid] = count
        return gbifid_counts



# .............................................................................
def testBison():
      
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

# .............................................................................
def testGbif():
    taxonid = 1000225
    output = GbifAPI.getTaxonomy(taxonid)
    print 'GBIF Taxonomy for {} = {}'.format(taxonid, output)

      
# .............................................................................
def testIdigbioTaxonIds():
    infname = '/tank/data/input/idigbio/taxon_ids.txt'
    testcount = 20
    
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
    pass

         
"""
import idigbio
import json
import os
import requests
import sys
from types import (BooleanType, DictionaryType, TupleType, FloatType, IntType, 
                   StringType, UnicodeType, ListType)
import unicodecsv
import urllib

from LmCommon.common.lmconstants import (BISON, BISON_QUERY, GBIF, ITIS, 
                                         IDIGBIO, IDIGBIO_QUERY, 
                                         URL_ESCAPES, HTTPStatus, DWCNames)
from LmCommon.common.lmXml import fromstring, deserialize
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import readyFilename
from LmCommon.common.apiquery import IdigbioAPI, GbifAPI


tids = [1000515, 1000519, 1000525, 1000541, 1000543, 1000575, 1000583]


names = ['Acrocystis nana', 'Bangia atropurpurea', 'Boergesenia forbesii', 
'Boodlea composita', 'Bostrychia tenella', 'Brachytrichia quoyi', 
'Caulerpa peltata', 'Caulerpa prolifera', 'Centroceras clavultum', 
'Chaetomorpha spiralis', 'Champia parvula', 'Chnoospora minima', 
'Chondracanthus intermedius', 'Cladophora herpestica', 'colpomenia sinuosa', 
'Corallina pilulifera', 'Dasya sessilis', 'Dictyosphaeria cavernosa', 
'Dictyota sp.', 'Enteromorpba clatbrata', 'Gelidiella acerosa', 
'Gracilaria coronopifolia', 'Grateloupia filicina', 'Hincksia breviarticulatus', 
'Hincksia mitchellae', 'Hypnea spinella', 'Marginosporum aberrans', 
'Microdictyon nigrescens', 'Monostroma nitidum', 
'non-articulate corallina alga', 'Peyssonnelia conchicola', 'Porphyra crispata', 
'Prionitis ramosissima', 'Ulthrix flaccida', 'Ulva conglobata', 
'Ulva intestinales', 'Ulva lactuca', 'Ulva prolifera', 'Valoniopsis pachynema', 
'Yamadaella cenomyce']

namestr = 'Ulva intestinales'
namestr = 'Enteromorpba clatbrata'
# namestr = 'Prionitis ramosissima'

gbif_resp = GbifAPI.getAcceptedNames(namestr)

fname = 'nmmst_species.txt'
outfname = 'nmmst_name_txkey.csv'

goodnames = []
nameclean = namestr.strip()

otherFilters={'name': nameclean, 'verbose': 'true'}
for namestr in names:
    nameclean = namestr.strip()
    otherFilters={'name': nameclean, 'verbose': 'true'}
    nameAPI = GbifAPI(service=GBIF.SPECIES_SERVICE, key='match', 
                      otherFilters=otherFilters)
    try:
        nameAPI.query()
        output = nameAPI.output
    except Exception, e:
        print ('Failed to get a response for species match on {}, ({})'
               .format(nameclean, str(e)))
        raise
    
    try:
        status = output['status']
    except:
        status = None
        
        
        
        
        
        
        
        
from LmDbServer.tools.partnerData import PartnerQuery

names = []
pq = PartnerQuery()

for line in open(fname, 'r'):
    nm = line.strip()
    names.append(nm)

try:
    alternatives = output['alternatives']
except:
    alternatives = []

if status == 'ACCEPTED':
    smallrec = nameAPI._getFldVals(output)
    goodnames.append(smallrec)
elif status is None and len(alternatives) > 0:
    # get first synonym
    for alt in alternatives:
        if alt['status'] == 'SYNONYM':
            smallrec = nameAPI._getFldVals(alt)
            goodnames.append(smallrec)
            break

# goodnames = GbifAPI.getAcceptedNames(namestr)










    
    

# with > 15 points
taxon_ids = [5150027, 2607722, 8409948, 1452524, 9087097, 5384831, 5357852, 
2572711, 8320154, 5185035, 8668348, 2893047, 2606239, 5261195, 4876021, 2373573, 
3068474, 2073214, 4647752, 3390174, 1683893, 2839327, 2928558, 3787473, 3172140, 
7457116, 1606974, 5784772, 3799619, 2346428, 2383491, 7369164, 9161708, 8275925, 
2430867, 2698174, 3852937, 1794625, 1701438, 1348298, 5664729, 7261706, 3139940, 
2476686, 7938306, 3135773, 3157812, 6991452, 2071815, 1403269, 5671599, 2261138, 
2643478, 1898448, 1338153, 3594155, 5307610, 2679965, 3975944, 2730554, 2926691, 
3191002, 1793178, 2239724, 2403296, 2456438, 4004516, 6971377, 2332833, 8421628, 
7341587, 5276162, 5276162, 2377762, 1670015, 3035999, 4028806, 7832286, 8267371, 
8139949, 5579647, 8937220, 1864949, 2371059, 8677128, 6126345, 2667728, 2657085, 
2367245, 2494063, 1950784, 2967734, 5335053, 2744933, 2889457, 3089185, 5331809, 
7319522, 6464155, 2418972]

unmatched = [5150027, 8409948, 2572711, 2373573, 2073214, 1606974, 8275925, 
1701438, 3157812, 6991452, 1403269, 1898448, 3594155, 3975944, 2456438, 8421628, 
7832286, 8139949, 8677128, 6126345, 1950784, 7319522]

# with > 100 points
taxon_ids = [1028165, 5209419, 7959131, 5390395, 5389255, 5357095, 5359658, 
5356310, 5360432, 5369919, 5358188, 5354646, 5331629, 5289943, 5285991, 5279721, 
5276999, 5261262, 5230689, 5240922, 5229698, 5228537, 5229124, 5229142, 5211635, 
5219881, 5217561, 5212077, 5219910, 5208834, 5127343, 5139954, 5137890, 5110326, 
5102199, 5109545, 5105181, 5112414, 5086821, 4994818, 4989014, 4992875, 4989794, 
4995911, 4988176, 4921818, 6708884, 4755542, 4750014, 4671947, 4642125, 4520807, 
4492211, 4462237, 4435781, 4448375, 4373503, 4363579, 4363697, 4368362, 3999133, 
3996461, 3992999, 3892843, 3888514, 3861748, 3852505, 3827542, 3813593, 3744721, 
3744738, 3745250, 3743196, 3731134, 3721746, 3200179, 3182578, 3176602, 3176484, 
3171240, 3175262, 3175330, 3158107, 3144610, 3136882, 3131105, 3092890, 3082427, 
3632715, 2705962, 9223269, 9130559, 7526196, 9111483, 9107939, 9033066, 8976657, 
8948865, 8914138, 8903148]

unmatched = [1028165, 5390395, 5357095, 5360432, 5369919, 5127343, 5139954, 
5102199, 5109545, 5105181, 4994818, 4995911, 3827542, 3745250, 3743196, 3731134, 
3175262, 2705962, 9223269, 7526196, 9107939, 8903148]


all_unmatched = [1028165, 5390395, 5357095, 5360432, 5369919, 5127343, 5139954, 
5102199, 5109545, 5105181, 4994818, 4995911, 3827542, 3745250, 3743196, 3731134, 
3175262, 2705962, 9223269, 7526196, 9107939, 8903148, 5150027, 8409948, 2572711, 
2373573, 2073214, 1606974, 8275925, 
1701438, 3157812, 6991452, 1403269, 1898448, 3594155, 3975944, 2456438, 8421628, 
7832286, 8139949, 8677128, 6126345, 1950784, 7319522]


idigAPI = IdigbioAPI()
point_output_file = '/state/partition1/lmscratch/temp/point_output_file.csv'
meta_output_file = '/state/partition1/lmscratch/temp/meta_output_file.json'
missing_id_file = '/state/partition1/lmscratch/temp/missing_id_file.csv'

summary = idigAPI.assembleIdigbioData(taxon_ids, point_output_file, 
                                        meta_output_file, 
                                        missing_id_file=missing_id_file)

unmatched_gbif_ids = summary['unmatched_gbif_ids']
for k, v in summary.iteritems():
    print k, v

    
# print('Missing: {}'.format(summary['unmatched_gbif_ids'])

(570242, 399675, 90421, 147019, 73025, 403142, 105648, 251580, 629740, 235469, 702004, 598047, 605296, 525564, 336509, 620128)
             
$PYTHON /opt/lifemapper/LmCompute/tools/common/get_idig_data.py \
/state/partition1/lmscratch/temp/user_taxon_ids_98006.txt \
tmp/user_taxon_ids_98006.csv \
tmp/user_taxon_ids_98006.json \
--missing_id_file=tmp/user_taxon_ids_98006.missing
"""

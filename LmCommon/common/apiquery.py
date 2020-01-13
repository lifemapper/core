"""Module containing functions for API Queries
"""
import idigbio
import json
import os
import requests
from types import (BooleanType, DictionaryType, TupleType, FloatType, IntType, 
                   StringType, UnicodeType, ListType)
import urllib.request, urllib.parse, urllib.error

from LmCommon.common.lmconstants import (BISON, BISON_QUERY, GBIF, ITIS, 
                                         IDIGBIO, IDIGBIO_QUERY, 
                                         URL_ESCAPES, HTTPStatus, DWCNames)
from LmCommon.common.lmXml import fromstring, deserialize
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import (readyFilename, get_unicodecsv_writer, 
                                       get_unicodecsv_reader)

# .............................................................................
class APIQuery(object):
    """
    Class to query APIs and return results.  
    @note: CSV files are created with tab delimiter
    """
    ENCODING = 'utf-8'
    DELIMITER = GBIF.DATA_DUMP_DELIMITER
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
    def addFilters(self, qFilters={}, otherFilters={}):
        """
        @summary: Add new or replace existing filters.  This does not remove 
               existing filters, unless existing keys are sent with new values.
        """
        self.output = None
        for k, v in qFilters.items():
            self._qFilters[k] = v
        for k, v in otherFilters.items():
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
        for k, v in ofDict.items():
            if isinstance(v, BooleanType):
                v = str(v).lower()
            ofDict[k] = str(v).encode('utf-8')         
        filterString = urllib.parse.urlencode(ofDict)
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
                print('Unexpected value type {}'.format(val))
        else:
            print('Unexpected value type {}'.format(val))
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
        for key, val in qDict.items():
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
        except Exception as e:
            try:
                retcode = response.status_code
                reason = response.reason
            except:
                reason = 'Unknown Error'
            print(('Failed on URL {}, code = {}, reason = {} ({})'
                 .format(self.url, retcode, reason, str(e))))
         
        if response.status_code == 200:
            if outputType == 'json':
                try:
                    self.output = response.json()
                except Exception as e:
                    output = response.content
                    self.output = deserialize(fromstring(output))
            elif outputType == 'xml':
                output = response.text
                self.output = deserialize(fromstring(output))
            else:
                print(('Unrecognized output type {}'.format(outputType)))
        else:
            print(('Failed on URL {}, code = {}, reason = {}'
                  .format(self.url, response.status_code, response.reason)))

    # ...........    ....................................
    def queryByPost(self, outputType='json', file=None):
        self.output = None
        # Post a file
        if file is not None:
            files = {'files': open(file, 'rb')}
            try:
                response = requests.post(self.baseurl, files=files)
            except Exception as e:
                try:
                    retcode = response.status_code
                    reason = response.reason
                except:
                    retcode = HTTPStatus.INTERNAL_SERVER_ERROR
                    reason = 'Unknown Error'
                print(("""Failed on URL {}, posting uploaded file {}, code = {}, 
                        reason = {} ({})""".format(self.url, file, retcode, 
                                                   reason, str(e))))
        # Post parameters
        else:
            allParams = self._otherFilters.copy()
            allParams[self._qKey] = self._qFilters
            queryAsString = json.dumps(allParams)
            try:
                response = requests.post(self.baseurl, data=queryAsString,
                                         headers=self.headers)
            except Exception as e:
                try:
                    retcode = response.status_code
                    reason = response.reason
                except:
                    retcode = HTTPStatus.INTERNAL_SERVER_ERROR
                    reason = 'Unknown Error'
                print(('Failed on URL {}, code = {}, reason = {} ({})'.format(
                                  self.url, retcode, reason, str(e))))
        
        if response.ok:
            try:
                if outputType == 'json':
                    try:
                        self.output = response.json()
                    except Exception as e:
                        output = response.content
                        self.output = deserialize(fromstring(output))
                elif outputType == 'xml':
                    output = response.text
                    self.output = deserialize(fromstring(output))
                else:
                    print(('Unrecognized output type {}'.format(outputType)))
            except Exception as e:
                print(('Failed to interpret output of URL {}, content = {}; ({})'
                      .format(self.baseurl, response.content, str(e))))
        else:
            try:
                retcode = response.status_code
                reason = response.reason
            except:
                retcode = HTTPStatus.INTERNAL_SERVER_ERROR
                reason = 'Unknown Error'
            print(('Failed ({}: {}) for baseurl {}'.format(retcode, reason, 
                                                          self.baseurl)))

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
        for key, val in qFilters.items():
            allQFilters[key] = val
           
        # Add/replace other filters to defaults for this instance
        allOtherFilters = BISON_QUERY.FILTERS.copy()
        for key, val in otherFilters.items():
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
                except KeyError as e:
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
            print('Reported count = {}, actual count = {}'.format(dataCount, 
                                                              len(dataList)))
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
        except Exception as e:
            print(str(e))
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
                print(('Missing {} for {}'.format(fieldname, self.url)))
                 
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
            val = str(tmp).encode('utf-8')
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
        except Exception as e:
            print(str(e))
            raise
        return (rankStr, scinameStr, canonicalStr, acceptedKey, acceptedStr, 
                nubKey, taxStatus, kingdomStr, phylumStr, classStr, orderStr, 
                familyStr, genusStr, speciesStr, genusKey, speciesKey, loglines)

    # ...............................................
    def _getTaiwanRow(self, occAPI, taxonKey, canonicalName, rec):
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
            row = [taxonKey, canonicalName, occKey, lonstr, latstr]
        return row
    
    # ...............................................
    @staticmethod
    def getOccurrences(taxonKey, canonicalName, outfname, otherFilters={}, maxpoints=None):
        """
        @summary: Return GBIF occurrences for this GBIF Taxon ID  
        """
        gapi = GbifAPI(service=GBIF.OCCURRENCE_SERVICE, key=GBIF.SEARCH_COMMAND,
                       otherFilters={'taxonKey': taxonKey, 
                                     'limit': GBIF.LIMIT, 
                                     'hasCoordinate': True,
                                     'has_geospatial_issue': False})
        gapi.addFilters(otherFilters)

        offset = 0
        currcount = 0
        lmtotal = 0
        gbiftotal = 0
        complete = False
        try:
            writer, f = get_unicodecsv_writer(outfname, GbifAPI.DELIMITER, doAppend=False)
     
            while not complete and offset <= gbiftotal:
                gapi.addFilters(otherFilters={'offset': offset})
                try:
                    gapi.query()
                except:
                    print('Failed on {}'.format(taxonKey))
                    currcount = 0
                else:
                    # First query, report count
                    if offset == 0:
                        gbiftotal = gapi.output['count']
                        print(("Found {} recs for key {}".format(gbiftotal, taxonKey)))
                        
                    recs = gapi.output['results']
                    currcount = len(recs)
                    lmtotal += currcount
                    # Write header
                    if offset == 0 and currcount > 0:
                        writer.writerow(['taxonKey', 'canonicalName', 'gbifID', 'decimalLongitude', 'decimalLatitude'])
                    # Write recs
                    for rec in recs:
                        row = gapi._getTaiwanRow(gapi, taxonKey, canonicalName, rec)
                        if row:
                            writer.writerow(row)
                    print(("  Retrieved {} records, starting at {}"
                          .format(currcount, offset)))
                    offset += GBIF.LIMIT
                    if maxpoints is not None and lmtotal >= maxpoints:
                        complete = True
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
        except Exception as e:
            print(('Failed to get a response for species match on {}, ({})'
                   .format(nameclean, str(e))))
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
                print(('No exact match on {}, returning top alternative of {}'
                       .format(nameclean, len(alternatives))))
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
                print(('No match or alternatives to return for {}'.format(nameclean)))
                
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
                print(('Failed on URL {} ({})'.format(url, str(e))))
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
                        print(('Failed to interpret output of URL {} ({})'
                            .format(url, str(e))))
            else:

                try:
                    retcode = response.status_code        
                    reason = response.reason
                except:
                    print(('Failed to find failure reason for URL {} ({})'
                        .format(url, str(e))))
                else:
                    print(('Failed on URL {} ({}: {})'
                            .format(url, retcode, reason)))
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
        except Exception as e:
            print(('Failed to get a response from GBIF name parser for data in file {}, ({})'
                   .format(filename, str(e))))
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
                        print(('Failed, err: {}'.format(str(e))))
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
        except Exception as e:
            print(str(e))
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
        for key, val in qFilters.items():
            allQFilters[key] = val
           
        # Add/replace other filters to defaults for this instance
        allOtherFilters = IDIGBIO_QUERY.FILTERS.copy()
        for key, val in otherFilters.items():
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
                for dataFld, dataVal in item[IDIGBIO.RECORD_CONTENT_KEY].items():
                    newitem[dataFld] = dataVal
                for idxFld, idxVal in item[IDIGBIO.RECORD_INDEX_KEY].items():
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
        fldnames = list(rec['indexTerms'].keys())
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
            print('Failed on {}'.format(gbifTaxonId))
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
                print('Failed on {}'.format(gbifTaxonId))
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
                print(("  Retrieved {} records, {} records starting at {}"
                      .format(len(recs), limit, offset)))
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
        print(('Retrieved {} of {} reported records for {}'.format(currcount, total, gbifTaxonId)))
        return currcount, fields

    
    # .............................................................................
    def assembleIdigbioData(self, taxon_ids, point_output_file, meta_output_file, 
                            missing_id_file=None): 
        if not(isinstance(taxon_ids, list)):
            taxon_ids = [taxon_ids]
            
        # Delete old files
        for fname in (point_output_file, meta_output_file):
            if os.path.exists(fname):
                print(('Deleting existing file {} ...'.format(fname)))
                os.remove(fname)
            
        summary = {self.GBIF_MISSING_KEY: []}
        writer, f = get_unicodecsv_writer(point_output_file, self.DELIMITER, 
                                          doAppend=False)
        try:             
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
            f = open(missing_id_file, 'w')
            try: 
                for gid in summary[self.GBIF_MISSING_KEY]:
                    f.write('{}\n'.format(gid))
            except Exception as e:
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
            print(('Point data {} does not exist'.format(ptFname)))
        elif not(os.path.exists(metaFname)):
            print(('Metadata {} does not exist'.format(metaFname)))
        else:
            occParser = OccDataParser(self.log, ptFname, metaFname, 
                                      delimiter=self.DELIMITER,
                                      pullChunks=True)
            occParser.initializeMe()  
            # returns dict with key = taxonid, val = (name, count)
            summary = occParser.readAllChunks()
            for taxid, (name, count) in summary.items():
                gbifid_counts[taxid] = count
        return gbifid_counts



# .............................................................................
def testBison():
      
    tsnList = [['100637', 31], ['100667', 45], ['100674', 24]]
     
    #       tsnList = BisonAPI.getTsnListForBinomials()
    for tsnPair in tsnList:
        tsn = int(tsnPair[0])
        count = int(tsnPair[1])
                
        newQ = {BISON.HIERARCHY_KEY: '*-{}-*'.format(tsn)}
        occAPI = BisonAPI(qFilters=newQ, otherFilters=BISON_QUERY.OCC_FILTERS)
        thisurl = occAPI.url
        occList = occAPI.getTSNOccurrences()
        count = None if not occList else len(occList)
        print('Received {} occurrences for TSN {}'.format(count, tsn))
        
        occAPI2 = BisonAPI.initFromUrl(thisurl)
        occList2 = occAPI2.getTSNOccurrences()
        count = None if not occList2 else len(occList2)
        print('Received {} occurrences from url init'.format(count))
         
        tsnAPI = BisonAPI(qFilters={BISON.HIERARCHY_KEY: '*-{}-'.format(tsn)}, 
                          otherFilters={'rows': 1})
        hier = tsnAPI.getFirstValueFor(BISON.HIERARCHY_KEY)
        name = tsnAPI.getFirstValueFor(BISON.NAME_KEY)
        print(name, hier)

# .............................................................................
def testGbif():
    taxonid = 1000225
    output = GbifAPI.getTaxonomy(taxonid)
    print('GBIF Taxonomy for {} = {}'.format(taxonid, output))

      
# .............................................................................
def testIdigbioTaxonIds():
    infname = '/tank/data/input/idigbio/taxon_ids.txt'
    testcount = 20
    
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
                    print(('Missing data in line {}'.format(line)))
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

# # .............................................................................
# def testGetTaiwanPoints():
#     pth = '/tank/zdata/taiwan/species'
#     basename = 'nmmst_species_data.csv'
#     infname = os.path.join(pth, basename)
#     try:
#         reader, inf = get_unicodecsv_reader(infname, ',')
#         # 0:taxonKey,1:canonicalName,2:id,3:Family,4:providedName,5:lat,6:lon,7:Loc,8:method
#         last_taxon_key = None
#         outf = None
#         header = reader.next()
#         row = reader.next()
#         while row:
#             try:
#                 taxon_key = int(row[0])
#                 canonical = row[1]
#             except:
#                 pass
#             else:
#                 outfname = '{}/gbif_occ_{}.csv'.format(pth, taxon_key)
#                 lat = row[5]
#                 lon = row[6]
#                 lmid = row[2]
#                 newrow = [taxon_key, canonical, lmid, lon, lat]
#             
#                 if taxon_key != last_taxon_key:
#                     try:
#                         outf.close()
#                     except:
#                         pass
#                     # Creates 0:taxonKey, 1:canonicalName, 2: gbifID, 3: decimalLongitude, 4: decimalLatitude'
#                     GbifAPI.getOccurrences(taxon_key, canonical, outfname, maxpoints=300)
#                     writer, outf = get_unicodecsv_writer(outfname, 
#                                         GBIF.DATA_DUMP_DELIMITER, doAppend=True)
#                 writer.writerow(newrow)
#             try:
#                 row = reader.next()
#             except StopIteration, e:
#                 row = None
#             except Exception, e:
#                 raise
#     finally:
#         inf.close()
#         try:
#             outf.close()
#         except:
#             pass
#         

    
# .............................................................................
# .............................................................................
if __name__ == '__main__':
    pass

         
"""
#            'advanced': 1,
#            'geometry': 'POLYGON((119 21,123 21,123 26,119 26,119 21))'}

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
from LmCommon.common.readyfile import readyFilename, get_unicodecsv_reader
from LmCommon.common.apiquery import IdigbioAPI, GbifAPI

pth = '/tank/zdata/taiwan/species'
basename = 'nmmst_species_data.csv'
infname = os.path.join(pth, basename)
reader, inf = get_unicodecsv_reader(infname, ',')
# 0:taxonKey,1:canonicalName,2:id,3:Family,4:providedName,5:lat,6:lon,7:Loc,8:method
last_taxon_key = None
outf = None
header = reader.next()
row = reader.next()


taxon_key = int(row[0])
canonical = row[1]
outfname = '{}/gbif_occ_{}.csv'.format(pth, taxon_key)
lat = row[5]
lon = row[6]
lmid = row[2]
newrow = [taxon_key, canonical, lmid, lon, lat]
    
if taxon_key != last_taxon_key:
    try:
        outf.close()
    except:
        pass
    
    GbifAPI.getOccurrences(taxon_key, outfname, maxpoints=300)
    writer, outf = get_unicodecsv_writer(outfname, 
                        GBIF.DATA_DUMP_DELIMITER, doAppend=True)


writer.writerow(newrow)

row = reader.next()

inf.close()
outf.close()
                                                                    
    
pth = '/tank/zdata/taiwan/species'

tkeys = [5277384, 2653778, 2644877, 8259272, 5278155, 5426431, 2643011, 5276780, 
         2644581, 2663441, 3199042, 5279147, 5729863, 3197958, 5276029, 2659095, 
         2644845, 3200250, 9402904, 2663047, 5278913, 5729905, 2668380, 2656192, 
         2645228, 2667810, 2653483, 10108706, 5273380, 5273394, 5273309, 5273366, 
         7491248, 9233726]

for tk in tkeys:
    outfname = '{}/gbif_occ_{}.csv'.format(pth, tk)
    gapi = GbifAPI.getOccurrences(tk, outfname, 
                                  one_page=True)

# for tk in tkeys:
#     outfname = '{}/gbif_occ_{}.csv'.format(pth, tk)
#     gapi = GbifAPI(service=GBIF.OCCURRENCE_SERVICE, key=GBIF.SEARCH_COMMAND,
#                    otherFilters={'taxonKey': tk, 
#                                  'limit': GBIF.LIMIT, 
#                                  'hasCoordinate': True,
#                                  'has_geospatial_issue': False})






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


Acrocystis nana
Bangia atropurpurea
Boergesenia forbesii
Boodlea composita
Bostrychia tenella
Brachytrichia quoyi
Caulerpa peltata
Caulerpa prolifera
Centroceras clavultum
Chaetomorpha spiralis
Champia parvula
Chnoospora minima
Chondracanthus intermedius
Cladophora herpestica
colpomenia sinuosa
'Corallina pilulifera', 'Dasya sessilis', 'Dictyosphaeria cavernosa', 
'Dictyota sp.', 'Enteromorpba clatbrata', 'Gelidiella acerosa', 
'Gracilaria coronopifolia', 'Grateloupia filicina', 'Hincksia breviarticulatus', 
'Hincksia mitchellae', 'Hypnea spinella', 'Marginosporum aberrans', 
'Microdictyon nigrescens', 'Monostroma nitidum', 
'non-articulate corallina alga', 'Peyssonnelia conchicola', 'Porphyra crispata', 
'Prionitis ramosissima', 'Ulthrix flaccida', 'Ulva conglobata', 
'Ulva intestinales', 'Ulva lactuca', 'Ulva prolifera', 'Valoniopsis pachynema', 
'Yamadaella cenomyce'

"""

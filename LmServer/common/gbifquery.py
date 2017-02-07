"""
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
from httplib import BadStatusLine
import json
from math import ceil
import mx.DateTime as dt
import time
from types import ListType, TupleType, BooleanType
import urllib, urllib2

from LmCommon.common.lmconstants import (GBIF, ONE_MIN, URL_ESCAPES)

from LmServer.base.lmobj import LMError, LMObject, LmHTTPError

# .............................................................................
class GBIFData(LMObject):
   def __init__(self, updateInterval, usr=None, pword=None):
      self.updateInterval = updateInterval
      self._gbifQueryTime = None
      self.signedIn = False
      self._nubUUID = None
      try:
         self._nubUUID = self._getGBIFTaxonomyUUID()
      except Exception, e:
         pass

   # ...............................................
   def isReady(self):
      """
      @note: Never call this when in possession of a lock
      """
      if self._nubUUID is None:
         self._waitForGBIF()
      elif self._gbifQueryTime is not None:
         timeLeft = dt.DateTimeDelta(GBIF.WAIT_TIME - 
                                     (dt.gmt().mjd - self._gbifQueryTime))
         if timeLeft > 0:
            secondsLeft = ceil(timeLeft.seconds)
            self.log.info('Give GBIF a %d second break ...' % secondsLeft)
            time.sleep(secondsLeft)
            
# ...............................................
   def _waitForGBIF(self):
      """
      @note: Never call this when in possession of the lock
      """
      if self.lockOwner:
         self._freeLock()
      self.log.info('Sleeping %d min while waiting for GBIF services ...' %
                    (GBIF.WAIT_TIME/ONE_MIN))
      elapsedTime = 0
      while elapsedTime < GBIF.WAIT_TIME:
         time.sleep(ONE_MIN)
         elapsedTime += ONE_MIN

   # ...............................................
   def _signInToGBIF(self, usr, pword):
      # create a password manager
      if usr is not None or pword is not None:
         passwordMgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
         passwordMgr.add_password('GBIF', GBIF.REST_URL, usr, pword)
         handler = urllib2.HTTPBasicAuthHandler(passwordMgr)
         
         # Create and Install opener - now all calls to urllib2.urlopen use our opener.
         opener = urllib2.build_opener(handler)
         urllib2.install_opener(opener)
         self.signedIn = True
      
# # ...............................................
#    def _giveGbifABreak(self):
#       """
#       @note: Never call this when in possession of the lock
#       """
#       if self._gbifQueryTime is not None:
#          timeLeft = dt.DateTimeDelta(GBIF.WAIT_TIME - 
#                                      (dt.gmt().mjd - self._gbifQueryTime))
#          if timeLeft > 0:
#             secondsLeft = ceil(timeLeft.seconds)
#             self.log.info('Give GBIF a %d second break ...' % secondsLeft)
#             time.sleep(secondsLeft)
            
# ...............................................
   def _getGBIFResults(self, url):
      """
      @note: assumes JSON response
      """
      total = None
      isEnd = True
      results = {}
      try:
         response = urllib2.urlopen(url.decode('utf-8'))
#          response = urllib2.urlopen(url)
      except BadStatusLine, e:
         msg = 'Failed GBIF request with %s (BadStatusLine: %s, reason: %s)' % (url, 
                                                   str(e.line), str(e.message))
      except urllib2.HTTPError, e:
         msg = 'Failed GBIF request with %s (HTTPException: %s, reason: %s)' % (url, 
                                                   str(e.args), str(e.message))
         self.log.error(msg)
         self._nubUUID = None
         raise LmHTTPError(e.code, msg=msg)
      except Exception, e:
         msg = 'Failed GBIF request with %s (HTTPException: %s, reason: %s)' % (url, 
                                                   str(e.args), str(e.message))
         self.log.error(msg)
         self._nubUUID = None
         raise LMError(msg=msg)
      output = response.read()
      try:
         outputDict = json.loads(output)
      except Exception, e:
         raise LMError('Unexpected non-JSON results: %s (%s)' % (str(output), 
                                                                 str(e)))
      if outputDict.has_key(GBIF.RESPONSE_RESULT_KEY):
         results = outputDict[GBIF.RESPONSE_RESULT_KEY]
         isEnd = outputDict[GBIF.RESPONSE_END_KEY]
         if outputDict.has_key(GBIF.RESPONSE_COUNT_KEY):
            total = outputDict[GBIF.RESPONSE_COUNT_KEY]
      else:
         results = outputDict
         if results:
            total = 1
      return total, isEnd, results

# ...............................................
   def _getGBIFDownloadRequestResults(self, taxonKey):
      if not self._signedIn:
         raise LMError('Must be signed in to access secure GBIF services')
      queryParams = GBIF.QUERY_PARAMS[GBIF.OCCURRENCE_SERVICE].copy()
      queryParams[GBIF.REQUEST_TAXON_KEY] = taxonKey
      
      jsonPredicate = self._assembleDownloadPredicate(queryParams)
      url = '%s/%s/%s/%s'% (GBIF.REST_URL, GBIF.OCCURRENCE_SERVICE, 
                         GBIF.DOWNLOAD_COMMAND, GBIF.DOWNLOAD_REQUEST_COMMAND)
      headers = {'Content-Type': 'application/json'}
   
      # POST
      self._gbifQueryTime = dt.gmt().mjd
      try:
         req = urllib2.Request(url, jsonPredicate, headers)
         response = urllib2.urlopen(req)
      except urllib2.HTTPError, e:
         msg = ('Failed GBIF request with url %s; predicate %s (args: %s, reason: %s)' 
                % (url, str(jsonPredicate), str(e.args), str(e.reason)))
         self.log.error(msg)
         self._nubUUID = None
         raise LmHTTPError(e.code, msg=msg)
      except Exception, e:
         raise LMError('Failed GBIF download request with %s (%s)' 
                       % (url, str(e.reason)))
      else:
         downloadKey = response.read()
      return downloadKey
   
# ...............................................
   def _getGBIFTaxonomyUUID(self):
      """
      @raise LmHTTPError: on GBIF service failure
      @raise LMError: on failure to find GBIF Backbone Taxonomy UUID
      """
      url = '%s/%s?%s=%s'% (GBIF.REST_URL, GBIF.DATASET_SERVICE, 
                            GBIF.REQUEST_SIMPLE_QUERY_KEY, 
                            GBIF.DATASET_BACKBONE_VALUE)
      for replaceStr, withStr in URL_ESCAPES:
         url = url.replace(replaceStr, withStr)
      total, isEnd, resultList = self._getGBIFResults(url)
      if total == 1: 
         uuid = resultList[0][GBIF.RESPONSE_IDENTIFIER_KEY]
         return uuid
      else:
         raise LMError('Unable to find dataset %s' % GBIF.DATASET_BACKBONE_VALUE)

# ...............................................
   def _getGBIFSpeciesMatchQuery(self, name, rank='SPECIES'):
      """
      """
      queryParams = GBIF.QUERY_PARAMS[GBIF.SPECIES_SERVICE].copy()
      url = '%s/%s/%s'% (GBIF.REST_URL, GBIF.SPECIES_SERVICE, GBIF.MATCH_COMMAND)
      # Simple taxa query
      try:
         # Equal = unicode(name, "utf-8", 'strict') 
         uname = name.decode('utf-8')
      except Exception, e:
         raise LMError('Failed to convert to unicode with utf-8: %s (%s)' 
                       % (name, str(e)))
      
      queryParams[GBIF.REQUEST_NAME_QUERY_KEY] = uname
      queryParams[GBIF.REQUEST_DATASET_KEY] = self._nubUUID
      queryParams[GBIF.REQUEST_RANK_KEY] = rank.upper()
      
      try:
         newurl = self._assembleUrl(url, queryParams)
      except Exception, e:
         raise LMError('Failed to convert to assembleUrl with utf-8: %s (%s)' 
                       % (newurl, str(e)))
      return newurl

# ...............................................
   def _getGBIFOccurrenceQuery(self, taxonKey, offset=0, limit=1):
      """
      @note: Add 'name' to queryParams
      """
      # Simple taxa query
      if taxonKey is None:
         raise Exception('Must provide taxonKey')
      queryParams = GBIF.QUERY_PARAMS[GBIF.OCCURRENCE_SERVICE].copy()
      queryParams[GBIF.REQUEST_TAXON_KEY] = taxonKey
      queryParams['offset'] = offset 
      queryParams['limit'] = limit
      
      url = '%s/%s/%s'% (GBIF.REST_URL, GBIF.OCCURRENCE_SERVICE, GBIF.SEARCH_COMMAND)
#       filterString = urllib.urlencode(queryParams)
#       if filterString:
#          url += '?%s' % filterString
      url = self._assembleUrl(url, queryParams)
      return url
   
# ...............................................
   def _getGBIFSpeciesGenusKeys(self, canonicalName):
      """
      @raise LmHTTPError: on GBIF service failure
      @raise LMError: on failure to get results
      """
      try:
         canonicalName.decode('ascii')
      except:
         print ('Here is a good one')

      speciesName = genusName = None
      speciesKey = genusKey = msg = None
      exactMatch = False
      # Count GBIF points for Name
      if len(canonicalName.split(' ')) == 1:
         rank = 'GENUS'
      else:
         rank = 'SPECIES'
      try:
         # Get the accepted name in GBIF Backbone Taxonomy
         spurl = self._getGBIFSpeciesMatchQuery(canonicalName, rank)
      except LMError, e:
         raise
      except Exception, e:
         raise LMError('Failed to get results for %s (%s)' % (canonicalName, str(e)))

      try:
         total, isEnd, results = self._getGBIFResults(spurl)
      except LMError, e:
         raise
      except Exception, e:
         raise LMError('Failed to get results for %s (%s)' % (spurl, str(e)))

      else:
         if results is None:
            self.log.info('   Failed to find %s in GBC Backbone' % (canonicalName))
         elif (results.has_key(GBIF.RESPONSE_MATCH_KEY) and
               results[GBIF.RESPONSE_MATCH_KEY].lower() 
               == GBIF.RESPONSE_NOMATCH_VALUE.lower()):
            self.log.info('   Failed to match %s in GBC Backbone' % (canonicalName))
#          elif (not results.has_key(GBIF.RESPONSE_GENUS_ID_KEY):
#             self.log.info('   Failed to find unique genus for %s in GBC Backbone (%s)' 
#                    % (canonicalName, str(results)))
         else:
            if results.has_key(GBIF.RESPONSE_GENUS_ID_KEY):
               genusKey = results[GBIF.RESPONSE_GENUS_ID_KEY]
               genusName = results[GBIF.RESPONSE_GENUS_KEY]
            if results.has_key(GBIF.RESPONSE_SPECIES_ID_KEY):
               speciesKey = results[GBIF.RESPONSE_SPECIES_ID_KEY]
               speciesName = results[GBIF.RESPONSE_SPECIES_KEY]
            try:
               exactMatch = (canonicalName.decode('utf-8') in 
                             (speciesName.decode('utf-8'), 
                              genusName.decode('utf-8')))
            except Exception, e:
               pass
            
            try:
               self.log.info('   Canonical %s; %s %s, %s in GBC Backbone' 
                             % (canonicalName, 'matches' if exactMatch else '',
                                str(genusName), str(speciesName)))
            except:
               self.log.info('   Stupid encoding (logging) errors')
      return [genusKey, genusName, rank == 'GENUS'], [speciesKey, speciesName, 
                rank == 'SPECIES'], exactMatch
      
# ...............................................
   def _getGBIFGeoPointCount(self, taxonKey):
      """
      @raise LMHTTPError: on GBIF service failure
      """
      try:
         ourl = self._getGBIFOccurrenceQuery(taxonKey)
         occTotal, isEnd, results = self._getGBIFResults(ourl)
      except LMError, e:
         raise
      except Exception, e:
         raise LMError('   Failed to count %d in GBC (%s)' % (taxonKey, str(e)))

      return occTotal

# ...............................................
   def _assembleDownloadPredicate(self, queryParams):
      fullPredicate = GBIF.QUERY_PARAMS[GBIF.DOWNLOAD_COMMAND].copy()
      predicates = []
      for key, val in queryParams.iteritems():
         if isinstance(val, ListType) or isinstance(val, TupleType):
            for v in val:
               pred = {"type": "equals", "key": key, "value": v}
         elif key not in ['offset', 'limit']:
            if isinstance(val, BooleanType):
               val = str(val).lower()
            pred = {"type": "equals", "key": key, "value": val}
         predicates.append(pred)
      predicate = {"type": "and",
                   "predicates": predicates}
      fullPredicate["predicate"] = predicate
      jsonPred  = json.dumps(fullPredicate)
      return jsonPred

# ...............................................
   def _assembleUrl(self, url, queryParams):
      for k, v in queryParams.iteritems():
         queryParams[k] = unicode(v).encode('utf-8')
               
      filterString = urllib.urlencode(queryParams)
      
      if filterString:
         url += '?%s' % filterString
         
      return url
   
# ...............................................
   def _requestGBIFUpdate(self, occ):
      """
      Update archive user (for GBIF datasource).  If invalid
      OccurrenceLayer name (not 'accepted usage' in GBIF Backbone Taxonomy), 
      just delete it.
      """
      success = False
      # Should already have lock
      # Should already be simple/canonical name
      simplename = self._simplifyName(occ.displayName)
      self.log.info('Updating existing Occ %d: %s (simplified %s)' 
                    % (occ.getId(), occ.displayName, simplename))
      
      # just in case we have leftover complex name, or redefine 'simple'
      if occ.displayName != simplename:
         # removes associated experiments too
         deleted = self._scribe.completelyRemoveOccurrenceSet(occ)
         success = deleted
      else:
         primaryEnv = self._identifyEnvironment(occ.displayName)
         try:
            # Get GBIF point count and LM occurrencesets matching name
            genusKeyNameOrig, speciesKeyNameOrig, exactMatch = \
                      self._getGBIFSpeciesGenusKeys(occ.displayName)
         except Exception, e:
            self.log.info('  Ignore for now; unable to _getGBIFSpeciesGenusKeys')
         else:
            try:                      
               if not exactMatch:
                  deletedOriginal = self._scribe.completelyRemoveOccurrenceSet(occ)
               else:
                  for key, name, isOrig in (genusKeyNameOrig, speciesKeyNameOrig):
                     origOcc = None
                     if isOrig:
                        origOcc = occ
                     if self._nubUUID is not None:
                        occSet, deletedOriginal = self._createUpdateDeleteName(name, key, 
                                                         primaryEnv, originalOcc=origOcc)            
            except Exception, e:
               if not isinstance(e, LMError):
                  e = LMError(currargs=e.args, lineno=self.getLineno(), 
                              location=self.getLocation())
               raise e


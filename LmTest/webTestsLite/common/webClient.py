"""
@summary: Client library for Lifemapper web services
@author: CJ Grady
@contact: cjgrady [at] ku [dot] edu
@organization: Lifemapper (http://lifemapper.org)
@version: 4.0.0
@status: alpha

@license: Copyright (C) 2017, University of Kansas Center for Research

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
import cookielib
import urllib
import urllib2
from urlparse import urlparse

from LmServer.common.localconstants import PUBLIC_FQDN
from LmWebServer.common.lmconstants import HTTPMethod
from LmServer.legion import algorithm
from LmTest.webTestsLite.api.v2 import occurrence
from mx.Tools.Tools import projection

# .............................................................................
class _SERVICE:
   ENVLAYER = 'envlayer'
   GLOBAL_PAM = 'globalpam'
   GRIDSET = 'gridset'
   HINT = 'hint'
   LAYER = 'layer'
   LOGIN = 'login'
   LOGOUT = 'logout'
   MATRIX = 'matrix'
   MATRIX_COLUMN = 'matrixcolumn'
   OCCURRENCE = 'occurrence'
   OGC = 'ogc'
   SCENARIO = 'scenario'
   SCENARIO_PACKAGE = 'scenpackage'
   SDM_PROJECT = 'sdmproject'
   SHAPEGRID = 'shapegrid'
   SIGNUP = 'signup'
   SNIPPET = 'snippet'
   TREE = 'tree'
   UPLOAD = 'upload'
   
   # ............................
   @staticmethod
   def userServices():
      return [_SERVICE.LOGIN, _SERVICE.LOGOUT, _SERVICE.SIGNUP]

# .............................................................................
class LmWebClient(object):
   """
   @summary: This class provides a light weight web client for accessing 
                Lifemapper services
   """
   # ............................
   def __init__(self, server=PUBLIC_FQDN, urlBase='api', version='v2'):
      self.server = server
      self.urlBase = urlBase
      self.version = version
   
   # ==================
   # = Count Services =
   # ==================
   # ............................
   def count_environmental_layers(self, afterTime=None, altPredCode=None, 
           beforeTime=None, dateCode=None, epsgCode=None, envCode=None, 
           envTypeId=None, gcmCode=None, scenarioId=None, headers=None,
           responseFormat=None):
      """
      @summary: Send a request to the server to count environmental layers
      """
      return self._make_request(self._build_base_url(_SERVICE.ENVLAYER, 
                  objectId='count', responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  altPredCode=altPredCode, beforeTime=beforeTime, 
                  dateCode=dateCode, epsgCode=epsgCode, envCode=envCode, 
                  envTypeId=envTypeId, gcmCode=gcmCode, scenarioId=scenarioId)
   
   # ............................
   def count_gridsets(self, afterTime=None, beforeTime=None, epsgCode=None, 
                      metaString=None, shapegridId=None, headers=None,
                      responseFormat=None):
      """
      @summary: Send a request to the server to count gridsets layers
      """
      return self._make_request(self._build_base_url(_SERVICE.GRIDSET, 
                  objectId='count', responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  metaString=metaString, shapegridId=shapegridId,
                  beforeTime=beforeTime, epsgCode=epsgCode)
   
   # ............................
   def count_layers(self, afterTime=None, beforeTime=None, epsgCode=None,
                          squid=None, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to count layers 
      """
      return self._make_request(self._build_base_url(_SERVICE.LAYER,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  beforeTime=beforeTime, epsgCode=epsgCode, squid=squid)
   
   # ............................
   def count_matrices(self, gridsetId, afterTime=None, altPredCode=None, 
                      beforeTime=None, dateCode=None, epsgCode=None, 
                      gcmCode=None, keyword=None, matrixType=None,  
                      status=None, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to count matrices
      """
      return self._make_request(self._build_base_url(_SERVICE.MATRIX, 
                  objectId='count', responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  altPredCode=altPredCode, beforeTime=beforeTime, 
                  dateCode=dateCode, epsgCode=epsgCode, gridsetId=gridsetId,
                  keyword=keyword, matrixType=matrixType, status=status, 
                  gcmCode=gcmCode)
   
   # ............................
   def count_occurrence_sets(self, afterTime=None, beforeTime=None, 
                  displayName=None, epsgCode=None, minimumNumberOfPoints=None, 
                  status=None, gridSetId=None, headers=None, 
                  responseFormat=None):
      """
      @summary: Send a request to the server to count occurrence sets
      """
      return self._make_request(self._build_base_url(_SERVICE.OCCURRENCE, 
                  objectId='count', responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  beforeTime=beforeTime, displayName=displayName, 
                  epsgCode=epsgCode, gridSetId=gridSetId, status=status, 
                  minimumNumberOfPoints=minimumNumberOfPoints)
   
   # ............................
   def count_scenarios(self, afterTime=None, alternatePredictionCode=None,
                 beforeTime=None, dateCode=None, epsgCode=None, gcmCode=None, 
                 headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to count scenarios
      """
      return self._make_request(self._build_base_url(_SERVICE.SCENARIO, 
                  objectId='count', responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime,
                  alternatePredictionCode=alternatePredictionCode, 
                  beforeTime=beforeTime, dateCode=dateCode, epsgCode=epsgCode,  
                  gcmCode=gcmCode)
   
   # ............................
   def count_scenario_packages(self, afterTime=None, beforeTime=None, 
                               scenarioId=None, headers=None, 
                               responseFormat=None):
      """
      @summary: Send a request to the server to count scenario packages
      """
      return self._make_request(self._build_base_url(_SERVICE.SCENARIO_PACKAGE, 
                  objectId='count', responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  beforeTime=beforeTime, scenarioId=scenarioId)
      
   # ............................
   def count_sdm_projection(self, afterStatus=None, afterTime=None, 
                           algorithmCode=None, beforeStatus=None, 
                           beforeTime=None, displayName=None, epsgCode=None, 
                           modelScenarioCode=None, occurrenceSetId=None,  
                           projectionScenarioCode=None, scenarioId=None, 
                           status=None, gridSetId=None, headers=None, 
                           responseFormat=None):
      """
      @summary: Send a request to the server to get a count of sdm projections
      """
      return self._make_request(self._build_base_url(_SERVICE.SDM_PROJECT, 
                              objectId='count', responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime,
                  scenarioId=scenarioId, beforeTime=beforeTime, 
                  afterStatus=afterStatus, algorithmCode=algorithmCode, 
                  beforeStatus=beforeStatus, displayName=displayName, 
                  epsgCode=epsgCode, modelScenarioCode=modelScenarioCode, 
                  status=status, occurrenceSetId=occurrenceSetId, 
                  projectionScenarioCode=projectionScenarioCode, 
                  gridSetId=gridSetId)
   
   # ............................
   def count_trees(self, name=None, isBinary=None, isUltrametric=None, 
                   hasBranchLengths=None, metaString=None, afterTime=None, 
                   beforeTime=None, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a count of trees
      """
      return self._make_request(self._build_base_url(_SERVICE.SDM_TREE,
                              objectId='count', responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime,
                  name=name, isBinary=isBinary, isUltrametric=isUltrametric,
                  hasBranchLengths=hasBranchLengths, metaString=metaString,
                  beforeTime=beforeTime)
   
   # ================
   # = Get Services =
   # ================
   # ............................
   def get_environmental_layer(self, layerId, headers=None, 
                               responseFormat=None):
      """
      @summary: Send a request to the server to get an environmental layer
      """
      return self._make_request(self._build_base_url(_SERVICE.ENVLAYER, 
                     objectId=layerId, responseFormat=responseFormat), 
                                method=HTTPMethod.GET, headers=headers)
   
   # ............................
   def get_gridset(self, gridsetId, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a grid set
      """
      return self._make_request(self._build_base_url(_SERVICE.GRIDSET, 
                                                objectId=gridsetId, 
                                                responseFormat=responseFormat), 
                                       method=HTTPMethod.GET, headers=headers)
   
   # ............................
   def get_layer(self, layerId, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a layer
      """
      return self._make_request(self._build_base_url(_SERVICE.LAYER, 
                     objectId=layerId, responseFormat=responseFormat), 
                                method=HTTPMethod.GET, headers=headers)
   
   # ............................
   def get_matrix(self, gridsetId, matrixId, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a matrix
      """
      return self._make_request(self._build_base_url(_SERVICE.MATRIX, 
                     objectId=matrixId, parentObjectId=gridsetId, 
                     responseFormat=responseFormat), method=HTTPMethod.GET, 
                                headers=headers)
   
   # ............................
   def get_occurrence_set(self, occId, headers=None, 
                               responseFormat=None):
      """
      @summary: Send a request to the server to get an occurrence set
      """
      return self._make_request(self._build_base_url(_SERVICE.OCCURRENCE, 
                     objectId=occId, responseFormat=responseFormat), 
                                method=HTTPMethod.GET, headers=headers)
   
   # ............................
   def get_scenario(self, scnId, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a scenario
      """
      return self._make_request(self._build_base_url(_SERVICE.SCENARIO, 
                     objectId=scnId, responseFormat=responseFormat), 
                                method=HTTPMethod.GET, headers=headers)
   
   # ............................
   def get_scenario_package(self, scnPkgId, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a scenario package
      """
      return self._make_request(self._build_base_url(_SERVICE.SCENARIO_PACKAGE, 
                           objectId=scnPkgId, responseFormat=responseFormat), 
                                method=HTTPMethod.GET, headers=headers)
   
   # ............................
   def get_sdm_projection(self, prjId, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a sdm projection
      """
      return self._make_request(self._build_base_url(_SERVICE.SDM_PROJECT, 
                           objectId=prjId, responseFormat=responseFormat), 
                                method=HTTPMethod.GET, headers=headers)
   
   # ............................
   def get_tree(self, treeId, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a phylogenetic tree
      """
      return self._make_request(self._build_base_url(_SERVICE.TREE, 
                           objectId=treeId, responseFormat=responseFormat), 
                                method=HTTPMethod.GET, headers=headers)
   
   # =================
   # = List Services =
   # =================
   # ............................
   def list_environmental_layers(self, afterTime=None, altPredCode=None, 
           beforeTime=None, dateCode=None, epsgCode=None, envCode=None, 
           envTypeId=None, gcmCode=None, scenarioId=None, limit=None,
           offset=None, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a list of environmental 
                   layers
      """
      return self._make_request(self._build_base_url(_SERVICE.ENVLAYER,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  altPredCode=altPredCode, beforeTime=beforeTime, 
                  dateCode=dateCode, epsgCode=epsgCode, envCode=envCode, 
                  envTypeId=envTypeId, gcmCode=gcmCode, scenarioId=scenarioId,
                  limit=limit, offset=offset)
   
   # ............................
   def list_gridsets(self, afterTime=None, beforeTime=None, epsgCode=None, 
                     limit=None, metaString=None, offset=None, 
                     shapegridId=None, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a list of gridsets 
      """
      return self._make_request(self._build_base_url(_SERVICE.GRIDSET,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  beforeTime=beforeTime, epsgCode=epsgCode, limit=limit, 
                  metaString=metaString, offset=offset, shapegridId=shapegridId)
   
   # ............................
   def list_layers(self, afterTime=None, beforeTime=None, epsgCode=None,
                   limit=None, offset=None, squid=None, headers=None, 
                   responseFormat=None):
      """
      @summary: Send a request to the server to get a list of layers 
      """
      return self._make_request(self._build_base_url(_SERVICE.LAYER,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  beforeTime=beforeTime, epsgCode=epsgCode, squid=squid,
                  limit=limit, offset=offset)
   
   # ............................
   def list_matrices(self, gridsetId, afterTime=None, altPredCode=None, 
                     beforeTime=None, dateCode=None, epsgCode=None, 
                     gcmCode=None, keyword=None, limit=None, matrixType=None, 
                     offset=None, status=None, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a list of matrices
      """
      return self._make_request(self._build_base_url(_SERVICE.MATRIX,
                      parentObjectId=gridsetId, responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  altPredCode=altPredCode, beforeTime=beforeTime, 
                  dateCode=dateCode, epsgCode=epsgCode, keyword=keyword, 
                  matrixType=matrixType, gcmCode=gcmCode, status=status,
                  limit=limit, offset=offset)
   
   # ............................
   def list_occurrence_sets(self, afterTime=None, beforeTime=None, 
           displayName=None, epsgCode=None, minimumNumberOfPoints=None, 
           limit=None, offset=None, status=None, gridSetId=None,
           fillPoints=None, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a list of occurrence sets
      """
      return self._make_request(self._build_base_url(_SERVICE.OCCURRENCE,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  beforeTime=beforeTime, displayName=displayName,  
                  epsgCode=epsgCode, minimumNumberOfPoints=minimumNumberOfPoints, 
                  status=status, gridSetId=gridSetId, fillPoints=fillPoints,
                  limit=limit, offset=offset)
   
   # ............................
   def list_scenarios(self, afterTime=None, alternatePredictionCode=None,
                 beforeTime=None, dateCode=None, epsgCode=None, gcmCode=None, 
                 limit=100, offset=0, headers=None, responseFormat=None):
      """
      @summary: Send a request to the server to get a list of scenarios
      """
      return self._make_request(self._build_base_url(_SERVICE.SCENARIO,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime, 
                  alternatePredictionCode=alternatePredictionCode, 
                  beforeTime=beforeTime, dateCode=dateCode, epsgCode=epsgCode, 
                  gcmCode=gcmCode, limit=limit, offset=offset)
   
   # ............................
   def list_scenario_packages(self, afterTime=None, beforeTime=None,  
                 limit=100, offset=0, scenarioId=None, headers=None, 
                 responseFormat=None):
      """
      @summary: Send a request to the server to get a list of scenario packages
      """
      return self._make_request(self._build_base_url(_SERVICE.SCENARIO_PACKAGE,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime,
                  scenarioId=scenarioId, beforeTime=beforeTime,  limit=limit, 
                  offset=offset)
   
   # ............................
   def list_sdm_projections(self, afterStatus=None, afterTime=None, 
                           algorithmCode=None, beforeStatus=None, 
                           beforeTime=None, displayName=None, epsgCode=None, 
                           limit=None, modelScenarioCode=None, 
                           occurrenceSetId=None, offset=None, 
                           projectionScenarioCode=None, scenarioId=None, 
                           status=None, gridSetId=None, headers=None, 
                 responseFormat=None):
      """
      @summary: Send a request to the server to get a list of sdm projections
      """
      return self._make_request(self._build_base_url(_SERVICE.SDM_PROJECT,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime,
                  scenarioId=scenarioId, beforeTime=beforeTime, limit=limit, 
                  offset=offset, afterStatus=afterStatus, 
                  algorithmCode=algorithmCode, beforeStatus=beforeStatus,
                  displayName=displayName, epsgCode=epsgCode, 
                  modelScenarioCode=modelScenarioCode, status=status, 
                  occurrenceSetId=occurrenceSetId, 
                  projectionScenarioCode=projectionScenarioCode, 
                  gridSetId=gridSetId)
   
   # ............................
   def list_trees(self, limit=None, offset=None, name=None, isBinary=None, 
                  isUltrametric=None, hasBranchLengths=None, metaString=None, 
                  afterTime=None, beforeTime=None, headers=None, 
                  responseFormat=None):
      """
      @summary: Send a request to the server to get a list of trees
      """
      return self._make_request(self._build_base_url(_SERVICE.SDM_TREE,
                                                responseFormat=responseFormat), 
                  method=HTTPMethod.GET, headers=headers, afterTime=afterTime,
                  name=name, isBinary=isBinary, isUltrametric=isUltrametric,
                  hasBranchLengths=hasBranchLengths, metaString=metaString,
                  beforeTime=beforeTime, limit=limit, offset=offset)
   
   # =================
   # = User Services =
   # =================
   # ............................
   def login(self, userId, passwd):
      """
      @summary: Log into the server
      """
      policyServer = urlparse(self.server).netloc
      policy = cookielib.DefaultCookiePolicy(allowed_domains=(policyServer,))
      self.cookieJar = cookielib.LWPCookieJar(policy=policy)
      opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cookieJar))
      urllib2.install_opener(opener)
      
      req = self._make_request(self._build_base_url(_SERVICE.LOGIN), 
                               username=userId, pword=passwd)
      resp = req.read()
      req.close()
      return resp
      
   # ............................
   def logout(self):
      """
      @summary: Log out of the server
      """
      req = self._make_request(self._build_base_url(_SERVICE.LOGOUT))
      resp = req.read()
      req.close()
      return resp
      
   # ............................
   def query_global_pam(self, algorithmCode=None, bbox=None, gridSetId=None, 
                 modelScenarioCode=None, pointMax=None, pointMin=None, 
                 prjScenCode=None, squid=None, taxonKingdom=None, 
                 taxonPhylum=None, taxonClass=None, taxonOrder=None, 
                 taxonFamily=None, taxonGenus=None, taxonSpecies=None,
                 headers=None):
      """
      @summary: Send a request to the server to get matching PAVs in the global
                   PAM
      """
      return self._make_request(self._build_base_url(_SERVICE.GLOBAL_PAM), 
                  method=HTTPMethod.GET, headers=headers, 
                  algorithmCode=algorithmCode, bbox=bbox, gridSetId=gridSetId, 
                  modelScenarioCode=modelScenarioCode, pointMax=pointMax,
                  pointMin=pointMin, prjScenCode=prjScenCode, squid=squid,
                  taxonKingdom=taxonKingdom, taxonPhylum=taxonPhylum,
                  taxonClass=taxonClass, taxonOrder=taxonOrder,
                  taxonFamily=taxonFamily, taxonGenus=taxonGenus, 
                  taxonSpecies=taxonSpecies)
   
   # ............................
   def _build_base_url(self, service, objectId=None, parentObjectId=None, 
                       responseFormat=None):
      """
      @summary: Build the base url for the service
      """
      url = '{}/{}'.format(self.server, self.urlBase)
      
      if service in _SERVICE.userServices():
         url = '{}/{}'.format(url, service)
      elif service == _SERVICE.MATRIX:
         if objectId is not None:
            url = '{}/{}/{}/{}/{}'.format(url, _SERVICE.GRIDSET, parentObjectId,
                                       _SERVICE.MATRIX, objectId)
         else:
            url = '{}/{}/{}/{}'.format(url, _SERVICE.GRIDSET, parentObjectId,
                                       _SERVICE.MATRIX)
      else:
         url = '{}/{}/{}'.format(url, self.version, service)
         if objectId is not None:
            url = '{}/{}'.format(url, objectId)
         if responseFormat is not None:
            url = '{}/{}'.format(url, responseFormat)
      return url.replace('//', '/')
      
   # ............................
   def _make_request(self, url, method=HTTPMethod.GET, body=None, headers=None,
                     **queryParameters):
      """
      @summary: Makes a request from the server and returns an open file-like
                   object to be handled by the requester
      @param url: The base url to make the request to
      @param method: The HTTP method used to make the request
      @param body: The body of the request if desired
      @param headers: Headers to send with the request
      @param queryParameters: Any additional optional parameters sent to this
                                 function will be wrapped as query parameters
                                 for the request
      """
      qParams = [(k, v) for k, v in dict(
                                 queryParameters).iteritems() if v is not None]
      urlParams = urllib.urlencode(qParams)

      if body is None and len(qParams) > 0 and method.upper() == HTTPMethod.POST:
         body = urlParams
      else:
         url = '{}?{}'.format(url, urlParams)
      
      if headers is None:
         headers = {}
      req = urllib2.Request(url, data=body, headers=headers)
      req.get_method = lambda: method.upper()
      
      return urllib2.urlopen(req)
      
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

# .............................................................................
class _SERVICE:
   ENVLAYER = 'envlayer'
   GLOBAL_PAM = 'globalpam'
   LOGIN = 'login'
   LOGOUT = 'logout'
   SIGNUP = 'signup'
   
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
   def get_environmental_layer(self, layerId, headers=None, 
                               responseFormat=None):
      """
      @summary: Send a request to the server to get an environmental layer
      """
      return self._make_request(self._build_base_url(_SERVICE.ENVLAYER, 
                     objectId=layerId, responseFormat=responseFormat), headers)
   
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
   def _build_base_url(self, service, objectId=None, responseFormat=None):
      """
      @summary: Build the base url for the service
      """
      url = '{}/{}'.format(self.server, self.urlBase)
      
      if service in _SERVICE.userServices():
         url = '{}/{}'.format(url, service)
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
      
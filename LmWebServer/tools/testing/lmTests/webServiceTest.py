"""
@summary: Module containing Lifemapper web test classes
@author: CJ Grady
@version: 1.0
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import urllib2
from urlparse import urlparse

from LmCommon.common.singleton import singleton
from LmCommon.tools.testing.lmTest import LMTestBuilder
from LmWebServer.tools.testing.lmTests.webTest import LMWebTest
from LmWebServer.tools.testing.validators.jsonValidator import JsonValidator
from LmWebServer.tools.testing.validators.xmlValidator import XmlValidator

# .............................................................................
class LMWebServiceTest(LMWebTest):
   """
   @summary: Base class for web tests.  This class can be used to test simple
                web requests that look for an HTTP status code or check the
                returning URL (for rewrite rule tests).
   """
   # ...............................
   def __init__(self, name, description, testLevel, url, method="GET", 
                httpStatus=200, parameters=[], body=None, headers={}, 
                opener=urllib2.urlopen, rewriteUrl=None, testResponse=None):
      """
      @summary: Constructor for web test
      @param name: The name of this test
      @param description: A description of this test
      @param testLevel: What level this test runs at
      @param url: The URL to check in this test
      @param method: The HTTP method to use when making the request
      @param httpStatus: The expected HTTP status of the response
      @param parameters: URL parameters to send with the request
      @param body: The body of the request to send
      @param headers: A dictionary of HTTP headers to send with the request
      @param opener: An opener to use for this request (this allows for 
                        authenticated requests)
      @param rewriteUrl: The expected URL in the response (for rewrite testing)
      @param testResponse: Test response information (deserialized from XML)
      """
      LMWebTest.__init__(self, name, description, testLevel, url, method=method,
                         httpStatus=httpStatus, parameters=parameters,
                         body=body, headers=headers, opener=opener, 
                         rewriteUrl=rewriteUrl)
      self.testResponse = testResponse
      
      
   # ...............................
   def evaluateTest(self):
      """
      @summary: Evaluate the results of the test.  Commands used to run system
                   tests should produce exit values of 0 for success, 1 for 
                   warning, and 2 for error
      """
      validator = None
      
      if self.testResponse.contentType == 'application/json':
         validator = JsonValidator()
      elif self.testResponse.contentType == 'application/xml':
         validator = XmlValidator()
      else:
         raise Exception, "Cannot currently validate: %s" % self.testResponse.contentType
      
      try:
         validator.validate(self.ret.readlines(), self.testResponse.ResponseItem)
         self.status = 0
      except Exception, e:
         self.message = str(e)
         self.status = 2

# .............................................................................
class LMWebServiceTestBuilder(LMTestBuilder):
   """
   @summary: Builder for creating system tests for things like checking 
                directory permissions and memory usage
   """
   name = "webservice"
   # ...........................
   def __init__(self, **kwargs):
      LMTestBuilder.__init__(self, kwargs)
      
      self.env = self.kwargs["env"]
      userId = self.kwargs["userId"]
      pwd = self.kwargs["pwd"]
      
      self.publicOpener, self.authOpener = self.getOpeners(self.env, userId, pwd)
      
   # ...........................
   def getOpeners(self, env, userId, pwd):
      """
      @summary: Get public and authenticated url openers for requests
      @param userId: The user id to log in with
      @param pwd: The user's password
      @return: Public and authenticated openers
      """
      publicOpener = MyOpener(env).getOpener()
      if userId is not None and pwd is not None:
         authOpener = MyOpener(env, userId=userId, pwd=pwd).getOpener()
      
         # Log in the authenticated opener
         url = "%s/login?username=%s&pword=%s" % (
                  env.getReplacementValue('#SERVER#'), userId, pwd)
         
         authOpener(url)
      else:
         authOpener = None
      return publicOpener, authOpener

   # ...........................
   def buildTests(self, testObj):
      """
      @summary: Build a a LMWebServiceTest object
      @param testObj: Deserialized XML with system test information
      """
      # Overall test metadata
      name = testObj.name
      description = testObj.description
      testLevel = int(testObj.testLevel)

      tests = []

      reqs = testObj.Request
      if not isinstance(reqs, list):
         reqs = [reqs]
      for req in reqs:
         try:
            desc = " ---", req.description
         except:
            desc = " --- %s %s" % (req.method, req.url)
         url = req.url
         method = req.method
         creds = req.credentials.lower() == "true"
         if creds:
            opener = self.authOpener
         else:
            opener = self.publicOpener
         
         try:
            rs = req.replaceString
            if not isinstance(rs, list):
               rs = [rs]
         except:
            rs = []
         for replaceString in rs:
            url = url.replace(replaceString, 
                              self.env.getReplacementValue(replaceString))

         parameters = []
         urlParts = url.split('?')
         if len(urlParts) > 1:
            url = urlParts[0]
            qParams = urlParts[1].split('&')
            for qp in qParams:
               parameters.append(tuple(qp.split('=')))

         tests.append(
            LMWebServiceTest(desc, desc, testLevel, url, method,
               parameters=parameters, opener=opener, testResponse=req.Response))
                                       
      return tests   

# .............................................................................
@singleton
class MyOpener(object):
   def __init__(self, env, userId=None, pwd=None):
      if userId is None or pwd is None:
         self.opener = urllib2.urlopen
      else:
         policyServer = urlparse(env.getReplacementValue("#SERVER#")).netloc
         policy = cookielib.DefaultCookiePolicy(allowed_domains=(policyServer,))
         cookieJar = cookielib.LWPCookieJar(policy=policy)
         self.opener = urllib2.build_opener(
                                   urllib2.HTTPCookieProcessor(cookieJar)).open
   def getOpener(self):
      return self.opener
   
      
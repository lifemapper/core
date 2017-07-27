"""
@summary: Module containing Lifemapper web test classes
@author: CJ Grady
@version: 1.0
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import urllib
import urllib2

from LmCommon.tools.testing.lmTest import LMTest, LMTestBuilder

# .............................................................................
class LMWebTest(LMTest):
   """
   @summary: Base class for web tests.  This class can be used to test simple
                web requests that look for an HTTP status code or check the
                returning URL (for rewrite rule tests).
   """
   # ...............................
   def __init__(self, name, description, testLevel, url, method="GET", 
                httpStatus=200, parameters=[], body=None, headers={}, 
                opener=urllib2.urlopen, rewriteUrl=None):
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
      """
      LMTest.__init__(self, name, description, testLevel)
      self.url = url
      self.method = method
      self.httpStatus = httpStatus
      self.parameters=parameters
      self.body = body
      self.headers = headers
      self.opener = opener
      self.rewriteUrl = rewriteUrl
      
   # ...............................
   def _removeNonesFromTupleList(self, paramsList):
      """
      @summary: Removes parameter values that are None
      @param paramsList: List of parameters (name, value) [list of tuples]
      @return: List of parameters that are not None [list of tuples]
      """
      ret = []
      for param in paramsList:
         if param[1] is not None:
            ret.append(param)
      return ret

   # ...............................
   def makeRequest(self):
      """
      @summary: Performs an HTTP request
      @return: Response from the server
      @note: This method is nearly the same as the version in the client library
      """
      url = self.url.replace(" ", "%20").replace(",", "%2C")
      parameters = self._removeNonesFromTupleList(self.parameters)
      urlparams = urllib.urlencode(parameters)
      body = None
         
      if self.body is None and len(parameters) > 0 and self.method.lower() == "post":
         body = urlparams
      else:
         url = "%s?%s" % (url, urlparams)
      req = urllib2.Request(url, data=body, headers=self.headers)
      #req.add_header('User-Agent', self.UA_STRING)
      req.get_method = lambda: self.method.upper()
      self.ret = self.opener(req)

   # ...............................
   def runTest(self):
      """
      @summary: Method called to run the test
      """
      try:
         self.makeRequest()
      except Exception, e:
         self.message = str(e)
         print self.url
         self.status = 2
      
   # ...............................
   def evaluateTest(self):
      """
      @summary: Evaluate the results of the test.  Commands used to run system
                   tests should produce exit values of 0 for success, 1 for 
                   warning, and 2 for error
      """
      if self.status is None or self.status < 2:
         try:
            self.status = 0
            self.message = "%s responded correctly" % self.url
            # Check for rewrite rule
            if self.rewriteUrl is not None:
               if self.ret.url != self.rewriteUrl:
                  self.message = "URL was not rewritten properly, %s != %s" % (
                                                       self.ret.url, self.rewriteUrl)
                  self.status = 2
            
            if self.ret.code != self.httpStatus:
               self.status = 2
               self.message = "%s did not respond with the expected HTTP status (%s != %s)" % (
                                            self.url, self.ret.code, self.httpStatus)
         except Exception, e:
            self.status = 2
            self.message = "Failed to evaluate test, error message: %s" % str(e)

# .............................................................................
class LMWebTestBuilder(LMTestBuilder):
   """
   @summary: Builder for creating system tests for things like checking 
                directory permissions and memory usage
   """
   name = "web"
   # ...........................
   def buildTests(self, testObj):
      """
      @summary: Build a a LMWebTest object
      @param testObj: Deserialized XML with system test information
      """
      
      # Now processing whole file
      name = testObj.name
      description = testObj.description
      testLevel = int(testObj.testLevel)

      tests = []
      
      for t in testObj.webTest:
         try:
            tName = t.name
         except:
            tName = name
         
         try:
            tDescription = t.description
         except:
            tDescription = description

         url = t.url

         try:
            method = t.method
         except:
            method = "GET"
         
         try:
            httpStatus = int(t.httpStatus)
         except:
            httpStatus = 200

         try:
            parameters = t.parameters
            # TODO: Process these
         except:
            parameters = []
            
         try:
            body = t.body
         except:
            body = None
         
         try:
            headers = t.headers
            # TODO: Process these
         except:
            headers = {}
         
         try:
            rewriteUrl = t.rewriteUrl
         except:
            rewriteUrl = None
         
         tests.append(LMWebTest(tName, tDescription, testLevel, url, 
            method=method, httpStatus=httpStatus, parameters=parameters, 
            body=body, headers=headers, rewriteUrl=rewriteUrl))
   
      return tests
   
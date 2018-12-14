"""
@summary: This module provides functions to bypass HTTP when using the client
             library.  This will allow you to step through the code and 
             interact with the back end services without doing so blindly over
             HTTP.
@author: CJ Grady
@version: 1.0
@status: alpha
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
import types
import urllib
import urllib2

from LmClient.lmClientLib import removeNonesFromTupleList

from LmDebug.tools.cherrypyWrapper import createRequest

def installHTTPBypass(cLib, remoteIP='127.0.0.1'):
   """
   @summary: Install a new makeRequest method that does not communicate with 
                the server via HTTP and instead just calls the web service
                code directly (where possible, should use HTTP for external
                servers)
   @param cl: The client library instance to override the make request method
                 for.
   @param remoteIp: The IP address where the requests should appear to come from
   @note: This will only allow access to server code on the local machine
   """
   def makeBypassRequest(self, url, method='GET', parameters=[], body=None,
                         headers={}, objectify=False):
      parameters = removeNonesFromTupleList(parameters)
      urlparams = urllib.urlencode(parameters)
      
      if body is None and len(parameters) > 0 and method.lower() == "post":
         body = urlparams
      else:
         url = "%s?%s" % (url, urlparams)
         
      headers['User-Agent'] = self.UA_STRING + " (HTTP bypass)"

      # Add cookie headers
      
      cookies = []
      for cookie in self.cookieJar:
         if cookie.domain == self.server.strip('http://'):
            cookies.append('%s=%s' % (cookie.name, cookie.value))
      if len(cookies) > 0:
         headers['Cookie'] = '; '.join(cookies)

      ret, req = createRequest(url, method, headers=headers, body=body, 
                             remoteIp=remoteIP)
      
      self.cookieJar.extract_cookies(ret, req)
      
      resp = ''.join(ret.body)
      if objectify:
         return self.objectify(resp)
      else:
         return resp
      
   cLib._cl.makeRequest = types.MethodType(makeBypassRequest, cLib._cl)


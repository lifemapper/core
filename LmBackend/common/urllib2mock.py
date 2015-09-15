"""
@summary: This module contains functions and a urllib2.HTTPHandler subclass 
             that allows URLs to be mapped to local data to prevent external
             requests to URLs that may or may not exist.  This is useful for 
             testing.
@author: CJ Grady
@status: beta
@version: 1.0

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
import httplib
import os
import urllib2

mappedUrls = {}

# .............................................................................
class MockHTTPHandler(urllib2.HTTPHandler):
   """
   @summary: This is a mock handler that maps URLs to local files
   """
   # ...............................
   def http_open(self, req):
      resp = _mockResponse(req)
      if resp is None:
         resp = self.do_open(httplib.HTTPConnection, req)
      return resp

# .............................................................................
def _mockResponse(req):
   """
   @summary: This method is used to return a local file instead of trying to
                pull content via HTTP.  This is used for testing.
   @param req: Request object
   """
   k = req.get_full_url()
   if mappedUrls.has_key(k):
      resp = urllib2.addinfourl(open(mappedUrls[k]), "This is a mock message", 
                                req.get_full_url())
      resp.code = 200
      resp.msg = "OK"
      return resp

# .............................................................................
def addUrlMapping(values, append=True):
   """
   @summary: Add a list of mapped URLs
   @param values: A list of (URL, local path) tuples
   @param append: (optional) Should the existing mapping be appended to or 
                     replaced
   """
   global mappedUrls
   if not append:
      mappedUrls = {}
   
   for url, localPath in values:
      mappedUrls[url] = localPath
      
# .............................................................................
def addUrlMappingFromFile(filename, append=True, basePath=None):
   """
   @summary: Add URL mappings from a file name
   @param filename: The name of the file to get the mappings from
   @param append: (optional) Should the existing mapping be appended to or
                     replaced
   @param basePath: (optional) If this is not None, file paths will be assumed 
                       to be relative to this base path
   """
   values = []
   
   if basePath is None:
      basePath = ''
   
   with open(filename) as f:
      for line in f:
         try:
            url, fPath = line.split(',')
            modPath = os.path.join(basePath, fPath.strip())
            values.append((url.strip(), modPath))
         except:
            pass
   addUrlMapping(values, append=append)

# .............................................................................
def installMockOpener():
   """
   @summary: Installs the mock opener.  This needs to happen before it will work
   """
   mockOpener = urllib2.build_opener(MockHTTPHandler)
   urllib2.install_opener(mockOpener)

   
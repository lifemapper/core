"""
@summary: Module for reporting Lifemapper errors
@author: CJ Grady
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
import hashlib
import os

try:
   import cPickle as pickle
except:
   import pickle
   
import traceback

from LmBackend.notifications.email import EmailNotifier
from LmServer.common.lmconstants import ERROR_LOG_PATH, LOG_PATH
from LmServer.common.localconstants import APP_PATH, TROUBLESHOOTERS

# .............................................................................
def reportError(err, cpRequest, cpResponse):
   """
   @summary: This function should check to see if this error has already been
                reported and, if not, it should write out error information to
                the file system and send out an email to the appropriate 
                parties indicating that a new error has occurred.
   """
   # Check for existance of error directory?
   eDir = os.path.join(APP_PATH, LOG_PATH, ERROR_LOG_PATH)
   if not os.path.exists(eDir):
      os.mkdir(eDir)
   
   # Build something that is hashable
   try:
      tb = err.getTraceback()
   except: # Not an LMError
      tb = None
   
   # Generate file name
   fn = getErrorHash(err, tb)
   fullFn = os.path.join(eDir, "%s.pkl" % fn)
   
   # Look to see if file exists
   if not os.path.exists(fullFn):
      try:
         body = cpRequest.body.read()
      except:
         body = str(cpRequest.body)
      pObj = {
              "Error" : err,
              "Traceback" : tb,
              "Request" : {
                 "URL" : {
                    "base" : cpRequest.base,
                    "queryParams" : cpRequest.query_string
                 },
                 "body" : body,
                 "bodyParams" : cpRequest.body_params,
                 "headers" : cpRequest.headers,
                 "method" : cpRequest.method,
                 "params" : cpRequest.params,
                 "vpath" : cpRequest.path_info,
                 "protocol" : cpRequest.protocol,
                 #"queryStringEncoding" : cpRequest.query_string_encoding
                 "remoteIp" : cpRequest.remote,
                 "scheme" : cpRequest.scheme
              },
              "Response" : {
                 "status" : cpResponse.status,
                 "time" : cpResponse.time,
                 "headers" : cpResponse.headers
              }
             }
      # Write the error information
      with open(fullFn, 'w') as f:
         pickle.dump(pObj, f)
   
      #   Send an email
      msg = """\
      Error: {err}
      Traceback: {tb}
      URL: 
        base: {urlBase}
        vpath: {vpath}
        params: {qParams}
      HTTP Method: {method}
      Remote IP: {remote}
      Headers: {headers}
      Status: {status}
      
      Report name: {reportName}
      File path: {filePath}
      """.format(err=err, tb=tb, urlBase=cpRequest.base, 
                 vpath=cpRequest.path_info, qParams=cpRequest.query_string, 
                 method=cpRequest.method, remote=cpRequest.remote, 
                 headers=cpRequest.headers, status=cpResponse.status,
                 reportName=fn, filePath=fullFn)
      emailer = EmailNotifier()
      emailer.sendMessage(TROUBLESHOOTERS, "A new error occurred", msg)
   
# .............................................................................
def getErrorHash(err, tb):
   """
   @summary: This function gets a hash of an error that will be used as a file 
                name.  This goal of this function is to have the same errors 
                always hash to the same value.  So, avoid specific values in
                the error.  We'll try this with the error type and a trace back.
   @param err: An error object
   @param tb: (optional) The traceback for the error
   """
   h1 = hashlib.md5(str(err.__class__))
   if tb is not None:
      if isinstance(tb, basestring):
         h2 = hashlib.md5(tb)
      else:
         h2 = hashlib.md5(str(traceback.format_tb(tb)))
   else:
      h2 = hashlib.md5('')
   h3 = hashlib.md5(''.join((h1.hexdigest(), h2.hexdigest())))
   return h3.hexdigest()

"""

*********************************************************************
This module has been deprecated and is replaced by web testing suite
*********************************************************************



@summary: This script runs through each of the client schemas defined in the
             "clients" directory and validates each of the requests to check if
             the response is what is expected
@author: CJ Grady
@status: deprecated
"""
import argparse
import cookielib
import glob
import os
import urllib
import urllib2
from urlparse import urlparse

from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.common.singleton import singleton
from LmCommon.common.unicode import toUnicode

from LmServer.common.localconstants import APP_PATH

from LmWebServer.tools.schemaTester.envMethods.server import LmServerEnv
from LmWebServer.tools.schemaTester.validators.jsonValidator import JsonValidator
from LmWebServer.tools.schemaTester.validators.xmlValidator import XmlValidator

CLIENTS_DIR = os.path.join(APP_PATH, 'LmWebServer/tests/config/clients')

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
         self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookieJar)).open
   def getOpener(self):
      return self.opener

# .............................................................................
   
# .............................................................................
def removeNonesFromTupleList(paramsList):
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

# .............................................................................
def makeRequest(url, method="GET", parameters=[], body=None, headers={}, opener=urllib2.urlopen):
   """
   @summary: Performs an HTTP request
   @param url: The url endpoint to make the request to
   @param method: (optional) The HTTP method to use for the request
   @param parameters: (optional) List of url parameters
   @param body: (optional) The payload of the request
   @param headers: (optional) Dictionary of HTTP headers
   @param opener: (optional) A urllib2 opener 
   @return: Response from the server
   @note: This method is nearly the same as the version in the client library
   """
   url = url.replace(" ", "%20").replace(",", "%2C")
   parameters = removeNonesFromTupleList(parameters)
   urlparams = urllib.urlencode(parameters)
      
   if body is None and len(parameters) > 0 and method.lower() == "post":
      body = urlparams
   else:
      url = "%s?%s" % (url, urlparams)
   req = urllib2.Request(url, data=body, headers=headers)
   #req.add_header('User-Agent', self.UA_STRING)
   req.get_method = lambda: method.upper()
   try:
      #ret = urllib2.urlopen(req)
      ret = opener(req)
   except urllib2.HTTPError, e:
      #print e.headers['Error-Message']
      raise e
   except Exception, e:
      raise Exception( 'Error returning from request to %s (%s)' % (url, toUnicode(e)))
   else:
      resp = ''.join(ret.readlines())
      return resp

# .............................................................................
def getOpeners(env, userId, pwd):
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
      url = "%s/login" % env.getReplacementValue('#SERVER#')
      urlParams = [("username", userId), ("pword", pwd)]
      makeRequest(url, parameters=urlParams, opener=authOpener)
   else:
      authOpener = None
   return publicOpener, authOpener
   
# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(
                description="This script tests the Lifemapper service responses return the expected content")
   parser.add_argument("userId", type=str, help="The user to use for credentialed requests")
   parser.add_argument("pwd", type=str, help="The user's password")
   
   args = parser.parse_args()
   
   userId = args.userId
   pwd = args.pwd
   
   env = LmServerEnv(userId=userId)
   # Arguments that we can use
   # user
   # password
   # server (optional)
   # env type
   # client file or directory
   successes = []
   failures = []
   
   
   publicOpener, authOpener = getOpeners(env, userId, pwd)
   # For each client
   for fn in glob.iglob(os.path.join(CLIENTS_DIR, "*")):
      with open(fn, 'r') as inF:
         schemaCnt = inF.read()
      lmSchema = deserialize(fromstring(schemaCnt))
      print "Checking %s, version %s" % (lmSchema.name, lmSchema.version)
      reqs = lmSchema.Request
      if not isinstance(reqs, list):
         reqs = [reqs]
      for req in reqs:
         try:
            desc = " ---", req.description
         except:
            desc = " --- %s %s" % (req.method, req.url)
         print desc
         url = req.url
         method = req.method
         creds = req.credentials.lower() == "true"
         
         try:
            rs = req.replaceString
            if not isinstance(rs, list):
               rs = [rs]
         except:
            rs = []
         for replaceString in rs:
            url = url.replace(replaceString, env.getReplacementValue(replaceString))
         print "URL: ", url
         
         parameters = []
         urlParts = url.split('?')
         if len(urlParts) > 1:
            url = urlParts[0]
            qParams = urlParts[1].split('&')
            for qp in qParams:
               parameters.append(tuple(qp.split('=')))
         
         if creds:
            opener = authOpener
         else:
            opener = publicOpener
         cnt = makeRequest(url, parameters=parameters, method=method, opener=opener)
         if req.Response.contentType == 'application/json':
            validator = JsonValidator()
         elif req.Response.contentType == 'application/xml':
            validator = XmlValidator()
         else:
            raise Exception, "Unknown content type %s" % req.Response.contentType
         try:
            validator.validate(cnt, req.Response.ResponseItem)
            print "Valid"
            successes.append((url, creds, desc))
         except Exception, e:
            print "Invalid"
            failures.append((url, creds, desc, str(e)))

   print "Successes:"
   for s in successes:
      print s[0], s[2]
      
   print
   print
   print "Failures:"
   for f in failures:
      print f[0], f[2], "Reason:", f[3]

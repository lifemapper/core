#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
@summary: This module provides REST services for service objects

@author: CJ Grady
@status: alpha

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
import cherrypy
from logging import handlers, DEBUG
import os
from StringIO import StringIO
from types import FileType

from LmCommon.common.lmconstants import DEFAULT_POST_USER, HTTPStatus, ENCODING
from LmCommon.common.lmconstants import LOGFILE_BACKUP_COUNT, LOGFILE_MAX_BYTES
from LmCommon.common.unicode import fromUnicode, toUnicode

from LmServer.base.lmobj import LmHTTPError, LMError
from LmServer.common.errorReporter import reportError
from LmServer.common.lmconstants import (DbUser, LOG_PATH, SESSION_DIR, 
                                         WEB_DIR)
from LmServer.common.localconstants import (APP_PATH, ARCHIVE_USER, 
                                            WEBSERVICES_ROOT)
from LmServer.common.lmuser import LMUser
from LmServer.common.log import (JobMuleLogger, LmPublicLogger, MapLogger, 
                                 UserLogger)
from LmServer.db.scribe import Scribe
from LmServer.base.utilities import (escapeString, getFileContents,
                                     getUrlParameter)

from LmWebServer.common.lmconstants import (DEFAULT_INTERFACE, 
                                            HTTP_ERRORS,  
                                            STATIC_DIR)
from LmWebServer.common.localconstants import CP_CONFIG_FILE, LM_LIB_PATH
from LmWebServer.formatters.formatterFactory import FormatterFactory
from LmWebServer.services.common.authentication import checkUserLogin
from LmWebServer.services.common.group import LMServiceGroup
from LmWebServer.services.common.jobMule import JobMule
from LmWebServer.services.ogc.sdmMapper import MapConstructor
from LmWebServer.solr.lmSolr import searchArchive, searchHintIndex
          
# .............................................................................
# Constants for CherryPy application
SESSION_KEY = '_cp_username'
REFERER_KEY = 'lm_referer'
SESSION_PATH = os.path.join(LM_LIB_PATH, SESSION_DIR)
STATIC_PATH = os.path.join(APP_PATH, WEB_DIR, STATIC_DIR)

# .............................................................................
class svc(object):
   """
   @summary: Class that exposes services
   """
   # ....................................................
   @cherrypy.expose
   def client(self, *vpath, **params):
       """
       @summary: Client service
       """
       virpath = list(vpath)
       cherrypy.response.headers["Content-Type"] = "application/json"
       return "{'status' : 0, 'message' : 'Up to date'}"
       
   # ....................................................
   @cherrypy.expose
   def hint(self, *vpath, **params):
      """
      @summary: Gets species hint
      @param *vpath: Url path parameters (/hint/{query})
      @param **params: Url named parameters (?param1=val1&param2=val2...)
      """
      try:
         queryType = "species"
         query = ""

         virpath = list(vpath)
         parameters = dict(params)
         parameters = dict([(k.lower(), parameters[k]) for k in parameters.keys()])
      
         if len(virpath) > 0:
            queryType = virpath.pop(0)
         if len(virpath) > 0:
            query = virpath.pop(0)
      
         if query == "":
            query = getUrlParameter("query", parameters)
      
         if query is None or len(query) == 0:
            return errorResponse(LmPublicLogger(), 
                     code=HTTPStatus.BAD_REQUEST, 
                     err="Query string cannot be empty")
      
         query = query.replace("\"", "").replace("'", "")
         query = query.replace("?", "").replace("*", "")

         if queryType == "species":
            frmt = getUrlParameter("format", parameters)
            numCols = getUrlParameter("columns", parameters)
            maxReturned = getUrlParameter("maxreturned", parameters)
            seeAll = getUrlParameter("seeall", parameters)
            
            if maxReturned is None:
               maxReturned = 100
              
            if seeAll is not None:
               if seeAll:
                  maxReturned = 0
         
            if frmt is None:
               frmt = "autocomplete"
            
            if frmt.lower() in ["json", "newjson"]:
               content_type = "application/json"
            else:
               content_type = "text/plain"
            
            if numCols is None:
               numCols = "3"

            cherrypy.response.headers["Content-Type"] = content_type
            return searchHintIndex(query, frmt, numCols, maxReturned).encode(ENCODING)
               
         elif queryType == "archive":
            content_type = "application/xml"
            cherrypy.response.headers["Content-Type"] = content_type
            return searchArchive(query)

      except Exception, e:
         err = LMError(e, doTrace=True)
         return errorResponse(LmPublicLogger(), HTTPStatus.INTERNAL_SERVER_ERROR, err=err)
   
   # ....................................................
   @cherrypy.expose
   def jobs(self, *vpath, **params):
      """
      @summary: End point for computational job interaction
      @param *vpath: Url path parameters (/jobs/{param1}/{param2}/...)
      @param **params: Url named parameters (?param1=val1&param2=val2...)
      """
      #virpath = list(vpath)
      parameters = dict(params)
      parameters = dict([(k.lower(), parameters[k]) for k in parameters.keys()])
      jLog = JobMuleLogger()
      
      if parameters.has_key('request'):
         jm = JobMule()
         request = parameters['request'].lower()
         try:
            ipAddress = cherrypy.request.remote.ip
         except:
            ipAddress = None
         try:
            if request == 'existjobs':
               jobTypes = [int(jt.strip()) for jt in parameters['jobtypes'].split(',')]
               # Remove Nones
               jobTypes = [i for i in jobTypes if i is not None]
               try:
                  users = [u.strip() for u in parameters['users'].split(',')]
               except:
                  users = []
               if 'all' in users:
                  users.remove('all')

               try:
                  threshold = int(parameters['threshold'])
               except:
                  threshold = 1
               ret = str(jm.areJobsAvailable(jobTypes, userIds=users, threshold=threshold))
            elif request == 'getjob':
               jobTypes = [int(jt.strip()) for jt in parameters['jobtypes'].split(',')]
               # Remove Nones
               jobTypes = [i for i in jobTypes if i is not None]
               try:
                  users = [u.strip() for u in parameters['users'].split(',')]
               except:
                  users = []
               
               if 'all' in users:
                  users.remove('all')
               
               # Get number to pull
               try:
                  numToPull = int(parameters['numjobs'])
               except: # not provided
                  numToPull = 1
               
               ret = jm.requestJobs(ipAddress, processTypes=jobTypes, 
                                    count=numToPull, userIds=users)
            elif request == 'postjob':
               jobType = int(parameters['jobtype'])
               jobId = int(parameters['jobid'])
               component = parameters['component'].lower()
               bodyRaw = cherrypy.request.body
               try: # Cherrypy 3.1
                  content = ''.join(bodyRaw.readlines())
               except:
                  try: # CherryPy 3.2
                     content = bodyRaw.fullvalue()
                  except:
                     content = None
               try:
                  contentType = cherrypy.request.headers["Content-Type"]
               except:
                  contentType = None
               ret = str(jm.postJob(jobType, jobId, content, component, contentType=contentType))
            elif request == 'requestpost':
               jobType = int(parameters['jobtype'])
               jobId = int(parameters['jobid'])
               component = parameters['component']
               ret = jm.requestPost(jobType, jobId, component)
            elif request == 'updatejob':
               jobType = int(parameters['jobtype'])
               jobId = int(parameters['jobid'])
               status = int(parameters['status']) if parameters.has_key('status') else None
               progress = int(parameters['progress']) if parameters.has_key('progress') else None
               ret = str(jm.updateJob(jobType, jobId, status, 
                                     computeIP=ipAddress, progress=progress))
            else:
               ret = errorResponse(jLog, HTTPStatus.BAD_REQUEST, msg="Unrecognized request: %s" % request)
         except LmHTTPError, lme:
            if lme.code == HTTPStatus.SERVICE_UNAVAILABLE:
               ret = errorResponse(jLog, HTTPStatus.SERVICE_UNAVAILABLE, msg="No jobs match request", err=lme)
            else:
               raise lme
         except KeyError, ke:
            err = LMError(ke, doTrace=True)
            jLog.error("The required parameter: %s was missing for the %s request" % (ke.message, request))
            ret = errorResponse(jLog, HTTPStatus.BAD_REQUEST, msg="The required parameter: %s was missing for the %s request" % (ke.message, request), err=err)
         finally:
            try:
               jm.close()
            except:
               pass
      else:
         ret = errorResponse(jLog, HTTPStatus.BAD_REQUEST, msg="Must provide a request")
      return ret
         

   # ....................................................
   @cherrypy.expose
   def login(self, *vpath, **params):
      """
      @summary: Attempts to log in a user
      @param *vpath: Url path parameters
                        (/login/{param1}/{param2}/...)
      @param **params: Url named parameters (?param1=val1&param2=val2...) 
      """
      virpath = list(vpath)
      parameters = dict(params)
      
      refererPage = WEBSERVICES_ROOT
      #refererCookie = None
      try:
         cookie = cherrypy.request.cookie
         if cookie.has_key(REFERER_KEY):
            refererPage = cookie[REFERER_KEY].value
         else:
            refererPage = cherrypy.request.headers["referer"]
            cookie = cherrypy.response.cookie
            cookie[REFERER_KEY] = refererPage
            cookie[REFERER_KEY]['path'] = '/login'
            cookie[REFERER_KEY]['max-age'] = 30
            cookie[REFERER_KEY]['version'] = 1
      except Exception, e:
         pass
      
      username = getUrlParameter("username", parameters)
      pword = getUrlParameter("pword", parameters)
      sessionUser = getUserName()
      
      if sessionUser != ARCHIVE_USER and sessionUser != DEFAULT_POST_USER:
         # Already logged in
         raise cherrypy.HTTPRedirect("/")
      elif username is None or pword is None:
         # Not logged in and not enough info to log in
         log = LmPublicLogger()
         try:
            if len(virpath) == 0:
               virpath.append("formLogin.shtml")
      
            try:
               retFile = getFileContents(os.path.join(STATIC_PATH, *virpath))
            except Exception, e:
               err = LMError(e, doTrace=True)
               return errorResponse(log, HTTPStatus.NOT_FOUND, url='/'.join((virpath)), err=err)
      
            #fileName = ""
            return globalWebsite("Lifemapper", retFile)
         except Exception, e:
            err = LMError(e, doTrace=True)
            return errorResponse(log, HTTPStatus.INTERNAL_SERVER_ERROR, err=err)
      elif checkUserLogin(username, pword):
         #log.debug("User: %s" % str(username))
         cherrypy.session.regenerate()
         cherrypy.session[SESSION_KEY] = cherrypy.request.login = username
         cookie = cherrypy.response.cookie
         cookie[REFERER_KEY] = refererPage
         cookie[REFERER_KEY]['expires'] = 0
         raise cherrypy.HTTPRedirect(refererPage or "/")
      else:
         # Failed login
         log = LmPublicLogger()
         log.debug("Failed login for user: %s" % (str(username)))
         try:
            retFile = getFileContents(os.path.join(STATIC_PATH, "failedLogin.shtml"))
      
            return errorResponse(log, HTTPStatus.UNAUTHORIZED)
         except Exception, e:
            err = LMError(e, doTrace=True)
            return errorResponse(log, HTTPStatus.INTERNAL_SERVER_ERROR, err=err)
   
   # ....................................................
   @cherrypy.expose
   def logout(self, *vpath, **params):
      """
      @summary: Attempts to log out a user
      @param *vpath: Url path parameters
                        (/logout/{param1}/{param2}/...)
      @param **params: Url named parameters (?param1=val1&param2=val2...) 
      """
      virpath = list(vpath)
      parameters = dict(params)

      cherrypy.lib.sessions.expire()
      cherrypy.session[SESSION_KEY] = cherrypy.request.login = None

      raise cherrypy.HTTPRedirect("/login")
   
   # ....................................................
   @cherrypy.expose
   def ogc(self, *vpath, **params):
      """
      @summary: Lifemapper ogc services
      @param *vpath: Url path parameters (/ogc/{param1}/{param2}/...)
      @param **params: Url named parameters (?param1=val1&param2=val2...) 
      """
      logger = MapLogger(isDev=True)
      ogcsvr = MapConstructor(logger)
      sessionUser = getUserName()
      
      virpath = list(vpath)
      logger.debug('URL path parameters: %s' % str(virpath))
      logger.debug('params: %s' % str(params))
      parameters = dict(params)
      logger.debug('URL named parameters: %s' % str(parameters))
      
      try:
         ogcsvr.assembleMap(parameters, sessionUser=sessionUser)
         content_type, content = ogcsvr.returnResponse()
      except LmHTTPError, e:
         if e.code == HTTPStatus.FORBIDDEN:
            return errorResponse(logger, HTTPStatus.FORBIDDEN, err=e)
         elif e.code == HTTPStatus.BAD_REQUEST:
            return errorResponse(logger, HTTPStatus.BAD_REQUEST, msg="A required parameter was missing", err=e)
      except Exception, e:
         logger.debug('\n<br />'.join(("Parameters:", str(parameters))))
         err = LMError(e, doTrace=True)
         return errorResponse(logger, HTTPStatus.INTERNAL_SERVER_ERROR, err=err)
      
      logger = None
      
      
      cherrypy.response.headers["Content-Type"] = content_type
      return content

   # ....................................................
   @cherrypy.expose
   def services(self, *vpath, **params):
      """
      @summary: Accesses Lifemapper services
      @param *vpath: Url path parameters
                        (/services/{param1}/{param2}/...)
      @param **params: Url named parameters (?param1=val1&param2=val2...) 
      """
      virpath = list(vpath)
      parameters = dict(params)
      sessionUser = getUserName()
      uLog = getUserLog(sessionUser)
      method = cherrypy.request.method
      bodyRaw = cherrypy.request.body
      try: # CherryPy 3.8 
         body =   cherrypy.request.params['request'].fullvalue().replace('\r\n', '\n').replace('\r', '\n')
      except:
         try: # CherryPy 3.1
            body = ''.join(bodyRaw.readlines())
         except:
            try: # CherryPy 3.2
               body = bodyRaw.fullvalue()
            except:
               body = None
      respFormat = DEFAULT_INTERFACE
      if len(virpath) > 0:
         respFormat = virpath[-1]
      parameters["url"] = "%s/services/%s/" % (WEBSERVICES_ROOT, '/'.join(virpath))
      
      try:
         conn = Scribe(uLog, dbUser=DbUser.WebService)
         conn.openConnections()
         try:
            ipAddress = cherrypy.request.remote.ip
         except:
            ipAddress = None
         #uLog.info("Remote IP: '%s'" % ipAddress)

         s = LMServiceGroup(method, conn, userId=sessionUser, body=body,
                            vpath=virpath, parameters=parameters,
                            basePath="%s/services" % WEBSERVICES_ROOT,
                            ipAddress=ipAddress)
         ret = s.doAction()
         conn.closeConnections()
         
         # Check to see if map service
         if isinstance(ret, tuple):
            contentType, content = ret
            cherrypy.response.headers["Content-Type"] = contentType
            return content
         else:
            ff = FormatterFactory(ret, format=respFormat, parameters=parameters)
            fmtr = ff.doGetFormatter()
            fResp = fmtr.format()
            cherrypy.response.headers['Content-Type'] = fResp.contentType
            for h in fResp.headers.keys():
               cherrypy.response.headers[h] = fResp.headers[h]
            cherrypy.response.status = getHttpStatus(ret, method)
            #if isgenerator(fResp.resp):
            if isinstance(fResp.resp, cherrypy.lib.file_generator):
               cherrypy.response.stream = True
               return fResp.resp
            if isinstance(fResp.resp, (FileType, StringIO)):
               return fResp.resp
            try:
               return fResp.resp.respond().encode(ENCODING)
            except:
               return str(fResp)
      except LmHTTPError, e:
         return errorResponse(uLog, e.code, msg=e.msg, err=e, url=parameters["url"])
      except Exception, e:
         uLog.debug(str(e))
         err = LMError(e, doTrace=True)
         return errorResponse(uLog, HTTPStatus.INTERNAL_SERVER_ERROR, err=err)

   # ....................................................
   @cherrypy.expose
   def signup(self, *vpath, **params):
      """
      @summary: Attempts to sign up a user
      @param *vpath: Url path parameters (/signup/{param1}/{param2}/...)
      @param **params: Url named parameters (?param1=val1&param2=val2...)
      """
      log = LmPublicLogger()
      try:
         virpath = list(vpath)
         parameters = dict(params)
         parameters = dict([(k.lower(), parameters[k]) for k in parameters.keys()])
         
         userId = getUrlParameter("userId", parameters)
         email = getUrlParameter("email", parameters)
         fName = getUrlParameter("firstName", parameters)
         lName = getUrlParameter("lastName", parameters)
         institution = getUrlParameter("institution", parameters)
         add1 = getUrlParameter("address1", parameters)
         add2 = getUrlParameter("address2", parameters)
         add3 = getUrlParameter("address3", parameters)
         phone = getUrlParameter("phone", parameters)
         pword = getUrlParameter("pword1", parameters)
         
         if userId is None or email is None or fName is None or pword is None:
            try:
               virpath.append("formSignUp.shtml")
               try:
                  retFile = getFileContents(os.path.join(STATIC_PATH, *virpath))
               except Exception, e:
                  err = LMError(e, doTrace=True)
                  return errorResponse(log, HTTPStatus.NOT_FOUND, url='/'.join(virpath), err=err)
         
               return globalWebsite("Sign up for a new Lifemapper account",
                                    retFile)
            except Exception, e:
               err = LMError(e, doTrace=True)
               return errorResponse(log, HTTPStatus.INTERNAL_SERVER_ERROR, err=err)
         else:
            #TODO: Add a verification method or something
            #TODO: Add constants for length
            # Check inputs
            if len(userId) > 20:
               return errorResponse(log, HTTPStatus.BAD_REQUEST, 
                           msg="User ID is too long (max length: 20)", 
                           err=LMError("User ID is too long (max length: 20)"))
            if len(fName) > 50:
               return errorResponse(log, HTTPStatus.BAD_REQUEST, 
                           msg="First name is too long (max length: 50)", 
                           err=LMError("First name is too long (max length: 50)"))
            if lName is not None and len(lName) > 50:
               return errorResponse(log, HTTPStatus.BAD_REQUEST, 
                           msg="Last name is too long (max length: 50)", 
                           err=LMError("Last name is too long (max length: 50)"))
            if phone is not None and len(phone) > 20:
               return errorResponse(log, HTTPStatus.BAD_REQUEST, 
                           msg="Phone is too long (max length: 20)", 
                           err=LMError("Phone is too long (max length: 20)"))
            if len(email) > 64:
               return errorResponse(log, HTTPStatus.BAD_REQUEST, 
                           msg="Email is too long (max length: 64)", 
                           err=LMError("Email is too long (max length: 64)"))
            if len(pword) > 32:
               return errorResponse(log, HTTPStatus.BAD_REQUEST, 
                           msg="Password is too long (max length: 32)", 
                           err=LMError("Password is too long (max length: 32)"))
            
            scribe = Scribe(log, dbUser=DbUser.WebService)
            scribe.openConnections()
            checkUser = scribe.findUser(userId, email)
            
            if checkUser is None:
               usr = LMUser(userId, email, pword, firstName=fName, lastName=lName,
                            institution=institution, addr1=add1, addr2=add2, 
                            addr3=add3, phone=phone)
               
               try:
                  _ = scribe.insertUser(usr)
               except Exception, e:
                  err = LMError(e, doTrace=True)
                  return errorResponse(log, HTTPStatus.INTERNAL_SERVER_ERROR, err=err)
                  
               scribe.closeConnections()
               
               cherrypy.session[SESSION_KEY] = cherrypy.request.login = userId
               
               welcomeMsg = """
                  <span class="signupWelcome">
                     Welcome to Lifemapper, {fName}!
                  </span>
                  Your user id is: <span class="signupData">{userId}</span> 
                  and your password is: <span class="signupData">{pword}</span>.
                  <br /><br />
                  If you forget your password, you will need to contact us at 
                  <span class="signupData">[lifemapper at ku dot edu]</span> to 
                  have it reset.<br />
                  <br />
                  Now that you have created a user, any jobs you submit or 
                  occurrence sets you upload, while logged in, will belong to 
                  you.  If you navigate to <a href="/services/">
                  {website}/services/</a> all of the services 
                  listed will return your data.<br />
                  <br />
                  Welcome again to Lifemapper!<br />
                  <br />
               """.format(fName=fromUnicode(toUnicode(fName)), 
                          userId=fromUnicode(toUnicode(userId)), 
                           pword=fromUnicode(toUnicode(pword)), 
                           website=fromUnicode(toUnicode(WEBSERVICES_ROOT)))
               return globalWebsite(
                   fromUnicode(toUnicode(
                        "Welcome {fName}!".format(
                              fName=fromUnicode(toUnicode(fName))
                        ))),
                   welcomeMsg)
            else:
               scribe.closeConnections()
               return errorResponse(log, HTTPStatus.CONFLICT, msg="Duplicate user credentials", err=LMError("Duplicate user credentials"))
      except Exception, e:
         err = LMError(e, doTrace=True)
         return errorResponse(log, HTTPStatus.INTERNAL_SERVER_ERROR, err=err)
   
   # ....................................................
   @cherrypy.expose
   def spLinks(self, *vpath, **params):
      """
      @summary: Produces links to the species page for each occurrence set
      @param *vpath: Url path parameters (/spLinks/{param1}/{param2}/...)
      @param **params: Url named parameters (?param1=val1&param2=val2...)
      """
      log = LmPublicLogger()
      try:
         virpath = list(vpath)
         parameters = dict(params)
         parameters = dict([(k.lower(), parameters[k]) for k in parameters.keys()])
         
         page = int(virpath[0]) if len(virpath) > 0 else 1
         if page <= 0:
            page = 1
         perPage = getUrlParameter("perPage", parameters)
         if perPage is None:
            perPage = 1000
         else:
            perPage = int(perPage)
         
         links = []
         peruser = Scribe(log)
         peruser.openConnections()
         occs = peruser.listOccurrenceSets((page-1)*perPage, perPage, 
                                              minOccurrenceCount=1, epsg=4326)
         count = peruser.countOccurrenceSets(minOccurrenceCount=1, epsg=4326)
         peruser.closeConnections()
         for occ in occs:
            try:
               name = escapeString(occ.title, 'xml')
               links.append(
                  "<a href='/species/{escName}'>{name}</a><br />".format(
                        escName=name.replace(' ', '%20'), name=name))
            except:
               pass
   
         if page > 1:
            links.append("<a href='/spLinks/{prvPage}?perPage={perPage}'>Previous page</a>".format(prvPage=(page-1), perPage=perPage))
         if page*perPage < count:
            links.append("<a href='/spLinks/{nxtPage}?perPage={perPage}'>Next page</a>".format(nxtPage=(page+1), perPage=perPage))
         cnt = '\n'.join(links)
   
   
         return globalWebsite("Lifemapper Species Page Links", cnt)
   
      except Exception, e:
         err = LMError(e, doTrace=True)
         return errorResponse(log, HTTPStatus.INTERNAL_SERVER_ERROR, err=err)

# .............................................................................
def getHttpStatus(obj, method):
   """
   @summary: Returns the HTTP status code for the resquest
   @param obj: The object that has been formatted
   @param method: The HTTP method used for the request
   """
   status = None
   if method.lower() == "post":
      try:
         status = obj.status
      except:
         try:
            status = obj.model.status
         except:
            pass
      if status is not None:
         return HTTPStatus.ACCEPTED
      else:
         return HTTPStatus.CREATED
   else:
      return HTTPStatus.OK
   

# .............................................................................
def getUserName():
   """
   @summary: Gets the username specifeid by the session cookie
   """
   log = LmPublicLogger()
   if cherrypy.request.method.lower() == "get":
      user = ARCHIVE_USER
   else:
      user = DEFAULT_POST_USER
   temp = None
   
   try:
      if os.path.isfile(os.path.join(SESSION_DIR, 'session-%s' % cherrypy.session.id)):
         temp = cherrypy.session.get(SESSION_KEY)
   except Exception, e:
      log.debug(' '.join(("Exception in getUserName:", str(e))))
   
   if temp is not None:
      user = temp
      
   return user

# .............................................................................
def isUser(name):
   """
   @summary: Checks to see if the service name is a user name
   @note: This allows for backwards compatibility with old urls
   """
   peruser = Scribe(LmPublicLogger(), dbUser=DbUser.WebService)
   peruser.openConnections()
   users = peruser.getUsers()
   peruser.close()
   unames = [u.userid for u in users]
   if name in unames:
      return True
   else:
      return False
   
# .............................................................................
def buildUrlParamsFromDict(parameters):
   """
   @summary: Creates a url parameter string from a parameter dictionary
   @param parameters: Dictionary of parameters
   """
   return ''.join(("?", '&'.join(
                   (['='.join(
                     (key, value)) for (key, value) in parameters.iteritems()]
                   ))))

# .............................................................................
def errorResponse(log, code, msg=None, err=None, url=None):
   """
   @summary: Returns an HTTP error page with the correct status code
   @param code: The HTTP status code of the response
   @param msg: (optional) A message for the user
   @param err: (optional) An error to be logged
   """
   log.debug("Catching error")
   if url is not None:
      log.error("Url: %s" % url)
   
   if msg is not None:
      log.debug(msg)
   
   if err is not None:
      log.debug(str(err))
      if not isinstance(err, LMError):
         err = LMError(err)

      log.error(err.getTraceback())

   msg = msg if msg is not None else HTTP_ERRORS[code]['msg']
   #log.debug(msg)
   cherrypy.response.status = code
   cherrypy.response.headers['Error-Message'] = msg

   if code == HTTPStatus.INTERNAL_SERVER_ERROR:
      reportError(err, cherrypy.request, cherrypy.response)

   return globalWebsite(HTTP_ERRORS[code]['title'], HTTP_ERRORS[code]['msg'])

# .............................................................................
def globalWebsite(title, body):
   """
   @summary: This method wraps the response so that it looks like the rest of
                the website.  It is now here so that we can get rid of the 
                Cheetah template that did the same thing
   @param title: The title of the web page
   @param body: The body of the page
   @todo: Work towards removing this in favor of just using the main website for these pages
   """
   return toUnicode("""\
<html xmlns="http://www.w3.org/1999/xhtml">
   <head>
      <title>{title}</title>            
   </head>   
   <body>
{body}
   </body>     
</html>
""").format(title=toUnicode(title), body=toUnicode(body))

# .............................................................................
def getUserLog(userId):
   """
   @summary: Gets a logger for the specified user.  Returns the regular 
                LmPublicLogger if the user is the archive (public) user
   """
   if userId is not None and userId not in [ARCHIVE_USER, DEFAULT_POST_USER]:
      return UserLogger(userId)
   else:
      return LmPublicLogger()

# .............................................................................
def customLogs(app):
   """
   @summary: This function sets up custom loggers for Cherrypy.  This allows us
                to have the logs rotate and control the default logging level
   """
   log = app.log
   
   log.error_file = ""
   log.access_file = ""
   
   efname = os.path.join(LOG_PATH, "cherrypyErrors.log")
   
   h = handlers.RotatingFileHandler(efname, 'a', LOGFILE_MAX_BYTES, 
                                    LOGFILE_BACKUP_COUNT)
   h.setLevel(DEBUG)
   h.setFormatter(cherrypy._cplogging.logfmt)
   log.error_log.addHandler(h)

   afname = os.path.join(LOG_PATH, "cherrypyAccess.log")
   h = handlers.RotatingFileHandler(afname, 'a', LOGFILE_MAX_BYTES,
                                    LOGFILE_BACKUP_COUNT)
   h.setLevel(DEBUG)
   h.setFormatter(cherrypy._cplogging.logfmt)
   log.access_log.addHandler(h)
   


# .............................................................................
def CORS():
   """
   @summary: Function to be called before processing a request.  This will add
                response headers required for CORS (Cross-Origin Resource 
                Sharing) requests.  This is needed for browsers running 
                JavaScript code from a different domain. 
   """
   cherrypy.response.headers["Access-Control-Allow-Origin"] = "*"
   cherrypy.response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
   cherrypy.response.headers["Access-Control-Allow-Headers"] = "*"
   cherrypy.response.headers["Access-Control-Allow-Credentials"] = "true"
   if cherrypy.request.method.lower() == 'options':
      cherrypy.response.headers['Content-Type'] = 'text/plain'
      return 'OK'

# Tell CherryPy to add headers needed for CORS
cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)

cherrypy.config.update(CP_CONFIG_FILE)
application = cherrypy.Application(svc(), script_name=None, config=CP_CONFIG_FILE)

customLogs(application)

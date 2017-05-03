#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
@deprecated: This is replaced by svcRoot.py.  It will be deleted once 
                everything is confirmed to be moved over

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
from LmServer.common.lmconstants import (DbUser, LOG_PATH, CHERRYPY_CONFIG_FILE)
from LmServer.common.localconstants import (PUBLIC_USER, WEBSERVICES_ROOT)
from LmServer.common.lmuser import LMUser
from LmServer.common.log import (JobMuleLogger, LmPublicLogger, MapLogger, 
                                 UserLogger)
from LmServer.db.scribe import Scribe
from LmServer.base.utilities import (escapeString, getFileContents,
                                     getUrlParameter)

from LmWebServer.common.lmconstants import (DEFAULT_INTERFACE, HTTP_ERRORS, 
                                            SESSION_PATH, STATIC_PATH)
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

# .............................................................................
class svc(object):
   """
   @summary: Class that exposes services
   """
       
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
def getUserLog(userId):
   """
   @summary: Gets a logger for the specified user.  Returns the regular 
                LmPublicLogger if the user is the archive (public) user
   """
   if userId is not None and userId not in [PUBLIC_USER, DEFAULT_POST_USER]:
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

cherrypy.config.update(CHERRYPY_CONFIG_FILE)
application = cherrypy.Application(svc(), script_name=None, config=CHERRYPY_CONFIG_FILE)

customLogs(application)

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

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import DEFAULT_POST_USER, HTTPStatus
from LmCommon.common.lmconstants import LOGFILE_BACKUP_COUNT, LOGFILE_MAX_BYTES

from LmServer.common.lmconstants import (LOG_PATH, CHERRYPY_CONFIG_FILE)
from LmServer.common.localconstants import (PUBLIC_USER)
from LmServer.common.log import (LmPublicLogger, UserLogger)
from LmServer.base.utilities import (escapeString, getUrlParameter)

          
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

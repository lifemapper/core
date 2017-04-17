"""
@summary: This module is for basic authentication for Lifemapper services.
@note: We will probably want to switch our authentication mechansim, at least
          for Lifemapper proper.  We may want to keep basic authentication for
          instances though, thus the name of this module is 'basicAuth'
@author: CJ Grady
@version: 2.0
@status: alpha

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
import cherrypy
import os

from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import LmPublicLogger, UserLogger
from LmWebServer.common.lmconstants import SESSION_KEY, SESSION_PATH

# .............................................................................
def getUserName():
   """
   @summary: Attempt to get the session user name
   """
   user = PUBLIC_USER
   log = LmPublicLogger()
   
   try:
      if os.path.isfile(os.path.join(SESSION_PATH, 
                                    'session-{}'.format(cherrypy.session.id))):
         user = cherrypy.session.get(SESSION_KEY)
         log = UserLogger(user)
   except Exception, e:
      log.debug("Exception in getUserName: {}".format(str(e)))
      
   cherrypy.session.user = user
   cherrypy.session.log = log
   print user
   
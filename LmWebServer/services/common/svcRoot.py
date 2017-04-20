"""
@summary: This is the Lifemapper services root
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

@note: Provide API access
@note; Provide static content access

"""
import cherrypy

from LmServer.common.lmconstants import CHERRYPY_CONFIG_FILE
from LmWebServer.services.api.v2.v2Root import ApiRootV2
from LmWebServer.services.common.userServices import (UserLogin, UserLogout, UserSignUp)
from LmWebServer.services.cpDispatchers.lmDispatch import LmDispatcher
from LmWebServer.services.cpTools.basicAuth import getUserName
from LmWebServer.services.cpTools.cors import CORS
from LmWebServer.services.cpTools.paramCaster import castParameters

# .............................................................................
@cherrypy.expose
class LmAPI(object):
   """
   @summary: Root class for Lifemapper API calls
   """
   v2 = ApiRootV2()
   login = UserLogin()
   logout = UserLogout()
   signup = UserSignUp()
   
   # ...........................
   def index(self):
      """
      @summary: Index page, should probably return information about available 
                   services
      """
      return "API index"

# .............................................................................
@cherrypy.expose
class LmServiceRoot(object):
   """
   @summary: Service root for all Lifemapper services
   """
   api = LmAPI()
   
# .............................................................................
   
# .............................................................................
if __name__ == '__main__':
   
   # Tell CherryPy to add headers eneded for CORS
   #cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)
   
   # Tell CherryPy to look for authenticated users
   #cherrypy.tools.BasicAuth = cherrypy.Tool('before_handler', getUserName)
   
   #cherrypy.config.update(CHERRYPY_CONFIG_FILE)
   #application = cherrypy.Application(LmServiceRoot(), 
   #                              script_name=None, config=CHERRYPY_CONFIG_FILE)
   cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)

   # Tell CherryPy to look for authenticated users
   cherrypy.tools.basicAuth = cherrypy.Tool('before_handler', getUserName)
   
   # Add the parameter caster to the tool box
   cherrypy.tools.paramCaster = cherrypy.Tool('before_handler', castParameters)

   appConfig = {
      '/' : {
         'request.dispatch' : LmDispatcher(),
         'tools.sessions.on' : True,
         'tools.basicAuth.on' : True,
         'tools.paramCaster.on' : True
      }
   }

   cherrypy.config.update(CHERRYPY_CONFIG_FILE)
   #cherrypy.mount(LmServiceRoot(), config=appConfig

   cherrypy.quickstart(LmServiceRoot(), '/', config=appConfig)

   #application = cherrypy.Application(LmServiceRoot(),
   #                              script_name=None)
   #cherrypy.server.start()
   #cherrypy.engine.start()



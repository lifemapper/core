"""This is the Lifemapper services root for CherryPy
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
def start_cherrypy_application(environ):
    """Start the WSGI-compliant CherryPy application and set up others
    """
    cherrypy.tree
    # .............................................................................
    # Tell CherryPy to add headers needed for CORS
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
    cherrypy.tree.mount(LmServiceRoot(), script_name=environ['SCRIPT_NAME'], config=appConfig)
    #return cherrypy.Application(LmServiceRoot(), script_name=None, config=appConfig)
    
    

"""This is the Lifemapper services root for CherryPy
"""
import cherrypy

from LmServer.common.lmconstants import CHERRYPY_CONFIG_FILE
from LmWebServer.services.api.v2.v2_root import ApiRootV2
from LmWebServer.services.common.user_services import (
    UserLogin, UserLogout, UserSignUp)
from LmWebServer.services.cp_dispatchers.lm_dispatch import LmDispatcher
from LmWebServer.services.cp_tools.basic_auth import get_user_name
from LmWebServer.services.cp_tools.cors import CORS
from LmWebServer.services.cp_tools.param_caster import cast_parameters


# .............................................................................
@cherrypy.expose
class LmAPI:
    """Root class for Lifemapper API calls
    """
    v2 = ApiRootV2()
    login = UserLogin()
    logout = UserLogout()
    signup = UserSignUp()

    # ...........................
    @staticmethod
    def index():
        """Index page
        """
        return "API index"


# .............................................................................
@cherrypy.expose
class LmServiceRoot:
    """Service root for all Lifemapper services
    """
    api = LmAPI()


# .............................................................................
def start_cherrypy_application(environ):
    """Start the WSGI-compliant CherryPy application and set up others
    """
    # .............................................................................
    # Tell CherryPy to add headers needed for CORS
    cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)

    # Tell CherryPy to look for authenticated users
    cherrypy.tools.basicAuth = cherrypy.Tool('before_handler', get_user_name)

    # Add the parameter caster to the tool box
    cherrypy.tools.paramCaster = cherrypy.Tool(
        'before_handler', cast_parameters)

    app_config = {
        '/': {
            'request.dispatch': LmDispatcher(),
            'tools.sessions.on': True,
            'tools.basicAuth.on': True,
            'tools.paramCaster.on': True
        }
    }

    cherrypy.config.update(CHERRYPY_CONFIG_FILE)
    cherrypy.tree.mount(
        LmServiceRoot(), script_name=environ['SCRIPT_NAME'], config=app_config)
    # return cherrypy.Application(
    #    LmServiceRoot(), script_name=None, config=app_config)

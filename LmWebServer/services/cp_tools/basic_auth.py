"""This module is for basic authentication for Lifemapper services.

Note:
    * We will probably want to switch our authentication mechanism, at least
        for Lifemapper proper.  We may want to keep basic authentication for
        instances though, thus the name of this module is 'basicAuth'
"""
import os

import cherrypy

from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import WebLogger, UserLogger
from LmWebServer.common.lmconstants import SESSION_KEY, SESSION_PATH


# .............................................................................
def get_user_name():
    """Attempt to get the session user name
    """
    user = PUBLIC_USER
    log = WebLogger()

    try:
        session_file_name = os.path.join(
            SESSION_PATH, 'session-{}'.format(cherrypy.session.id))
        if os.path.isfile(session_file_name):
            user = cherrypy.session.get(SESSION_KEY)
            if user is None:
                user = PUBLIC_USER
            log = UserLogger(user)
    except Exception as e:
        log.error('Exception in get_user_name: {}'.format(str(e)))

    cherrypy.session.user = user
    cherrypy.session.log = log

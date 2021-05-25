"""WSGI script that sits between Apache and CherryPy
"""
import os

import cherrypy

LM_ENV_VARS = ['LIFEMAPPER_SERVER_CONFIG_FILE', 'LIFEMAPPER_SITE_CONFIG_FILE']


# .............................................................................
def application(environ, start_response):
    """Mod_WSGI application hook

    Args:
        environ: The mod_wsgi environment sent to the request
        start_response: _
    """
    # Set environment variables that our CherryPy application can access
    for var in LM_ENV_VARS:
        os.environ[var] = environ[var]

    # Note: Must import after we have set environment variables or it will fail
    from LmWebServer.services.common.svc_root import start_cherrypy_application
    start_cherrypy_application(environ)
    return cherrypy.tree(environ, start_response)

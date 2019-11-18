"""WSGI script that sits between Apache and CherryPy
"""
import cherrypy
import os

LM_ENV_VARS = ['LIFEMAPPER_SERVER_CONFIG_FILE', 'LIFEMAPPER_SITE_CONFIG_FILE']

# .............................................................................
def application(environ, start_response):
    """Mod_WSGI application hook

    Args:
        environ: The mod_wsgi environment sent to the request
        start_response: 
    """
    # Set environment variables that our CherryPy application can access
    for var in LM_ENV_VARS:
        os.environ[var] = environ[var]

    from LmWebServer.services.common.svcRoot import start_cherrypy_application
    start_cherrypy_application(environ)
    return cherrypy.tree(environ, start_response)

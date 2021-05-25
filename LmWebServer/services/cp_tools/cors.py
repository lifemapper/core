"""This tool allows Cross-Origin Resource Sharing (CORS)
"""
import cherrypy


# .............................................................................
def CORS():
    """This function enables CORS for a web request

    Function to be called before processing a request.  This will add response
    headers required for CORS (Cross-Origin Resource Sharing) requests.  This
    is needed for browsers running JavaScript code from a different domain.
    """
    cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
    cherrypy.response.headers[
        'Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    cherrypy.response.headers['Access-Control-Allow-Headers'] = '*'
    cherrypy.response.headers['Access-Control-Allow-Credentials'] = 'true'
    if cherrypy.request.method.lower() == 'options':
        cherrypy.response.headers['Content-Type'] = 'text/plain'
        return 'OK'

"""This module contains functions for wrapping CherryPy interactions

!!!! This is for old services and needs to be updated !!!!

Note:
    * This was written using CherryPy 3.1.2
    * Based this on 
        https://bitbucket.org/Lawouach/cherrypy-recipes/src/tip/testing/unit/serverless/
"""
import cherrypy
import os
import pickle
from StringIO import StringIO
import urllib
from urlparse import urlparse

from LmServer.common.localconstants import WEBSERVICES_ROOT
from LmCommon.common.singleton import singleton
from LmWebServer.services.common.svc import svc 
from LmServer.common.lmconstants import CHERRYPY_CONFIG_FILE


LOCAL_IP = '127.0.0.1'

# .............................................................................
@singleton
class AppWrapper(object):
    """
    @summary: This is a wrapper class around the cherrypy application we will
        use for direct debugging.  This class is a singleton so that we always
        work with the same one in a single debugging run.  I did this in case
        there were issues with sessions across instances and so that we don't
        keep resetting session options
    """
    # ..............................
    def __init__(self):
        cherrypy.config.update(CHERRYPY_CONFIG_FILE)
        self.app = cherrypy.Application(svc(), config=CHERRYPY_CONFIG_FILE)
        
# .............................................................................
def getApp():
    """
    @summary: This function is a wrapper around the AppWrapper class that is 
                     used to get a cherrypy application
    """
    wrapper = AppWrapper()
    return wrapper.app

# .............................................................................
def createRequest(url, method, headers={}, body=None, remoteIp='127.0.0.1'):
    """
    @summary: Create the CherryPy request with the specified parameters
    @param url: The full URL for the request
    @param method: The HTTP method to use for the request
    @param headers: A dictionary of HTTP headers to send as part of the request
    @param body: The body of the request
    @param remoteIp: The IP address that this request is sent from (spoofed)
    """
    
    urlParts = urlparse(url)
    vpath = urlParts.path
    scheme = urlParts.scheme
    # Creates a dictionary of query parameters by splitting the query string
    #    at the ampersands and then splitting up the param=value pairs at the 
    #    equal signs
    # 'param1=val1&param2=val2' -> {'param1': 'val1', 'param2' : 'val2'} 
    if len(urlParts.query) > 1:
        qParams = dict([i.split('=') for i in urlParts.query.split('&')])
    else:
        qParams = {}
    
    return executeRequest(vpath=vpath, method=method, scheme=scheme, data=body,
                                 headers=headers, remoteIp=remoteIp, **qParams)

# .............................................................................
def createRequestFromErrorPickle(pickleFn):
    """
    @summary: Given a pickle file name, recreate the corresponding request
    """
    if os.path.exists(pickleFn):
        with open(pickleFn, 'r') as f:
            pObj = pickle.load(f)
            req = pObj["Request"]
            vpath = req["vpath"]
            method = req["method"]
            scheme = req["scheme"]
            body = req["body"]
            headers = req["headers"]
            remoteIp = req["remote"].ip
            
            qParams = dict([i.split('=') for i in req["URL"]["queryParams"].split('&')])
            
            return executeRequest(vpath=vpath, method=method, scheme=scheme, data=body,
                                 headers=headers, remoteIp=remoteIp, **qParams)
    else:
        raise Exception, "Pickle file %s, does not exist" % pickleFn
    
# .............................................................................
class MockedInfo(object):
    """
    @summary: This is a mock class to provide an info call for a response
    @note: Only provides getheaders method so the headers can be processed for
                 authentication
    """
    # .................................
    def __init__(self, response):
        self.response = response

    # .................................
    def getheaders(self, name):
        ret = []
        for header, value in self.response.header_list:
            if header == name:
                ret.append(value)
        return ret
    
# .............................................................................
def executeRequest(vpath='/', method='GET', scheme='http', 
                         proto='HTTP/1.1', data=None, headers=None, 
                         remoteIp="127.0.0.1", **kwargs):
    """
    @summary: Performs a request with the given parameters
    @param vpath: The path portion of a URL
    @param method: The HTTP method of the request to be performed
    @param scheme: The URI scheme of the request
    @param proto: The protocol to use with this request
    @param data: The body of the request
    @param headers: An optional dictionary of request headers
    @param remoteIp: The IP address that this request is coming from (where you
                              want the request to come from, not necessarily the 
                              machine making the call)
    @param **kwargs: Keyword arguments to be used as query parameters for the 
                              request
    """
    localAddress = cherrypy.lib.httputil.Host(LOCAL_IP, 50000, "")
    remoteAddress = cherrypy.lib.httputil.Host(remoteIp, 50001, "")
    
    # This is a required header when running HTTP/1.1
    h = {"Host" : WEBSERVICES_ROOT}
    
    if headers is not None:
        h.update(headers)

    # If we have a POST/PUT request but no data
    # we urlencode the named arguments in **kwargs
    # and set the content-type header
    if method in ('POST', 'PUT') and not data:
        data = urllib.urlencode(kwargs)
        kwargs = None
        h['content-type'] = 'application/x-www-form-urlencoded'

    # If we did have named arguments, let's
    # urlencode them and use them as a querystring
    qs = None
    if kwargs:
        qs = urllib.urlencode(kwargs)

    # if we had some data passed as the request entity
    # let's make sure we have the content-length set
    fd = None
    if data is not None:
        h['content-length'] = '%d' % len(data)
        fd = StringIO(data)

    # Get our application and run the request against it
    #app = cherrypy.Application(svc(), config=conf)
    app = getApp()

    # Let's fake the local and remote addresses
    request, response = app.get_serving(localAddress, remoteAddress, scheme, proto)
    try:
        h = [(k, v) for k, v in h.iteritems()]
        response = request.run(method, vpath, qs, proto, h, fd)
    finally:
        if fd:
            fd.close()
            fd = None

    if response.status.startswith('500'):
        print response.body
        raise AssertionError("Unexpected error")

    # Add an info function
    response.info = lambda : MockedInfo(response)

    # Add functions to request needed for cookie processing
    request.get_full_url = lambda : WEBSERVICES_ROOT
    request.is_unverifiable = lambda : False # This is just for testing so this is okay

    # collapse the response into a bytestring
    response.collapse_body()
    return response, request

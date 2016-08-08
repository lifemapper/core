Debugging Web Requests
======================

CherryPy Wrapper
------------------

   The cherrypy wrapper module contains functions that allow for access of services through Cherry Py without going through HTTP.  

 Create Request
   The createRequest method allows you to specify parameters for a request and have it execute without going through HTTP.  It has two required parameters, url and method, and three optional parameters, headers, body, and remoteIp.  You can use this function to interact with any of the services.
     * url - The full URL to send the request to
     * method - The HTTP method to use for the request (GET, POST, PUT, DELETE)
     * headers - A dictionary of HTTP headers
     * body - The body of the HTTP request (Usually posted data, such as the contents of a layer data file)
     * remoteIp - The request will look like it came from this IP address

   Examples:

   Test that some user's data can be accessed from a privileged IP address but not from one that is not

   .. code-block:: python    

     >>> from LmDebug.tools.cherrypyWrapper import createRequest
     >>> url = 'http://svc.lifemapper.org/services/sdm/layers/5316/GTiff' # User layer
     >>> r1 = createRequest(url, 'GET', remoteIp='111.11.1.111') # Some unprivileged IP
     >>> r2 = createRequest(url, 'GET', remoteIp='129.237.201.247') # Privileged IP
     >>> r1.status
     '403 Forbidden'
     >>> r2.status
     '200 OK'

  Create Request from Error Pickle
    The createRequestFromErrorPickle method allows you to recreate the request that generated the error so that the code can be stepped through to find the error.  You just need to provide the function with the path to the Python pickle containing the error information

    .. code-block:: python
     
      >>> from LmDebug.tools.cherrypyWrapper import createRequestFromErrorPickle
      >>> r = createRequestFromErrorPickle(pickleFilename)

  Execute Request
    The executeRequest method does the work of wrapping a CherryPy application.  This method is called by createRequest and createRequestFromErrorPickle.  I considered making this a private method, but it is possible that you would want to call it directly.  

    .. code-block:: python

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

----

Client Library Bypass HTTP
==========================

   The clientLibBypassHTTP module only contains one method, installHTTPBypass.  This method monkey patches the client library by changing the client library instance's client's makeRequest method.  That method is used to make the requests to the outside world by building a urllib2 request and executing it.  The monkey patch uses the CherryPy wrapper instead to just call the cherrypy application directly.  This method takes the client library instance as an argument and an optional argument for an IP address to make the requests look like they come from.


"""Module containing functions for bypassing HTTP communications

This module provides functions to bypass HTTP when using the client library.
    This will allow you to step through the code and interact with the back end
    services without doing so blindly over HTTP.

Note:
    * This is out of date and needs to be updated
"""
import types
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse

from LmClient.lmClientLib import removeNonesFromTupleList

from LmDebug.tools.cherrypyWrapper import createRequest

def installHTTPBypass(cLib, remoteIP='127.0.0.1'):
    """
    @summary: Install a new makeRequest method that does not communicate with 
                     the server via HTTP and instead just calls the web service
                     code directly (where possible, should use HTTP for external
                     servers)
    @param cl: The client library instance to override the make request method
                      for.
    @param remoteIp: The IP address where the requests should appear to come from
    @note: This will only allow access to server code on the local machine
    """
    def makeBypassRequest(self, url, method='GET', parameters=[], body=None,
                                 headers={}, objectify=False):
        parameters = removeNonesFromTupleList(parameters)
        urlparams = urllib.parse.urlencode(parameters)
        
        if body is None and len(parameters) > 0 and method.lower() == "post":
            body = urlparams
        else:
            url = "%s?%s" % (url, urlparams)
            
        headers['User-Agent'] = self.UA_STRING + " (HTTP bypass)"

        # Add cookie headers
        
        cookies = []
        for cookie in self.cookieJar:
            if cookie.domain == self.server.strip('http://'):
                cookies.append('%s=%s' % (cookie.name, cookie.value))
        if len(cookies) > 0:
            headers['Cookie'] = '; '.join(cookies)

        ret, req = createRequest(url, method, headers=headers, body=body, 
                                      remoteIp=remoteIP)
        
        self.cookieJar.extract_cookies(ret, req)
        
        resp = ''.join(ret.body)
        if objectify:
            return self.objectify(resp)
        else:
            return resp
        
    cLib._cl.makeRequest = types.MethodType(makeBypassRequest, cLib._cl)


"""This module is for basic authentication for Lifemapper services.

Note:
    * We will probably want to switch our authentication mechanism, at least
        for Lifemapper proper.  We may want to keep basic authentication for
        instances though, thus the name of this module is 'basicAuth'
"""
from flask import session, request, url_for
import os
# from urlparse import urlparse, urljoin
from urllib.parse import urlparse, urljoin

from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import WebLogger, UserLogger
from LmWebServer.common.lmconstants import SESSION_PATH

# .............................................................................
def get_user_name():
    """Attempt to get the session user name"""
    user = PUBLIC_USER
    log = WebLogger()

    try:
        session_file_name = os.path.join(SESSION_PATH, 'session-{}'.format(session['username']))
        if os.path.isfile(session_file_name):
            try:
                user = session['username']
            except:
                user = PUBLIC_USER
            log = UserLogger(user)
    except Exception as e:
        log.error('Exception in get_user_name: {}'.format(str(e)))

    session['username'] = user
    session['log'] = log


# .............................................................................
def is_safe_url(target):
    """Use to test before redirecting

    Note: From archived Flask snippets site: https://web.archive.org/web/20190128010142/http://flask.pocoo.org/snippets/62/
    """
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and ref_url.netloc == test_url.netloc
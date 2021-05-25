"""This module contains tests for Lifemapper URLs that should be forwarded
"""
import urllib.request

from LmCommon.common.lmconstants import HTTPStatus

ORIG_URL_KEY = 'orig_url'  # The original URL that should be rewritten
REWRITE_URL_KEY = 'rewrite_url'  # The desired rewritten URL
TEST_STATUS_KEY = 'test_status'  # The HTTP status we expect for this rewrite

# Note: URLs can be absolute or relative
FORWARDED_URLS = [
    {
        ORIG_URL_KEY: 'http://svc.lifemapper.org/species/',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/?page_id=863',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://svc.lifemapper.org/species/Acer',
        REWRITE_URL_KEY:
            'http://svc.lifemapper.org/?page_id=863&amp;speciesname=Acer',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://lifemapper.org/cleints/algorithms.xml',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/clients/algorithms.xml',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://lifemapper.org/clients/algorithms.json',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/clients/algorithms.json',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://lifemapper.org/login',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/login',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://lifemapper.org/logout',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/logout',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://lifemapper.org/signup',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/signup',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://lifemapper.org/api',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/api',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://lifemapper.org/spLinks',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/spLinks',
        TEST_STATUS_KEY: HTTPStatus.OK
    },
    {
        ORIG_URL_KEY: 'http://lifemapper.org/robots.txt',
        REWRITE_URL_KEY: 'http://svc.lifemapper.org/robots.txt',
        TEST_STATUS_KEY: HTTPStatus.OK
    }
]


# .............................................................................
def get_absolute_url(url, local_server):
    """Gets the absolute URL from the provided URL

    Args:
        url (:obj:`str`): The absolute or relative URL to process

    Note:
        * Sets the server for the URL to be the local test system if it is a
            relative URL.

    Returns:
        * If an absolute URL is provided, it is returned.  If a relative URL is
            provided, the local server is prefixed to it.
    """
    if url.startswith('http'):
        return url

    return '{}{}'.format(local_server, url)


# .............................................................................
class Test_url_writing:
    """Tests that URLs are properly rewritten
    """

    # .....................................
    @staticmethod
    def test_url_rewrites():
        """This test checks all URLs to be rewritten
        """
        for url_rewrite_info in FORWARDED_URLS:
            req = urllib.request.Request(url_rewrite_info[ORIG_URL_KEY])
            ret = urllib.request.urlopen(req)
            # Check that URL is rewritten
            assert ret.url == url_rewrite_info[REWRITE_URL_KEY]
            # Check that HTTP status is what we expect
            assert ret.code == url_rewrite_info[TEST_STATUS_KEY]

"""Test configuration fixtures

Note:
    * pytest processes this module and creates test fixtures that can then be
        used with the tests it discovers
"""
import os
import pytest

from LmServer.common.lmconstants import WEBSERVICES_ROOT
from LmServer.common.log import UnittestLogger
from LmServer.db.borg_scribe import BorgScribe
from LmTest.service_tests.tools.lm_client import LmWebClient

# .............................................................................
# .                                 Constants                                 .
# .............................................................................
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DATA_PATH = os.path.join(THIS_DIR, 'data_dir')


# .............................................................................
@pytest.fixture(scope='session')
def public_client():
    """Gets a web client that uses the public user
    """
    return LmWebClient(WEBSERVICES_ROOT)


# .............................................................................
@pytest.fixture(scope='session')
def scribe():
    """Gets a scribe instance as a text fixture

    Note:
        * Tests that take 'scribe' as an argument will get this fixture
    """
    _scribe = BorgScribe(UnittestLogger())
    _scribe.open_connections()
    return _scribe


# .............................................................................
@pytest.fixture(scope='session')
def webservices_root():
    """Gets the local web server
    """
    return WEBSERVICES_ROOT

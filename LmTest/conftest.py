"""Test configuration fixtures

Note:
    * pytest processes this module and creates test fixtures that can then be
        used with the tests it discovers
"""
import os

import pytest

from LmServer.db.borgscribe import BorgScribe
from LmServer.common.log import UnittestLogger
from LmServer.common.localconstants import WEBSERVICES_ROOT

# .............................................................................
# .                                 Constants                                 .
# .............................................................................
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DATA_PATH = os.path.join(THIS_DIR, 'data_dir')


# .............................................................................
@pytest.fixture(scope='session')
def webservices_root():
    """Gets the local web server
    """
    return WEBSERVICES_ROOT

# .............................................................................
@pytest.fixture(scope='session')
def scribe():
    """Gets a scribe instance as a text fixture

    Note:
        * Tests that take 'scribe' as an argument will get this fixture
    """
    scribe = BorgScribe(UnittestLogger())
    scribe.openConnections()
    return scribe

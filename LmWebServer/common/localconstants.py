"""Local configuration constants for LmWebServer
"""
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (SERVER_ENV_HEADING)

_CFG = Config()

# Relative Home of Results packaging code
PACKAGING_DIR = _CFG.get(SERVER_ENV_HEADING, 'PACKAGING_DIR')

# Max size for anonymous uploads (in lines)
try:
    MAX_ANON_UPLOAD_SIZE = _CFG.get(SERVER_ENV_HEADING, 'MAX_ANON_UPLOAD_SIZE')
except Exception:
    MAX_ANON_UPLOAD_SIZE = 20000

"""Local configuration constants for LmWebServer
"""
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (SERVER_ENV_HEADING)


cfg = Config()

# Relative Home of Results packaging code
PACKAGING_DIR = cfg.get(SERVER_ENV_HEADING, 'PACKAGING_DIR')

# Max size for anonymous uploads (in lines)
try:
    MAX_ANON_UPLOAD_SIZE = cfg.get(SERVER_ENV_HEADING, 'MAX_ANON_UPLOAD_SIZE')
except:
    MAX_ANON_UPLOAD_SIZE = 20000

"""Local configuration values for LmServer
"""
import os

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (
    SERVER_ENV_HEADING, BoomKeys, SERVER_PIPELINE_HEADING, SERVER_DB_HEADING,
    SERVER_MATT_DAEMON_HEADING)

cfg = Config()

# LmServer (LmDbServer and LmWebServer)
PUBLIC_FQDN = cfg.get(SERVER_ENV_HEADING, 'PUBLIC_FQDN')
PUBLIC_USER = cfg.get(SERVER_ENV_HEADING, 'PUBLIC_USER')
APP_PATH = cfg.get(SERVER_ENV_HEADING, 'APP_PATH')
DATA_PATH = cfg.get(SERVER_ENV_HEADING, 'DATA_PATH')
SHARED_DATA_PATH = cfg.get(SERVER_ENV_HEADING, "SHARED_DATA_PATH")
SCRATCH_PATH = cfg.get(SERVER_ENV_HEADING, 'SCRATCH_PATH')
PID_PATH = cfg.get(SERVER_ENV_HEADING, 'PID_PATH')
BOOM_PID_FILE = os.path.join(PID_PATH, 'daboom.pid')
LM_DISK = cfg.get(SERVER_ENV_HEADING, 'LM_DISK')
SPECIES_DIR = cfg.get(SERVER_ENV_HEADING, 'SPECIES_DATA_DIR')

# TEMP_PATH = cfg.get(SERVER_ENV_HEADING, 'TEMP_PATH')

SMTP_SERVER = cfg.get(SERVER_ENV_HEADING, 'SMTP_SERVER')
SMTP_SENDER = cfg.get(SERVER_ENV_HEADING, 'SMTP_SENDER')
WEBSERVICES_ROOT = cfg.get(SERVER_ENV_HEADING, 'WEBSERVICES_ROOT')

# Installed data and software versions
p = cfg.get(SERVER_ENV_HEADING, 'PYTHON')
PYTHON_VERSION = p[len('python'):]
PG_VERSION = cfg.get(SERVER_ENV_HEADING, 'PG_VERSION')
LMCODE_VERSION = cfg.get(SERVER_ENV_HEADING, 'LMCODE_VERSION')
LMVIZ_VERSION = cfg.get(SERVER_ENV_HEADING, 'LMVIZ_VERSION')
GBIF_VERSION = cfg.get(SERVER_ENV_HEADING, 'GBIF_VERSION')

# BoomKeys
POINT_COUNT_MIN = cfg.getint(SERVER_PIPELINE_HEADING, BoomKeys.POINT_COUNT_MIN)
POINT_COUNT_MAX = cfg.getint(SERVER_PIPELINE_HEADING, 'POINT_COUNT_MAX')
DEFAULT_EPSG = cfg.getint(SERVER_PIPELINE_HEADING, 'DEFAULT_EPSG')

TROUBLESHOOTERS = cfg.getlist(
    SERVER_PIPELINE_HEADING, BoomKeys.TROUBLESHOOTERS)
CONNECTION_PORT = int(cfg.get(SERVER_DB_HEADING, 'CONNECTION_PORT'))
DB_HOSTNAME = cfg.get(SERVER_DB_HEADING, 'DB_HOSTNAME')

# Makeflow
MAX_MAKEFLOWS = cfg.getint(SERVER_MATT_DAEMON_HEADING, 'MAX_MAKEFLOWS')
MAX_WORKERS = cfg.getint(SERVER_MATT_DAEMON_HEADING, 'MAX_WORKERS')
try:
    WORKER_PATH = cfg.get(SERVER_MATT_DAEMON_HEADING, 'WORKER_PATH')
except Exception:
    WORKER_PATH = os.path.join(SCRATCH_PATH, 'worker')

try:
    MASTER_WORKER_PATH = cfg.get(
        SERVER_MATT_DAEMON_HEADING, 'MASTER_WORKER_PATH')
except Exception:
    MASTER_WORKER_PATH = os.path.join(SCRATCH_PATH, 'worker')

# Catalog server
CS_PORT = cfg.get(SERVER_MATT_DAEMON_HEADING, 'CATALOG_SERVER_PORT')
EXTRA_CS_OPTIONS = cfg.get(SERVER_MATT_DAEMON_HEADING, 'EXTRA_CS_OPTIONS')

# Extra options (appended to the default options
EXTRA_WORKER_OPTIONS = cfg.get(
    SERVER_MATT_DAEMON_HEADING, 'EXTRA_WORKER_OPTIONS')
EXTRA_MAKEFLOW_OPTIONS = cfg.get(
    SERVER_MATT_DAEMON_HEADING, 'EXTRA_MAKEFLOW_OPTIONS')
EXTRA_WORKER_FACTORY_OPTIONS = cfg.get(
    SERVER_MATT_DAEMON_HEADING, 'EXTRA_WORKER_FACTORY_OPTIONS')

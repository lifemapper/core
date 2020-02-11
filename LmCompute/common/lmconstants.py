"""Module containing the constants set by Lifemapper
"""
import os

from LmCompute.common.localconstants import (
    CONVERT_JAVA_INIT_MEM_OPTION, CONVERT_JAVA_MAX_MEM_OPTION, JAVA_EXE,
    JAVA_INIT_MEM_OPTION, JAVA_MAX_MEM_OPTION, LM_DISK, LM_PATH, SCRATCH_PATH)

# ============================================================================
# =                             Paths/Directories                            =
# ============================================================================
BIN_PATH = os.path.join(LM_PATH, 'bin')
ME_CMD = os.path.join(LM_PATH, 'LmCompute/apps/maxent.jar')

# JOB_OUTPUT_PATH --> WORKSPACE_PATH
WORKSPACE_PATH = os.path.join(SCRATCH_PATH, 'work')
TEMPORARY_FILE_PATH = os.path.join(SCRATCH_PATH, 'temp')
COMPUTE_LOG_PATH = os.path.join(SCRATCH_PATH, 'log')

SAMPLE_JOBS_PATH = os.path.join(LM_DISK, 'tests/config/sampleJobs')
SAMPLE_LAYERS_PATH = os.path.join(LM_DISK, 'tests/data/layers/testLayers.txt')
SAMPLE_DATA_PATH = os.path.join(LM_DISK, 'tests/data')
METRICS_STORAGE_PATH = os.path.join(LM_DISK, 'metrics')
SGE_PATH = os.path.join(LM_DISK, 'sge')

JAVA_SYSTEM_ROOT = os.path.join(SCRATCH_PATH, '.java')
JAVA_USER_ROOT = os.path.join(JAVA_SYSTEM_ROOT, '.userPrefs')

JAVA_CMD = '{} {} {} {} {} -cp'.format(
    JAVA_EXE, '-Xms{}'.format(JAVA_INIT_MEM_OPTION),
    '-Xmx{}'.format(JAVA_MAX_MEM_OPTION),
    '-Djava.util.prefs.systemRoot={}'.format(JAVA_SYSTEM_ROOT),
    '-Djava.util.prefs.userRoot={}'.format(JAVA_USER_ROOT))

CONVERT_JAVA_CMD = '{} {} {} {} {} -cp'.format(
    JAVA_EXE, '-Xms{}'.format(CONVERT_JAVA_INIT_MEM_OPTION),
    '-Xmx{}'.format(CONVERT_JAVA_MAX_MEM_OPTION),
    '-Djava.util.prefs.systemRoot={}'.format(JAVA_SYSTEM_ROOT),
    '-Djava.util.prefs.userRoot={}'.format(JAVA_USER_ROOT))

GDALINFO_CMD = os.path.join(BIN_PATH, 'gdalinfo')

# ============================================================================
# =                             Commands                                     =
# ============================================================================
# Maxent
MDL_TOOL = 'density.MaxEnt'
PRJ_TOOL = 'density.Project'
CONVERT_TOOL = 'density.Convert'
ME_VERSION = '3.3.3k'

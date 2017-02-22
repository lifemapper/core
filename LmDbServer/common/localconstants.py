"""
@summary: Local configuration constants for LmDbServer
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import OutputFormat

_CONFIG_HEADING = "LmServer - pipeline"

# Data Archive Pipeline
WORKER_JOB_LIMIT = Config().getint(_CONFIG_HEADING, 'WORKER_JOB_LIMIT')
DEFAULT_ALGORITHMS = Config().getlist(_CONFIG_HEADING, 'DEFAULT_ALGORITHMS')
DEFAULT_MODEL_SCENARIO = Config().get(_CONFIG_HEADING, 'DEFAULT_MODEL_SCENARIO')
DEFAULT_PROJECTION_SCENARIOS = Config().getlist(_CONFIG_HEADING, 
                                                'DEFAULT_PROJECTION_SCENARIOS')
SCENARIO_PACKAGE = Config().get(_CONFIG_HEADING, 'SCENARIO_PACKAGE')
DEFAULT_GRID_NAME = Config().get(_CONFIG_HEADING, 'DEFAULT_GRID_NAME')
DEFAULT_GRID_CELLSIZE = Config().get(_CONFIG_HEADING, 'DEFAULT_GRID_CELLSIZE')

INTERSECT_FILTERSTRING = Config().get(_CONFIG_HEADING, 'INTERSECT_FILTERSTRING')
INTERSECT_VALNAME = Config().get(_CONFIG_HEADING, 'INTERSECT_VALNAME')
INTERSECT_MINPERCENT = Config().get(_CONFIG_HEADING, 'INTERSECT_MINPERCENT')
INTERSECT_MINPRESENCE = Config().get(_CONFIG_HEADING, 'INTERSECT_MINPRESENCE')
INTERSECT_MAXPRESENCE = Config().get(_CONFIG_HEADING, 'INTERSECT_MAXPRESENCE')

USER_OCCURRENCE_CSV_FILENAME = Config().get(_CONFIG_HEADING, 'USER_OCCURRENCE_CSV')
USER_OCCURRENCE_META_FILENAME = Config().get(_CONFIG_HEADING, 'USER_OCCURRENCE_META')
try:
   USER_OCCURRENCE_DATA = Config().get(_CONFIG_HEADING, 'USER_OCCURRENCE_DATA')
   USER_OCCURRENCE_CSV_FILENAME = USER_OCCURRENCE_DATA + OutputFormat.CSV
   USER_OCCURRENCE_META_FILENAME = USER_OCCURRENCE_DATA + OutputFormat.METADATA
except:
   USER_OCCURRENCE_DATA = None
   USER_OCCURRENCE_CSV_FILENAME = Config().get(_CONFIG_HEADING, 'USER_OCCURRENCE_CSV')
   USER_OCCURRENCE_META_FILENAME = Config().get(_CONFIG_HEADING, 'USER_OCCURRENCE_META')

# GBIF data
GBIF_TAXONOMY_FILENAME = Config().get(_CONFIG_HEADING, 'GBIF_TAXONOMY_FILENAME')
GBIF_OCCURRENCE_FILENAME = Config().get(_CONFIG_HEADING, 'GBIF_OCCURRENCE_FILENAME')
GBIF_PROVIDER_FILENAME = Config().get(_CONFIG_HEADING, 'GBIF_PROVIDER_FILENAME')


# BISON data
BISON_TSN_FILENAME = Config().get(_CONFIG_HEADING, 'BISON_TSN_FILENAME')

# iDigBio data
IDIG_FILENAME = Config().get(_CONFIG_HEADING, 'IDIG_FILENAME')

# Both
SPECIES_EXP_YEAR = Config().getint(_CONFIG_HEADING, 'SPECIES_EXP_YEAR')
SPECIES_EXP_MONTH = Config().getint(_CONFIG_HEADING, 'SPECIES_EXP_MONTH')
SPECIES_EXP_DAY = Config().getint(_CONFIG_HEADING, 'SPECIES_EXP_DAY')


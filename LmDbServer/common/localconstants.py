"""
@summary: Local configuration constants for LmDbServer
@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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

# GBIF data
TAXONOMY_FILENAME = Config().get(_CONFIG_HEADING, 'TAXONOMY_FILENAME')
OCCURRENCE_FILENAME = Config().get(_CONFIG_HEADING, 'OCCURRENCE_FILENAME')
PROVIDER_FILENAME = Config().get(_CONFIG_HEADING, 'PROVIDER_FILENAME')

# BISON data
TSN_FILENAME = Config().get(_CONFIG_HEADING, 'TSN_FILENAME')

# iDigBio data
BINOMIAL_FILENAME = Config().get(_CONFIG_HEADING, 'BINOMIAL_FILENAME')

# Both
MARINE_FILENAME = Config().get(_CONFIG_HEADING, 'MARINE_FILENAME')
SPECIES_EXP_YEAR = Config().getint(_CONFIG_HEADING, 'SPECIES_EXP_YEAR')
SPECIES_EXP_MONTH = Config().getint(_CONFIG_HEADING, 'SPECIES_EXP_MONTH')
SPECIES_EXP_DAY = Config().getint(_CONFIG_HEADING, 'SPECIES_EXP_DAY')


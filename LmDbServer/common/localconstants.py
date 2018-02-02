"""
@summary: Local configuration constants for LmDbServer
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
from LmCommon.common.lmconstants import SERVER_PIPELINE_HEADING

# Data Archive Pipeline
WORKER_JOB_LIMIT = Config().getint(SERVER_PIPELINE_HEADING, 
                                   'WORKER_JOB_LIMIT')
ASSEMBLE_PAMS = Config().getboolean(SERVER_PIPELINE_HEADING, 'ASSEMBLE_PAMS')
GRID_NAME = Config().get(SERVER_PIPELINE_HEADING, 'GRID_NAME')
GRID_CELLSIZE = Config().get(SERVER_PIPELINE_HEADING, 'GRID_CELLSIZE')
GRID_NUM_SIDES = Config().getint(SERVER_PIPELINE_HEADING, 'GRID_NUM_SIDES')

SCENARIO_PACKAGE = Config().get(SERVER_PIPELINE_HEADING, 
                                'SCENARIO_PACKAGE')

INTERSECT_FILTERSTRING = Config().get(SERVER_PIPELINE_HEADING, 
                                      'INTERSECT_FILTERSTRING')
INTERSECT_VALNAME = Config().get(SERVER_PIPELINE_HEADING, 
                                 'INTERSECT_VALNAME')
INTERSECT_MINPERCENT = Config().get(SERVER_PIPELINE_HEADING, 
                                    'INTERSECT_MINPERCENT')
INTERSECT_MINPRESENCE = Config().get(SERVER_PIPELINE_HEADING, 
                                     'INTERSECT_MINPRESENCE')
INTERSECT_MAXPRESENCE = Config().get(SERVER_PIPELINE_HEADING, 
                                     'INTERSECT_MAXPRESENCE')

USER_OCCURRENCE_DATA = Config().get(SERVER_PIPELINE_HEADING, 'USER_OCCURRENCE_DATA')
USER_OCCURRENCE_DATA_DELIMITER = Config().get(SERVER_PIPELINE_HEADING, 
                                    'USER_OCCURRENCE_DATA_DELIMITER')

# GBIF data
GBIF_TAXONOMY_FILENAME = Config().get(SERVER_PIPELINE_HEADING, 
                                      'GBIF_TAXONOMY_FILENAME')
GBIF_OCCURRENCE_FILENAME = Config().get(SERVER_PIPELINE_HEADING, 
                                        'GBIF_OCCURRENCE_FILENAME')
GBIF_PROVIDER_FILENAME = Config().get(SERVER_PIPELINE_HEADING, 
                                      'GBIF_PROVIDER_FILENAME')

# BISON data
BISON_TSN_FILENAME = Config().get(SERVER_PIPELINE_HEADING, 
                                  'BISON_TSN_FILENAME')

# iDigBio data
IDIG_OCCURRENCE_DATA = Config().get(SERVER_PIPELINE_HEADING, 'IDIG_OCCURRENCE_DATA')
IDIG_OCCURRENCE_DATA_DELIMITER = Config().get(SERVER_PIPELINE_HEADING, 
                                    'IDIG_OCCURRENCE_DATA_DELIMITER')

# Both
SPECIES_EXP_YEAR = Config().getint(SERVER_PIPELINE_HEADING, 
                                   'SPECIES_EXP_YEAR')
SPECIES_EXP_MONTH = Config().getint(SERVER_PIPELINE_HEADING, 
                                    'SPECIES_EXP_MONTH')
SPECIES_EXP_DAY = Config().getint(SERVER_PIPELINE_HEADING, 
                                  'SPECIES_EXP_DAY')


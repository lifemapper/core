"""
@summary: Local configuration constants for LmDbServer
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
from LmCommon.common.lmconstants import SERVER_PIPELINE_HEADING, BoomKeys

# Data Archive Pipeline
WORKER_JOB_LIMIT = Config().getint(SERVER_PIPELINE_HEADING, 
                                   'WORKER_JOB_LIMIT')

INTERSECT_FILTERSTRING = Config().get(SERVER_PIPELINE_HEADING, 
                                      BoomKeys.INTERSECT_FILTER_STRING)
INTERSECT_VALNAME = Config().get(SERVER_PIPELINE_HEADING, 
                                 BoomKeys.INTERSECT_VAL_NAME)
INTERSECT_MINPERCENT = Config().get(SERVER_PIPELINE_HEADING, 
                                    BoomKeys.INTERSECT_MIN_PERCENT)
INTERSECT_MINPRESENCE = Config().get(SERVER_PIPELINE_HEADING, 
                                     BoomKeys.INTERSECT_MIN_PRESENCE)
INTERSECT_MAXPRESENCE = Config().get(SERVER_PIPELINE_HEADING, 
                                     BoomKeys.INTERSECT_MAX_PRESENCE)

# GBIF data
GBIF_TAXONOMY_FILENAME = Config().get(SERVER_PIPELINE_HEADING, 
                                      BoomKeys.GBIF_TAXONOMY_FILENAME)
GBIF_PROVIDER_FILENAME = Config().get(SERVER_PIPELINE_HEADING, 
                                      BoomKeys.GBIF_PROVIDER_FILENAME)
GBIF_OCCURRENCE_FILENAME = Config().get(SERVER_PIPELINE_HEADING, 
                                        'GBIF_OCCURRENCE_FILENAME')

OCC_EXP_YEAR = Config().getint(SERVER_PIPELINE_HEADING, BoomKeys.OCC_EXP_YEAR)
OCC_EXP_MONTH = Config().getint(SERVER_PIPELINE_HEADING, BoomKeys.OCC_EXP_MONTH)
OCC_EXP_DAY = Config().getint(SERVER_PIPELINE_HEADING, BoomKeys.OCC_EXP_DAY)


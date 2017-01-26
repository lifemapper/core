"""
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
import os.path 

from LmServer.common.localconstants import PID_PATH
from LmServer.common.lmconstants import SPECIES_DATA_PATH
from LmDbServer.common.localconstants import (GBIF_TAXONOMY_FILENAME, 
     GBIF_OCCURRENCE_FILENAME, GBIF_PROVIDER_FILENAME, BISON_TSN_FILENAME , 
     IDIG_FILENAME, USER_OCCURRENCE_CSV_FILENAME, USER_OCCURRENCE_META_FILENAME)                 

# ............................................................................

BOOM_PID_FILE = os.path.join(PID_PATH, 'lmboom.pid')
GBIF_DUMP_FILE = os.path.join(SPECIES_DATA_PATH, GBIF_OCCURRENCE_FILENAME)
GBIF_TAXONOMY_DUMP_FILE = os.path.join(SPECIES_DATA_PATH, GBIF_TAXONOMY_FILENAME)
GBIF_PROVIDER_DUMP_FILE = os.path.join(SPECIES_DATA_PATH, GBIF_PROVIDER_FILENAME)
BISON_TSN_FILE = os.path.join(SPECIES_DATA_PATH, BISON_TSN_FILENAME)
IDIGBIO_FILE = os.path.join(SPECIES_DATA_PATH, IDIG_FILENAME)

USER_OCCURRENCE_CSV = os.path.join(SPECIES_DATA_PATH, USER_OCCURRENCE_CSV_FILENAME)
USER_OCCURRENCE_META = os.path.join(SPECIES_DATA_PATH, USER_OCCURRENCE_META_FILENAME)

GBIF_DATASOURCE = 'GBIF'
BISON_DATASOURCE = 'BISON'
IDIGBIO_DATASOURCE = 'IDIGBIO'

# Key must match DATASOURCE in config/config.ini
TAXONOMIC_SOURCE = {
   GBIF_DATASOURCE: {'name': 'GBIF Backbone Taxonomy',
            'url': 'http://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c'},
   BISON_DATASOURCE: {'name':  'ITIS Taxonomy',
            'url': 'http://www.itis.gov'},
   IDIGBIO_DATASOURCE: {'name': 'GBIF Backbone Taxonomy',
            'url': 'http://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c'}}

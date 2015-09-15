"""
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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

from LmServer.common.lmconstants import SPECIES_DATA_PATH
from LmServer.common.localconstants import DATA_PATH
from LmDbServer.common.localconstants import TAXONOMY_FILENAME, \
     OCCURRENCE_FILENAME, MARINE_FILENAME, PROVIDER_FILENAME, TSN_FILENAME , \
     BINOMIAL_FILENAME                      

# ............................................................................

MARINE_EXCLUDE_FILE = os.path.join(DATA_PATH, SPECIES_DATA_PATH, 
                                   MARINE_FILENAME)
OCC_DUMP_FILE = os.path.join(DATA_PATH, SPECIES_DATA_PATH, 
                             OCCURRENCE_FILENAME)
TAXONOMY_DUMP_FILE = os.path.join(DATA_PATH, SPECIES_DATA_PATH, 
                                  TAXONOMY_FILENAME)
PROVIDER_DUMP_FILE = os.path.join(DATA_PATH, SPECIES_DATA_PATH, 
                                  PROVIDER_FILENAME)
BISON_TSN_FILE = os.path.join(DATA_PATH, SPECIES_DATA_PATH, 
                              TSN_FILENAME)
IDIGBIO_BINOMIAL_FILE = os.path.join(DATA_PATH, SPECIES_DATA_PATH, 
                                     BINOMIAL_FILENAME)


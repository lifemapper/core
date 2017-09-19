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
try:
   from osgeo.ogr import wkbPolygon
except:
   wkbPolygon = 3

from LmCommon.common.lmconstants import LMFormat
from LmServer.common.localconstants import PID_PATH
from LmServer.common.lmconstants import SPECIES_DATA_PATH
from LmDbServer.common.localconstants import (GBIF_TAXONOMY_FILENAME, 
     GBIF_OCCURRENCE_FILENAME, GBIF_PROVIDER_FILENAME, BISON_TSN_FILENAME , 
     IDIG_OCCURRENCE_DATA, USER_OCCURRENCE_DATA)                  

# ............................................................................

BOOM_PID_FILE = os.path.join(PID_PATH, 'daboom.pid')
GBIF_DUMP_FILE = os.path.join(SPECIES_DATA_PATH, GBIF_OCCURRENCE_FILENAME)
GBIF_TAXONOMY_DUMP_FILE = os.path.join(SPECIES_DATA_PATH, GBIF_TAXONOMY_FILENAME)
GBIF_PROVIDER_DUMP_FILE = os.path.join(SPECIES_DATA_PATH, GBIF_PROVIDER_FILENAME)
BISON_TSN_FILE = os.path.join(SPECIES_DATA_PATH, BISON_TSN_FILENAME)

IDIG_OCCURRENCE_DIR = os.path.join(SPECIES_DATA_PATH, 
                                   IDIG_OCCURRENCE_DATA)
IDIG_OCCURRENCE_CSV = os.path.join(SPECIES_DATA_PATH, 
                                   IDIG_OCCURRENCE_DATA + LMFormat.CSV.ext)
IDIG_OCCURRENCE_META = os.path.join(SPECIES_DATA_PATH, 
                                    IDIG_OCCURRENCE_DATA + LMFormat.METADATA.ext)

USER_OCCURRENCE_CSV = os.path.join(SPECIES_DATA_PATH, 
                                   USER_OCCURRENCE_DATA + LMFormat.CSV.ext)
USER_OCCURRENCE_META = os.path.join(SPECIES_DATA_PATH, 
                                    USER_OCCURRENCE_DATA + LMFormat.METADATA.ext)

class SpeciesDatasource:
   """
   @summary: These are species data sources with defined data formats.
             The default or boom config file must specify DATASOURCE as one of 
             the specified strings., `Existing` indicates that existing OccurrenceSet ids 
             the provided input, and, with proper permissions, are used as-is 
             or copied to the User's data space.
   @ivar IDIGBIO: The default or boom config file must specify the filename
             IDIG_OCCURRENCE_DATA (without extension) pointing to a CSV file,
             and IDIG_OCCURRENCE_DATA_DELIMITER. If the GBIF_TAXONOMY_FILENAME 
             (with extension) is present, it will contain CSV data
             for the GBIF backbone taxonomy of the species in the data file and
             will be connected to the IDIGBIO data.
   @ivar BISON: The default or boom config file must specify the filename 
             BISON_TSN_FILENAME (with extension).  This file contains a 
             list of BISON TSNs that may be used to query BISON APIs for  
             species occurrence sets. 
   @ivar GBIF: The default or boom config file must specify 
             GBIF_OCCURRENCE_FILENAME, a CSV file (with extension)
             grouped by TaxonId.  Two additional files may be provided:
             1) GBIF_TAXONOMY_FILENAME (with extension) containing CSV data
             for the GBIF backbone taxonomy of the species in the data file; and
             2) GBIF_PROVIDER_FILENAME (with extension) containing CSV data
             for the data providers referenced in the data file.
   @ivar USER: The default or boom config file must specify USER_OCCURRENCE_DATA
             (without extension) which points to the basename of 2 files: 
             1) a .csv file of data and 2) a .meta file of metadata 
             describing the csv data.  The config file must also specify 
             USER_OCCURRENCE_DATA_DELIMITER.
   @ivar EXISTING: The default or boom config file must specify an 
             OCCURRENCE_ID_FILENAME containing OccurrenceSet database IDs 
             for public or user data to serve as input to a BOOM process. 
   """
   IDIGBIO = 'IDIGBIO'
   BISON = 'BISON'
   GBIF = 'GBIF'
   USER = 'USER'
   EXISTING = 'EXISTING'
# ...............................................
   @staticmethod
   def isUser(datasource):
      if datasource in (SpeciesDatasource.BISON, SpeciesDatasource.GBIF, 
                        SpeciesDatasource.IDIGBIO):
         return False
      return True
      

# Key must match DATASOURCE in config/config.ini
TAXONOMIC_SOURCE = {
   SpeciesDatasource.GBIF: {'name': 'GBIF Backbone Taxonomy',
            'url': 'http://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c'},
   SpeciesDatasource.BISON: {'name':  'ITIS Taxonomy',
            'url': 'http://www.itis.gov'},
   SpeciesDatasource.IDIGBIO: {'name': 'GBIF Backbone Taxonomy',
            'url': 'http://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c'}}

class TNCMetadata:
   """
   @summary: Metadata describing The Nature Conservancy Global Ecoregion data.
   """
   title = 'tnc_terr_ecoregions'
   author = 'The Nature Conservancy'
   isCategorical = True
   description = ' '.join(('Global Ecoregions, Major Habitat Types,',
   'Biogeographical Realms and The Nature Conservancy Terrestrial Assessment', 
   'Units as of December 14, 2009 Purpose: Developed originally by Olson, D. M.', 
   'and E. Dinerstein (2002), Bailey (1995) and Environment Canada (Wiken,', 
   '1986), these data layers were modified by The Nature Conservancy (TNC) to',
   'be used in its Biodiversity Planning exercises in the process known as',
   'Ecoregional Assessments. Several Ecoregions were modified from the',
   'originals by TNC staff developing the aforementioned assessments. The',
   'modifications are based on ecological, bio-physical and political',
   'rationales; most changes are noted in the accompanying documentation',
   '(attributes). Ecoregions in Canada and Mexico were modified mainly at the',
   'border with US territory, where TNC modified-Bailey (1995) ecoregions',
   'crossed over the country boundaries and the Olson, D. M. and E. Dinerstein',
   '(2002) and (Wiken, 1986) were replaced where the TNC modified-Bailey (1995)',
   'overlayed them. This layer was split from the terrestrial ecoregional',
   'assessment layer in June 2008.'))
   keywords = ['Terrestrial Ecoregions', 'Major Habitat Types', 
               'Biogeographic Realms', 'TNC', 'World', 'Global']
   url = 'http://maps.tnc.org'
   citation = ' '.join(('Olson, D. M. and E. Dinerstein. 2002. The Global 200:',
   'Priority ecoregions for global conservation. (PDF file) Annals of the',
   'Missouri Botanical Garden 89:125-126. -The Nature Conservancy, USDA Forest',
   'Service and U.S. Geological Survey, based on Bailey, Robert G. 1995.',
   'Description of the ecoregions of the United States (2nd ed.). Misc. Pub.',
   'No. 1391, Map scale 1:7,500,000. USDA Forest Service. 108pp. -The Nature',
   'Conservancy (2003), based on Wiken, E.B.(compiler). 1986. Terrestrial',
   'ecozones of Canada. Ecological Land Classification Series No. 19.',
   'Environment Canada, Hull, Que. 26 pp. + map.'))
   bbox = (-180.0, -90.0, 180.0, 83.0)
   ogrType = wkbPolygon
   valAttribute = 'WWF_MHTNAM'
   filename = 'tnc_terr_ecoregions'

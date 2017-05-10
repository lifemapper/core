"""
@summary: Module containing occurrence set processing functions
@author: Aimee Stewart / CJ Grady
@version: 4.0.0
@status: beta

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
import json
import os

from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger

# .............................................................................
def createBisonShapefile(url, outFile, bigFile, maxPoints):
   """
   @summary: Retrieves a BISON url, pulls in the data, and creates a shapefile
   @param url: The url to pull data from
   @param outFile: The file location to write the modelable occurrence set
   @param bigFile: The file location to write the full occurrence set 
   @param maxPoints: The maximum number of points to be included in the regular
                        shapefile
   """
   occAPI = BisonAPI.initFromUrl(url)
   occList = occAPI.getTSNOccurrences()
   count = len(occList)
   return parseCsvData(occList, ProcessType.BISON_TAXA_OCCURRENCE, outFile,  
                       bigFile, count, maxPoints)

# .............................................................................
def createGBIFShapefile(pointCsvFn, outFile, bigFile, reportedCount, maxPoints):
   """
   @summary: Parses a CSV blob from GBIF and saves it to a shapefile
   @param pointCsvFn: The file location of the CSV data to process
   @param outFile: The file location to write the modelable occurrence set
   @param bigFile: The file location to write the full occurrence set 
   @param reportedCount: The reported number of entries in the CSV file
   @param maxPoints: The maximum number of points to be included in the regular
                        shapefile
   """
   with open(pointCsvFn) as inF:
      csvInputBlob = inF.readlines()
   
   if len(csvInputBlob) == 0:
      raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                        "The provided CSV was empty")
   return parseCsvData(csvInputBlob, ProcessType.GBIF_TAXA_OCCURRENCE, outFile, 
                       bigFile, reportedCount, maxPoints)
   
# .............................................................................
def createIdigBioShapefile(taxonKey, outFile, bigFile, maxPoints):
   """
   @summary: Retrieves an iDigBio url, pulls in the data, and creates a 
                shapefile
   @param taxonKey: The GBIF taxonID (in iDigBio) for which to pull data
   @param outFile: The file location to write the modelable occurrence set
   @param bigFile: The file location to write the full occurrence set 
   @param maxPoints: The maximum number of points to be included in the regular
                        shapefile
   @todo: Change this back to GBIF TaxonID when iDigBio API is fixed!
   """
   occAPI = IdigbioAPI()
#    occList = occAPI.queryByGBIFTaxonId(taxonKey)
   # TODO: Un-hack this - here using canonical name instead
   occList = occAPI.queryBySciname(taxonKey)
   count = len(occList)
   return parseCsvData(occList, ProcessType.IDIGBIO_TAXA_OCCURRENCE, outFile, 
                       bigFile, count, maxPoints)
   
# .............................................................................
def createUserShapefile(pointCsvFn, meta, outFile, bigFile, maxPoints):
   """
   @summary: Processes a user-provided CSV dataset
   @param pointCsvFn: CSV file of points
   @param meta: A JSON string of metadata for these occurrences
   @param outFile: The file location to write the modelable occurrence set
   @param bigFile: The file location to write the full occurrence set 
   @param maxPoints: The maximum number of points to be included in the regular
                        shapefile
   """
   with open(pointCsvFn) as inF:
      csvInputBlob = inF.read()
      
   # Assume there is a header, could be sniffed if we want to
   count = len(csvInputBlob.split('\n')) - 2
   return parseCsvData(csvInputBlob, ProcessType.USER_TAXA_OCCURRENCE, outFile, 
                       bigFile, count, maxPoints, metadata=meta, isUser=True)
      
# .............................................................................
def parseCsvData(rawData, processType, outFile, bigFile, count, maxPoints,
                 metadata=None, isUser=False):
   """
   @summary: Parses a CSV-format dataset and saves it to a shapefile in the 
                specified location
   @param rawData: Raw occurrence data for processing
   @param processType: The Lifemapper process type to use for processing the CSV
   @param outFile: The file location to write the modelable occurrence set
   @param bigFile: The file location to write the full occurrence set 
   @param count: The number of records in the raw data
   @param maxPoints: The maximum number of points to be included in the regular
                        shapefile
   @param metadata: Metadata that can be used for processing the CSV
   @todo: handle write exception before writing dummy file? 
   """
   # TODO: evaluate logging here
   readyFilename(outFile, overwrite=True)
   readyFilename(bigFile, overwrite=True)
   if count <= 0:
      f = open(outFile, 'w')
      f.write('Zero data points')
      f.close()
      f = open(bigFile, 'w')
      f.write('Zero data points')
      f.close()
   else:
      logger = LmComputeLogger(os.path.basename(__file__))
      shaper = ShapeShifter(processType, rawData, count, logger=logger, 
                            metadata=metadata)
      shaper.writeOccurrences(outFile, maxPoints=maxPoints, bigfname=bigFile, 
                              isUser=isUser)
      if not os.path.exists(bigFile):
         f = open(bigFile, 'w')
         f.write('No excess data')
         f.close()
      

"""
import json
import os

from LmCommon.common.apiquery import *
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger
from LmServer.db.borgscribe import BorgScribe

from LmCommon.common.readyfile import readyFilename


logger = LmComputeLogger('crap')

from LmCompute.plugins.single.occurrences.csvOcc import *
scribe = BorgScribe(logger)
success = scribe.openConnections()

keys = {1967: 'Trichotria pocillum', 1034: 'Antiphonus conatus', 
2350: 'Antheromorpha', 8133: 'Scytonotus piger', 1422: 'Helichus suturalis', 
3799: 'Anacaena debilis', 1393: 'Discoderus papagonis', 653: 'Cicindela repanda', 
896: 'Cicindela purpurea', 1762: 'Cicindela tranquebarica', 
1419: 'Cicindela duodecimguttata', 3641: 'Cicindela punctulata', 
1818: 'Cicindela oregona', 895: 'Cicindela hirticollis', 
1298: 'Cicindela sexguttata', 1507: 'Cicindela ocellata', 
1219: 'Cicindela formosa', 1042: 'Amara obesa', 927: 'Brachinus elongatulus', 
1135: 'Brachinus mexicanus'}

lmapi = IdigbioAPI()

olist = lmapi.queryBySciname('Trichotria pocillum')
   
for key, name in keys.iteritems():
   olist = lmapi.queryByGBIFTaxonId(key)
   countkey = api.count_records(rq={'taxonid':key, 'basisofrecord': 'preservedspecimen'})
   countname = api.count_records(rq={'scientificname':name, 'basisofrecord': 'preservedspecimen'})
   print '{}: lm={}, iTaxonkey={}, iName={}'.format(key, len(olist), countkey, countname) 

occAPI = IdigbioAPI()
for taxonKey in keys:
   occList = occAPI.queryByGBIFTaxonId(taxonKey)
   count = len(occList)
   print '{}: {}'.format(taxonKey, count)
   
   
return parseCsvData(occList, ProcessType.IDIGBIO_TAXA_OCCURRENCE, outFile, 
                    bigFile, count, maxPoints)
 
"""
   

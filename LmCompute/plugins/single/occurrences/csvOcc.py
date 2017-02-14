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

from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.common.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCompute.common.lmObj import LmException

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
      csvInputBlob = inF.read()
   
   if len(csvInputBlob.strip()) == 0:
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
   """
   occAPI = IdigbioAPI()
   occList = occAPI.queryByGBIFTaxonId(taxonKey)
   count = len(occList)
   return parseCsvData(occList, ProcessType.IDIGBIO_TAXA_OCCURRENCE, outFile, 
                       bigFile, count, maxPoints)
   
# .............................................................................
def createUserShapefile(pointCsvFn, metadataFn, outFile, bigFile, maxPoints):
   """
   @summary: Processes a user-provided CSV dataset
   @param pointCsvFn: CSV file of points
   @param metadataFn: A JSON file location of metadata for these occurrences
   @param outFile: The file location to write the modelable occurrence set
   @param bigFile: The file location to write the full occurrence set 
   @param maxPoints: The maximum number of points to be included in the regular
                        shapefile
   """
   with open(pointCsvFn) as inF:
      csvInputBlob = inF.read()
      
   with open(metadataFn) as metaIn:
      metadata = json.load(metaIn)
   
   # Assume there is a header, could be sniffed if we want to
   count = len(csvInputBlob.split('\n')) -1 
   return parseCsvData(csvInputBlob, ProcessType.USER_TAXA_OCCURRENCE, outFile, 
                       bigFile, count, maxPoints, metadata=metadata)
      
# .............................................................................
def parseCsvData(rawData, processType, outFile, bigFile, count, maxPoints,
                 metadata=None):
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
   @todo: Fullfname needs to be handled by shaper
   """
   shaper = ShapeShifter(processType, rawData, count, metadata=metadata)
   shaper.writeOccurrences(outFile, maxPoints=maxPoints,
                           bigfname=bigFile)
   
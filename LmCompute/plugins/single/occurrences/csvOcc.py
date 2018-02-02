"""
@summary: Module containing occurrence set processing functions
@author: Aimee Stewart / CJ Grady
@version: 4.0.0
@status: beta

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
import os

from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType, ENCODING
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
   return parseCsvData(''.join(occList), ProcessType.BISON_TAXA_OCCURRENCE, outFile,  
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
   return parseCsvData(''.join(csvInputBlob), ProcessType.GBIF_TAXA_OCCURRENCE, outFile, 
                       bigFile, len(csvInputBlob), maxPoints)
   
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
   return parseCsvData(''.join(occList), ProcessType.IDIGBIO_TAXA_OCCURRENCE, outFile, 
                       bigFile, count, maxPoints)
   
# .............................................................................
def createUserShapefile(pointCsvFn, metadata, outFile, bigFile, maxPoints):
   """
   @summary: Processes a user-provided CSV dataset
   @param pointCsvFn: CSV file of points
   @param metadata: A file or dictionary of metadata for these occurrences
   @param outFile: The file location to write the modelable occurrence set
   @param bigFile: The file location to write the full occurrence set 
   @param maxPoints: The maximum number of points to be included in the regular
                        shapefile
   """
   with open(pointCsvFn) as inF:
      csvInputBlob = inF.read()
   
   # Assume no header
   lines = csvInputBlob.split('\n')
   origCount = len(lines)
   # remove non-encodeable lines
   cleanLines = []
   for ln in lines:
      try: 
         clnLn = ln.encode(ENCODING)
      except:
         pass
      else:
         cleanLines.append(clnLn)
   cleanCount = len(cleanLines)
   cleanBlob = '\n'.join(cleanLines)
   
   print('createUserShapefile, Cleaned blob of non-encodable lines, orig {}, new {}'
         .format(origCount, cleanCount))
      
   return parseCsvData(cleanBlob, ProcessType.USER_TAXA_OCCURRENCE, outFile, 
                       bigFile, cleanCount, maxPoints, metadata=metadata, isUser=True)
      
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
      logname, ext = os.path.splitext(os.path.basename(__file__))
      logger = LmComputeLogger(logname, addConsole=True)
      shaper = ShapeShifter(processType, rawData, count, logger=logger, 
                            metadata=metadata)
      shaper.writeOccurrences(outFile, maxPoints=maxPoints, bigfname=bigFile, 
                              isUser=isUser)
      if not os.path.exists(bigFile):
         f = open(bigFile, 'w')
         f.write('No excess data')
         f.close()
      

"""
$PYTHON /opt/lifemapper/LmCompute/tools/single/user_points.py \
        /share/lm/data/archive/biotaphytest/000/000/006/277/pt_6277.csv \
        /share/lm/data/archive/biotaphytest/dirtyNAPlants_1m.meta \
        /tmp/mf_6418/pt_6277/pt_6277.shp \
        /tmp/mf_6418/pt_6277/bigpt_6277.shp \
        500

$PYTHON /opt/lifemapper/LmCompute/tools/single/user_points.py \
        /share/lm/data/archive/biotaphytest/000/000/006/275/pt_6275.csv  \
        /share/lm/data/archive/biotaphytest/dirtyNAPlants_1m.meta \
        /tmp/mf_6416/pt_6275/pt_6275.shp  \
        /tmp/mf_6416/pt_6275/bigpt_6275.shp \
        500

from LmCompute.plugins.single.occurrences.csvOcc import createUserShapefile
import json
import os
from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.common.occparse import OccDataParser
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger
from LmCompute.plugins.single.occurrences.csvOcc import parseCsvData
from osgeo import ogr, osr
from LmCommon.common.lmconstants import *
from types import UnicodeType, StringType
import StringIO

pointsCsvFn = '/share/lm/data/archive/biotaphytest/000/000/006/275/pt_6275.csv'
metadataFile = '/share/lm/data/archive/biotaphytest/dirtyNAPlants_1m.meta'
outFile = '/tmp/mf_6416/pt_6275/pt_6275.shp'
bigFile = '/tmp/mf_6416/pt_6275/bigpt_6275.shp'
maxPoints = 500
from LmCommon.common.readyfile import readyFilename

# user_points.py
meta, _, doMatchHeader = OccDataParser.readMetadata(metadataFile)
# createUserShapefile(pointsCsvFn, meta, outFile, 
#                        bigFile, maxPoints)
isUser=True

with open(pointsCsvFn) as inF:
   csvInputBlob = inF.read()
   lines = csvInputBlob.split('\n')
   origCount = len(lines)
   cleanLines = []
   for ln in lines:
      try: 
         clnLn = ln.encode(ENCODING)
      except:
         pass
      else:
         cleanLines.append(clnLn)
   cleanCount = len(cleanLines)
   cleanBlob = '\n'.join(cleanLines)
   
print('Cleaned blob of non-encodable lines, orig {}, new {}'
         .format(origCount, cleanCount))

# parseCsvData(cleanBlob, ProcessType.USER_TAXA_OCCURRENCE, outFile, 
#              bigFile, cleanCount, maxPoints, metadata=meta, isUser=True)
processType = ProcessType.USER_TAXA_OCCURRENCE
rawData = cleanBlob
metadata = meta

readyFilename(outFile, overwrite=True)
readyFilename(bigFile, overwrite=True)
logger = LmComputeLogger(os.path.basename('crap'), addConsole=True)
shaper = ShapeShifter(processType, rawData, count, logger=logger, 
                      metadata=metadata)
op = shaper.op
op._csvreader, op._file = op.getReader(rawData, delimiter)
(cr, f) = (op._csvreader, op._file)

# shaper.writeOccurrences(outFile, maxPoints=maxPoints, bigfname=bigFile, 
#                         isUser=isUser)
(outfname, bigfname, overwrite) = (outFile, bigFile, True)
readyFilename(outfname, overwrite=overwrite)
discardIndices = []
outDs = shaper._createDataset(outfname)
outLyr = shaper._addUserFieldDef(outDs)
lyrDef = outLyr.GetLayerDefn()












meta, _, doMatchHeader = OccDataParser.readMetadata(metadataFile)

(rawData, processType, outFile, bigFile, count, maxPoints,
 metadata=None, isUser=False = (pointsCsvFn, meta, outFile, bigFile, 500)
                 
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
   logger = LmComputeLogger('crap', addConsole=True)
   shaper = ShapeShifter(processType, rawData, count, logger=logger, 
                         metadata=metadata)
   shaper.writeOccurrences(outFile, maxPoints=maxPoints, bigfname=bigFile, 
                           isUser=isUser)
   if not os.path.exists(bigFile):
      f = open(bigFile, 'w')
      f.write('No excess data')
      f.close()



fnames = []
for i in range(21):
   fnames.append(lyrDef.GetFieldDefn(i).name)

#####################################
loop
#####################################
recDict = shaper._getRecord()
print recDict

# shaper._createFillFeat(lyrDef, recDict, outLyr)
feat = ogr.Feature(lyrDef)

# _fillFeature
try:
   x = recDict[shaper.op.xIdx]
   y = recDict[shaper.op.yIdx]
except:
   x = recDict[shaper.xField]
   y = recDict[shaper.yField]


wkt = 'POINT ({} {})'.format(x, y)
feat.SetField(LM_WKT_FIELD, wkt)
geom = ogr.CreateGeometryFromWkt(wkt)
feat.SetGeometryDirectly(geom)
specialFields = (shaper.idField, shaper.linkField, shaper.providerKeyField, 
                 shaper.computedProviderField)


shaper._handleSpecialFields(feat, recDict)

for name in recDict.keys():
   if (name in feat.keys() and name not in specialFields):
      fldname = shaper._lookup(name)
      if fldname is not None:
         val = recDict[name]
         if val is not None and val != 'None':
            if isinstance(val, UnicodeType):
               val = fromUnicode(val)
            feat.SetField(fldname, val)


lyr.CreateFeature(feat)
feat.Destroy()
#####################################
                     
(minX, maxX, minY, maxY) = outLyr.GetExtent()
geomtype = lyrDef.GetGeomType()
fcount = outLyr.GetFeatureCount()
outDs.Destroy()
shaper._finishWrite(outFile, minX, maxX, minY, maxY, geomtype, fcount)

"""
   

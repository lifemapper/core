"""Module containing occurrence set processing functions
"""
import os

from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import (
    ENCODING, JobStatus, LMFormat, ProcessType)
    
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger

# # .............................................................................
# def createBisonShapefile(url, outFile, bigFile, maxPoints, log=None):
#     """
#     @summary: Retrieves a BISON url, pulls in the data, and creates a shapefile
#     @param url: The url to pull data from
#     @param outFile: The file location to write the modelable occurrence set
#     @param bigFile: The file location to write the full occurrence set 
#     @param maxPoints: The maximum number of points to be included in the regular
#                                 shapefile
#     @param log: If provided, use this logger
#     """
#     occAPI = BisonAPI.initFromUrl(url)
#     occList = occAPI.getTSNOccurrences()
#     count = len(occList)
#     return parseCsvData(''.join(occList), ProcessType.BISON_TAXA_OCCURRENCE, 
#                               outFile, bigFile, count, maxPoints, log=log)

# .............................................................................
def createGBIFShapefile(pointCsvFn, outFile, bigFile, maxPoints, log=None):
    """
    @summary: Parses a CSV blob from GBIF and saves it to a shapefile
    @param pointCsvFn: The file location of the CSV data to process
    @param outFile: The file location to write the modelable occurrence set
    @param bigFile: The file location to write the full occurrence set 
    @param reportedCount: The reported number of entries in the CSV file
    @param maxPoints: The maximum number of points to be included in the regular
                                shapefile
    @param log: If provided, use this logger
    """
    with open(pointCsvFn) as inF:
        csvInputBlob = inF.readlines()
    
    if len(csvInputBlob) == 0:
        raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                                "The provided CSV was empty")
    return parseCsvData(
        ''.join(csvInputBlob), ProcessType.GBIF_TAXA_OCCURRENCE, outFile,
        bigFile, len(csvInputBlob), maxPoints, log=log)
    
# # .............................................................................
# def createIdigBioShapefile(taxonKey, outFile, bigFile, maxPoints, log=None):
#     """
#     @summary: Retrieves an iDigBio url, pulls in the data, and creates a 
#                      shapefile
#     @param taxonKey: The GBIF taxonID (in iDigBio) for which to pull data
#     @param outFile: The file location to write the modelable occurrence set
#     @param bigFile: The file location to write the full occurrence set 
#     @param maxPoints: The maximum number of points to be included in the regular
#                                 shapefile
#     @param log: If provided, use this logger
#     @todo: Change this back to GBIF TaxonID when iDigBio API is fixed!
#     """
#     occAPI = IdigbioAPI()
# #     occList = occAPI.queryByGBIFTaxonId(taxonKey)
#     # TODO: Un-hack this - here using canonical name instead
#     occList = occAPI.queryBySciname(taxonKey)
#     count = len(occList)
#     return parseCsvData(''.join(occList), ProcessType.IDIGBIO_TAXA_OCCURRENCE, 
#                               outFile, bigFile, count, maxPoints, log=log)
    
# .............................................................................
def createUserShapefile(pointCsvFn, metadata, outFile, bigFile, maxPoints,
                        log=None):
    """
    @summary: Processes a user-provided CSV dataset
    @param pointCsvFn: CSV file of points
    @param metadata: A file or dictionary of metadata for these occurrences
    @param outFile: The file location to write the modelable occurrence set
    @param bigFile: The file location to write the full occurrence set 
    @param maxPoints: The maximum number of points to be included in the regular
                                shapefile
    @param log: If provided, use this logger
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
    
    msg = 'createUserShapefile, {}, orig {}, new {}'.format(
        'Cleaned blob of non-encodable lines', origCount, cleanCount)
    if log is not None:
        log.debug(msg)
    else:
        print(msg)
        
    return parseCsvData(
        cleanBlob, ProcessType.USER_TAXA_OCCURRENCE, outFile, bigFile,
        cleanCount, maxPoints, metadata=metadata, isUser=True, log=log)
        
# .............................................................................
def parseCsvData(rawData, processType, outFile, bigFile, count, maxPoints,
                      metadata=None, isUser=False, log=None):
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
    @param log: If provided, use this logger.  If not, will create new
    """
    # Initialize logger if necessary
    if log is None:
        logname, _ = os.path.splitext(os.path.basename(__file__))
        log = LmComputeLogger(logname, addConsole=True)
    
    # Ready file names
    readyFilename(outFile, overwrite=True)
    readyFilename(bigFile, overwrite=True)

    if count <= 0:
        status = JobStatus.LM_RAW_POINT_DATA_ERROR
        log.debug('Count was set to zero, return {}'.format(status))
    else:
        try:
            shaper = ShapeShifter(
                processType, rawData, count, logger=log, metadata=metadata)
            shaper.writeOccurrences(
                outFile, maxPoints=maxPoints, bigfname=bigFile, isUser=isUser)
            status = JobStatus.COMPUTED
            log.debug('Shaper wrote occurrences, return {}'.format(status))
            
            # Test generated shapefiles, throws exceptions if bad
            ShapeShifter.testShapefile(outFile)
            
            # Test the big shapefile if it exists
            if os.path.exists(bigFile):
                ShapeShifter.testShapefile(bigFile)
            
        except LmException, lme:
            log.debug(lme.msg)
            status = lme.code
            log.debug('Failed to write occurrences, return {}'.format(status))
            log.debug('Delete shapefiles if they exist')
            
            # TODO: Find a better way to delete (existing function maybe?)
            if os.path.exists(outFile):
                out_base = os.path.splitext(outFile)[0]
                for ext in LMFormat.SHAPE.getExtensions():
                    fn = '{}{}'.format(out_base, ext)
                    if os.path.exists(fn):
                        os.remove(fn)
                        
            if os.path.exists(bigFile):
                big_base = os.path.splitext(bigFile)[0]
                for ext in LMFormat.SHAPE.getExtensions():
                    fn = '{}{}'.format(big_base, ext)
                    if os.path.exists(fn):
                        os.remove(fn)
        except Exception, e:
            log.debug(str(e))
            status = JobStatus.LM_POINT_DATA_ERROR
            log.debug('Failed to write occurrences, return {}'.format(status))
            
    return status
        

"""
import os
from osgeo import ogr, osr
from types import UnicodeType, StringType
import StringIO
import json
import requests
from types import (BooleanType, DictionaryType, FloatType, IntType, ListType, 
                         StringType, TupleType, UnicodeType)
import urllib
import xml.etree.ElementTree as ET

from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType, ENCODING
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger

from LmCompute.plugins.single.occurrences.csvOcc import *
from LmCommon.common.lmconstants import *

maxPoints = 500

url='https://bison.usgs.gov/solr/occurrences/select?q=decimalLongitude%3A%5B-125+TO+-66%5D+AND+decimalLatitude%3A%5B24+TO+50%5D+AND+hierarchy_homonym_string%3A%2A-1006869-%2A+NOT+basisOfRecord%3Aliving+NOT+basisOfRecord%3Afossil' 
outfile='mf_17/pt_54/pt_54.shp' 
bigfile='mf_17/pt_54/bigpt_54.shp'

occAPI = BisonAPI.initFromUrl(url)
occAPI.queryByGet(outputType='json')

xmloutput = ET.fromstring(output)

occList = occAPI.getTSNOccurrences()
count = len(occList)
# parseCsvData(''.join(occList), ProcessType.BISON_TAXA_OCCURRENCE, outFile,  
#                           bigFile, count, maxPoints)

# createBisonShapefile(url, outFile, bigFile, maxPoints)


# user_points.py
meta, doMatchHeader = OccDataParser.readMetadata(metadataFile)
# createUserShapefile(pointsCsvFn, meta, outFile, 
#                                bigFile, maxPoints)
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
#                  bigFile, cleanCount, maxPoints, metadata=meta, isUser=True)
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
#                                 isUser=isUser)
(outfname, bigfname, overwrite) = (outFile, bigFile, True)
readyFilename(outfname, overwrite=overwrite)
discardIndices = []
outDs = shaper._createDataset(outfname)
outLyr = shaper._addUserFieldDef(outDs)
lyrDef = outLyr.GetLayerDefn()












meta, doMatchHeader = OccDataParser.readMetadata(metadataFile)

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
    

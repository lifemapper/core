"""Module containing occurrence set processing functions
"""
import os

# from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
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
            log.debug('Shaper wrote occurrences')
            
            # Test generated shapefiles, throws exceptions if bad
            status = JobStatus.COMPUTED
            goodData, featCount = ShapeShifter.testShapefile(outFile)
            if not goodData:
                raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                                  'Shaper tested, failed newly created occset')
            
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
import LmCompute.plugins.single.occurrences.csvOcc as csv_occ
import os
import logging
from LmServer.common.log import ScriptLogger


# from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import (
    ENCODING, JobStatus, LMFormat, ProcessType)
    
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger
import csv
import json
import os
from osgeo import ogr, osr
import StringIO
import subprocess
from types import UnicodeType, StringType

from LmCommon.common.lmconstants import (ENCODING, GBIF, GBIF_QUERY,
                    PROVIDER_FIELD_COMMON, 
                    LM_WKT_FIELD, ProcessType, JobStatus,
                    DWCNames, LMFormat, DEFAULT_EPSG)
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import readyFilename
from LmCommon.common.unicode import fromUnicode, toUnicode
from LmCompute.common.lmObj import LmException
try:
    from LmServer.common.lmconstants import BIN_PATH
except:
    from LmCompute.common.lmconstants import BIN_PATH




csv_fn = '/share/lm/data/archive/taffy2/000/000/396/221/pt_396221.csv'
metadata_fn = '/share/lm/data/archive/taffy2/heuchera.json'
out_fn = '/state/partition1/lmscratch/temp/test_out'
big_out_fn = '/state/partition1/lmscratch/temp/big_test_out.csv'


pointCsvFn = csv_fn
metadata = metadata_fn
outFile = out_fn
bigFile = big_out_fn
maxPoints=500
log = ScriptLogger('testshp', level=logging.DEBUG)

with open(pointCsvFn) as inF:
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

(rawData, processType, count, isUser, log) = (
 cleanBlob, ProcessType.USER_TAXA_OCCURRENCE, cleanCount, True, log)


readyFilename(outFile, overwrite=True)
readyFilename(bigFile, overwrite=True)

shaper = ShapeShifter(
    processType, rawData, count, logger=log, metadata=metadata)
    
(outfname, bigfname, isUser, overwrite) = (outFile, bigFile, isUser, True)


self = shaper
discardIndices = self._getSubset(maxPoints)

outDs = bigDs = None

outDs = self._createDataset(outfname)
outLyr = self._addUserFieldDef(outDs)

lyrDef = outLyr.GetLayerDefn()


recDict = self._getRecord()
# while recDict is not None:
#     self._createFillFeat(lyrDef, recDict, outLyr)
#     recDict = self._getRecord()

feat = ogr.Feature(lyrDef)
x = recDict[self.xField]
y = recDict[self.yField]

wkt = 'POINT ({} {})'.format(x, y)
feat.SetField(LM_WKT_FIELD, wkt)
geom = ogr.CreateGeometryFromWkt(wkt)
feat.SetGeometryDirectly(geom)
            
specialFields = (self.idField, self.linkField, self.providerKeyField, 
                              self.computedProviderField)
self._handleSpecialFields(feat, recDict)
for name in recDict.keys():
    if (name in feat.keys() and name not in specialFields):
        fldname = self._lookup(name)
        print name, fldname
        if fldname is not None:
            fldidx = feat.GetFieldIndex(str(fldname))
            val = recDict[name]
            if val is not None and val != 'None':
                if isinstance(val, UnicodeType):
                    val = fromUnicode(val)
                feat.SetField(fldidx, val)


lyr.CreateFeature(feat)
feat.Destroy()
                            
# Return metadata
(minX, maxX, minY, maxY) = outLyr.GetExtent()
geomtype = lyrDef.GetGeomType()
fcount = outLyr.GetFeatureCount()
# Close dataset and flush to disk
outDs.Destroy()
self._finishWrite(outfname, minX, maxX, minY, maxY, geomtype, fcount)
                        
        
  
# shaper.writeOccurrences(
#     outFile, maxPoints=maxPoints, bigfname=bigFile, isUser=isUser)
log.debug('Shaper wrote occurrences')

# Test generated shapefiles, throws exceptions if bad
status = JobStatus.COMPUTED
goodData, featCount = ShapeShifter.testShapefile(outFile)
if not goodData:
    raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                      'Shaper tested, failed newly created occset')

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

# csv_occ.parseCsvData(
#         cleanBlob, ProcessType.USER_TAXA_OCCURRENCE, outFile, bigFile,
#         cleanCount, maxPoints, metadata=metadata, isUser=True, log=log)
#         
# csv_occ.createUserShapefile(csv_fn, metadata_fn, out_fn, big_out_fn, 500)

"""
    

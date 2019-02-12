"""Module containing occurrence set processing functions
"""
import os

# from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import (
    ENCODING, JobStatus, LMFormat, ProcessType, GBIF_QUERY)
    
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger

# .............................................................................
def createGBIFShapefile(pointCsvFn, outFile, bigFile, maxPoints, log=None):
    """
    @summary: Parses a CSV blob from GBIF and saves it to a shapefile
    @param out_fname: The file location of the CSV data to process
    @param outFile: The file location to write the modelable occurrence set
    @param bigFile: The file location to write the full occurrence set 
    @param reportedCount: The reported number of entries in the CSV file
    @param maxPoints: The maximum number of points to be included in the regular
                                shapefile
    @param log: If provided, use this logger
    """
    with open(pointCsvFn) as inF:
        lines = inF.readlines()
         
    if len(lines) == 0:
        raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                                "The provided CSV was empty")
    return parseCsvData(
        ''.join(lines), ProcessType.GBIF_TAXA_OCCURRENCE, outFile,
        bigFile, len(lines), maxPoints, metadata=GBIF_QUERY.EXPORT_FIELDS, 
        log=log)
    
# .............................................................................
def createShapefile(csv_fname, metadata, out_fname, big_fname, max_points,
                    log=None):
    """
    @summary: Processes a CSV dataset
    @param csv_fname: CSV file of points
    @param metadata: A file or dictionary of metadata for these occurrences
    @param out_fname: The file location to write the modelable occurrence set
    @param big_fname: The file location to write the full occurrence set 
    @param max_points: The maximum number of points to be included in the regular
                       shapefile
    @param log: If provided, use this logger
    """
    with open(csv_fname) as inF:
        lines = inF.readlines()
    
    # Assume no header
    if len(lines) == 0:
        raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                                "The provided CSV was empty")
    
    # remove non-encodeable lines
    cleanLines = []
    for ln in lines:
        try: 
            clnLn = ln.encode(ENCODING)
        except:
            pass
        else:
            cleanLines.append(clnLn)
    cleanBlob = '\n'.join(cleanLines)
    
    msg = 'createUserShapefile, {}, orig {}, new {}'.format(
        'Cleaned blob of non-encodable lines', len(lines), len(cleanLines))
    if log is not None:
        log.debug(msg)
    else:
        print(msg)
        
    return parseCsvData(
        cleanBlob, ProcessType.USER_TAXA_OCCURRENCE, out_fname, big_fname,
        len(cleanLines), max_points, metadata=metadata, isGbif=False, log=log)
        
# .............................................................................
def parseCsvData(rawdata, metadata, out_fname, big_fname, count, max_points,
                 isGbif=False, log=None):
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
    readyFilename(out_fname, overwrite=True)
    readyFilename(big_fname, overwrite=True)

    if count <= 0:
        status = JobStatus.LM_RAW_POINT_DATA_ERROR
        log.debug('Count was set to zero, return {}'.format(status))
    else:
        try:
            shaper = ShapeShifter(rawdata, metadata, count, logger=log, 
                                  isGbif=isGbif)
            shaper.writeOccurrences(
                out_fname, maxPoints=max_points, bigfname=big_fname)
            log.debug('Shaper wrote occurrences')
            
            # Test generated shapefiles, throws exceptions if bad
            status = JobStatus.COMPUTED
            goodData, featCount = ShapeShifter.testShapefile(out_fname)
            if not goodData:
                raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                                  'Shaper tested, failed newly created occset')
            
            # Test the big shapefile if it exists
            if os.path.exists(big_fname):
                ShapeShifter.testShapefile(big_fname)
            
        except LmException, lme:
            log.debug(lme.msg)
            status = lme.code
            log.debug('Failed to write occurrences, return {}'.format(status))
            log.debug('Delete shapefiles if they exist')
            
            # TODO: Find a better way to delete (existing function maybe?)
            if os.path.exists(out_fname):
                out_base = os.path.splitext(out_fname)[0]
                for ext in LMFormat.SHAPE.getExtensions():
                    fn = '{}{}'.format(out_base, ext)
                    if os.path.exists(fn):
                        os.remove(fn)
                        
            if os.path.exists(big_fname):
                big_base = os.path.splitext(big_fname)[0]
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
from LmCompute.plugins.single.occurrences.csvOcc import *
import os
import logging
import csv
import json
import os
from osgeo import ogr, osr
import StringIO
import subprocess
from types import UnicodeType, StringType

from LmServer.common.log import ScriptLogger

from LmCommon.shapes.createshape2 import ShapeShifter
from LmCommon.common.lmconstants import (
    ENCODING, JobStatus, LMFormat, ProcessType)
    
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger

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


csv_fn = '/share/lm/data/archive/kubi/000/000/398/505/pt_398505.csv'
out_fn = '/state/partition1/lmscratch/temp/test_points'
big_out_fn = '/state/partition1/lmscratch/temp/big_test_points'
metadata = '/share/lmserver/data/species/gbif_occ_subset-2019.01.10.json'
delimiter = '\t'

(pointCsvFn, outFile, bigFile, maxPoints) = (csv_fn, out_fn, big_out_fn, 500)

with open(pointCsvFn) as inF:
    blob = inF.readlines()

(rawData, count) = (''.join(csvInputBlob), len(csvInputBlob))
rawdata = rawData
    
logname = 'csvocc_testing'
log = LmComputeLogger(logname, addConsole=True)
logger=log
    
readyFilename(outFile, overwrite=True)
readyFilename(bigFile, overwrite=True)

# shaper = ShapeShifter(processType, rawData, count, logger=log, metadata=metadata)

shaper = ShapeShifter(rawData, metadata, count, logger=log)
    
self = shaper
bigfname=bigFile 
outfname=outFile


shaper.writeOccurrences(
    outFile, maxPoints=maxPoints, bigfname=bigFile)
                            


status = JobStatus.COMPUTED
goodData, featCount = ShapeShifter.testShapefile(outFile)
if not goodData:
    raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                      'Shaper tested, failed newly created occset')


# x = parseCsvData(
#     ''.join(csvInputBlob), ProcessType.GBIF_TAXA_OCCURRENCE, outFile,
#     bigFile, len(csvInputBlob), maxPoints, metadata=GBIF_QUERY.EXPORT_FIELDS)


# csv_occ.createGBIFShapefile(csv_fn, out_fn, big_out_fn, 500)



"""
    

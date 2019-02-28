"""Module containing occurrence set processing functions
"""
import os

from LmCommon.common.lmconstants import (ENCODING, JobStatus, LMFormat)
from LmCommon.common.readyfile import get_unicodecsv_reader
from LmCommon.shapes.createshape import ShapeShifter
    
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger

# .............................................................................

# ...............................................
def _getLineAsString(csvreader, delimiter, recno):
    '''
    @summary: Return a CSV line as a single string while keeping track of the 
              line number and errors
    '''
    success = False
    linestr = None
    while not success and csvreader is not None:
        try:
            line = csvreader.next()
            if line:
                recno += 1
                linestr = delimiter.join(line)
            success = True
        except OverflowError, e:
            recno += 1
            print( 'Overflow on record {}, line {} ({})'
                                 .format(recno, csvreader.line, str(e)))
        except StopIteration:
            success = True
        except Exception, e:
            recno += 1
            print('Bad record on record {}, line {} ({})'
                                .format(recno, csvreader.line, e))
    return linestr, recno
            
# .............................................................................
def createShapefileFromCSV(csv_fname, metadata, out_fname, big_fname, max_points, 
                           delimiter='\t', is_gbif=False, log=None):
    """
    @summary: Parses a CSV-format dataset and saves it to a shapefile in the 
                     specified location
    @param csv_fname: Raw occurrence data for processing
    @param metadata: Metadata that can be used for processing the CSV
    @param out_fname: The file location to write the modelable occurrence set
    @param big_fname: The file location to write the full occurrence set 
    @param max_points: The maximum number of points to be included in the regular
                                shapefile
    @param is_gbif: Flag to indicate special processing for GBIF link, lookup keys 
    @param log: If provided, use this logger.  If not, will create new
    """    
    # Ready file names
    readyFilename(out_fname, overwrite=True)
    readyFilename(big_fname, overwrite=True)

    # Initialize logger if necessary
    if log is None:
        logname, _ = os.path.splitext(os.path.basename(__file__))
        log = LmComputeLogger(logname, addConsole=True)

    try:
        shaper = ShapeShifter(csv_fname, metadata, logger=log, 
                              delimiter=delimiter, isGbif=is_gbif)
        shaper.writeOccurrences(out_fname, maxPoints=max_points, 
                                bigfname=big_fname)
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
from LmCompute.plugins.single.occurrences.csvOcc import _prepareInputs, createShapefileFromCSV
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

from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import (
    ENCODING, JobStatus, LMFormat, ProcessType)
    
from LmCommon.common.readyfile import readyFilename, get_unicodecsv_reader
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



(csv_fname, out_fname, big_fname, log) = (url_fn_or_key, out_file, big_out_file, None)


rawdata, count, log = _prepareInputs(csv_fname, out_fname, big_fname, log)

shaper = ShapeShifter(rawdata, metadata, count, logger=log, 
                              isGbif=is_gbif)

# 
# shaper.writeOccurrences(
#     out_fname, maxPoints=max_points, bigfname=big_fname)
#     
self = shaper                        
(outfname, maxPoints, bigfname, overwrite) = (out_fname, max_points, big_fname, True) 

readyFilename(outfname, overwrite=overwrite)
outDs = self._createDataset(outfname)
outLyr = self._addUserFieldDef(outDs)
lyrDef = outLyr.GetLayerDefn()

recDict = self._getRecord()

self._createFillFeat(lyrDef, recDict, outLyr)
recDict = self._getRecord()
                            
# Return metadata
(minX, maxX, minY, maxY) = outLyr.GetExtent()
geomtype = lyrDef.GetGeomType()
fcount = outLyr.GetFeatureCount()
# Close dataset and flush to disk
outDs.Destroy()
self._finishWrite(outfname, minX, maxX, minY, maxY, geomtype, fcount)






# x = parseCsvData(
#     ''.join(csvInputBlob), ProcessType.GBIF_TAXA_OCCURRENCE, outFile,
#     bigFile, len(csvInputBlob), maxPoints, metadata=GBIF_QUERY.EXPORT_FIELDS)


# csv_occ.createGBIFShapefile(csv_fn, out_fn, big_out_fn, 500)



"""
    

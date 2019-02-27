"""
@summary: Module containing functions to create a shapefile from occurrence data
@author: Aimee Stewart

@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
from osgeo import ogr, osr
import subprocess
from types import UnicodeType, StringType

from LmCommon.common.lmconstants import (GBIF, PROVIDER_FIELD_COMMON, 
        LM_WKT_FIELD, JobStatus, DWCNames, LMFormat, DEFAULT_EPSG)
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import readyFilename
from LmCommon.common.unicode import fromUnicode, toUnicode
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger

try:
    from LmServer.common.lmconstants import BIN_PATH
except:
    from LmCompute.common.lmconstants import BIN_PATH

# .............................................................................
class ShapeShifter(object):
# .............................................................................
    """
    Class to write a shapefile from GBIF CSV output or BISON JSON output 
    """
# ............................................................................
# Constructor
# .............................................................................
    def __init__(self, csv_fname, metadata, logger=None, 
                 delimiter='\t', isGbif=False):
        """
        @param csv_fname: File containing CSV data of species occurrence records
        @param metadata: dictionary or filename containing JSON format metadata
        @param logger: logger for debugging output
        @param delimiter: delimiter of values in csv records
        @param isGbif: boolean flag to indicate whether data contains GBIF/DwC 
               fields.
        
        """
        if not os.path.exists(csv_fname):
            raise LmException(JobStatus.LM_RAW_POINT_DATA_ERROR, 
                              'Raw data file {} does not exist')
        if not metadata:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Failed to get metadata')
        if logger is None:
            logname, _ = os.path.splitext(os.path.basename(__file__))
            logger = LmComputeLogger(logname, addConsole=True)

        self._reader = None
        # If necessary, map provider dictionary keys to our field names
        self.lookupFields = None
        self._currRecum = 0
        count = sum(1 for line in open(csv_fname))
        self._recCount = count
        self.linkField = None
        self.linkUrl = None
        self.providerKeyField = None
        self.computedProviderField = None
        self.op = None
        
        if isGbif:
            self.linkField = GBIF.LINK_FIELD
            self.linkUrl = GBIF.LINK_PREFIX
            self.linkIdField = GBIF.ID_FIELD
            self.providerKeyField = GBIF.PROVIDER_FIELD
            self.computedProviderField = PROVIDER_FIELD_COMMON
        
        self.op = OccDataParser(logger, csv_fname, metadata, delimiter=delimiter,
                                        pullChunks=False)
        self.op.initializeMe()
        if self.op.header is not None:
            self._recCount = self._recCount - 1
        self.idField = self.op.idFieldName
        if self.op.xFieldName is not None: 
            self.xField = self.op.xFieldName
        else:
            self.xField = DWCNames.DECIMAL_LONGITUDE['SHORT']
        if self.op.yFieldName is not None:
            self.yField = self.op.yFieldName
        else:
            self.yField = DWCNames.DECIMAL_LATITUDE['SHORT']
        self.ptField = self.op.ptFieldName

        self.specialFields = (self.idField, self.linkField, self.providerKeyField, 
                              self.computedProviderField)


# .............................................................................
# Private functions
# .............................................................................
    def _createFillFeat(self, lyrDef, recDict, lyr):
        feat = ogr.Feature(lyrDef)
        try:
            self._fillFeature(feat, recDict)
        except Exception, e:
            print('Failed to _createFillFeat, e = {}'.format(fromUnicode(toUnicode(e))))
            raise e
        else:
            # Create new feature, setting FID, in this layer
            lyr.CreateFeature(feat)
            feat.Destroy()


# .............................................................................
# Public functions
# .............................................................................

# ...............................................
    @staticmethod
    def testShapefile(dlocation):
        """
        @todo: This should go into a LmCommon base layer class
        """
        goodData = True
        featCount = 0
        if dlocation is not None and os.path.exists(dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
            try:
                ds = drv.Open(dlocation)
            except Exception, e:
                goodData = False
            else:
                try:
                    slyr = ds.GetLayer(0)
                except Exception, e:
                    goodData = False
                else:  
                    featCount = slyr.GetFeatureCount()
        if not goodData: 
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                                    'Failed to open dataset or layer {}'.format(dlocation))
        elif featCount == 0:
            raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                                    'Failed to create shapefile with > 0 points {}'.format(dlocation))
        return goodData, featCount

    # .............................................................................
    def writeOccurrences(self, outfname, maxPoints=None, bigfname=None, 
                         overwrite=True):
        if not readyFilename(outfname, overwrite=overwrite):
            raise LmException('{} is not ready for write (overwrite={})'.format
                                    (outfname, overwrite))
        discardIndices = self._getSubset(maxPoints)
        # Create empty datasets with field definitions
        outDs = bigDs = None
        try:
            outDs = self._createDataset(outfname)
            outLyr = self._addUserFieldDef(outDs)
            lyrDef = outLyr.GetLayerDefn()

            # Do we need a BIG dataset?
            if len(discardIndices) > 0 and bigfname is not None:
                if not readyFilename(bigfname, overwrite=overwrite):
                    raise LmException('{} is not ready for write (overwrite={})'
                                            .format(bigfname, overwrite))
                bigDs = self._createDataset(bigfname)
                bigLyr = self._addUserFieldDef(bigDs)
        except Exception, e:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                                    'Unable to create field definitions ({})'.format(e))
        # Fill datasets with records
        try:
            # Loop through records
            recDict = self._getRecord()
            while recDict is not None:
                try:
                    # Add non-discarded features to regular layer
                    if self._currRecum not in discardIndices:
                        self._createFillFeat(lyrDef, recDict, outLyr)
                    # Add all features to optional "Big" layer
                    if bigDs is not None:
                        self._createFillFeat(lyrDef, recDict, bigLyr)
                except Exception, e:
                    print('Failed to create record ({})'.format((e)))
                recDict = self._getRecord()
                                        
            # Return metadata
            (minX, maxX, minY, maxY) = outLyr.GetExtent()
            geomtype = lyrDef.GetGeomType()
            fcount = outLyr.GetFeatureCount()
            # Close dataset and flush to disk
            outDs.Destroy()
            self._finishWrite(outfname, minX, maxX, minY, maxY, geomtype, fcount)
                                    
            # Close Big dataset and flush to disk
            if bigDs is not None:
                bigcount = bigLyr.GetFeatureCount()
                bigDs.Destroy()
                self._finishWrite(bigfname, minX, maxX, minY, maxY, geomtype, bigcount)
                
        except LmException, e:
            raise
        except Exception, e:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                                    'Unable to read or write data ({})'
                                    .format(e))
                
# .............................................................................
# Private functions
# .............................................................................
    # .............................................................................
    def _createDataset(self, fname):
        drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
        newDs = drv.CreateDataSource(fname)
        if newDs is None:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                                    'Dataset creation failed for {}'.format(fname))
        return newDs

    # .............................................................................
    def _getSubset(self, maxPoints):  
        discardIndices = []
        if maxPoints is not None and self._recCount > maxPoints: 
            from random import shuffle
            discardCount = self._recCount - maxPoints
            allIndices = range(self._recCount)
            shuffle(allIndices)
            discardIndices = allIndices[:discardCount]
        return discardIndices
    
    # .............................................................................
    def _finishWrite(self, outfname, minX, maxX, minY, maxY, geomtype, fcount):
        print('Closed/wrote {}-feature dataset {}'.format(fcount, outfname))
        
        # Write shapetree index for faster access
        try:
            shpTreeCmd = os.path.join(BIN_PATH, "shptree")
            retcode = subprocess.call([shpTreeCmd, "%s" % outfname])
            if retcode != 0: 
                print 'Unable to create shapetree index on %s' % outfname
        except Exception, e:
            print 'Unable to create shapetree index on %s: %s' % (outfname, str(e))
        
        # Test output data
        goodData, featCount = self.testShapefile(outfname)
        if not goodData: 
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                                    'Failed to create shapefile {}'.format(outfname))
        elif featCount == 0:
            raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                                    'Failed to create shapefile {}'.format(outfname))
        
        # Write metadata as JSON
        basename, ext = os.path.splitext(outfname)
        self._writeMetadata(basename, geomtype, fcount, minX, minY, maxX, maxY)

    # ...............................................
    def _writeMetadata(self, basename, geomtype, count, minx, miny, maxx, maxy):
        metaDict = {'ogrformat': LMFormat.getDefaultOGR().driver, 'geomtype': geomtype, 
                        'count': count,  'minx': minx, 'miny': miny, 'maxx': maxx, 
                        'maxy': maxy}
        with open(basename+'.meta', 'w') as outfile:
            json.dump(metaDict, outfile)
        
    # ...............................................
    def _lookup(self, name):
        if self.lookupFields is not None:
            try:
                val = self.lookupFields[name]
                return val
            except Exception, e:
                return None
        else:
            return name
    
    # ...............................................
    def _getRecord(self):
        success = False
        tmpDict = {}
        recDict = None
        badRecCount = 0
        # skip lines w/o valid coordinates
        while not success and not self.op.closed:
            try:
                self.op.pullNextValidRec()
                thisrec = self.op.currLine
                if thisrec is not None:
                    x, y = OccDataParser.getXY(thisrec, self.op.xIdx, self.op.yIdx, 
                                                        self.op.ptIdx)
                    # Unique identifier field is not required, default to FID
                    # ignore records without valid lat/long; all occ jobs contain these fields
                    tmpDict[self.xField] = float(x)
                    tmpDict[self.yField] = float(y)
                    success = True
            except StopIteration, e:
                success = True
            except OverflowError, e:
                badRecCount += 1
            except ValueError, e:
                badRecCount += 1
            except Exception, e:
                badRecCount += 1
                print('Exception reading line {} ({})'.format(self.op.currRecnum, 
                                                                      fromUnicode(toUnicode(e))))
        if success:
            for idx, vals in self.op.columnMeta.iteritems():
                if vals is not None and idx not in (self.op.xIdx, self.op.yIdx):
                    fldname = self.op.columnMeta[idx][OccDataParser.FIELD_NAME_KEY]
                    tmpDict[fldname] = thisrec[idx]
            recDict = tmpDict
        
        if badRecCount > 0:
            print('Skipped over {} bad records'.format(badRecCount))
        
        if recDict is not None:
            self._currRecum += 1

        return recDict

    # ...............................................
    def _addUserFieldDef(self, newDataset):
        spRef = osr.SpatialReference()
        spRef.ImportFromEPSG(DEFAULT_EPSG)
        maxStrlen = LMFormat.getStrlenForDefaultOGR()
     
        newLyr = newDataset.CreateLayer('points', geom_type=ogr.wkbPoint, srs=spRef)
        if newLyr is None:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                                    'Layer creation failed')
        
        for idx, vals in self.op.columnMeta.iteritems():
            if vals is not None:
                fldname = str(vals[OccDataParser.FIELD_NAME_KEY])
                fldtype = vals[OccDataParser.FIELD_TYPE_KEY] 
                fldDef = ogr.FieldDefn(fldname, fldtype)
                if fldtype == ogr.OFTString:
                    fldDef.SetWidth(maxStrlen)
                returnVal = newLyr.CreateField(fldDef)
                if returnVal != 0:
                    raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                                         'Failed to create field {}'.format(fldname))
                
        # Add wkt field
        fldDef = ogr.FieldDefn(LM_WKT_FIELD, ogr.OFTString)
        fldDef.SetWidth(maxStrlen)
        returnVal = newLyr.CreateField(fldDef)
        if returnVal != 0:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                                    'Failed to create field {}'.format(fldname))
        
        return newLyr
            

    # ...............................................
    def _handleSpecialFields(self, feat, recDict):
        try:
            # Find or assign a (dataset) unique id for each point
            if self.idField is not None:
                try:
                    ptid = recDict[self.idField]
                except:
                    # Set LM added id field
                    ptid = self._currRecum 
                    feat.SetField(self.idField, ptid)

            # If data has a Url link field
            if self.linkField is not None:
                try:
                    searchid = recDict[self.linkField]
                except:
                    pass
                else:
                    pturl = '{}{}'.format(self.linkUrl, str(searchid))
                    feat.SetField(self.linkField, pturl)
                            
            # If data has a provider field and value to be resolved
            if self.computedProviderField is not None:
                prov = ''
                try:
                    prov = recDict[self.providerKeyField]
                except:
                    pass
                if not (isinstance(prov, StringType) or isinstance(prov, UnicodeType)):
                    prov = ''
                feat.SetField(self.computedProviderField, prov)

        except Exception, e:
            print('Failed to set optional field in rec {}, e = {}'.format(str(recDict), e))
            raise e

    # ...............................................
    def _fillFeature(self, feat, recDict):
        """
        @note: This *should* return the modified feature
        """
        try:
            x = recDict[self.op.xIdx]
            y = recDict[self.op.yIdx]
        except:
            x = recDict[self.xField]
            y = recDict[self.yField]
            
        try:
            # Set LM added fields, geometry, geomwkt
            wkt = 'POINT ({} {})'.format(x, y)
            feat.SetField(LM_WKT_FIELD, wkt)
            geom = ogr.CreateGeometryFromWkt(wkt)
            feat.SetGeometryDirectly(geom)
        except Exception, e:
            print('Failed to create/set geometry, e = {}'.format(e))
            raise e
            
        self._handleSpecialFields(feat, recDict)

        try:
            # Add values out of the line of data
            for name in recDict.keys():
                if (name in feat.keys() and name not in self.specialFields):
                    # Handles reverse lookup for BISON metadata
                    # TODO: make this consistent!!!
                    # For User data, name = fldname
                    fldname = self._lookup(name)
                    if fldname is not None:
                        fldidx = feat.GetFieldIndex(str(fldname))
                        val = recDict[name]
                        if val is not None and val != 'None':
                            if isinstance(val, UnicodeType):
                                val = fromUnicode(val)
                            feat.SetField(fldidx, val)
        except Exception, e:
            print('Failed to fillFeature with recDict {}, e = {}'.format(str(recDict), e))
            raise e
        
# ...............................................
if __name__ == '__main__':
    print ('__main__ is not implemented')

"""
from osgeo import ogr, osr
import StringIO
import subprocess
from types import ListType, TupleType, UnicodeType, StringType

from LmBackend.common.occparse import OccDataParser
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import (ENCODING, BISON, BISON_QUERY,
                    GBIF, GBIF_QUERY, IDIGBIO, IDIGBIO_QUERY, PROVIDER_FIELD_COMMON, 
                    LM_ID_FIELD, LM_WKT_FIELD, ProcessType, JobStatus,
                    DWCNames, LMFormat)
from LmServer.common.log import ScriptLogger
import ast

log = LmComputeLogger('csvocc_testing', addConsole=True)

# ......................................................
# User test
csvfname = '/share/lm/data/archive/taffy/000/000/396/487/pt_396487.csv'
metafname = '/share/lm/data/archive/taffy/heuchera.json'
outfname = '/state/partition1/lmscratch/temp/testpoints.shp'
bigfname = '/state/partition1/lmscratch/temp/testpoints_big.shp'

with open(csvfname, 'r') as f:
    blob = f.read()

with open(csvfname, 'r') as f:
    blob2 = f.readlines()

# with open(metafname, 'r') as f:
#     metad = ast.literal_eval(f.read())

shaper = ShapeShifter(blob, metafname, logger=logger)
shaper.writeOccurrences(outfname, maxPoints=50, bigfname=bigfname)


# ......................................................
# GBIF test
csv_fn = '/share/lm/data/archive/kubi/000/000/398/505/pt_398505.csv'
out_fn = '/state/partition1/lmscratch/temp/test_points'
big_out_fn = '/state/partition1/lmscratch/temp/big_test_points'
metadata = '/share/lmserver/data/species/gbif_occ_subset-2019.01.10.json'
delimiter = '\t'
maxPoints = 500
readyFilename(out_fn, overwrite=True)
readyFilename(big_out_fn, overwrite=True)


with open(csv_fn) as inF:
    csvInputBlob = inF.readlines()

rawdata = ''.join(csvInputBlob)
count = len(csvInputBlob)
    
log = LmComputeLogger('csvocc_testing', addConsole=True)
    
shaper = ShapeShifter(rawData, metadata, count, logger=log, delimiter='\t', isGbif=True)
shaper.writeOccurrences(out_fn, maxPoints=maxPoints, bigfname=big_out_fn)
                            
status = JobStatus.COMPUTED
goodData, featCount = ShapeShifter.testShapefile(outFile)

# ......................................................
    
"""
"""
@summary: Module containing functions to create a shapefile from occurrence data
@author: Aimee Stewart

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
import csv
import glob
import json
import os
from osgeo import ogr, osr
import StringIO
import subprocess
from types import UnicodeType, StringType

from LmCommon.common.lmconstants import (ENCODING, BISON, BISON_QUERY,
               GBIF, GBIF_QUERY, IDIGBIO, IDIGBIO_QUERY, PROVIDER_FIELD_COMMON, 
               LM_ID_FIELD, LM_WKT_FIELD, ProcessType, JobStatus,
               DWCNames, LMFormat, DEFAULT_EPSG)
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.readyfile import readyFilename
from LmCommon.common.unicode import fromUnicode, toUnicode
from LmCompute.common.lmObj import LmException
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
   def __init__(self, processType, rawdata, count, logger=None, metadata=None, 
                delimiter=','):
      """
      @param processType: ProcessType constant, either GBIF_TAXA_OCCURRENCE,
                          BISON_TAXA_OCCURRENCE or IDIGBIO_TAXA_OCCURRENCE  
      @param rawdata: Either csv blob of GBIF, iDigBio, or User data 
                      or list of dictionary records of BISON data
      """
      self._reader = None
      # If necessary, map provider dictionary keys to our field names
      self.lookupFields = None
      self._currRecum = 0
      self._recCount = count
      self.processType = processType
      self.rawdata = rawdata
      self.linkField = None
      self.linkUrl = None
      self.computedProviderField = None
      self.op = None
      
      # All raw Occdata must contain ShortDWCNames.DECIMAL_LATITUDE and 
      #                              ShortDWCNames.DECIMAL_LONGITUDE
      if processType == ProcessType.USER_TAXA_OCCURRENCE:
         if not logger:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Failed to get a logger')
         if not metadata:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Failed to get metadata')
         self.op = OccDataParser(logger, rawdata, metadata, delimiter=delimiter)
         self.op.initializeMe()
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

      elif processType == ProcessType.GBIF_TAXA_OCCURRENCE:
         self.dataFields = GBIF_QUERY.EXPORT_FIELDS
         self.idField = GBIF.ID_FIELD
         self.linkField = GBIF.LINK_FIELD
         self.linkUrl = GBIF.LINK_PREFIX
         self.linkIdField = GBIF.ID_FIELD
         self.providerKeyField = GBIF.PROVIDER_FIELD
         self.computedProviderField = PROVIDER_FIELD_COMMON
         self.xField = DWCNames.DECIMAL_LONGITUDE['SHORT']
         self.yField = DWCNames.DECIMAL_LATITUDE['SHORT']
         self._reader = self._getCSVReader()
         
      elif processType == ProcessType.IDIGBIO_TAXA_OCCURRENCE:
         self.dataFields = IDIGBIO_QUERY.RETURN_FIELDS
         self.lookupFields = self._mapAPIResponseNames()
         self.idField = IDIGBIO.ID_FIELD
         self.linkField = IDIGBIO.LINK_FIELD
         self.linkUrl = IDIGBIO.LINK_PREFIX
         self.linkIdField = IDIGBIO.ID_FIELD
         self.computedProviderField = PROVIDER_FIELD_COMMON
         self.xField = DWCNames.DECIMAL_LONGITUDE['SHORT']
         self.yField = DWCNames.DECIMAL_LATITUDE['SHORT']

      elif processType == ProcessType.BISON_TAXA_OCCURRENCE:
         self.dataFields = BISON_QUERY.RESPONSE_FIELDS
         self.lookupFields = self._mapAPIResponseNames()
         self.linkField = BISON.LINK_FIELD
         self.linkUrl = BISON.LINK_PREFIX
         self.linkIdField = DWCNames.OCCURRENCE_ID['SHORT']
         self.idField = LM_ID_FIELD  
         self.xField = self._lookupReverse(DWCNames.DECIMAL_LONGITUDE['SHORT'])
         self.yField = self._lookupReverse(DWCNames.DECIMAL_LATITUDE['SHORT'])
         
      else:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Invalid processType {}'.format(processType))

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
   # .............................................................................
   @staticmethod
   def getOgrFieldType(typeString):
      typestr = typeString.lower()
      if typestr == 'integer':
         return ogr.OFTInteger
      elif typestr == 'string':
         return ogr.OFTString
      elif typestr == 'real':
         return ogr.OFTReal
      else:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Unsupported field type {} (integer, string, or real)'
                           .format(typeString))

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
                           'Failed to create shapefile {}'.format(dlocation))
      elif featCount == 0:
         raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                           'Failed to create shapefile {}'.format(dlocation))
      return goodData, featCount

   # .............................................................................
   def writeOccurrences(self, outfname, maxPoints=None, bigfname=None, 
                        isUser=False, overwrite=True):
      if not readyFilename(outfname, overwrite=overwrite):
         raise LmException('{} is not ready for write (overwrite={})'.format
                           (outfname, overwrite))
      discardIndices = self._getSubset(maxPoints)
      # Create empty datasets with field definitions
      outDs = bigDs = None
      try:
         outDs = self._createDataset(outfname)
         if isUser:
            outLyr = self._addUserFieldDef(outDs)
         else:
            outLyr = self._addFieldDef(outDs)
         lyrDef = outLyr.GetLayerDefn()

         # Do we need a BIG dataset?
         if len(discardIndices) > 0 and bigfname is not None:
            if not readyFilename(bigfname, overwrite=overwrite):
               raise LmException('{} is not ready for write (overwrite={})'
                                 .format(bigfname, overwrite))
            bigDs = self._createDataset(bigfname)
            if isUser:
               bigLyr = self._addUserFieldDef(bigDs)
            else:
               bigLyr = self._addFieldDef(bigDs)
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
            
   # .............................................................................
   def _mapAPIResponseNames(self):
      lookupDict = {}
      for bisonkey, flddesc in self.dataFields.iteritems():
         if flddesc is not None:
            lookupDict[bisonkey] = flddesc[0]
      return lookupDict

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
   def _lookupReverse(self, name):
      if self.lookupFields is not None:
         for key, val in self.lookupFields.iteritems():
            if val == name:
               return key
         # Not found
         return None
      else:
         return name

   # ...............................................
   def _getCSVReader(self, header=False):
      if header:
         fldnames = None
      else:
         fldnames = []
         idxs = self.dataFields.keys()
         idxs.sort()
         for idx in idxs:
            fldnames.append(self.dataFields[idx][0])
      csvData = StringIO.StringIO()
      csvData.write(fromUnicode(toUnicode(self.rawdata, encoding=ENCODING),
                                encoding=ENCODING))
      csvData.seek(0)
      reader = csv.DictReader(csvData, fieldnames=fldnames)
      return reader
   
   # ...............................................
   def _getRecord(self):
      if self.processType == ProcessType.USER_TAXA_OCCURRENCE:
         recDict = self._getUserCSVRec() 
      elif self.processType == ProcessType.GBIF_TAXA_OCCURRENCE:
         recDict = self._getCSVRec()
      else:
         # get BISON from web service
         recDict = self._getAPIResponseRec()
      if recDict is not None:
         self._currRecum += 1
      return recDict
      
   # ...............................................
   def _getAPIResponseRec(self):
      """
      @note: We modify BISON returned fieldnames, they are too long for shapefiles
      """
      recDict = None
      success = False
      badRecCount = 0
      while not success and len(self.rawdata) > 0: 
         try:
            tmpDict = self.rawdata.pop()
         except:
            # End of data
            success = True
         else:
            try:
               float(tmpDict[self.xField])
               float(tmpDict[self.yField])
            except Exception, e:
               badRecCount += 1
            else:
               success = True
               recDict = tmpDict
      if badRecCount > 0:
         print('Skipped over {} bad records'.format(badRecCount))
      return recDict
   
   # ...............................................
   def _getUserCSVRec(self):
      success = False
      tmpDict = {}
      recDict = None
      badRecCount = 0
      # skip lines w/o valid coordinates
      while not success and not self.op.eof():
         try:
            self.op.pullNextValidRec()
            thisrec = self.op.currLine
            if not self.op.eof():
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
         for idx, vals in self.op.fieldIndexMeta.iteritems():
            if vals is not None and idx not in (self.op.xIdx, self.op.yIdx):
               fldname = self.op.fieldIndexMeta[idx][OccDataParser.FIELD_NAME_KEY]
               tmpDict[fldname] = thisrec[idx]
         recDict = tmpDict
      if badRecCount > 0:
         print('Skipped over {} bad records'.format(badRecCount))
      return recDict

   # ...............................................
   def _getCSVRec(self):
      success = False
      recDict = None
      badRecCount = 0
      # skip bad lines
      while not success:
         try:
            tmpDict = self._reader.next()
            # ignore records without valid lat/long; all occ jobs contain these fields
            tmpDict[DWCNames.DECIMAL_LATITUDE['SHORT']] = \
                  float(tmpDict[DWCNames.DECIMAL_LATITUDE['SHORT']])
            tmpDict[DWCNames.DECIMAL_LONGITUDE['SHORT']] = \
                  float(tmpDict[DWCNames.DECIMAL_LONGITUDE['SHORT']])
            success = True
            recDict = tmpDict
         except StopIteration, e:
            success = True
         except OverflowError, e:
            badRecCount += 1
         except ValueError, e:
            badRecCount += 1
         except Exception, e:
            print('Exception reading line {} ({})'.format(self._currRecum, 
                                             fromUnicode(toUnicode(e))))
            badRecCount += 1
#             success = True
      if badRecCount > 0:
         print('Skipped over {} bad records'.format(badRecCount))
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
      
      for idx, vals in self.op.fieldIndexMeta.iteritems():
         if vals is not None:
            fldname = vals[OccDataParser.FIELD_NAME_KEY]
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
   def _addFieldDef(self, newDataset):
      spRef = osr.SpatialReference()
      spRef.ImportFromEPSG(DEFAULT_EPSG)
    
      newLyr = newDataset.CreateLayer('points', geom_type=ogr.wkbPoint, srs=spRef)
      if newLyr is None:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                           'Layer creation failed')
       
      for fielddesc in self.dataFields.values():
         if fielddesc is not None:
            fldname = fielddesc[0]
            fldtype = fielddesc[1]
            fldDef = ogr.FieldDefn(fldname, fldtype)
            if fldtype == ogr.OFTString:
               fldDef.SetWidth(LMFormat.getStrlenForDefaultOGR())
            returnVal = newLyr.CreateField(fldDef)
            if returnVal != 0:
               raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                                 'Failed to create field {}'.format(fldname))
            
      # Add wkt field to all
      fldDef = ogr.FieldDefn(LM_WKT_FIELD, ogr.OFTString)
      fldDef.SetWidth(LMFormat.getStrlenForDefaultOGR())
      returnVal = newLyr.CreateField(fldDef)
      if returnVal != 0:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Failed to create field{}'.format(fldname))
      
      # Add URL field to GBIF/iDigBio data
      if self.linkField is not None:
         fldDef = ogr.FieldDefn(self.linkField, ogr.OFTString)
         fldDef.SetWidth(LMFormat.getStrlenForDefaultOGR())
         returnVal = newLyr.CreateField(fldDef)
         if returnVal != 0:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Failed to create field {}'.format(self.linkField))
            
      # Add Provider field to GBIF/iDigBio data (for resolution from key or attribution)
      if self.computedProviderField is not None:
         fldDef = ogr.FieldDefn(self.computedProviderField, ogr.OFTString)
         fldDef.SetWidth(LMFormat.getStrlenForDefaultOGR())
         returnVal = newLyr.CreateField(fldDef)
         if returnVal != 0:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Failed to create field {}'
                              .format(self.computedProviderField))
      # Add id field to BISON data
      if self.processType == ProcessType.BISON_TAXA_OCCURRENCE:
         fldDef = ogr.FieldDefn(LM_ID_FIELD, ogr.OFTString)
         returnVal = newLyr.CreateField(fldDef)
         if returnVal != 0:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Failed to create field {}'.format(LM_ID_FIELD))
    
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
         
      specialFields = (self.idField, self.linkField, self.providerKeyField, 
                       self.computedProviderField)
      self._handleSpecialFields(feat, recDict)

      try:
         # Add values out of the line of data
         for name in recDict.keys():
            if (name in feat.keys() and name not in specialFields):
               # Handles reverse lookup for BISON metadata
               # TODO: make this consistent!!!
               # For User data, name = fldname
               fldname = self._lookup(name)
               if fldname is not None:
                  val = recDict[name]
                  if val is not None and val != 'None':
                     if isinstance(val, UnicodeType):
                        val = fromUnicode(val)
                     feat.SetField(fldname, val)
      except Exception, e:
         print('Failed to fillFeature with recDict {}, e = {}'.format(str(recDict), e))
         raise e
      
# ...............................................
if __name__ == '__main__':
   from LmCommon.common.apiquery import IdigbioAPI, BisonAPI
   gbif = idigbio = bison = False
   outfilename = '/tmp/testpoints.shp'
   subsetOutfilename = '/tmp/testpoints_sub.shp'
   
   if os.path.exists(outfilename):
      import glob
      basename, ext = os.path.splitext(outfilename)
      fnames = glob.glob(basename + '*')
      for fname in fnames:
         print('Removing {}'.format(fname))
         os.remove(fname)
   if gbif:
      testFname = '/opt/lifemapper/LmCommon/tests/data/gbif_chunk.csv'      
      f = open(testFname, 'r')
      datachunk = f.read()
      f.close()
   
      count = len(datachunk.split('\n'))
      shaper = ShapeShifter(ProcessType.GBIF_TAXA_OCCURRENCE, datachunk, count)
      shaper.writeOccurrences(outfilename, maxPoints=20, 
                              subsetfname=subsetOutfilename)

   if idigbio:
      taxid = 2437967
      occAPI = IdigbioAPI()
      occList = occAPI.queryByGBIFTaxonId(taxid)
       
      count = len(occList)
       
      shaper = ShapeShifter(ProcessType.IDIGBIO_TAXA_OCCURRENCE, occList, count)
      shaper.writeOccurrences(outfilename, maxPoints=40, 
                              subsetfname=subsetOutfilename)
      
   if bison:
      url = 'http://bison.usgs.ornl.gov/solrproduction/occurrences/select?q=decimalLongitude%3A%5B-125+TO+-66%5D+AND+decimalLatitude%3A%5B24+TO+50%5D+AND+hierarchy_homonym_string%3A%2A-103383-%2A+NOT+basisOfRecord%3Aliving+NOT+basisOfRecord%3Afossil'
      occAPI = BisonAPI.initFromUrl(url)
      occList = occAPI.getTSNOccurrences()
      shaper = ShapeShifter(ProcessType.BISON_TAXA_OCCURRENCE, occList, len(occList))
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

# ......................................................
# User test
csvfname = '/share/lm/data/archive/ryan/000/000/000/059/pt_59.csv'
metafname = '/share/lm/data/archive/ryan/heuchera_all.meta'
outfname = '/tmp/testpoints.shp'
bigfname = '/tmp/testpoints_big.shp'
logger = ScriptLogger('testing')

with open(csvfname, 'r') as f:
   blob = f.read()
with open(metafname, 'r') as f:
   metad = ast.literal_eval(f.read())
ptype = ProcessType.USER_TAXA_OCCURRENCE
shaper = ShapeShifter(ptype, blob, 32, logger=logger, metadata=metad)
shaper.writeOccurrences(outfname, maxPoints=50, bigfname=bigfname, isUser=True)


# ......................................................
# GBIF test
pointsCsvFn = '/share/lm/data/archive/kubi/000/000/000/235/pt_235.csv'
count =  456
outFile = '/tmp/pt_235.shp'
bigFile = '/tmp/bigpt_235.shp'
maxPoints = 500
ptype=ProcessType.GBIF_TAXA_OCCURRENCE

with open(pointsCsvFn) as inF:
   blob = inF.read()
shaper = ShapeShifter(ptype, blob, count, logger=logger)
shaper.writeOccurrences(outFile, maxPoints=maxPoints, bigfname=bigFile)


# ......................................................
# IDIG test
   
"""
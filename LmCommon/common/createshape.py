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
import json
import os
from osgeo import ogr, osr
import StringIO
import subprocess
from types import ListType, TupleType, UnicodeType, StringType

from LmBackend.common.occparse import OccDataParser
from LmCommon.common.lmconstants import (ENCODING, BISON, BISON_QUERY,
               GBIF, GBIF_QUERY, IDIGBIO, IDIGBIO_QUERY, PROVIDER_FIELD_COMMON, 
               LM_ID_FIELD, LM_WKT_FIELD, ProcessType, JobStatus,
               SHAPEFILE_MAX_STRINGSIZE, DWCNames, DEFAULT_OGR_FORMAT)
from LmCompute.common.lmObj import LmException
from LmCommon.common.unicode import fromUnicode, toUnicode
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
      @param data: Either csv blob of GBIF data or list of dictionary records
                   of BISON data
      @param processType: ProcessType constant, either GBIF_TAXA_OCCURRENCE,
                          BISON_TAXA_OCCURRENCE or IDIGBIO_TAXA_OCCURRENCE  
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
         self.idField = self.op.idFieldName
         self.xField = self.op.xFieldName
         self.yField = self.op.yFieldName

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
         print('Failed to fillOGRFeature, e = {}'.format(fromUnicode(toUnicode(e))))
         raise e
      else:
         # Create new feature, setting FID, in this layer
         lyr.CreateFeature(feat)
         feat.Destroy()

   # .............................................................................
   def _getMetadata(self, origfldnames):
      fldmeta = self._readMetadata()
      
      for i in range(len(origfldnames)):         
         oname = origfldnames[i]
         shortname = fldmeta[oname][0]
         ogrtype = self.getOgrFieldType(fldmeta[oname][1])
         self.fieldNames.append(shortname)
         self.fieldTypes.append(ogrtype)
         
         if len(fldmeta[oname]) == 3:
            if type(fldmeta[oname][2]) in (ListType, TupleType):
               acceptedVals = fldmeta[oname][2]
               if ogrtype == ogr.OFTString:
                  acceptedVals = [val.lower() for val in fldmeta[oname][2]]
               self.filters[i] = acceptedVals 
            else:
               role = fldmeta[oname][2].lower()
               if role == 'id':
                  self._idIdx = i
               elif role == 'longitude':
                  self._xIdx = i
               elif role == 'latitude':
                  self._yIdx = i
               elif role == 'groupby':
                  self._sortIdx = i
               elif role == 'dataname':
                  self._nameIdx = i
      self.fieldCount = len(self.fieldNames)
      
      if self._idIdx == None:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Missing \'id\' unique identifier field')
      if self._xIdx == None:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Missing \'longitude\' georeference field')
      if self._yIdx == None:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Missing \'latitude\' georeference field')
      if self._sortIdx == None:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Missing \'groupby\' sorting field')
      if self._nameIdx == None:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Missing \'dataname\' dataset name field')

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
         drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
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
   def writeOccurrences(self, outfname, maxPoints=None, bigfname=None, isUser=False):
      discardIndices = self._getSubset(maxPoints)

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
            bigDs = self._createDataset(bigfname)
            if isUser:
               bigLyr = self._addUserFieldDef(bigDs)
            else:
               bigLyr = self._addFieldDef(bigDs)
                        
      except Exception, e:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                           'Unable to create field definitions ({})'.format(e))

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
            
      except Exception, e:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                           'Unable to read or write data ({})'
                           .format(e))
            
# .............................................................................
# Private functions
# .............................................................................
   # .............................................................................
   def _createDataset(self, fname):
      drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
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
      metaDict = {'ogrformat': DEFAULT_OGR_FORMAT, 'geomtype': geomtype, 
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
      csvData.write(self.rawdata.encode(ENCODING))
      csvData.seek(0)
      reader = csv.DictReader(csvData, fieldnames=fldnames)
      return reader
   
   # ...............................................
   def _getRecord(self):
      if self.processType == ProcessType.USER_TAXA_OCCURRENCE:
         recDict = self._getUserCSVRec() 
      elif self.processType == ProcessType.GBIF_TAXA_OCCURRENCE:
         recDict = self._getCSVRec()
      # handle BISON, iDigBio the same
      else:
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
      # skip bad lines
      while not success and not self.op.eof():
         try:
            self.op.pullNextValidRec()
            if not self.op.eof():
               # Unique identifier field is not required, default to FID
               # ignore records without valid lat/long; all occ jobs contain these fields
               tmpDict[self.op.xFieldName] = float(self.op.xValue)
               tmpDict[self.op.yFieldName] = float(self.op.yValue)
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
         for i in range(len(self.op.fieldNames)):
            tmpDict[self.op.fieldNames[i]] = self.op.currLine[i]
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
      spRef.ImportFromEPSG(4326)
    
      newLyr = newDataset.CreateLayer('points', geom_type=ogr.wkbPoint, srs=spRef)
      if newLyr is None:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Layer creation failed')
       
      for pos in range(len(self.op.fieldNames)):
         fldname = self.op.fieldNames[pos]
         fldtype = self.op.fieldTypes[pos]
         fldDef = ogr.FieldDefn(fldname, fldtype)
         if fldtype == ogr.OFTString:
            fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
         returnVal = newLyr.CreateField(fldDef)
         if returnVal != 0:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                            'Failed to create field {}'.format(fldname))
            
      # Add wkt field
      fldDef = ogr.FieldDefn(LM_WKT_FIELD, ogr.OFTString)
      fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
      returnVal = newLyr.CreateField(fldDef)
      if returnVal != 0:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Failed to create field {}'.format(fldname))
      
      return newLyr
         
   # ...............................................
   def _addFieldDef(self, newDataset):
      spRef = osr.SpatialReference()
      spRef.ImportFromEPSG(4326)
    
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
               fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
            returnVal = newLyr.CreateField(fldDef)
            if returnVal != 0:
               raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                                 'Failed to create field {}'.format(fldname))
            
      # Add wkt field to all
      fldDef = ogr.FieldDefn(LM_WKT_FIELD, ogr.OFTString)
      fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
      returnVal = newLyr.CreateField(fldDef)
      if returnVal != 0:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                           'Failed to create field{}'.format(fldname))
      
      # Add URL field to GBIF/iDigBio data
      if self.linkField is not None:
         fldDef = ogr.FieldDefn(self.linkField, ogr.OFTString)
         fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
         returnVal = newLyr.CreateField(fldDef)
         if returnVal != 0:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Failed to create field {}'.format(self.linkField))
            
      # Add Provider field to GBIF/iDigBio data (for resolution from key or attribution)
      if self.computedProviderField is not None:
         fldDef = ogr.FieldDefn(self.computedProviderField, ogr.OFTString)
         fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
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
   def _fillFeature(self, feat, recDict):
      """
      @note: This *should* return the modified feature
      """
      try:
         # Set LM added fields, geometry, geomwkt
         wkt = 'POINT ({} {})'.format(recDict[self.xField], recDict[self.yField])
         feat.SetField(LM_WKT_FIELD, wkt)
         geom = ogr.CreateGeometryFromWkt(wkt)
         feat.SetGeometryDirectly(geom)
         
         # If data has a unique id for each point
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
               searchid = recDict[self.linkIdField]
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

         # Add values out of the line of data
         for name in recDict.keys():
            # Handles reverse lookup for BISON metadata
            # TODO: make this consistent!!!
            fldname = self._lookup(name)
            if fldname is not None:
               val = recDict[name]
               if val is not None and val != 'None':
                  if isinstance(val, UnicodeType):
                     val = fromUnicode(val)
                  feat.SetField(fldname, val)
      except Exception, e:
         print('Failed to fillFeature, e = {}'.format(fromUnicode(toUnicode(e))))
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
from LmCommon.common.lmconstants import (ENCODING, BISON, BISON_QUERY,
               GBIF, GBIF_QUERY, IDIGBIO, IDIGBIO_QUERY, PROVIDER_FIELD_COMMON, 
               LM_ID_FIELD, LM_WKT_FIELD, ProcessType, JobStatus,
               SHAPEFILE_MAX_STRINGSIZE, DWCNames, DEFAULT_OGR_FORMAT)
import ast

csvfname = '/share/lm/data/archive/ryan/000/000/000/059/pt_59.csv'
metafname = '/share/lm/data/archive/ryan/heuchera_all.meta'
outFname = '/tmp/testpoints.shp'
bigFname = '/tmp/testpoints_big.shp'
logger = ScriptLogger('testing')

with open(csvfname, 'r') as f:
   blob = f.read()
   

with open(metafname, 'r') as f:
   metad = ast.literal_eval(f.read())

shaper = ShapeShifter(ptype, blob, 32, logger=logger, metadata=metad)
shaper.writeOccurrences(outFname, maxPoints=50, bigfname=bigFname, 
                           isUser=True)


drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
newDs = drv.CreateDataSource(outFname)
bigDs = drv.CreateDataSource(bigFname)

spRef = osr.SpatialReference()
spRef.ImportFromEPSG(4326)

newLyr = newDs.CreateLayer('points', geom_type=ogr.wkbPoint, srs=spRef)
 
for pos in range(len(shaper.op.fieldNames)):
   fldname = shaper.op.fieldNames[pos]
   fldtype = shaper.op.fieldTypes[pos]
   fldDef = ogr.FieldDefn(fldname, fldtype)
   if fldtype == ogr.OFTString:
      fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
   returnVal = newLyr.CreateField(fldDef)
      
# Add wkt field
fldDef = ogr.FieldDefn(LM_WKT_FIELD, ogr.OFTString)
fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
returnVal = newLyr.CreateField(fldDef)
if returnVal != 0:
   print 'Failed to create field {}'.format(fldname)

discardIndices = []
# Loop through records
recDict = shaper._getRecord()
while recDict is not None:
   try:
      # Add non-discarded features to regular layer
      if shaper._currRecum not in discardIndices:
         shaper._createFillFeat(lyrDef, recDict, outLyr)
   except Exception, e:
      print('Failed to create record ({})'.format((e)))
   recDict = shaper._getRecord()
                     
# Return metadata
(minX, maxX, minY, maxY) = outLyr.GetExtent()
geomtype = lyrDef.GetGeomType()
fcount = outLyr.GetFeatureCount()
# Close dataset and flush to disk
outDs.Destroy()
self._finishWrite(outfname, minX, maxX, minY, maxY, geomtype, fcount)



taxid = 2427616

if os.path.exists(outfilename):
   import glob
   basename, ext = os.path.splitext(outfilename)
   fnames = glob.glob(basename + '*')
   for fname in fnames:
      print('Removing {}'.format(fname))
      os.remove(fname)

occAPI = IdigbioAPI()
occList = occAPI.queryByGBIFTaxonId(taxid)

count = len(occList)

shaper = ShapeShifter(ProcessType.IDIGBIO_TAXA_OCCURRENCE, occList, count)

shaper.writeOccurrences(outfilename, maxPoints=40, 
                        subsetfname=subsetOutfilename)
   
"""
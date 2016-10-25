"""
@summary: Module containing GBIF functions
@author: Aimee Stewart
@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
from types import ListType, TupleType, UnicodeType, StringType

from LmBackend.common.occparse import OccDataParser
from LmCommon.common.lmconstants import (ENCODING, BISON_RESPONSE_FIELDS,
      GBIF_EXPORT_FIELDS, GBIF_ID_FIELD, GBIF_LINK_PREFIX, BISON_LINK_PREFIX,
      IDIGBIO_RETURN_FIELDS, IDIGBIO_ID_FIELD, IDIGBIO_LINK_PREFIX, 
      LM_ID_FIELD, LM_WKT_FIELD, ProcessType, JobStatus,
      SHAPEFILE_MAX_STRINGSIZE, DWCNames, DEFAULT_OGR_FORMAT,
      GBIF_PROVIDER_FIELD, PROVIDER_NAME_FIELD, 
      GBIF_LINK_FIELD, IDIGBIO_LINK_FIELD, BISON_LINK_FIELD)
from LmCompute.common.lmObj import LmException
from LmCommon.common.unicode import fromUnicode, toUnicode

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
         self.op = OccDataParser(logger, rawdata, metadata, delimiter=',')
         self.idField = self.op.idFieldName
         self.xField = self.op.xFieldName
         self.yField = self.op.yFieldName

      elif processType == ProcessType.GBIF_TAXA_OCCURRENCE:
         self.dataFields = GBIF_EXPORT_FIELDS
         self.idField = GBIF_ID_FIELD
         self.linkField = GBIF_LINK_FIELD
         self.linkUrl = GBIF_LINK_PREFIX
         self.linkIdField = GBIF_ID_FIELD
         self.providerKeyField = GBIF_PROVIDER_FIELD
         self.computedProviderField = PROVIDER_NAME_FIELD
         self.xField = DWCNames.DECIMAL_LONGITUDE['SHORT']
         self.yField = DWCNames.DECIMAL_LATITUDE['SHORT']
         self._reader = self._getCSVReader()
         
      elif processType == ProcessType.IDIGBIO_TAXA_OCCURRENCE:
         self.dataFields = IDIGBIO_RETURN_FIELDS
         self.lookupFields = self._mapAPIResponseNames()
         self.idField = IDIGBIO_ID_FIELD
         self.linkField = IDIGBIO_LINK_FIELD
         self.linkUrl = IDIGBIO_LINK_PREFIX
         self.linkIdField = IDIGBIO_ID_FIELD
         self.computedProviderField = PROVIDER_NAME_FIELD
         self.xField = DWCNames.DECIMAL_LONGITUDE['SHORT']
         self.yField = DWCNames.DECIMAL_LATITUDE['SHORT']

      elif processType == ProcessType.BISON_TAXA_OCCURRENCE:
         self.dataFields = BISON_RESPONSE_FIELDS
         self.lookupFields = self._mapAPIResponseNames()
         self.linkField = BISON_LINK_FIELD
         self.linkUrl = BISON_LINK_PREFIX
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

# .............................................................................
# Public functions
# .............................................................................
   def writeUserOccurrences(self, outfname, maxPoints=None, subsetfname=None):      
      if subsetfname is not None:
         if maxPoints is not None and self._recCount > maxPoints: 
            from random import shuffle
            subsetIndices = range(self._recCount)
            shuffle(subsetIndices)
            subsetIndices = subsetIndices[:maxPoints]

      subsetDs = None
      try:
         drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
         newDs = drv.CreateDataSource(outfname)
         if newDs is None:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Dataset creation failed for {}'.format(outfname))
         if subsetfname is not None and subsetIndices:
            subsetDs = drv.CreateDataSource(subsetfname)
            subsetMetaDict = {'ogrFormat': DEFAULT_OGR_FORMAT}
            if subsetDs is None:
               raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                                 'Dataset creation failed for {}'.format(subsetfname))
         
         newLyr = self._addUserFieldDef(newDs)
         if subsetDs is not None:
            subsetLyr = self._addUserFieldDef(subsetDs)
         # same lyrDef for both datasets
         lyrDef = newLyr.GetLayerDefn()
         
         # Loop through records
         recDict = self._getRecord()
         while recDict:
            try:
               self._createFillFeat(lyrDef, recDict, newLyr)
               if subsetDs is not None and self._currRecum in subsetIndices:
                  self._createFillFeat(lyrDef, recDict, subsetLyr)
            except Exception, e:
               print('Failed to create record ({})'.format(fromUnicode(toUnicode(e))))
            recDict = self._getRecord()
                              
         # Return metadata
         (minX, maxX, minY, maxY) = newLyr.GetExtent()
         geomtype = lyrDef.GetGeomType()
         fcount = newLyr.GetFeatureCount()
         # Close dataset and flush to disk
         newDs.Destroy()
         print('Closed/wrote []-feature dataset {}'.format(fcount, outfname))
         basename, ext = os.path.splitext(outfname)
         self._writeMetadata(basename, DEFAULT_OGR_FORMAT, geomtype, 
                             fcount, minX, minY, maxX, maxY)
         
         if subsetDs is not None:
            sfcount = subsetLyr.GetFeatureCount()
            subsetDs.Destroy()
            print('Closed/wrote {}-feature dataset {}'.format(sfcount, subsetfname))
            basename, ext = os.path.splitext(subsetfname)
            self._writeMetadata(basename, DEFAULT_OGR_FORMAT, geomtype, 
                                sfcount, minX, minY, maxX, maxY)
      except Exception, e:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                           'Unable to read or write data ({})'
                           .format(fromUnicode(toUnicode(e))))
      
      self._finishWrite(outfname, minX, maxX, minY, maxY, geomtype, fcount,
                        subsetFname=subsetfname, subsetCount=sfcount)
   #    try:
   #       shpTreeCmd = os.path.join(appPath, "shptree")
   #       retcode = subprocess.call([shpTreeCmd, "%s" % outfname])
   #       if retcode != 0: 
   #          print 'Unable to create shapetree index on %s' % outfname
   #    except Exception, e:
   #       print 'Unable to create shapetree index on %s: %s' % (outfname, str(e))

# ...............................................
   @staticmethod
   def testShapefile(dlocation):
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
   def writeOccurrences(self, outfname, maxPoints=None, subsetfname=None):
      if subsetfname is not None:
         if maxPoints is not None and self._recCount > maxPoints: 
            from random import shuffle
            subsetIndices = range(self._recCount)
            shuffle(subsetIndices)
            subsetIndices = subsetIndices[:maxPoints]

      newDs = subsetDs = sfcount = None
      try:
         drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
         newDs = drv.CreateDataSource(outfname)
         if newDs is None:
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                              'Dataset creation failed for {}'.format(outfname))
            
         if subsetfname is not None and subsetIndices:
            subsetDs = drv.CreateDataSource(subsetfname)
            if subsetDs is None:
               raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                                 'Dataset creation failed for {}'.format(subsetfname))
         
         newLyr = self._addFieldDef(newDs)
         if subsetDs is not None:
            subsetLyr = self._addFieldDef(subsetDs)
         # same lyrDef for both datasets
         lyrDef = newLyr.GetLayerDefn()
         
         # Loop through records
         recDict = self._getRecord()
         while recDict is not None:
            try:
               self._createFillFeat(lyrDef, recDict, newLyr)
               if subsetDs is not None and self._currRecum in subsetIndices:
                  self._createFillFeat(lyrDef, recDict, subsetLyr)
            except Exception, e:
               print('Failed to create record ({})'.format(fromUnicode(toUnicode(e))))
            recDict = self._getRecord()
                              
         # Return metadata
         (minX, maxX, minY, maxY) = newLyr.GetExtent()
         geomtype = lyrDef.GetGeomType()
         fcount = newLyr.GetFeatureCount()
         # Close dataset and flush to disk
         newDs.Destroy()
         print('Closed/wrote {}-feature dataset {}'.format(fcount, outfname))
                           
         if subsetDs is not None:
            sfcount = subsetLyr.GetFeatureCount()
            subsetDs.Destroy()
            print('Closed/wrote {}-feature dataset {}'.format(sfcount, subsetfname))
            
      except Exception, e:
         raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                           'Unable to read or write data ({})'
                           .format(fromUnicode(toUnicode(e))))
      
      self._finishWrite(outfname, minX, maxX, minY, maxY, geomtype, fcount,
                        subsetFname=subsetfname, subsetCount=sfcount)

   # .............................................................................
   def _finishWrite(self, outFname, minX, maxX, minY, maxY, geomtype, fcount,
                    subsetFname=None, subsetCount=None):
      # Test output data
      goodData, featCount = self.testShapefile(outFname)

      # Write metadata as JSON
      basename, ext = os.path.splitext(outFname)
      self._writeMetadata(basename, geomtype, fcount, minX, minY, maxX, maxY)

      if subsetFname is not None:
         basename, ext = os.path.splitext(subsetFname)
         self._writeMetadata(basename, geomtype, subsetCount, minX, minY, maxX, maxY)
         
         goodData, featCount = self.testShapefile(subsetFname)
         if not goodData: 
            raise LmException(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR, 
                              'Failed to create shapefile {}'.format(subsetFname))
         elif featCount == 0:
            raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                              'Failed to create shapefile {}'.format(subsetFname))
            
# .............................................................................
# Private functions
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
         wkt = 'POINT (%s  %s)' % (str(recDict[self.xField]), 
                                   str(recDict[self.yField]))
         feat.SetField(LM_WKT_FIELD, wkt)
         geom = ogr.CreateGeometryFromWkt(wkt)
         feat.SetGeometryDirectly(geom)
         
         try:
            ptid = recDict[self.idField]
         except:
            # Set LM added id field
            ptid = self._currRecum 
            feat.SetField(self.idField, ptid)

         # Set linked Url field
         if self.linkField is not None:
            try:
               searchid = recDict[self.linkIdField]
            except:
               pass
            else:
               pturl = '{}{}'.format(self.linkUrl, str(searchid))
               feat.SetField(self.linkField, pturl)
                     
         # Set linked Url field
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
from LmCommon.common.createshape import ShapeShifter
from LmCommon.common.apiquery import IdigbioAPI
from LmCommon.common.lmconstants import ProcessType

outfilename = '/tmp/testidigpoints.shp'
subsetOutfilename = '/tmp/testidigpoints_sub.shp'
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
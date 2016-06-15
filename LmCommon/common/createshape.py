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
from types import ListType, TupleType, UnicodeType

from LmBackend.common.occparse import OccDataParser
from LmCommon.common.unicode import fromUnicode, toUnicode
from LmCommon.common.lmconstants import (ENCODING, BISON_RESPONSE_FIELDS,
      GBIF_EXPORT_FIELDS, GBIF_ID_FIELD, GBIF_LINK_FIELD, GBIF_OCCURRENCE_URL, 
      IDIGBIO_RETURN_FIELDS, IDIGBIO_ID_FIELD, IDIGBIO_LINK_PREFIX,
      IDIGBIO_LINK_FIELD, LM_ID_FIELD, LM_WKT_FIELD, ProcessType, 
      SHAPEFILE_MAX_STRINGSIZE, DWCNames)


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
      self.op = None
      
      # All raw Occdata must contain ShortDWCNames.DECIMAL_LATITUDE and 
      #                              ShortDWCNames.DECIMAL_LONGITUDE
      if processType == ProcessType.USER_TAXA_OCCURRENCE:
         if not logger:
            raise Exception('Failed to get a logger')
         if not metadata:
            raise Exception('Failed to get metadata')
         self.op = OccDataParser(logger, rawdata, metadata, delimiter=',')
         self.idField = self.op.idFieldName
         self.xField = self.op.xFieldName
         self.yField = self.op.yFieldName

      elif processType == ProcessType.GBIF_TAXA_OCCURRENCE:
         self.dataFields = GBIF_EXPORT_FIELDS
         self.idField = GBIF_ID_FIELD
         self.linkField = GBIF_LINK_FIELD
         self.linkUrl = GBIF_OCCURRENCE_URL
         self.xField = DWCNames.DECIMAL_LONGITUDE['SHORT']
         self.yField = DWCNames.DECIMAL_LATITUDE['SHORT']
         self._reader = self._getCSVReader()
         
      elif processType == ProcessType.IDIGBIO_TAXA_OCCURRENCE:
         self.dataFields = IDIGBIO_RETURN_FIELDS
         self.lookupFields = self._mapAPIResponseNames()
         self.idField = IDIGBIO_ID_FIELD
         self.linkField = IDIGBIO_LINK_FIELD
         self.linkUrl = IDIGBIO_LINK_PREFIX
         self.xField = DWCNames.DECIMAL_LONGITUDE['SHORT']
         self.yField = DWCNames.DECIMAL_LATITUDE['SHORT']

      elif processType == ProcessType.BISON_TAXA_OCCURRENCE:
         self.dataFields = BISON_RESPONSE_FIELDS
         self.lookupFields = self._mapAPIResponseNames()
         self.idField = LM_ID_FIELD
         self.xField = self._lookupReverse(DWCNames.DECIMAL_LONGITUDE['SHORT'])
         self.yField = self._lookupReverse(DWCNames.DECIMAL_LATITUDE['SHORT'])
         
      else:
         raise Exception('Invalid processType {}'.format(processType))

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
         raise Exception('Missing \'id\' unique identifier field')
      if self._xIdx == None:
         raise Exception('Missing \'longitude\' georeference field')
      if self._yIdx == None:
         raise Exception('Missing \'latitude\' georeference field')
      if self._sortIdx == None:
         raise Exception('Missing \'groupby\' sorting field')
      if self._nameIdx == None:
         raise Exception('Missing \'dataname\' dataset name field')


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
         raise Exception('Unsupported field type %s (use integer, string, or real)' 
                         % typeString)

# .............................................................................
# Public functions
# .............................................................................
   def writeUserOccurrences(self, outfname, maxPoints=None, subsetfname=None):
      ogrFormat = 'ESRI Shapefile'
      
      if subsetfname is not None:
         if maxPoints is not None and self._recCount > maxPoints: 
            from random import shuffle
            subsetIndices = range(self._recCount)
            shuffle(subsetIndices)
            subsetIndices = subsetIndices[:maxPoints]

      subsetDs = None
      try:
         drv = ogr.GetDriverByName(ogrFormat)
         newDs = drv.CreateDataSource(outfname)
         if newDs is None:
            raise Exception('Dataset creation failed for %s' % outfname)
         if subsetfname is not None and subsetIndices:
            subsetDs = drv.CreateDataSource(subsetfname)
            subsetMetaDict = {'ogrFormat': ogrFormat}
            if subsetDs is None:
               raise Exception('Dataset creation failed for %s' % subsetfname)
         
         newLyr = self._addUserFieldDef(newDs)
         if subsetDs is not None:
            subsetLyr = self._addUserFieldDef(subsetDs)
         # same lyrDef for both datasets
         lyrDef = newLyr.GetLayerDefn()
         
         # Loop through records
         recDict = self._getRecord()
         while recDict:
            self._createFillFeat(lyrDef, recDict, newLyr)
            if subsetDs is not None and self._currRecum in subsetIndices:
               self._createFillFeat(lyrDef, recDict, subsetLyr)
            recDict = self._getRecord()
                              
         # Return metadata
         (minX, maxX, minY, maxY) = newLyr.GetExtent()
         geomtype = lyrDef.GetGeomType()
         fcount = newLyr.GetFeatureCount()
         # Close dataset and flush to disk
         newDs.Destroy()
         print('Closed/wrote dataset {}'.format(outfname))
         basename, ext = os.path.splitext(outfname)
         self._writeMetadata(basename, ogrFormat, geomtype, 
                             fcount, minX, minY, maxX, maxY)
         
         if subsetDs is not None:
            sfcount = subsetLyr.GetFeatureCount()
            subsetDs.Destroy()
            print('Closed/wrote dataset {}'.format(subsetfname))
            basename, ext = os.path.splitext(subsetfname)
            self._writeMetadata(basename, ogrFormat, geomtype, 
                                sfcount, minX, minY, maxX, maxY)
      except Exception, e:
         print('Unable to read or write data ({})'.format(fromUnicode(toUnicode(e))))
         raise e
   #    try:
   #       shpTreeCmd = os.path.join(appPath, "shptree")
   #       retcode = subprocess.call([shpTreeCmd, "%s" % outfname])
   #       if retcode != 0: 
   #          print 'Unable to create shapetree index on %s' % outfname
   #    except Exception, e:
   #       print 'Unable to create shapetree index on %s: %s' % (outfname, str(e))


   # .............................................................................
   def writeOccurrences(self, outfname, maxPoints=None, subsetfname=None):
      ogrFormat = 'ESRI Shapefile'
      
      if subsetfname is not None:
         if maxPoints is not None and self._recCount > maxPoints: 
            from random import shuffle
            subsetIndices = range(self._recCount)
            shuffle(subsetIndices)
            subsetIndices = subsetIndices[:maxPoints]

      newDs = subsetDs = None
      try:
         drv = ogr.GetDriverByName(ogrFormat)
         newDs = drv.CreateDataSource(outfname)
         if newDs is None:
            raise Exception('Dataset creation failed for {}'.format(outfname))
            
         if subsetfname is not None and subsetIndices:
            subsetDs = drv.CreateDataSource(subsetfname)
            if subsetDs is None:
               raise Exception('Dataset creation failed for {}'.format(subsetfname))
         
         newLyr = self._addFieldDef(newDs)
         if subsetDs is not None:
            subsetLyr = self._addFieldDef(subsetDs)
         # same lyrDef for both datasets
         lyrDef = newLyr.GetLayerDefn()
         
         # Loop through records
         recDict = self._getRecord()
         while recDict is not None:
            self._createFillFeat(lyrDef, recDict, newLyr)
            if subsetDs is not None and self._currRecum in subsetIndices:
               self._createFillFeat(lyrDef, recDict, subsetLyr)
            recDict = self._getRecord()
                              
         # Return metadata
         (minX, maxX, minY, maxY) = newLyr.GetExtent()
         geomtype = lyrDef.GetGeomType()
         fcount = newLyr.GetFeatureCount()
         # Close dataset and flush to disk
         newDs.Destroy()
         print('Closed/wrote dataset {}'.format(outfname))
         basename, ext = os.path.splitext(outfname)
         self._writeMetadata(basename, ogrFormat, geomtype, 
                             fcount, minX, minY, maxX, maxY)
         
         if subsetDs is not None:
            sfcount = subsetLyr.GetFeatureCount()
            subsetDs.Destroy()
            print('Closed/wrote dataset {}'.format(subsetfname))
            basename, ext = os.path.splitext(subsetfname)
            self._writeMetadata(basename, ogrFormat, geomtype, 
                                sfcount, minX, minY, maxX, maxY)
      except Exception, e:
         print('Unable to read or write data (%s)' % fromUnicode(toUnicode(e)))
         raise e
   #    try:
   #       shpTreeCmd = os.path.join(appPath, "shptree")
   #       retcode = subprocess.call([shpTreeCmd, "%s" % outfname])
   #       if retcode != 0: 
   #          print 'Unable to create shapetree index on %s' % outfname
   #    except Exception, e:
   #       print 'Unable to create shapetree index on %s: %s' % (outfname, str(e))

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
   def _writeMetadata(self, basename, orgformat, geomtype, count, 
                      minx, miny, maxx, maxy):
      metaDict = {'ogrformat': orgformat, 'geomtype': geomtype, 'count': count,  
                  'minx': minx, 'miny': miny, 'maxx': maxx, 'maxy': maxy}
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
      while not success and len(self.rawdata) > 0: 
         try:
            recDict = self.rawdata.pop()
         except:
            success = True
         else:
            try:
               float(recDict[self.xField])
               float(recDict[self.yField])
            except OverflowError, e:
               print('OverflowError ({}), moving on'.format(fromUnicode(toUnicode(e))))
            except ValueError, e:
               print('Ignoring invalid lat {}, long {} data'
                     .format(recDict[self.xField], recDict[self.yField]))
            except Exception, e:
               print('Exception ({})'.format(fromUnicode(toUnicode(e))))
            else:
               success = True
      return recDict
   
   # ...............................................
   def _getUserCSVRec(self):
      success = False
      recDict = {}
      # skip bad lines
      while not success and not self.op.eof():
         try:
            self.op.pullNextValidRec()
            if not self.op.eof():
               # ignore records without valid lat/long; all occ jobs contain these fields
               recDict[self.op.xFieldName] = float(self.op.xValue)
               recDict[self.op.yFieldName] = float(self.op.yValue)
               success = True
         except OverflowError, e:
            print('OverflowError on %d (%s), moving on' % (self._currRecum, fromUnicode(toUnicode(e))))
         except ValueError, e:
            print('Ignoring invalid lat {}, long {} data'.format(self.op.xValue, 
                                                                 self.op.yValue))
         except Exception, e:
            print('Exception reading line {} ({})'.format(self.op.currRecnum, 
                                                     fromUnicode(toUnicode(e))))
         except StopIteration, e:
            pass
         
         if success:
            for i in range(len(self.op.fieldNames)):
               recDict[self.op.fieldNames[i]] = self.op.currLine[i]
      return recDict

   # ...............................................
   def _getCSVRec(self):
      success = False
      recDict = None
      # skip bad lines
      while not success:
         try:
            recDict = self._reader.next()
            # ignore records without valid lat/long; all occ jobs contain these fields
            recDict[DWCNames.DECIMAL_LATITUDE['SHORT']] = \
                  float(recDict[DWCNames.DECIMAL_LATITUDE['SHORT']])
            recDict[DWCNames.DECIMAL_LONGITUDE['SHORT']] = \
                  float(recDict[DWCNames.DECIMAL_LONGITUDE['SHORT']])
            success = True
         except OverflowError, e:
            print('OverflowError on {} ({}), moving on'
                  .format(self._currRecum, fromUnicode(toUnicode(e))))
         except ValueError, e:
            print('Ignoring invalid lat {}, long {} data'
                  .format(recDict[DWCNames.DECIMAL_LATITUDE]['SHORT'],
                     recDict[DWCNames.DECIMAL_LONGITUDE]['SHORT']))
         except StopIteration, e:
            success = True
         except Exception, e:
            print('Exception reading line %d (%s)' 
                  % (self._currRecum, fromUnicode(toUnicode(e))))
            success = True
      return recDict

   # ...............................................
   def _addUserFieldDef(self, newDataset):
      spRef = osr.SpatialReference()
      spRef.ImportFromEPSG(4326)
    
      newLyr = newDataset.CreateLayer('points', geom_type=ogr.wkbPoint, srs=spRef)
      if newLyr is None:
         raise Exception('Layer creation failed')
       
      for pos in range(len(self.op.fieldNames)):
         fldname = self.op.fieldNames[pos]
         fldtype = self.op.fieldTypes[pos]
         fldDef = ogr.FieldDefn(fldname, fldtype)
         if fldtype == ogr.OFTString:
            fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
         returnVal = newLyr.CreateField(fldDef)
         if returnVal != 0:
            raise Exception('Failed to create field %s' % fldname)
            
      # Add wkt field
      fldDef = ogr.FieldDefn(LM_WKT_FIELD, ogr.OFTString)
      fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
      returnVal = newLyr.CreateField(fldDef)
      if returnVal != 0:
         raise Exception('Failed to create field %s' % fldname)
      
      return newLyr
         
   # ...............................................
   def _addFieldDef(self, newDataset):
      spRef = osr.SpatialReference()
      spRef.ImportFromEPSG(4326)
    
      newLyr = newDataset.CreateLayer('points', geom_type=ogr.wkbPoint, srs=spRef)
      if newLyr is None:
         raise Exception('Layer creation failed')
       
      for fielddesc in self.dataFields.values():
         if fielddesc is not None:
            fldname = fielddesc[0]
            fldtype = fielddesc[1]
            fldDef = ogr.FieldDefn(fldname, fldtype)
            if fldtype == ogr.OFTString:
               fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
            returnVal = newLyr.CreateField(fldDef)
            if returnVal != 0:
               raise Exception('Failed to create field %s' % fldname)
            
      # Add wkt field to all
      fldDef = ogr.FieldDefn(LM_WKT_FIELD, ogr.OFTString)
      fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
      returnVal = newLyr.CreateField(fldDef)
      if returnVal != 0:
         raise Exception('Failed to create field %s' % fldname)
      
      # Add URL field to GBIF/iDigBio data
      if self.linkField is not None:
         fldDef = ogr.FieldDefn(self.linkField, ogr.OFTString)
         fldDef.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
         returnVal = newLyr.CreateField(fldDef)
         if returnVal != 0:
            raise Exception('Failed to create field %s' % self.linkField)
         
      # Add id field to BISON data
      if self.processType == ProcessType.BISON_TAXA_OCCURRENCE:
         fldDef = ogr.FieldDefn(LM_ID_FIELD, ogr.OFTString)
         returnVal = newLyr.CreateField(fldDef)
         if returnVal != 0:
            raise Exception('Failed to create field %s' % LM_ID_FIELD)
    
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
            pturl = '%s/%s' % (self.linkUrl, str(ptid))
            feat.SetField(self.linkField, pturl)
                     
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
   outfilename = '/tmp/testidigpoints.shp'
   subsetOutfilename = '/tmp/testidigpoints_sub.shp'
   taxid = 2437967
   
   if os.path.exists(outfilename):
      import glob
      basename, ext = os.path.splitext(outfilename)
      fnames = glob.glob(basename + '*')
      for fname in fnames:
         print('Removing {}'.format(fname))
         os.remove(fname)

   from LmCommon.common.apiquery import IdigbioAPI
   
   occAPI = IdigbioAPI()
   occList = occAPI.queryByGBIFTaxonId(taxid)
   
   count = len(occList)
   
   shaper = ShapeShifter(ProcessType.IDIGBIO_TAXA_OCCURRENCE, occList, count)
   shaper.writeOccurrences(outfilename, maxPoints=40, 
                           subsetfname=subsetOutfilename)
   
   

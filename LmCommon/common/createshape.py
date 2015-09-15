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
from types import UnicodeType, FloatType

from LmCommon.common.localconstants import ENCODING
from LmCommon.common.unicode import fromUnicode, toUnicode
from LmCommon.common.lmconstants import (BISON_RESPONSE_FIELDS,
      GBIF_EXPORT_FIELDS, GBIF_ID_FIELD, GBIF_LINK_FIELD, GBIF_OCCURRENCE_URL, IDIGBIO_EXPORT_FIELDS, IDIGBIO_ID_FIELD,
      IDIGBIO_LINK_FIELD, IDIGBIO_URL_PREFIX, IDIGBIO_OCCURRENCE_POSTFIX, 
      IDIGBIO_SEARCH_POSTFIX, LM_ID_FIELD, LM_WKT_FIELD, ProcessType, 
      SHAPEFILE_MAX_STRINGSIZE, ShortDWCNames)

# .............................................................................
class ShapeShifter(object):
# .............................................................................
   """
   Class to write a shapefile from GBIF CSV output or BISON JSON output 
   """
# ............................................................................
# Constructor
# .............................................................................
   def __init__(self, processType, rawdata, count):
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
      
      # All raw Occdata must contain ShortDWCNames.DECIMAL_LATITUDE and 
      #                              ShortDWCNames.DECIMAL_LONGITUDE
      if processType == ProcessType.GBIF_TAXA_OCCURRENCE:
         self.dataFields = GBIF_EXPORT_FIELDS
         self.idField = GBIF_ID_FIELD
         self.linkField = GBIF_LINK_FIELD
         self.linkUrl = GBIF_OCCURRENCE_URL
         self.xField = ShortDWCNames.DECIMAL_LONGITUDE
         self.yField = ShortDWCNames.DECIMAL_LATITUDE
         self._reader = self._getCSVReader()
         
      elif processType == ProcessType.IDIGBIO_TAXA_OCCURRENCE:
         self.dataFields = IDIGBIO_EXPORT_FIELDS
         self.idField = IDIGBIO_ID_FIELD
         self.linkField = IDIGBIO_LINK_FIELD
         self.linkUrl = '/'.join([IDIGBIO_URL_PREFIX, IDIGBIO_OCCURRENCE_POSTFIX, 
                                  IDIGBIO_SEARCH_POSTFIX])
         self.xField = ShortDWCNames.DECIMAL_LONGITUDE
         self.yField = ShortDWCNames.DECIMAL_LATITUDE
         self._reader = self._getCSVReader()

      elif processType == ProcessType.BISON_TAXA_OCCURRENCE:
         self.dataFields = BISON_RESPONSE_FIELDS
         self.lookupFields = self._mapBisonNames()
         self.idField = LM_ID_FIELD
         self.linkField = None
         self.linkUrl = None
         self.xField = self._lookupReverse(ShortDWCNames.DECIMAL_LONGITUDE)
         self.yField = self._lookupReverse(ShortDWCNames.DECIMAL_LATITUDE)
         
      else:
         raise Exception('Invalid processType %s' % (str(processType)))

# .............................................................................
# Private functions
# .............................................................................
   def _createFillFeat(self, lyrDef, recDict, lyr):
      feat = ogr.Feature(lyrDef)
      try:
         self._fillFeature(feat, recDict)
      except Exception, e:
         print('Failed to fillOGRFeature, e = %s' % fromUnicode(toUnicode(e)))
         raise e
      else:
         # Create new feature, setting FID, in this layer
         lyr.CreateFeature(feat)
         feat.Destroy()

# .............................................................................
# Public functions
# .............................................................................
   def writeOccurrences(self, outfname, maxPoints=None, subsetfname=None):
      ogrFormat = 'ESRI Shapefile'
      
      recIdx = -1
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
         print('Closed/wrote dataset %s' % outfname)
         basename, ext = os.path.splitext(outfname)
         self._writeMetadata(basename, ogrFormat, geomtype, 
                             fcount, minX, minY, maxX, maxY)
         
         if subsetDs is not None:
            sfcount = subsetLyr.GetFeatureCount()
            subsetDs.Destroy()
            print("Closed/wrote dataset %s" % subsetfname)
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
   def _mapBisonNames(self):
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
   def _getCSVReader(self):
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
      if self.processType in (ProcessType.GBIF_TAXA_OCCURRENCE,
                              ProcessType.IDIGBIO_TAXA_OCCURRENCE):
         recDict = self._getCSVRec()
      else:
         recDict = self._getBISONRec()
      if recDict is not None:
         self._currRecum += 1
      return recDict
      
   # ...............................................
   def _getBISONRec(self):
      """
      @note: We modify BISON returned fieldnames, they are too long for shapefiles
      """
      recDict = None
      try:
         recDict = self.rawdata.pop()
      except:
         pass
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
            recDict[ShortDWCNames.DECIMAL_LATITUDE] = float(recDict[ShortDWCNames.DECIMAL_LATITUDE])
            recDict[ShortDWCNames.DECIMAL_LATITUDE] = float(recDict[ShortDWCNames.DECIMAL_LATITUDE])
            success = True
         except OverflowError, e:
            print('OverflowError on %d (%s), moving on' % (self._currRecum, fromUnicode(toUnicode(e))))
         except ValueError, e:
            print('Ignoring invalid lat %s, long %s data' 
                  % (str(recDict[ShortDWCNames.DECIMAL_LATITUDE]),
                     str(recDict[ShortDWCNames.DECIMAL_LONGITUDE])))
         except StopIteration, e:
            success = True
         except Exception, e:
            print('Exception reading line %d (%s)' 
                  % (self._currRecum, fromUnicode(toUnicode(e))))
            success = True
      return recDict

#    # ...............................................
#    def _addField(self, newLyr, fldname, fldtype):
#       # TODO: Try to use this function, may not work to pass layer
#       fldDef = ogr.FieldDefn(fldname, fldtype)
#       returnVal = newLyr.CreateField(fldDef)
#       if returnVal != 0:
#          raise Exception('Failed to create field %s' % fldname)

   # ...............................................
   def _addFieldDef(self, newDataset):
      spRef = osr.SpatialReference()
      spRef.ImportFromEPSG(4326)
    
      newLyr = newDataset.CreateLayer('points', geom_type=ogr.wkbPoint, srs=spRef)
      if newLyr is None:
         raise Exception('Layer creation failed')
       
      idIdx = None
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
            fldname = self._lookup(name)
            if fldname is not None:
               val = recDict[name]
               if val is not None and val != 'None':
                  if isinstance(val, UnicodeType):
                     val = fromUnicode(val)
                  feat.SetField(fldname, val)
      except Exception, e:
         print('Failed to fillFeature, e = %s' % fromUnicode(toUnicode(e)))
         raise e
      
# ...............................................
if __name__ == '__main__':

   provfname = '/share/data/species/iDigBio_20150222/zonotrichia leucophrys/zonotrichia leucophrys_attr.json'         
   csvfname = '/share/data/species/iDigBio_20150222/zonotrichia leucophrys/zonotrichia leucophrys_sp.csv'
   binomialfilename = '/share/data/species/iDigBio_20150222.txt'
   
   outputfname = '/tmp/testidigpoints.shp'
   subsetfname = '/tmp/testidigpoints_sub.shp'
   
   if os.path.exists(outputfname):
      import glob
      basename, ext = os.path.splitext(outputfname)
      fnames = glob.glob(basename + '*')
      for fname in fnames:
         os.remove(fname)
      
   csvblob = ''
   count = -1            # don't count first line
   f = open(csvfname, 'r')
   for line in f:
      csvblob += line
      count += 1
   f.close()
   
   shp = ShapeShifter(ProcessType.IDIGBIO_TAXA_OCCURRENCE, csvblob, count)
      
   shp.writeOccurrences(outputfname, 10, subsetfname)
   
   f = open(provfname, 'r')
   jsondata = f.read()
   f.close()
   provdict = json.loads(jsondata)
   print(str(provdict))

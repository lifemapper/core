"""
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
try:
   from cStringIO import StringIO
except:
   from StringIO import StringIO
import json
import glob
import os

from osgeo import gdal, gdalconst, ogr, osr
import subprocess
from types import ListType, TupleType
import zipfile

from LmCommon.common.lmAttObject import LmAttObj
from LmCommon.common.lmconstants import (SHAPEFILE_EXTENSIONS, 
                  DEFAULT_OGR_FORMAT, SHAPEFILE_MAX_STRINGSIZE, LegalMapUnits,
   OFTInteger, OFTString)
from LmCommon.common.verify import computeHash, verifyHash

from LmServer.base.lmobj import LMError, LMObject, LMSpatialObject
from LmServer.base.serviceobject2 import ServiceObject

from LmServer.common.lmconstants import (UPLOAD_PATH, OccurrenceFieldNames, 
            GDALFormatCodes, GDALDataTypes, OGRFormats, OGRDataTypes, 
            OutputFormat, LMServiceType, LMServiceModule)
from LmServer.common.localconstants import APP_PATH, DEFAULT_EPSG

# .............................................................................
class _Layer(LMSpatialObject, ServiceObject):
   META_IS_CATEGORICAL = 'isCategorical'
   META_IS_DISCRETE = 'isDiscrete'
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, name, userId, epsgcode, lyrId=None, 
                squid=None, verify=None, dlocation=None, 
                metadata={}, dataFormat=None, gdalType=None, ogrType=None,
                valUnits=None, valAttribute=None, 
                nodataVal=None, minVal=None, maxVal=None, 
                mapunits=None, resolution=None, 
                # LMSpatialObject
                bbox=None,
                # ServiceObject
                svcObjId=None, serviceType=LMServiceType.LAYERS, 
                moduleType=LMServiceModule.LM,
                metadataUrl=None, parentMetadataUrl=None, modTime=None):
      """
      @summary Layer superclass constructor
      @copydoc LmServer.base.lmobj.LMSpatialObject::__init__()
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @note: svcObjId may be filled with the base LayerId or a unique 
             parameterized id.
      @param name: Short name, unique with userid and epsq.  
      @param lyrId: Database id of the layer object
      @param squid: unique identifier maintaining 'Species thread' for a user
      @param verify: Hash of the data for verification
      @param dlocation: Data location (url, file path, ...)
      @param metadata: Dictionary of metadata key/values; uses class or 
                       superclass attribute constants META_* as keys
      @param dataFormat: Data file format (ogr or gdal codes, used to choose
                  driver for read/write). 
                  GDAL Raster Format code at http://www.gdal.org/formats_list.html.
                  OGR Vector Format code at http://www.gdal.org/ogr_formats.html
      @param gdalType:  Integer indicating osgeo.gdalconst datatype
                  GDALDataType in http://www.gdal.org/gdal_8h.html
      @param ogrType: OGR geometry type (wkbPoint, ogr.wkbPolygon, etc)
                  OGRwkbGeometryType in http://www.gdal.org/ogr/ogr__core_8h.html
      @param valUnits: Units of measurement for data
      @param valAttribute: Field containing data values of interest
      @param nodataVal: Value indicating feature/pixel does not contain data
      @param minVal: Smallest value in data
      @param maxVal: Largest value in data
      @param mapunits: mapunits of measurement. These are keywords as used in 
                    mapserver, choice of [feet|inches|kilometers|meters|miles|dd],
                    described in http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
      @param resolution: resolution of the data - pixel size in @mapunits
      """
      if svcObjId is None:
         svcObjId = lyrId
      LMSpatialObject.__init__(self, epsgcode, bbox)
      ServiceObject.__init__(self,  userId, svcObjId, serviceType, 
                             moduleType=moduleType, metadataUrl=metadataUrl,
                             parentMetadataUrl=parentMetadataUrl, modTime=modTime)
#      ogr.UseExceptions()
      self.name = name
      self._layerUserId = userId
      self._layerId = lyrId
      self.squid = squid
      self._dlocation = None
      self.setDLocation(dlocation)
      self._verify = None
      self._setVerify(verify=verify)
      self.lyrMetadata = {}
      self.loadLyrMetadata(metadata)
      self._dataFormat = dataFormat
      self._gdalType = gdalType
      self._ogrType = ogrType
      self._setUnits(mapunits)
      self.valUnits = valUnits
      self._valAttribute = valAttribute
      self.nodataVal = nodataVal
      self.minVal = minVal 
      self.maxVal = maxVal
      self._mapunits = None 
      self._setUnits(mapunits)
      self.resolution = resolution
      self._mapFilename = None
      
# ...............................................
   def setLayerId(self, lyrid):
      """
      @summary: Sets the database id of the Layer record, which can be used  
                by multiple Parameterized Layer objects
      @param lyrid: The record id for the database 
      """
      self._layerId = lyrid

   def getLayerId(self):
      """
      @summary: Returns the database id of the Layer record, which can be used  
                by multiple Parameterized Layer objects
      """
      return self._layerId

# ...............................................
   def setLayerUserId(self, lyruserid):
      """
      @summary: Sets the User id of the Layer record, which can be used by 
                by multiple Parameterized Layer objects
      @param lyruserid: The user id for the layer 
      """
      self._layerUserId = lyruserid

   def getLayerUserId(self):
      """
      @summary: Returns the User id of the Layer record, which can be used by 
                by multiple Parameterized Layer objects
      """
      return self._layerUserId
   
# ...............................................
   def getValAttribute(self):
      return self._valAttribute

# ...............................................
   @property
   def dataFormat(self):
      return self._dataFormat
   
   @property
   def gdalType(self):
      return self._gdalType

   @property
   def ogrType(self):
      return self._ogrType

   
# ...............................................
   def _setUnits(self, mapunits):
      """
      @summary Set the units parameter for the layer
      @param mapunits: The new units type
      @raise LMError: If the new units type is not one of the pre-determined 
               legal unit types (feet, inches, kilometers, meters, miles, dd, ds)
      """
      if mapunits is None or mapunits == '':
         self._mapunits = ''
      else:
         mapunits = mapunits.lower()
         try:
            LegalMapUnits.index(mapunits)
         except:
            raise LMError(['Illegal Unit type', mapunits])
         else:
            self._mapunits = mapunits
   
   @property
   def mapUnits(self):
      return self._mapunits

# ...............................................
   def readData(self, dlocation, driverType):
      """
      @summary: Read OGR- or GDAL data and save the on the _Layer object
      @param dlocation: Location of the data
      @param ogrType: GDAL or OGR-supported data format type code, available at
                      http://www.gdal.org/formats_list.html and
                      http://www.gdal.org/ogr/ogr_formats.html
      @return: boolean for success/failure 
      @raise LMError: on failure to read data.
      """
      raise LMError(currargs='readData must be implemented in Subclass')

# ...............................................
   def computeHash(self, dlocation=None, content=None):
      """
      @summary: Compute the sha256sum of the file at dlocation.
      @return: hash string for data file
      """
      if content is not None:
         value = computeHash(content=content)
      else:
         if dlocation is None:
            dlocation = self._dlocation
         value = computeHash(dlocation=dlocation)

      return value

# ...............................................
   def verifyHash(self, hashval, dlocation=None, content=None):
      """
      @summary: Compute the sha256sum of the file at dlocation.
      @param hash: hash string to compare with data
      """
      if content is not None:
         verified = verifyHash(hashval, content=content)
      else:
         if dlocation is None:
            dlocation = self._dlocation
         verified = verifyHash(hashval, dlocation=dlocation)
      return verified
   
# ...............................................
   def _setVerify(self, verify=None, dlocation=None, content=None):
      value = None
      if verify is not None:
         self._verify = verify
      else:
         if content is not None:
            value = self.computeHash(content=content)
         else:
            if dlocation is None:
               dlocation = self._dlocation  
            if dlocation is not None and os.path.exists(dlocation):       
               value = self.computeHash(dlocation=dlocation)
         self._verify = value

# ...............................................
   @property
   def verify(self):
      return self._verify

# ...............................................
   def getMetaLocation(self): 
      return self._metalocation

# ...............................................
   def createLocalDLocation(self, extension):
      from LmServer.common.datalocator import EarlJr
      earlJr = EarlJr()
      dloc = earlJr.createOtherLayerFilename(self._layerUserId, self._epsg, 
                                             self.name, ext=extension)
      return dloc
   
   def getDLocation(self): 
      return self._dlocation
   
   def clearDLocation(self): 
      self._dlocation = None
      self._absolutePath = None 
      self._baseFilename = None
   
   def setDLocation(self, dlocation=None):
      """
      @summary: Set the Layer._dlocation attribute if it is None.  Use dlocation
                if provided, otherwise calculate it.
      @note: If _dlocation is already present, this does nothing.
      """
      # Only set DLocation if it is currently None
      if self._dlocation is None:
         if dlocation is None: 
            dlocation = self.createLocalDLocation()
         self._dlocation = dlocation
      # Populate absolutePath and baseFilename attributes
      if self._dlocation is not None:
         self._absolutePath, self._baseFilename = os.path.split(self._dlocation)
      else:
         self._absolutePath, self._baseFilename = None, None
                  
# ...............................................
   def clearData(self):
      raise LMError('Method must be implemented in subclass')
                  
# ...............................................
   def copyData(self):
      raise LMError('Method must be implemented in subclass')
                  
# ...............................................
   def getAbsolutePath(self):
      if self._absolutePath is None and self._dlocation is not None:
         self._absolutePath, self._baseFilename = os.path.split(self._dlocation)
      return self._absolutePath
      
# ...............................................
   def getBaseFilename(self):
      if self._baseFilename is None and self._dlocation is not None:
         self._absolutePath, self._baseFilename = os.path.split(self._dlocation)
      return self._baseFilename
   
# ...............................................
   def getSRSAsWkt(self):
      try:
         srs = self.getSRS()
      except Exception, e:
         raise
      else:
         wkt = srs.ExportToWkt()
         return wkt 
                  
# ...............................................
   def dumpLyrMetadata(self):
      return LMObject._dumpMetadata(self, self.lyrMetadata)
 
# ...............................................
   def loadLyrMetadata(self, newMetadata):
      self.lyrMetadata = LMObject._loadMetadata(self, newMetadata)

# ...............................................
   def addLyrMetadata(self, newMetadataDict):
      self.lyrMetadata = LMObject._addMetadata(self, newMetadataDict, 
                                  existingMetadataDict=self.lyrMetadata)

# ...............................................
# Properties
# ...............................................           
# .............................................................................
class _LayerParameters(LMObject):
   # Constants for metadata dictionary keys
   PARAM_FILTER_STRING = 'filterString'
   PARAM_VAL_NAME = 'valName'
   PARAM_VAL_UNITS = 'valUnits'
   
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, userId, paramId=None, matrixIndex=-1, metadata={}, modTime=None):
      """
      @summary Initialize the _LayerParameters class instance
      @param userId: Id for the owner of these data.  If these 
                      parameters are not held in a separate table, this value
                      is the layerUserId.  
      @param paramId: The database Id for the parameter values.  If these 
                      parameters are not held in a separate table, this value
                      is the layerId.  
      @param matrixIndex: Index of the position in PAM or other matrix.  If this 
                      Parameterized Layer is not a Matrix input, value is -1.
      @param metadata: Dictionary of metadata keys/values; key constants are 
                       class attributes.
      @param modTime: time/date last modified
      """
      self._paramUserId = userId
      self._paramId = paramId
      self.paramMetadata = {}
      self.loadParamMetadata(metadata)
      self._matrixIndex = matrixIndex
      self.paramModTime = modTime
      
# ...............................................
   def dumpParamMetadata(self):
      return super(_LayerParameters, self)._dumpMetadata(self.paramMetadata)
 
# ...............................................
   def loadParamMetadata(self, newMetadata):
      self.paramMetadata = super(_LayerParameters, self)._loadMetadata(newMetadata)

# ...............................................
   def addParamMetadata(self, newMetadataDict):
      self.paramMetadata = super(_LayerParameters, self)._addMetadata(
                                       newMetadataDict, 
                                       existingMetadataDict=self.paramMetadata)

# ...............................................
   def setParametersId(self, paramid):
      """
      @summary: Sets the database id of the Layer Parameters (either 
                PresenceAbsence or AncillaryValues) record, which can be used by 
                multiple Parameterized Layer objects
      @param paramid: The record id for the database 
      """
      self._paramId = paramid

   def getParamId(self):
      """
      @summary: Returns the database id of the Layer Parameters (either 
                PresenceAbsence or AncillaryValues) record, which can be used by 
                multiple Parameterized Layer objects
      """
      return self._paramId

# ...............................................
   def setParamUserId(self, usr):
      """
      @summary: Sets the User id of the Layer Parameters (either 
                PresenceAbsence or AncillaryValues) record, which can be used by 
                multiple Parameterized Layer objects
      @param usr: The user id for the parameters 
      """
      self._paramUserId = usr

   def getParamUserId(self):
      """
      @summary: Returns the User id of the Layer Parameters (either 
                PresenceAbsence or AncillaryValues) record, which can be used by 
                multiple Parameterized Layer objects
      """
      return self._paramUserId
   
# ...............................................
   def setMatrixIndex(self, matrixIdx):
      """
      @summary: Sets the _matrixIndex on the object.  This identifies 
                the position of the parameterized layer object in the 
                appropriate MatrixLayerset (PAM or GRIM)
      @param matrixIndex: The matrixIndex
      """
      self._matrixIndex = matrixIdx

   def getMatrixIndex(self):
      """
      @summary: Returns _matrixIndex on the layer.  This identifies 
                the position of the parameterized layer object in a 
                MatrixLayerset (and the PAM or GRIM)
      """
      return self._matrixIndex
   
# ...............................................
   def setTreeIndex(self, treeIdx):
      """
      @summary: Sets the _treeIndex on the layer.  This identifies 
                the position of the layer in a tree
      @param treeIdx: The treeIndex
      """
      self._treeIndex = treeIdx

   def getTreeIndex(self):
      """
      @summary: Returns _treeIndex on the object.  This identifies 
                the position of the layer in a tree
      """
      return self._treeIndex

# .............................................................................
# Raster class (inherits from _Layer)
# .............................................................................
class Raster(_Layer):
   """
   Class to hold information about a raster dataset.
   """
   # ...............................................       
   def __init__(self, name, userId, epsgcode, lyrId=None, 
                squid=None, verify=None, dlocation=None, 
                metadata={}, dataFormat=None, gdalType=None, 
                valUnits=None, nodataVal=None, minVal=None, maxVal=None, 
                mapunits=None, resolution=None, 
                bbox=None,
                svcObjId=None, serviceType=LMServiceType.LAYERS, 
                moduleType=LMServiceModule.LM,
                metadataUrl=None, parentMetadataUrl=None, modTime=None):
      """
      @summary Raster constructor, inherits from _Layer
      @copydoc LmServer.base.layer2._Layer::__init__()
      """
      self._verifyDataDescription(gdalType, dataFormat)
      self.size = None
      self.srs = None
      self.geoTransform = None
      # Only used to read from external sources, then return or write to disk,
      # in the same or alternate GDAL-supported format 
      self._data = None
      # Update layer parameters values if not provided
      (srs, geoTransform, size, dataFormat, gdalType, dlocation, resolution, 
       minVal, maxVal, nodataVal, msgs) = self.populateStats(dlocation, 
                                             gdalType, dataFormat, bbox, 
                                             resolution, 
                                             minVal, maxVal, nodataVal)
      self.srs = srs
      self.geoTransform = geoTransform
      self.size = size
      if msgs:
         print 'Layer.populateStats Warning: \n{}'.format('\n'.join(msgs))
      _Layer.__init__(self, name, userId, epsgcode, lyrId=lyrId, 
                squid=squid, verify=verify, dlocation=dlocation, 
                metadata=metadata, dataFormat=dataFormat, gdalType=gdalType, 
                valUnits=valUnits, valAttribute='pixel', 
                nodataVal=nodataVal, minVal=minVal, maxVal=maxVal, 
                mapunits=mapunits, resolution=resolution, 
                bbox=bbox,
                svcObjId=svcObjId, serviceType=serviceType, moduleType=moduleType,
                metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl, 
                modTime=modTime)

# ...............................................
   def getFormatLongName(self):
      name = ''
      drv = gdal.GetDriverByName(self._dataFormat)
      if drv is not None:
         name = drv.GetMetadataItem('DMD_LONGNAME')
      return name
         
# ...............................................
   def _setIsDiscreteData(self, isDiscreteData, isCategorical):
      if isDiscreteData is None:
         if isCategorical:
            isDiscreteData = True
         else:
            isDiscreteData = False
      self._isDiscreteData = isDiscreteData
   
# ...............................................
   def createLocalDLocation(self, ext=None):
      """
      @summary: Create local filename for this layer.  
      @param ext: File extension for filename
      @note: Data files which are not default User data files (stored 
              locally and using this method)
             (in /UserData/<userid>/<epsgcode>/Layers directory) should be 
             created in the appropriate Subclass (EnvironmentalLayer, OccurrenceSet, 
             SDMProjections) 
      """
      if ext is None:
         if self._dataFormat is None:
            ext = OutputFormat.TMP
         else:
            ext = GDALFormatCodes[self._dataFormat]['FILE_EXT']
      dloc = super(_Layer, self).createLocalDLocation(ext)
      return dloc

# .............................................................................
# Properties
# .............................................................................
   def _verifyDataDescription(self, gdalType, gdalFormat):
      """
      @summary Verifies that the dataType and format are either LM-supported 
               GDAL types or None.
      @raise LMError: Thrown when type is not legal for a Raster.  
      """
      if gdalType is not None and gdalType not in GDALDataTypes:
         raise LMError(['Unsupported Raster type', gdalType])      
      if gdalFormat is not None and gdalFormat not in GDALFormatCodes.keys():
         raise LMError(['Unsupported Raster format', gdalFormat])

# .............................................................................
# Public methods
# .............................................................................
# ...............................................
   def _openWithGDAL(self, dlocation=None, bandnum=1):
      """
      @return: a GDAL dataset object
      """
      if dlocation is None:
         dlocation = self._dlocation
      try:
         dataset = gdal.Open(str(dlocation), gdalconst.GA_ReadOnly)
         band = dataset.GetRasterBand(1)
      except Exception, e:
         raise LMError(['Unable to open dataset or band {} with GDAL ({})'
                        .format(dlocation, str(e))])
      return dataset, band

# ...............................................
   def getHistogram(self, bandnum=1):
      """
      @return: a list of data values present in the dataset
      @note: this returns only a list, not a true histogram.  
      @note: this only works on 8-bit data.
      """
      vals = []
      dataset, band = self._openWithGDAL(bandnum=bandnum)
      
      # Get histogram only for 8bit data (projections)
      if band.DataType == gdalconst.GDT_Byte:
         hist = band.GetHistogram()
         for i in range(len(hist)):
            if i > 0 and i != self.nodataVal and hist[i] > 0:
               vals.append(i)
      else:
         print 'Histogram calculated only for 8-bit data'
      return vals
   
# ...............................................
   def getIsDiscreteData(self):
      return self._isDiscreteData
   
# ...............................................
   def getSize(self, bandnum=1):
      """
      @summary: Return a tuple of xsize and ysize (in pixels).
      @return: A tuple of size 2, where the first number is the number of 
               columns and the second number is the number of rows.
      """
      dataset, band = self._openWithGDAL(bandnum=bandnum)
      size = (dataset.RasterXSize, dataset.RasterYSize)
      return size
   
# ...............................................
   def populateStats(self, dlocation, gdalType, dataFormat, bbox, resolution,
                     minVal, maxVal, nodataVal, bandnum=1):
      """
      @summary: Updates or fills layer parameters by reading the data.
      @postcondition: prints warning if file is invalid raster
      @postcondition: prints warning if data format and type differ from GDAL-reported
      @postcondition: renames file with supported extension if it differs
      """
      srs = geoTransform = size = msgs = None
      if dlocation is not None:
         msgs = []
         if not os.path.exists(dlocation):
            msgs.append('File does not exist: {}'.format(dlocation))
         else:
            dataset, band = self._openWithGDAL(dlocation=dlocation, bandnum=bandnum)
            srs = dataset.GetProjection()
            size = (dataset.RasterXSize, dataset.RasterYSize)
            geoTransform = dataset.GetGeoTransform()
            ulx = geoTransform[0]
            xPixelSize = geoTransform[1]
            uly = geoTransform[3]
            yPixelSize = geoTransform[5]
            
            drv = dataset.GetDriver()
            gdalFormat = drv.GetDescription()
            if dataFormat is None:
               dataFormat = gdalFormat
            elif dataFormat != gdalFormat:
               msgs.append('Invalid gdalFormat {}, changing to {} for layer {}'
                           .format(dataFormat, gdalFormat, dlocation))
               dataFormat = gdalFormat
            # Fix extension if incorrect
            head, ext = os.path.splitext(dlocation)
            correctExt = GDALFormatCodes[dataFormat]['FILE_EXT']
            if ext != correctExt:
               msgs.append('Invalid extension {}, renaming to {} for layer {}'
                           .format(ext, correctExt, dlocation))
               oldDl = dlocation
               dlocation = head + correctExt
               os.rename(oldDl, dlocation)
   
            # Assumes square pixels
            if resolution is None:
               resolution = xPixelSize
            if bbox is None:
               lrx = ulx + xPixelSize * dataset.RasterXSize
               lry = uly + yPixelSize * dataset.RasterYSize
               bbox = [ulx, lry, lrx, uly]
            if gdalType is None:
               gdalType = band.DataType
            elif gdalType != band.DataType:
               msgs.append('Invalid datatype {}, changing to {} for layer {}'
                           .format(gdalType, band.DataType, dlocation))
               gdalType = band.DataType
            bmin, bmax, bmean, bstddev = band.GetStatistics(False,True)
            if minVal is None:
               minVal = bmin
            if maxVal is None:
               maxVal = bmax
            if nodataVal is None:
               nodataVal = band.GetNoDataValue()
      return (srs, geoTransform, size, dataFormat, gdalType, dlocation, 
              resolution, minVal, maxVal, nodataVal, msgs)
         
# ...............................................
   def readFromUploadedData(self, datacontent, overwrite=False, 
                            extension=OutputFormat.GTIFF):
      """
      @summary: Read from uploaded data by writing to temporary file, saving 
                temporary filename in dlocation.  
      @raise LMError: on failure to write data or read temporary files.
      """
      self.clearDLocation()
      # Create temp location and write layer to it
      outLocation = os.path.join(UPLOAD_PATH, self.name+extension)
      self.writeLayer(srcData=datacontent, outFile=outLocation, overwrite=True)
      self.setDLocation(dlocation=outLocation)
          
# ...............................................
   def writeLayer(self, srcData=None, srcFile=None, outFile=None, overwrite=False):
      """
      @summary: Writes raster data to file.
      @param data: A stream, string, or file of valid raster data
      @param overwrite: True/False directing whether to overwrite existing 
                file or not
      @postcondition: The raster file is written to the filesystem.
      @raise LMError: on 1) failure to write file 
                         2) attempt to overwrite existing file with overwrite=False
                         3) _dlocation is None  
      """
      if outFile is None:
         outFile = self.getDLocation()
      if outFile is not None:
         self._readyFilename(outFile, overwrite=overwrite)
         
         # Copy from input file using GDAL (no test necessary later)
         if srcFile is not None:
            self.copyData(srcFile, targetDataLocation=outFile)
            
         # Copy from input stream
         elif srcData is not None:
            try:
               f = open(outFile,"w")
               f.write(srcData)
               f.close()
            except Exception, e:
               raise LMError(currargs='Error writing data to raster %s (%s)' 
                             % (outFile, str(e)))
            else:
               self.setDLocation(dlocation=outFile)
            # Test input with GDAL
            try:
               self.populateStats()
            except Exception, e:
               success, msg = self._deleteFile(outFile)
               raise LMError(currargs='Invalid data written to %s (%s); Deleted (success=%s, %s)' 
                             % (outFile, str(e), str(success), msg))
         else:
            raise LMError(currargs=
                          'Source data or source filename required for write to %s' 
                          % self._dlocation)
      else:
         raise LMError(['Must setDLocation before writing file'])
      

# .............................................
   def _copyGDALData(self, bandnum, infname, outfname, format='GTiff', kwargs={}):
      """
      @summary: Copy the dataset into a new file.  
      @param bandnum: The band number to read.
      @param outfname: Filename to write this dataset to.
      @param format: GDAL-writeable raster format to use for new dataset. 
                     http://www.gdal.org/formats_list.html
      @param doFlip: True if data begins at the southern edge of the region
      @param doShift: True if the leftmost edge of the data should be shifted 
             to the center (and right half shifted around to the beginning) 
      @param nodata: Value used to indicate nodata in the new file.
      @param srs: Spatial reference system to use for the data. This is only 
                  necessary if the dataset does not have an SRS present.  This
                  will NOT project the dataset into a different projection.
      """
      options = []
      if format == 'AAIGrid':
         options = ['FORCE_CELLSIZE=True']
         #kwargs['FORCE_CELLSIZE'] = True
         #kwargs['DECIMAL_PRECISION'] = 4
      driver = gdal.GetDriverByName(format)
      metadata = driver.GetMetadata()
      if not (metadata.has_key(gdal.DCAP_CREATECOPY) 
                and metadata[gdal.DCAP_CREATECOPY] == 'YES'):
         raise LMError(currargs='Driver %s does not support CreateCopy() method.' 
                       % format)
      inds = gdal.Open( infname )
      try:
         outds = driver.CreateCopy(outfname, inds, 0, options)
      except Exception, e:
         raise LMError(currargs='Creation failed for %s from band %d of %s (%s)'
                                % (outfname, bandnum, infname, str(e)))
      if outds is None:
         raise LMError(currargs='Creation failed for %s from band %d of %s)'
                                % (outfname, bandnum, infname))
      # Close new dataset to flush to disk
      outds = None
      inds = None
      
# ...............................................
   def copyData(self, sourceDataLocation, targetDataLocation=None, 
                format='GTiff'):
      if not format in GDALFormatCodes.keys():
         raise LMError(currargs='Unsupported raster format %s' % format)
      if sourceDataLocation is not None and os.path.exists(sourceDataLocation):
         if targetDataLocation is not None:
            dlocation = targetDataLocation
         elif self._dlocation is not None:
            dlocation = self._dlocation
         else:
            raise LMError('Target location is None')
      else:
         raise LMError('Source location %s is invalid' % str(sourceDataLocation))
      
      if not dlocation.endswith(GDALFormatCodes[format]['FILE_EXT']):
         dlocation += GDALFormatCodes[format]['FILE_EXT']
      
      self._readyFilename(dlocation)

      try:
         self._copyGDALData(1, sourceDataLocation, dlocation, format=format)
      except Exception, e:
         raise LMError(currargs='Failed to copy data source from %s to %s (%s)' 
                       % (sourceDataLocation, dlocation, str(e)))
               
# ...............................................
   def getSRS(self):
      if (self._dlocation is not None and os.path.exists(self._dlocation)):
         ds = gdal.Open(str(self._dlocation), gdalconst.GA_ReadOnly)
         wktSRS = ds.GetProjection()
         if wktSRS is not '':
            srs = osr.SpatialReference()
            srs.ImportFromWkt(wktSRS)
         else:
            srs = self.createSRSFromEPSG()
         ds = None
         return srs
      else:
         raise LMError(currargs='Input file %s does not exist' % self._dlocation)

# ...............................................
   def writeSRS(self, srs):
      """
      @summary: Writes spatial reference system information to this raster file.
      @param srs: An osgeo.osr.SpatialReference object or
                  A WKT string describing the desired spatial reference system.  
                    Raster.populateStats will populate the Raster.srs 
                    attribute with a correctly formatted string.  Correctly 
                    formatted strings are also output by:
                      * osgeo.gdal.Dataset.GetProjection
                      * osgeo.osr.SpatialReference.ExportToWKT 
      @postcondition: The raster file is updated with new srs information.
      @raise LMError: on failure to open dataset or write srs
      """
      from LmServer.common.geotools import GeoFileInfo

      if (self._dlocation is not None and os.path.exists(self._dlocation)):
         geoFI = GeoFileInfo(self._dlocation, updateable=True)
         if isinstance(srs, osr.SpatialReference):
            srs = srs.ExportToWkt()
         geoFI.writeWktSRS(srs)
            
# ...............................................
   def copySRSFromFile(self, fname):
      """
      @summary: Writes spatial reference system information from provided file 
                to this raster file.
      @param fname: Filename for dataset from which to copy spatial reference 
                system. 
      @postcondition: The raster file is updated with new srs information.
      @raise LMError: on failure to open dataset or write srs
      """
      from LmServer.common.geotools import GeoFileInfo
      
      if (fname is not None and os.path.exists(fname)):
         srs = GeoFileInfo.getSRSAsWkt(fname)
         self.writeSRS(srs)
      else:
         raise LMError(['Unable to read file %s' % fname])
      
# ...............................................
   def isValidDataset(self):
      """
      @summary: Checks to see if dataset is a valid raster
      @return: True if raster is a valid GDAL dataset; False if not
      """
      valid = True
      if (self._dlocation is not None and os.path.exists(self._dlocation)):
         try:
            self._dataset = gdal.Open(self._dlocation, gdalconst.GA_ReadOnly)
         except Exception, e:
            valid = False
            
      return valid

# ...............................................
   def deleteData(self, dlocation=None, isTemp=False):
      """
      @summary: Deletes the local data file(s) on disk
      @note: Does NOT clear the dlocation attribute
      """
      success = False
      if dlocation is None:
         dlocation = self._dlocation
      if (dlocation is not None and os.path.isfile(dlocation)):
         drv = gdal.GetDriverByName(self._dataFormat)
         result = drv.Delete(dlocation)
         if result == 0:
            success = True
      if not isTemp:
         pth, fname = os.path.split(dlocation)
         if os.path.isdir(pth) and len(os.listdir(pth)) == 0:
            try:
               os.rmdir(pth)
            except:
               print 'Unable to rmdir %s' % pth
      return success

# .............................................................................
# Superclass _Layer methods overridden
# .............................................................................
# ...............................................
   def getWCSRequest(self, bbox=None, resolution=None):
      """
      @note: All implemented _Rasters will also be a Subclass of ServiceObject 
      """
      raise LMError(currargs='getWCSRequest must be implemented in Subclasses also inheriting from ServiceObject')

# .............................................................................
# Vector class (inherits from _Layer)
# .............................................................................
class Vector(_Layer):
   """
   Class to hold information about a vector dataset.
   """
   # ...............................................       
   def __init__(self, name, userId, epsgcode, lyrId=None, 
                squid=None, verify=None, dlocation=None, 
                metadata={}, dataFormat=None, ogrType=None,
                valUnits=None, valAttribute=None, 
                nodataVal=None, minVal=None, maxVal=None, 
                mapunits=None, resolution=None, 
                bbox=None,
                svcObjId=None, serviceType=LMServiceType.LAYERS, 
                moduleType=LMServiceModule.LM,
                metadataUrl=None, parentMetadataUrl=None, modTime=None,
                featureCount=0, featureAttributes={}, features={}, fidAttribute=None):
      """
      @summary Vector constructor, inherits from _Layer
      @copydoc LmServer.base.layer2._Layer::__init__()
      @param featureCount: number of features in this layer.  This is stored in
                    database and may be populated even if the features are not.
      @param featureAttributes: Dictionary with key attributeName and value
                    attributeFeatureType (ogr.OFTString, ogr.OFTReal, etc) for 
                    the features in this dataset 
      @param features: Dictionary of features, where the key is the unique 
                    identifier (this could be the Feature Identifier, FID) 
                    and the value is a list of attribute values.  
                    The position of each value in the list corresponds to the 
                    key index in featureAttributes. 
      @param fidAttribute: Attribute containing the unique identifier or 
                    feature ID (FID) for this layer/shapefile.
      """
      self._geomIdx = None
      self._geomFieldName = OccurrenceFieldNames.GEOMETRY_WKT[0]
      self._geomFieldType = OFTString      
      self._geometry = None
      self._convexHull = None      
      self._localIdIdx = None
      self._localIdFieldName = OccurrenceFieldNames.LOCAL_ID[0]
      self._localIdFieldType = OFTInteger
      self._fidAttribute = fidAttribute
      self._featureAttributes = {}
      self._features = {}
      self._featureCount = 0

      _Layer.__init__(self, name, userId, epsgcode, lyrId=lyrId, 
                squid=squid, verify=verify, dlocation=dlocation, 
                metadata=metadata, dataFormat=dataFormat, ogrType=ogrType, 
                valUnits=valUnits, valAttribute=valAttribute, 
                nodataVal=nodataVal, minVal=minVal, maxVal=maxVal, 
                mapunits=mapunits, resolution=resolution, 
                bbox=bbox,
                svcObjId=svcObjId, serviceType=serviceType, moduleType=moduleType,
                metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl, 
                modTime=modTime)
      self._verifyDataDescription(ogrType, dataFormat)
      # The following may be reset by setFeatures:
      # features, featureAttributes, featureCount, geomIdx, localIdIdx, geom, convexHull
      self.setFeatures(features, featureAttributes, featureCount=featureCount)
      try:
         # sets features, featureAttributes, and featureCount (if doReadData)
         (newBBox, localIdIdx, geomIdx) = self.readData(dlocation=dlocation, 
                                        dataFormat=dataFormat, doReadData=False)
      # Reset some attributes based on data
         if newBBox is not None:
            self.bbox = newBBox
         self._geomIdx = geomIdx
         self._localIdIdx = localIdIdx   
      except Exception, e:
         print 'Warning in Vector.__init__: %s' % str(e)
      
# .............................................................................
# Static methods
# .............................................................................
# LM field definitions for data fields (geomwkt, ufid, url) written to shapefiles. 
   
# .............................................................................
# Properties
# .............................................................................

# ...............................................
   def getFormatLongName(self):
      return self._dataFormat 
   
# ...............................................
   def _verifyDataDescription(self, ogrType, ogrFormat):
      """
      @summary Sets the data type for the vector
      @param ogrType: OGR type of the vector, valid choices are in OGRDataTypes
      @param ogrFormat: OGR Vector Format, only a subset (in OGRFormats) are 
                        valid here
      @raise LMError: Thrown when type is not legal for a Vector.  
      """
      if ogrType is not None and ogrType not in OGRDataTypes:
         raise LMError(['Unsupported Vector type', ogrType])
      if ogrFormat is not None and ogrFormat not in OGRFormats.keys():
         raise LMError(['Unsupported Vector format', ogrFormat])
      
# ...............................................
   def _getFeatureCount(self):
      if self._featureCount is None:
         if self._features:
            self._featureCount = len(self._features)
      return self._featureCount
   
   def _setFeatureCount(self, count):
      """
      If Vector._features are present, the length of that list takes precedent 
      over the count parameter.
      """
      if self._features:
         self._featureCount = len(self._features)
      else:
         self._featureCount = count

   featureCount = property(_getFeatureCount, _setFeatureCount)
   
   def isFilled(self):
      """
      Has the layer been populated with its features.  An empty dataset is 
      considered 'filled' if featureAttributes are present, even if no features 
      exist.  
      """
      if self._featureAttributes:
         return True
      else:
         return False
   
   # ..................................
   @property
   def features(self):
      """
      @summary: Converts the private dictionary of features into a list of 
                   LmAttObjs
      @note: Uses list comprehensions to create a list of LmAttObjs and another
                to create a list of (key, value) pairs for the attribute 
                dictionary
      @return: A list of LmAttObjs
      """
      return [LmAttObj(dict([
                     (self._featureAttributes[k2][0], self._features[k1][k2]) \
                     for k2 in self._featureAttributes]), 
                       "Feature") for k1 in self._features]
   
   # ..................................
   @property
   def featureAttributes(self):
      return self._featureAttributes
   
   # ..................................
   @property
   def fidAttribute(self):
      return self._fidAttribute
   
# .............................................................................
   def setFeatures(self, features, featureAttributes, featureCount=0):
      """
      @summary: Sets Vector attributes: 
                   _features, _featureAttributes and featureCount.  
                Also sets one or more of:
                   _geomIdx, _localIdIdx, _geometry, _convexHull
      @param features: a dictionary of features, with key the featureid (FID) or
                localid of the feature, and value a list of values for the 
                feature.  Values are ordered in the same order as 
                in featureAttributes.
      @param featureAttributes: a dictionary of featureAttributes, with key the 
                index of this attribute in each feature, and value a tuple of
                (field name, field type (OGR))
      @param featureCount: the number of features in these data
      """
      if featureAttributes:
         self._featureAttributes = featureAttributes
         self._setGeometryIndex()
         self._setLocalIdIndex()
      else:
         self._featureAttributes = {}
         self._geomIdx = None
         self._localIdIdx = None

      if features:
         self._features = features
         self._setGeometry()
         self._featureCount = len(features)
      else:
         self._features = {}
         self._geometry = None
         self._convexHull = None
         self._featureCount = featureCount

   # ..................................
   def getFeatures(self):
      """
      @summary: Gets Vector._features as a dictionary of FeatureIDs (FID) with 
                a list of values
      """
      return self._features

   # ..................................
   def clearFeatures(self):
      """
      @summary: Clears Vector._features, Vector._featureAttributes, and 
                Vector.featureCount      
      """
      del self._featureAttributes
      del self._features 
      self.setFeatures(None, None)

   # ..................................
   def addFeatures(self, features):
      """
      @summary: Adds to Vector._features and updates Vector.featureCount
      @param features: a dictionary of features, with key the featureid (FID) or
                localid of the feature, and value a list of values for the 
                feature.  Values are ordered in the same order as 
                in featureAttributes.
      """
      if features:
         for fid, vals in features.iteritems():
            self._features[fid] = vals
         self._featureCount = len(self._features)
         
   def getFeatureAttributes(self):
      return self._featureAttributes

# ...............................................
   def setValAttribute(self, valAttribute):
      """
      @summary: Sets Vector._valAttribute.  If the featureAttributes are 
                present, check to make sure valAttribute exists in the dataset.
      @param valAttribute: field name for the attribute to map 
      """
      self._valAttribute = None
      if self._featureAttributes:
         if valAttribute:
            for idx in self._featureAttributes.keys():
               fldname, fldtype = self._featureAttributes[idx]
               if fldname == valAttribute:
                  self._valAttribute = valAttribute
                  break
            if self._valAttribute is None:
               raise LMError('Map attribute %s not present in dataset %s' 
                             % (valAttribute, self._dlocation))
      else:
         self._valAttribute = valAttribute

# ...............................................
   def _setGeometryIndex(self):
      if self._geomIdx is None and self._featureAttributes:
         for idx, (colname, coltype) in self._featureAttributes.iteritems():
            if colname == self._geomFieldName:
               self._geomIdx = idx
               break

   def _getGeometryIndex(self):
      if self._geomIdx is None:
         self._setGeometryIndex()
      return self._geomIdx

# ...............................................
   def _setLocalIdIndex(self):
      if self._localIdIdx is None and self._featureAttributes:
         for idx, (colname, coltype) in self._featureAttributes.iteritems():
            if colname in OccurrenceFieldNames.LOCAL_ID:
               self._localIdIdx = idx
               break

   def getLocalIdIndex(self):
      if self._localIdIdx is None:
         self._setLocalIdIndex()
      return self._localIdIdx

# ...............................................
   def createLocalDLocation(self, ext=OutputFormat.SHAPE):
      """
      @summary: Create local filename for this layer.  
      @param ext: File extension for filename
      @note: Data files which are not default User data files (stored 
              locally and using this method)
             (in /UserData/<userid>/<epsgcode>/Layers directory) should be 
             created in the appropriate Subclass (EnvironmentalLayer, OccurrenceSet, 
             SDMProjections) 
      """
      dloc = super(Vector, self).createLocalDLocation(ext)
      return dloc

# .............................................................................
# Public methods
# .............................................................................

   def getShapefiles(self, otherlocation=None):
      shpnames = []
      if otherlocation is not None:
         dloc = otherlocation
      else:
         dloc = self._dlocation
      if dloc is not None:
         base, ext = os.path.splitext(dloc)
         fnames = glob.glob(base + '.*')
         for fname in fnames:
            base, ext = os.path.splitext(fname)
            if ext in SHAPEFILE_EXTENSIONS:
               shpnames.append(fname)
      return shpnames
   
# ...............................................
   def zipShapefiles(self, baseName=None):
      """
      @summary: Returns a wrapper around a tar gzip file stream
      @param baseName: (optional) If provided, this will be the prefix for the 
                          names of the shape file's files in the zip file.
      """
      fnames = self.getShapefiles()
      tgStream = StringIO()      
      zipf = zipfile.ZipFile(tgStream, mode="w", 
                             compression=zipfile.ZIP_DEFLATED, allowZip64=True)
      if baseName is None:
         baseName = os.path.splitext(os.path.split(fnames[0])[1])[0]
      
      for fname in fnames:
         ext = os.path.splitext(fname)[1]
         zipf.write(fname, "%s%s" % (baseName, ext))
      zipf.close()
      
      tgStream.seek(0)
      ret = ''.join(tgStream.readlines())
      tgStream.close()
      return ret

# # ...............................................
#    def populateStats(self, dlocation, dataFormat):
#       """
#       @summary: Sets self.ogrType and self._bbox by opening dataset
#       """
#       msgs = []
#       if dlocation is not None:
#          if not os.path.exists(dlocation):
#             msgs.append('Vector file does not exist: {}'.format(dlocation))
#          else:
#             try:
#                ds = ogr.Open(dlocation)
#                ds = None
#             except Exception, e:
#                print('Unable to open vector dlocation {}: {}'
#                      .format(dlocation, str(e)) )
#             try:
#                self.readData(dlocation=dlocation, dataFormat=dataFormat)           
#             except LMError, e:
#                raise
#             except Exception, e:
#                raise LMError(currargs='Unable to read vector data {}: {}'
#                              .format(dlocation, str(e)))
                   
# ...............................................
   def getMinFeatures(self):
      """
      @summary: Returns a dictionary of all feature identifiers with their 
                well-known-text geometry.
      @return dictionary of {feature id (fid): wktGeometry} 
      """
      feats = {}
      if self._features and self._featureAttributes:
         self._setGeometryIndex()
            
         for fid in self._features.keys():
            feats[fid] = self._features[fid][self._geomIdx]
         
      return feats


# ...............................................
   def isValidDataset(self, dlocation=None):
      """
      @summary: Checks to see if the dataset at self.dlocations is a valid 
                vector readable by OGR.
      @return: True if dataset is a valid OGR dataset; False if not
      """
      valid = False
      if dlocation is None:
         dlocation = self._dlocation
      if (dlocation is not None
          and (os.path.isdir(dlocation) 
               or os.path.isfile(dlocation)) ):
         try:
            ds = ogr.Open(dlocation)
            ds.GetLayer(0)
         except Exception, e:
            pass
         else:
            valid = True
      return valid

# ...............................................
   def deleteData(self, dlocation=None, isTemp=False):
      """
      @summary: Deletes the local data file(s) on disk
      @note: Does NOT clear the dlocation attribute
      @note: May be extended to delete remote data controlled by us.
      """
      if dlocation is None:
         dlocation = self._dlocation
      deleteDir = False
      if not isTemp:
         self.clearLocalMapfile()
         deleteDir = True
      self._deleteFile(dlocation, deleteDir=deleteDir)
#       self.clearDLocation()

# ...............................................
   @staticmethod
   def getXY(wkt):
      startidx = wkt.find('(')
      if wkt[:startidx].strip().lower() == 'point':
         tmp = wkt[startidx+1:]
         endidx = tmp.find(')')
         tmp = tmp[:endidx]
         vals = tmp.split()
         if len(vals)  == 2:
            try:
               x = float(vals[0])
               y = float(vals[1])
               return x, y
            except:
               return None
         else:
            return None
      else:
         return None

# ...............................................
   def writeLayer(self, srcData=None, srcFile=None, outFile=None, overwrite=False):
      """
      @summary: Writes vector data to file and sets dlocation.
      @param srcData: A stream, or string of valid vector data
      @param srcFile: A filename for valid vector data.  Currently only 
                      supports CSV and ESRI shapefiles.
      @param overwrite: True/False directing whether to overwrite existing 
                file or not
      @postcondition: The raster file is written to the filesystem.
      @raise LMError: on 1) failure to write file 
                         2) attempt to overwrite existing file with overwrite=False
                         3) _dlocation is None  
      """
      if srcFile is not None:
         self.readData(dlocation=srcFile)
      if outFile is None:
         outFile = self.getDLocation()
      if self.features is not None:
         self.writeShapefile(dlocation=outFile, overwrite=overwrite)
      # No file, no features, srcData is iterable, write as CSV
      elif srcData is not None:
         if isinstance(srcData, ListType) or isinstance(srcData, TupleType):
            if not outFile.endswith(OutputFormat.CSV):
               raise LMError('Iterable input vector data can only be written to CSV')
            else:
               self.writeCSV(dlocation=outFile, overwrite=overwrite)
         else:
            raise LMError('Writing vector is currently supported only for file or iterable input data')
      self.setDLocation(dlocation=outFile)

# ...............................................
   @staticmethod
   def _createPointShapefile(drv, outpath, spRef, lyrname, lyrDef=None,  
                      fldnames=None, idCol=None, xCol=None, yCol=None, 
                      overwrite=True):
      nameChanges = {}
      dlocation = os.path.join(outpath, lyrname + '.shp')
      if os.path.isfile(dlocation):
         if overwrite:
            drv.DeleteDataSource(dlocation)
         else:
            raise LMError('Layer %s exists, creation failed' % dlocation)
      newDs = drv.CreateDataSource(dlocation)
      if newDs is None:
         raise LMError('Dataset creation failed for %s' % dlocation)
      newLyr = newDs.CreateLayer(lyrname, geom_type=ogr.wkbPoint, srs=spRef)
      if newLyr is None:
         raise LMError('Layer creation failed for %s' % dlocation)
      
      # If LayerDefinition is provided, create and add each field to new layer
      if lyrDef is not None:
         for i in range(lyrDef.GetFieldCount()):
            fldDef = lyrDef.GetFieldDefn(i)
#             fldName = fldDef.GetNameRef()
            returnVal = newLyr.CreateField(fldDef)
            if returnVal != 0:
               raise LMError('CreateField failed for \'%s\' in %s' 
                             % (fldDef.GetNameRef(), dlocation))
      # If layer fields are not yet defined, create from fieldnames
      elif (fldnames is not None and idCol is not None and 
            xCol is not None and yCol is not None):
         # Create field definitions
         fldDefList = []
         for fldname in fldnames:
            if fldname in (xCol, yCol):
               fldDefList.append(ogr.FieldDefn(fldname, ogr.OFTReal))
            elif fldname == idCol:
               fldDefList.append(ogr.FieldDefn(fldname, ogr.OFTInteger))
            else:
               fdef = ogr.FieldDefn(fldname, ogr.OFTString)
               fldDefList.append(fdef)
         # Add field definitions to new layer
         for fldDef in fldDefList:
            try:
               returnVal = newLyr.CreateField(fldDef)
               if returnVal != 0:
                  raise LMError('CreateField failed for \'%s\' in %s' 
                                % (fldname, dlocation))
               lyrDef = newLyr.GetLayerDefn()
               lastIdx = lyrDef.GetFieldCount() - 1
               newFldName = lyrDef.GetFieldDefn(lastIdx).GetNameRef()
               oldFldName = fldDef.GetNameRef()
               if newFldName != oldFldName:
                  nameChanges[oldFldName] = newFldName

            except Exception, e:
               print str(e)
      else:
         raise LMError('Must provide either LayerDefinition or Fieldnames and Id, X, and Y column names')
   
      return newDs, newLyr, nameChanges

# ...............................................
   @staticmethod
   def _finishShapefile(newDs):
      wrote = None
      dloc = newDs.GetName()
      try:
         # Closes and flushes to disk
         newDs.Destroy()
      except Exception, e:
         wrote = None
      else:
         print('Closed/wrote dataset %s' % dloc)
         wrote = dloc
         
         try:
            retcode = subprocess.call(["shptree", "%s" % dloc])
            if retcode != 0: 
               print 'Unable to create shapetree index on %s' % dloc
         except Exception, e:
            print 'Unable to create shapetree index on %s: %s' % (dloc, str(e))
      return wrote
      
# ...............................................
   @staticmethod
   def _getSpatialRef(srsEPSGOrWkt, layer=None):
      spRef = None
      if layer is not None:
         spRef = layer.GetSpatialRef()
         
      if spRef is None:
         spRef = osr.SpatialReference()
         try:
            spRef.ImportFromEPSG(srsEPSGOrWkt)
         except:
            try:
               spRef.ImportFromWkt(srsEPSGOrWkt)
            except Exception, e:
               raise LMError('Unable to get Spatial Reference System from %s; Error %s'
                             % (str(srsEPSGOrWkt), str(e)))
      return spRef
   
# ...............................................
   @staticmethod
   def _copyFeature(originalFeature):
      newFeat = None
      try:
         newFeat = originalFeature.Clone()
      except Exception, e:
         print 'Failure to create new feature; Error: %s' % (str(e))
      return newFeat
   
# ...............................................
   @staticmethod
   def createPointFeature(oDict, xCol, yCol, lyrDef, newNames):
      ptFeat = None
      try:
         ptgeom = ogr.Geometry(ogr.wkbPoint)
         ptgeom.AddPoint(float(oDict[xCol]), float(oDict[yCol]))
      except Exception, e:
         print 'Failure %s:  Point = %s, %s' % (str(e), str(oDict[xCol]), 
                                               str(oDict[yCol]))
      else:
         # Create feature for combo layer
         ptFeat = ogr.Feature(lyrDef)
         ptFeat.SetGeometryDirectly(ptgeom)
         # set other fields to match original values
         for okey in oDict.keys():
            if okey in newNames.keys():
               ptFeat.SetField(newNames[okey], oDict[okey])
            else:
               ptFeat.SetField(okey, oDict[okey])
      return ptFeat
   
# ...............................................
   @staticmethod
   def splitCSVPointsToShapefiles(outpath, dlocation, groupByField, comboLayerName,
                                 srsEPSGOrWkt=DEFAULT_EPSG,
                                 delimiter=';', quotechar='\"',
                                 idCol='id', xCol='lon', yCol='lat',
                                 overwrite=False):
      """
      @summary: Read OGR-accessible data and write to a single shapefile and 
                individual shapefiles defined by the value of <fieldname>
      @param outpath: Directory for output datasets.
      @param dlocation: Location of original combined dataset.
      @param groupByField: Field containing attribute to group on.
      @param comboLayerName: Write the original combined data using this name.
      @param srsEPSGOrWkt: Spatial reference as an integer EPSG code or 
                           Well-Known-Text
      @param overwrite: Overwrite or fail if data already exists.
      @raise LMError: on failure to read data.
      """
      ogr.UseExceptions()
      import csv

      data = {}
      successfulWrites = []
      
      ogr.RegisterAll()
      drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
      spRef = Vector._getSpatialRef(srsEPSGOrWkt)
            
      f = open(dlocation, 'rb')
      ptreader = csv.DictReader(f, delimiter=delimiter, quotechar=quotechar)
      ((idName, idPos), (xName, xPos), 
       (yName, yPos)) = Vector._getIdXYNamePos(ptreader.fieldnames, idName=idCol, 
                                               xName=xCol, yName=yCol)
      comboDs, comboLyr, nameChanges = Vector._createPointShapefile(drv, outpath, 
                                              spRef, comboLayerName, 
                                              fldnames=ptreader.fieldnames,
                                              idCol=idName, xCol=xName, yCol=yName, 
                                              overwrite=overwrite)
      lyrDef = comboLyr.GetLayerDefn()
      # Iterate through records 
      for oDict in ptreader:
         # Create and add feature to combo layer 
         ptFeat1 = Vector.createPointFeature(oDict, xCol, yCol, lyrDef, nameChanges)
         if ptFeat1 is not None:
            comboLyr.CreateFeature(ptFeat1)
            ptFeat1.Destroy()
            # Create and save point for individual species layer
            ptFeat2 = Vector.createPointFeature(oDict, xCol, yCol, lyrDef, nameChanges)
            thisGroup = oDict[groupByField]
            if thisGroup not in data.keys():
               data[thisGroup] = [ptFeat2]
            else:
               data[thisGroup].append(ptFeat2)
            
      dloc = Vector._finishShapefile(comboDs)
      successfulWrites.append(dloc)
      f.close()

      for group, pointFeatures in data.iteritems():
         indDs, indLyr, nameChanges = Vector._createPointShapefile(drv, outpath, 
                                              spRef, group, lyrDef=lyrDef, 
                                              overwrite=overwrite)
         for pt in pointFeatures:
            indLyr.CreateFeature(pt)
            pt.Destroy()
         dloc = Vector._finishShapefile(indDs)
         successfulWrites.append(dloc)   

      ogr.DontUseExceptions()
      return successfulWrites

# ...............................................
   def writeCSV(self, dataRecords, dlocation=None, overwrite=False, header=None):
      """
      @summary: Writes vector data to a CSV file.
      @param iterableData: a sequence of vector data records, each record is
             a sequence  
      @param dlocation: Location to write the data
      @param overwrite: True if overwrite existing outfile, False if not
      @return: boolean for success/failure 
      @postcondition: The vector file is written in CSV format (tab-delimited) 
                      to the filesystem.
      @raise LMError: on failure to write file.
      @note: This does NOT set the  self._dlocation attribute
      """
      import csv
      if dlocation is None:
         dlocation = self._dlocation
      didWrite = False
      success = self._readyFilename(dlocation, overwrite=overwrite)
      if success:
         try:
            with open(dlocation, 'wb') as csvfile:
               spamwriter = csv.writer(csvfile, delimiter=',')
               if header:
                  spamwriter.writerow(header)
               for rec in dataRecords:
                  try:
                     spamwriter.writerow(rec)
                  except Exception, e:
                     # Report and move on
                     print ('Failed to write record {} ({})'.format(rec, str(e)))
            didWrite = True
         except Exception, e:
            print ('Failed to write file {} ({})'.format(dlocation, str(e)))
      return didWrite

# ...............................................
   def writeShapefile(self, dlocation=None, overwrite=False):
      """
      @summary: Writes vector data in the feature attribute to a shapefile.  
      @param dlocation: Location to write the data
      @param overwrite: True if overwrite existing shapefile, False if not
      @return: boolean for success/failure 
      @postcondition: The shapefile files are written to the filesystem.
      @raise LMError: on failure to write file.
      """
      success = False
      if dlocation is None:
         dlocation = self._dlocation
         
      if not self._features:
         return success

      if overwrite:
         self.deleteData(dlocation=dlocation)
      elif os.path.isfile(dlocation):
         print('Dataset exists: %s' % dlocation)
         return success
      
      self.setDLocation(dlocation) 
      self._readyFilename(self._dlocation)        
            
      try:
         # Create the file object, a layer, and attributes
         tSRS = osr.SpatialReference()
         tSRS.ImportFromEPSG(self.epsgcode)
         drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)

         ds = drv.CreateDataSource(self._dlocation)
         if ds is None:
            raise LMError('Dataset creation failed for %s' % self._dlocation)
         
         lyr = ds.CreateLayer(ds.GetName(), geom_type=self._ogrType, srs=tSRS)
         if lyr is None:
            raise LMError('Layer creation failed for %s.' % self._dlocation)
   
         # Define the fields
         for idx in self._featureAttributes.keys():
            fldname, fldtype = self._featureAttributes[idx]
            if fldname != self._geomFieldName:
               fldDefn = ogr.FieldDefn(fldname, fldtype)
               # Special case to handle long Canonical, Provider, Resource names
               if (fldname.endswith('name') and fldtype == ogr.OFTString):
                  fldDefn.SetWidth(SHAPEFILE_MAX_STRINGSIZE)
               returnVal = lyr.CreateField(fldDefn)
               if returnVal != 0:
                  raise LMError('CreateField failed for %s in %s' 
                                % (fldname, self._dlocation)) 
                  
         # For each feature
         for i in self._features.keys():
            fvals = self._features[i]
            feat = ogr.Feature( lyr.GetLayerDefn() )
            try:
               self._fillOGRFeature(feat, fvals)
            except Exception, e:
               print 'Failed to fillOGRFeature, e = %s' % str(e)
            else:
               # Create new feature, setting FID, in this layer
               lyr.CreateFeature(feat)
               feat.Destroy()
   
         # Closes and flushes to disk
         ds.Destroy()
         print('Closed/wrote dataset %s' % self._dlocation)
         success = True
         try:
            retcode = subprocess.call(["shptree", "%s" % self._dlocation])
            if retcode != 0: 
               print 'Unable to create shapetree index on %s' % self._dlocation
         except Exception, e:
            print 'Unable to create shapetree index on %s: %s' % (self._dlocation, 
                                                                  str(e))
      except Exception, e:
         raise LMError(['Failed to create shapefile %s' % self._dlocation, str(e)])
         
      return success
                  
# ...............................................
   def readFromUploadedData(self, data, uploadedType='shapefile', overwrite=True):
      """
      @summary: Read from uploaded data by writing to temporary files, saving 
                temporary filename in dlocation.  
      @raise LMError: on failure to write data or read temporary files.
      """
      if uploadedType == 'shapefile':
      # Writes zipped stream to temp file and sets dlocation on layer
         self.writeFromZippedShapefile(data, isTemp=True, overwrite=overwrite)
         self._dataFormat = DEFAULT_OGR_FORMAT
         try:
            # read to make sure it's valid (and populate stats)
            self.readData()
         except Exception, e:
            raise LMError('Invalid uploaded data in temp file %s (%s)' 
                          % (self._dlocation, str(e)), doTrace=True )
      elif uploadedType == 'csv':
         self.writeTempFromCSV(data)
         self._dataFormat = 'CSV'
         try:
            # read to make sure it's valid (and populate stats)
            self.readData()
         except Exception, e:
            raise LMError('Invalid uploaded data in temp file %s (%s)' 
                          % (self._dlocation, str(e)) )
      
# ...............................................
   @staticmethod
   def _getIdXYNamePos(fieldnames, idName=None, xName=None, yName=None):
      idPos = xPos = yPos = None
      if idName is not None:
         try:
            idPos = fieldnames.index(idName)
         except:
            idName = None
      if xName is not None:
         try:
            xPos = fieldnames.index(xName)
         except:
            xName = None
      if yName is not None:
         try:
            yPos = fieldnames.index(yName)
         except:
            yName = None
            
      if not (idName and xName and yName):
         for i in range(len(fieldnames)):
            fldname = fieldnames[i].lower()
            if xName is None and fldname in OccurrenceFieldNames.LONGITUDE:
               xName = fldname
               xPos = i
            if yName is None and fldname in OccurrenceFieldNames.LATITUDE:
               yName = fldname
               yPos = i
            if idName is None and fldname in OccurrenceFieldNames.LOCAL_ID:
               idName = fldname
               idPos = i
            
      return ((idName, idPos), (xName, xPos), (yName, yPos))

# ...............................................
   def writeFromZippedShapefile(self, zipdata, isTemp=True, overwrite=False):
      """
      @summary: Write a shapefile from a zipped stream of shapefile files to
                temporary files.  Read vector info into layer attributes, 
                Reset dlocation. 
      @raise LMError: on failure to write file.
      """
      newfnamewoext = None
      outStream = StringIO()
      outStream.write(zipdata)
      outStream.seek(0)   
      z = zipfile.ZipFile(outStream, allowZip64=True)
      
      # Get filename, prepare directory, delete if overwrite=True
      if isTemp:
         zfnames = z.namelist()
         for zfname in zfnames:
            if zfname.endswith(OutputFormat.SHAPE):
               pth, basefilename = os.path.split(zfname)
               pth = UPLOAD_PATH
               basename, dext = os.path.splitext(basefilename)
               newfnamewoext = os.path.join(pth, basename)
               outfname = os.path.join(UPLOAD_PATH, basefilename)
               ready = self._readyFilename(outfname, overwrite=overwrite)
               break
         if outfname is None:
            raise Exception('Invalid shapefile, zipped data does not contain .shp')
      else:
         if self._dlocation is None:
            self.setDLocation()
         outfname = self._dlocation
         if outfname is None:
            raise LMError('Must setDLocation prior to writing shapefile')
         pth, basefilename = os.path.split(outfname)
         basename, dext = os.path.splitext(basefilename)
         newfnamewoext = os.path.join(pth, basename)
         ready = self._readyFilename(outfname, overwrite=overwrite)
      
      if ready:
         # unzip zip file stream
         for zname in z.namelist():
            tmp, ext = os.path.splitext(zname)
            # Check file extension and only unzip valid files
            if ext in SHAPEFILE_EXTENSIONS:
               newname = newfnamewoext + ext
               success, msg = self._deleteFile(newname)
               z.extract(zname, pth)
               if not isTemp:
                  oldname = os.path.join(pth, zname)
                  os.rename(oldname, newname)
         # Reset dlocation on successful write
         self.clearDLocation()
         self.setDLocation(outfname)
      else:
         raise LMError(currargs='{} exists, overwrite = False'.format(outfname))
            
# ...............................................
   def writeTempFromCSV(self, csvdata):
      """
      @summary: Write csv from a stream of csv data to temporary file.  
                Read vector info into layer attributes;
                DO NOT delete temporary files or reset dlocation 
      @raise LMError: on failure to write file.
      """
      import mx.DateTime
      currtime = str(mx.DateTime.gmt().mjd)
      pid = str(os.getpid())
      dumpname = os.path.join(UPLOAD_PATH, '%s_%s_dump.csv' % (currtime, pid))
      f1 = open(dumpname, 'w')
      f1.write(csvdata)
      f1.close()
      f1 = open(dumpname, 'rU')
      tmpname = os.path.join(UPLOAD_PATH, '%s_%s.csv' % (currtime, pid))
      f2 = open(tmpname, 'w')
      try:
         for line in f1:
            f2.write(line)
      except Exception, e:
         raise LMError(currargs=['Unable to parse input CSV data', str(e)])
      finally:
         f1.close()
         f2.close()
      self.clearDLocation()
      self.setDLocation(dlocation=tmpname)
      
# ...............................................
   def _setGeometry(self):
      if self._geometry is None and self._convexHull is None and self._features:
         if self._ogrType == ogr.wkbPoint:
            gtype = ogr.wkbMultiPoint
         elif self._ogrType == ogr.wkbLineString:
            gtype = ogr.wkbMultiLineString
         elif self._ogrType == ogr.wkbPolygon:
            gtype = ogr.wkbMultiPolygon
         elif self._ogrType == ogr.wkbMultiPolygon:
            gtype = ogr.wkbGeometryCollection 
         else:
            raise LMError('Only osgeo.ogr types wkbPoint, wkbLineString, ' + 
                          'wkbPolygon, and wkbMultiPolygon are currently supported')
         geom = ogr.Geometry(gtype)
         srs = self.createSRSFromEPSG()   
         gidx = self._getGeometryIndex()
         
         for fvals in self._features.values():
            wkt = fvals[gidx]
            fgeom = ogr.CreateGeometryFromWkt(wkt, srs)
            if fgeom is None:
               print('What happened on point %s?' % 
                     (str(fvals[self.getLocalIdIndex()])) )
            else:
               geom.AddGeometryDirectly(fgeom)
         
         self._geometry = geom
         
         # Now set convexHull
         tmpGeom = self._geometry.ConvexHull()
         if tmpGeom.GetGeometryType() != ogr.wkbPolygon:
            self._convexHull = tmpGeom.Buffer(.1, 2)
         else:
            self._convexHull = tmpGeom
            
         # Don't reset Bounding Box for artificial geometry of stacked 3d data
         minx, maxx, miny, maxy = self._convexHull.GetEnvelope()
         self._setBBox((minx, miny, maxx, maxy))
            
# ...............................................
   def getConvexHullWkt(self):
      """
      @summary: Return Well Known Text (wkt) of the polygon representing the 
                convex hull of the data.
      @note: If the geometry type is Point, and a ConvexHull is a single point, 
             buffer the point to create a small polygon. 
      """
      wkt = None
      self._setGeometry()         
      if self._convexHull is not None: 
         wkt = self._convexHull.ExportToWkt()
      return wkt

# ...............................................
   def getFeaturesWkt(self):
      """
      @summary: Return Well Known Text (wkt) of the data features.
      """
      wkt = None
      self._setGeometry()         
      if self._geometry is not None: 
         wkt = self._geometry.ExportToWkt()
      return wkt

# ...............................................
   def _getGeomType(self, lyr, lyrDef):
      # Special case to handle multi-polygon datasets that are identified
      # as polygon, this because of a broken driver 
      geomtype = lyrDef.GetGeomType()
      if geomtype == ogr.wkbPolygon:
         feature = lyr.GetNextFeature()
         while feature is not None:            
            fgeom = feature.GetGeometryRef()
            geomtype = fgeom.GetGeometryType()
            if geomtype == ogr.wkbMultiPolygon:
               break
            feature = lyr.GetNextFeature()
      return geomtype

# ...............................................
   def copyData(self, sourceDataLocation, targetDataLocation=None, 
                format=DEFAULT_OGR_FORMAT):
      """
      Copy sourceDataLocation dataset to targetDataLocation or this layer's 
      dlocation.
      """
      if sourceDataLocation is not None and os.path.exists(sourceDataLocation):
         if targetDataLocation is None:
            if self._dlocation is not None:
               targetDataLocation = self._dlocation
            else:
               raise LMError('Target location is None')
      else:
         raise LMError('Source location %s is invalid' % str(sourceDataLocation))

      ogr.RegisterAll()
      drv = ogr.GetDriverByName(format)
      try:
         ds = drv.Open(sourceDataLocation)
      except Exception, e:
         raise LMError(['Invalid datasource' % sourceDataLocation, str(e)])
      
      try:
         newds = drv.CopyDataSource(ds, targetDataLocation)
         newds.Destroy()
      except Exception, e:
         raise LMError(currargs='Failed to copy data source')

# ...............................................
   def verifyField(self, dlocation, ogrFormat, attrname):
      """
      @summary: Read OGR-accessible data and save the features and 
                featureAttributes on the Vector object
      @param dlocation: Location of the data
      @param ogrFormat: OGR-supported data format code, available at
                      http://www.gdal.org/ogr/ogr_formats.html
      @return: boolean for success/failure 
      @raise LMError: on failure to read data.
      @note: populateStats calls this
      """
      if attrname is None:
         fieldOk = True
      else:
         fieldOk = False
         if dlocation is not None and os.path.exists(dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(ogrFormat)
            try:
               ds = drv.Open(dlocation)
            except Exception, e:
               raise LMError(['Invalid datasource' % dlocation, str(e)])
            
            lyrDef = ds.GetLayer(0).GetLayerDefn()
            # Make sure given field exists and is the correct type
            for i in range(lyrDef.GetFieldCount()):
               fld = lyrDef.GetFieldDefn(i)
               fldname = fld.GetNameRef()
               if attrname == fldname:
                  fldtype = fld.GetType()
                  if fldtype in (ogr.OFTInteger, ogr.OFTReal, ogr.OFTBinary):
                     fieldOk = True
                  break
      return fieldOk
      
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
                  
      return goodData, featCount
      
# ...............................................
   @staticmethod
   def indexShapefile(dlocation):
      try:
         shpTreeCmd = os.path.join(APP_PATH, 'shptree')
         retcode = subprocess.call([shpTreeCmd, '{}'.format(dlocation)])
         if retcode != 0: 
            print 'Failed to create shptree index on {}'.format(dlocation)
      except Exception, e:
         print 'Failed create shptree index on {}: {}'.format(dlocation, str(e))
                  
                  
# ...............................................
   def readCSVPointsWithIDs(self, dlocation=None, featureLimit=None, 
                            doReadData=False):
      """
      @summary: Read data and set features and featureAttributes
      @return: localId position, featAttrs, featureCount, 
               and features and BBox (if read data)
      @note: We are saving only latitude, longitude and localid if it exists.  
             If localid does not exist, we create one.
      @note: If column headers are not present, assume 
             columns 0 = id, 1 = longitude, 2 = latitude
      @todo: Save the rest of the fields using Vector.splitCSVPointsToShapefiles
      @todo: remove featureLimit, read subsetDLocation if there is a limit 
      """
      import csv
      thisBBox = None
      feats = {}
      featAttrs = self.getUserPointFeatureAttributes()
      localid = None
      if dlocation is None:
         dlocation = self._dlocation
      self.clearFeatures()
      infile = open(dlocation, 'rU')
      reader = csv.reader(infile)
      
      # Read row with possible fieldnames
      row = reader.next()      
      hasHeader = True
      ((idName, idPos), (xName, xPos), (yName, yPos)) = Vector._getIdXYNamePos(row)
      if not idPos:
         # If no id column, create it
         if (xPos and yPos):
            localid = 0
         # If no headers, assume the positions
         else:
            hasHeader = False
            idPos = 0
            xPos = 1
            yPos = 2
      if xPos is None or yPos is None:
         raise LMError('Must supply longitude and latitude')
      
      if not doReadData:
         featureCount = sum(1 for row in reader)
         if not hasHeader:
            featureCount += 1
      else:
         eof = False
         try:
            row = reader.next()
         except StopIteration, e:
            eof = True
         Xs = []
         Ys = []
         while not eof:
            try:
               if localid is None:
                  thisid = row[idPos]
               else:
                  localid += 1
                  thisid = localid
               x = float(row[xPos])
               y = float(row[yPos])
               Xs.append(x)
               Ys.append(y)
               feats[thisid] = self.getUserPointFeature(thisid, x, y)
               if featureLimit is not None and len(feats) >= featureLimit:
                  break
            except Exception, e:
               # Skip point if fails.  This could be a blank row or data error
               pass
            # Read next row
            try:
               row = reader.next()
            except StopIteration, e:
               eof = True
               
         featureCount = len(feats)
         if featureCount == 0:
            raise LMError('Unable to read points from CSV') 
         try:
            minX = min(Xs)
            minY = min(Ys)
            maxX = max(Xs)
            maxY = max(Ys)
            thisBBox = (minX, minY, maxX, maxY)
         except Exception, e:
            raise LMError('Failed to get valid coordinates ({})'.format(str(e)))
         
      infile.close()
      return (thisBBox, idPos, feats, featAttrs, featureCount)

# ...............................................
   def readWithOGR(self, dlocation, ogrFormat, featureLimit=None, doReadData=False):
      """
      @summary: Read OGR-accessible data and set the features and 
                featureAttributes on the Vector object
      @param dlocation: Location of the data
      @param ogrFormat: OGR-supported data format code, available at
                      http://www.gdal.org/ogr/ogr_formats.html
      @return: boolean for success/failure 
      @raise LMError: on failure to read data.
      @note: populateStats calls this
      @todo: remove featureLimit, read subsetDLocation if there is a limit 
      """
      thisBBox = localIdIdx = geomIdx = feats = featAttrs = None
      if dlocation is not None and os.path.exists(dlocation):
         ogr.RegisterAll()
         drv = ogr.GetDriverByName(ogrFormat)
         try:
            ds = drv.Open(dlocation)
         except Exception, e:
            raise LMError(['Invalid datasource' % dlocation, str(e)])
                    
         self.clearFeatures() 
         try:
            slyr = ds.GetLayer(0)
         except Exception, e:
            raise LMError(currargs='#### Failed to GetLayer from %s' % dlocation,
                          prevargs=e.args, doTrace=True)

         # Get bounding box
         (minX, maxX, minY, maxY) = slyr.GetExtent()
         thisBBox = (minX, minY, maxX, maxY)
 
         # .........................
         # Read field structure (featAttrs)
         lyrDef = slyr.GetLayerDefn()
         fldCount = lyrDef.GetFieldCount()
         foundLocalId = False         
         geomtype = self._getGeomType(slyr, lyrDef)
         # Read Fields (indexes start at 0)
         featAttrs = {}
         for i in range(fldCount):
            fld = lyrDef.GetFieldDefn(i)
            fldname = fld.GetNameRef()
            # Provided attribute name takes precedence
            if fldname == self._fidAttribute:
               localIdIdx = i
               foundLocalId = True
            # Don't reset if already found
            if not foundLocalId and fldname in OccurrenceFieldNames.LOCAL_ID:
               localIdIdx = i
               foundLocalId = True
            featAttrs[i] = (fld.GetNameRef(), fld.GetType())

         # .........................
         # Add fields FID (if not present) and geom to featAttrs
         i = fldCount
         if not foundLocalId:
            featAttrs[i] = (self._localIdFieldName, self._localIdFieldType)
            localIdIdx = i
            i += 1
         featAttrs[i] = (self._geomFieldName, self._geomFieldType)
         geomIdx = i
         
         # .........................
         # Read data (features)
         feats = {}
         featCount = slyr.GetFeatureCount()
         if doReadData:
            # Limit the number of features to read (for mapping and modeling)
            if featureLimit is not None and featureLimit < featCount:
               featCount = featureLimit
            try:
               for j in range(featCount):
                  currFeat = slyr.GetFeature(j)
                  if currFeat is not None:
                     currFeatureVals = []
                     for k in range(fldCount):
                        val = currFeat.GetField(k)
                        currFeatureVals.append(val)
                        if k == localIdIdx:
                           localid = val
                     # Add values localId (if not present) and geom to features
                     if not foundLocalId:
                        localid = currFeat.GetFID()
                        currFeatureVals.append(localid)
                     currFeatureVals.append(currFeat.geometry().ExportToWkt())
                     
                     # Add the feature values with key=localId to the dictionary 
                     feats[localid] = currFeatureVals
            except Exception, e:
               raise LMError(currargs='Failed to read features from %s (%s)' 
                             % (dlocation, str(e)), doTrace=True)
         
#          self.setFeatures(features, featAttrs, featureCount=featCount)
      else:
         raise LMError('dlocation %s does not exist' % str(dlocation))
      return thisBBox, localIdIdx, geomIdx, feats, featAttrs, featCount

      
# ...............................................
   def _transformBBox(self, origEpsg=None, origBBox=None):
      if origEpsg is None:
         origEpsg = self._epsg
      if origBBox is None:
         origBBox = self._bbox
      minX, minY, maxX, maxY = origBBox
         
      if origEpsg != DEFAULT_EPSG:
         srcSRS = osr.SpatialReference()
         srcSRS.ImportFromEPSG(origEpsg)
         dstSRS = osr.SpatialReference()
         dstSRS.ImportFromEPSG(DEFAULT_EPSG)
         
         spTransform = osr.CoordinateTransformation(srcSRS, dstSRS)
         # Allow for return of either (x, y) or (x, y, z) 
         retvals = spTransform.TransformPoint(minX, minY)
         newMinX, newMinY = retvals[0], retvals[1]
         retvals = spTransform.TransformPoint(maxX, maxY)
         newMaxX, newMaxY = retvals[0], retvals[1]
         return (newMinX, newMinY, newMaxX, newMaxY)
      else:
         return origBBox
      
# ...............................................
   def readData(self, dlocation=None, dataFormat=None, featureLimit=None, 
                doReadData=False):
      """
      @summary: Read the file at dlocation and fill the featureCount and 
                featureAttributes dictionary.  If doReadData is True, read and 
                fill features dictionary.
      @param dlocation: file location, overrides local attribute
      @param dataFormat: OGR format driver name, overrides local attribute
      @param featureLimit: limits number of features to be read into self._features
      @param doReadData: True to read all features; False to read only the 
             featureAttributes and feature count. 
      @return: new bbox string, indices of the localId int and geometry fields
      @todo: remove featureLimit, read subsetDLocation if there is a limit 
      """
      newBBox = localIdIdx = geomIdx = None
      if dlocation is None:
         dlocation = self._dlocation
      if dataFormat is None:
         dataFormat = self._dataFormat
      if os.path.exists(dlocation):
         if dataFormat == DEFAULT_OGR_FORMAT:
            (thisBBox, localIdIdx, geomIdx, features, featureAttributes, 
             featureCount) = self.readWithOGR(dlocation, dataFormat, 
                                              featureLimit=featureLimit, 
                                              doReadData=doReadData)
         # only for Point data
         elif dataFormat == 'CSV':
            (thisBBox, localIdIdx, features, featureAttributes, 
             featureCount) = self.readCSVPointsWithIDs(dlocation=dlocation, 
                                                       featureLimit=featureLimit, 
                                                       doReadData=doReadData)
         self.setFeatures(features, featureAttributes, featureCount=featureCount)
         newBBox = self._transformBBox(origBBox=thisBBox)
      return (newBBox, localIdIdx, geomIdx)
   
# ...............................................
   def getOGRLayerTypeName(self, ogrWKBType=None):
      if ogrWKBType is None:
         ogrWKBType = self._ogrType
      # Subset of all ogr layer types
      if ogrWKBType == ogr.wkbPoint:
         return 'ogr.wkbPoint'
      elif ogrWKBType == ogr.wkbLineString:
         return 'ogr.wkbLineString'
      elif ogrWKBType == ogr.wkbPolygon:
         return 'ogr.wkbPolygon'
      elif ogrWKBType == ogr.wkbMultiPolygon:
         return 'ogr.wkbMultiPolygon'
      
   def getFieldMetadata(self):
      if self._featureAttributes:
         fldMetadata = {}
         for idx, featAttrs in self._featureAttributes.iteritems():
            fldMetadata[idx] = (featAttrs[0], 
                                self._getOGRFieldTypeName(featAttrs[1]))
      return fldMetadata

   def _getOGRFieldTypeName(self, ogrOFTType):
#      return ogr.GetFieldTypeName(ogrOFTType)
      if ogrOFTType == ogr.OFTBinary: 
         return 'ogr.OFTBinary'
      elif ogrOFTType == ogr.OFTDate:
         return 'ogr.OFTDate'
      elif ogrOFTType == ogr.OFTDateTime:
         return 'ogr.OFTDateTime'
      elif ogrOFTType == ogr.OFTInteger:
         return 'ogr.OFTInteger'
      elif ogrOFTType == ogr.OFTReal:
         return 'ogr.OFTReal'
      elif ogrOFTType == ogr.OFTString:
         return 'ogr.OFTString'
      else:
         return 'ogr Field Type constant: ' + str(ogrOFTType)

# ...............................................
   def getFeatureValByFieldName(self, fieldname, featureFID):
      fieldIdx = self.getFieldIndex(fieldname)
      return self.getFeatureValByFieldIndex(fieldIdx, featureFID)

# ...............................................
   def getFeatureValByFieldIndex(self, fieldIdx, featureFID):
      if self._features:
         if self._features.has_key(featureFID):
            return self._features[featureFID][fieldIdx]
         else:
            raise LMError ('Feature ID %s not found in dataset %s' % 
                              (str(featureFID), self._dlocation))
      else:
         raise LMError('Dataset features are empty.')

# ...............................................
   def getFieldIndex(self, fieldname):      
      if self._featureAttributes:
         fieldIdx = None
         if fieldname in OccurrenceFieldNames.LOCAL_ID:
            findLocalId = True
         else:
            findLocalId = False
         for fldidx, (fldname, fldtype) in self._featureAttributes.iteritems():
            
            if fldname == fieldname:
               fieldIdx = fldidx
               break
            elif findLocalId and fldname in OccurrenceFieldNames.LOCAL_ID:
               fieldIdx = fldidx
               break
            
         return fieldIdx
      else:
         raise LMError('Dataset featureAttributes are empty.')

      
# ...............................................
   def getSRS(self):
      if self._dlocation is not None and os.path.exists(self._dlocation):
         ogr.RegisterAll()
         drv = ogr.GetDriverByName(self._dataFormat)
         try:
            ds = drv.Open(self._dlocation)
         except Exception, e:
            raise LMError(['Invalid datasource' % self._dlocation, str(e)])

         vlyr = ds.GetLayer(0)
         srs = vlyr.GetSpatialRef()
         if srs is None:
            srs = self.createSRSFromEPSG()
         return srs
      else:
         raise LMError(currargs='Input file %s does not exist' % self._dlocation)
      
# .............................................................................
# Private methods
# .............................................................................
   def _fillOGRFeature(self, feat, fvals):
      # Fill the fields
      for j in self._featureAttributes.keys():
         fldname, fldtype = self._featureAttributes[j]
         val = fvals[j]
         if fldname == self._geomFieldName:
            geom = ogr.CreateGeometryFromWkt(val)
            feat.SetGeometryDirectly(geom)
         elif val is not None and val != 'None':
            feat.SetField(fldname, val)
      

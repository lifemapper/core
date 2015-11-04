"""
@summary: Module containing compute environment layer management code
@author: CJ Grady
@status: beta
@version: 3.0.0

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
from hashlib import md5
import numpy
import os
from osgeo import gdal, gdalconst
import re
import sqlite3
from StringIO import StringIO
from time import sleep
import urllib2
import zipfile

from LmCommon.common.lmconstants import JobStatus
from LmCompute.common.lmconstants import INPUT_LAYER_DIR
from LmCompute.common.lmObj import LmException

TIMEOUT = 600
WAIT_SECONDS = 30

# .............................................................................
class LayerManager(object):
   """
   @summary: Manages the storage of layers on the file system through sqlite
   """
   # .................................
   def __init__(self, dataDir):
      dbFile = os.path.join(dataDir, INPUT_LAYER_DIR, "layers.db")
      self.lyrBasePath = os.path.join(dataDir, INPUT_LAYER_DIR)
      createDb = False
      if not os.path.exists(dbFile):
         createDb = True
      self.con = sqlite3.connect(dbFile, isolation_level=None)
      if createDb:
         self._createLayerDb()
   
   # .................................
   def close(self):
      self.con.close()
      
   # .................................
   def getLayerFilename(self, layerUrl):
      """
      @summary: Gets the path to the file created when storing the layer found
                   at the web address specified by layerUrl.  This layer is
                   downloaded and information is stored in the database.
      """
      #print "%s: %s" % (logId, lyr)
      host, uPath, key = self._getLayerUrlParts(layerUrl)
      
      fn = os.path.join(self.lyrBasePath, host, uPath, key)
      
      # Gets the status of the layer on the node (
      #     0: exists and writing by another process, 
      #     1: exists and stored, 
      #     2: new)
      insertStatus = self._getOrInsertLayer(host, uPath, key)
      
      if insertStatus == 1:
         # Layer exists and is stored
         pass
      elif insertStatus == 0:
         # Layer exists but has not been stored
         waitTime = 0
         while self._getOrInsertLayer(host, uPath, key) != 1 and \
                        waitTime < TIMEOUT:
            sleep(WAIT_SECONDS)
            waitTime = waitTime + WAIT_SECONDS
         if waitTime >= TIMEOUT:
            raise LmException(JobStatus.IO_WAIT_ERROR, 
                    "Layer took too long write: {0}, {1}".format(layerUrl, fn))
      elif insertStatus == 2:
         # Write file
         if self._writeLayer(layerUrl, fn):
            self._updateLayerAsStored(host, uPath, key)
         else:
            #TODO: delete layer if failed to write
            self._deleteLayer(host, uPath, key)
            raise LmException(JobStatus.IO_WRITE_ERROR, 
                                 "Failed to write layer: {0}".format(layerUrl))
      else:
         raise LmException(JobStatus.DB_READ_ERROR, 
                          "Unknown insertion status: {0}".format(insertStatus))
      return fn
   
   # .................................
   def seedLayer(self, layerUrl, localFile):
      """
      @summary: Seeds the layer database with a layer file that is already 
                   stored on the local system.  This prevents extra downloads
                   of data when it is already present
      @param layerUrl: The url to be used for the database
      @param localFile: The local file location of this layer
      @note: To remain consistent with the rest of the layers, a symbolic link
                will be created for the layer rather than storing a location
                in the database
      """
      host, uPath, key = self._getLayerIdentifierParts(layerUrl)
      
      fn = os.path.join(self.lyrBasePath, host, uPath, key)
   
      # Gets the status of the layer on the node (
      #     0: exists and writing by another process, 
      #     1: exists and stored, 
      #     2: new)
      insertStatus = self._getOrInsertLayer(host, uPath, key)
      
      if insertStatus == 1:
         # Layer exists and is stored
         pass
      elif insertStatus == 0:
         # Layer exists but has not been stored
         waitTime = 0
         while self._getOrInsertLayer(host, uPath, key) != 1 and \
                      waitTime < TIMEOUT:
            sleep(WAIT_SECONDS)
            waitTime = waitTime + WAIT_SECONDS
         if waitTime >= TIMEOUT:
            raise LmException(JobStatus.IO_WAIT_ERROR, 
                    "Layer took too long write: {0}, {1}".format(layerUrl, fn))
      elif insertStatus == 2:
         # Write file 
         fDir = os.path.dirname(fn)
         if not os.path.exists(fDir):
            os.makedirs(fDir)
         os.symlink(localFile, fn)
         self._updateLayerAsStored(host, uPath, key)
      else:
         raise LmException(JobStatus.DB_READ_ERROR, 
                          "Unknown insertion status: {0}".format(insertStatus))
      return fn

   # .................................
   def _createLayerDb(self):
      """
      @summary: Attempts to create a layers database table
      """
      try:
         self.con.execute("CREATE TABLE layers(host TEXT, upath TEXT, paramhash TEXT, stored INT, PRIMARY KEY (host, upath, paramhash))")
      except:
         pass

   # .................................
   def _deleteLayer(self, hostname, uPath, key):
      """
      @summary: Deletes a layer from the database
      @param hostname: The name of the layer host
      @param uPath: The url path (between the host and parameters)
      @param key: The key for the layer
      @note: This could be useful if the layer failed to store
      """
      with self.con:
         cur = self.con.cursor()
         cur.execute("DELETE FROM layers WHERE host='{host}' AND upath='{uPath}' AND paramhash='{key}'".format(host=hostname, uPath=uPath, key=key))
      
   # .................................
   def _getLayerIdentifierParts(self, layerIdent):
      if layerIdent.startswith('http'):
         host, uPath, key = self._getLayerUrlParts(layerIdent)
      else:
         host = 'localhost'
         # remove trailing slash if present
         uPath = layerIdent.strip('/')
         key = md5(uPath).hexdigest()
      return host, uPath, key
         
   # .................................
   def _getLayerUrlParts(self, layerUrl):
      """
      @summary: Breaks a url into host name and parameters and then returns the 
                   hash of the set of url parameters (ensures a unique key for any 
                   given set of url parameters even in a different order)
      """
      parts = layerUrl.split("?")
      # remove trailing slash and leading http:// if present
      base = parts[0].replace('http://', '').strip('/')
      pathParts = base.split('/')
      host = pathParts[0].replace('.', '_')
      
      uPath = ''
      try:
         uPath = '/'.join(pathParts[1:])
      except:
         pass

      try:
         params = set([tuple(param.split('=')) for param in parts[1].split('&')])
         key = md5(str(params)).hexdigest()
      except:
         key = pathParts[-1]
      
      return host, uPath, key

   # .................................
   def _getOrInsertLayer(self, hostname, uPath, key):
      """
      @summary: Inserts a layer into the database.
      @return: Returns, 0: inserted by another process and not stored
                        1: inserted and stored on file system
                        2: new
      """
      try:
         with self.con:
            # check to see if layer exists
            cur = self.con.cursor()
            cur.execute("SELECT stored FROM layers WHERE host = '{host}' AND upath='{uPath}' AND paramhash = '{key}'".format(host=hostname, uPath=uPath, key=key))
            rows = cur.fetchall()
            if len(rows) == 0:
               # New insert
               cur.execute("INSERT into layers VALUES ('{host}', '{uPath}', '{key}', 0)".format(host=hostname, uPath=uPath, key=key))
               ret = 2
            else:
               ret = rows[0][0] # 0 if not stored, 1 if stored
      except sqlite3.IntegrityError: # Item was inserted between statements
         ret = 0
      return ret

   # .................................
   def _updateLayerAsStored(self, hostname, uPath, key):
      """
      @summary: Marks the layer as stored on the file system
      """
      with self.con:
         cur = self.con.cursor()
         cur.execute("UPDATE layers SET stored = 1 WHERE host='{host}' AND upath='{uPath}' AND paramhash='{key}'".format(host=hostname, uPath=uPath, key=key))

   # .................................
   def _writeLayer(self, layerUrl, filename):
      """
      @summary: Writes a layer to the file system.
      @return: Boolean value indicating success
      """
      try:
         fDir = os.path.dirname(filename)
         if not os.path.exists(fDir):
            os.makedirs(fDir)
            
         if layerUrl.lower().find('aaigrid') >= 0:
            try:
               processASCIILayer(layerUrl, filename)
            except: # Try again
               processASCIILayer(layerUrl, filename)
         else:
            content = urllib2.urlopen(layerUrl).readlines()
            f = open(filename, 'w')
            f.write(''.join(content))
            f.close()
            
            ds = gdal.Open(filename, gdalconst.GA_Update)
            band = ds.GetRasterBand(1)
            if band.GetNoDataValue() is None:
               valMin, _ = band.ComputeRasterMinMax(1)
               if valMin < -9000:
                  band.SetNoDataValue(valMin)
            ds = None
         
         return True
      except Exception, e:
         print(str(e))
         return False

# .............................................................................
def convertLayer(srcFn, dstFn, outputFormat="GTiff"):
   """
   @summary: Converts a layer to a different, GDAL supported, format
   @param srcFn: The filename of the source file
   @param dstFn: The filename of the destination file
   @param outputFormat: The GDAL format to convert to
   """
   # Open the existing data set
   src_ds = gdal.Open(srcFn)
   
   # Open the output format driver
   driver = gdal.GetDriverByName(outputFormat)
   
   # Output to new format
   dst_ds = driver.CreateCopy(dstFn, src_ds, 0)
   
   # Properly close to flush to disk
   driver = None
   dst_ds = None
   src_ds = None

# .............................................................................
def multiplyAndConvertLayer(srcFn, dstFn, outputFormat='GTiff', 
                            multiplier=10000, noDataVal=127, dataType="int"):
   """
   @summary: Multiplies a layer and converts it to the specified format
   @note: This can be used to reduce the file size of maxent outputs from float64
   @param srcFn: The filename of the source file
   @param dstFn: The filename of the destination file
   @param outputFormat: The GDAL format to convert to
   @param multiplier: Multiply non-NO_DATA values by this
   @param noDataVal: The no data value to use for the new layer
   @param dataType: The data type for the resulting raster
   @raise Division by zero: If the min and max value are the same, there will 
                               be division by zero
   """
   if dataType.lower() == "int":
      npType = numpy.uint16
      gdalType = gdal.GDT_UInt16
   else:
      raise Exception, "Unknown data type"
   
   
   src_ds = gdal.Open(srcFn)
   band = src_ds.GetRasterBand(1)
   band.GetStatistics(0,1)

   ndVal = band.GetNoDataValue()

   def multFn(x):
      if x == ndVal:
         return noDataVal
      else:
         return multiplier * x
   
   data = src_ds.ReadAsArray(0, 0, src_ds.RasterXSize, src_ds.RasterYSize)
   
   data = numpy.vectorize(multFn)(data)
   
   data = data.astype(npType)
   driver = gdal.GetDriverByName(outputFormat)
   dst_ds = driver.Create(dstFn, src_ds.RasterXSize, src_ds.RasterYSize, 1, gdalType)
   
   dst_ds.GetRasterBand(1).WriteArray(data)
   dst_ds.GetRasterBand(1).SetNoDataValue(noDataVal)
   dst_ds.GetRasterBand(1).ComputeStatistics(True)
   
   dst_ds.SetProjection(src_ds.GetProjection())
   dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
   
   driver = None
   dst_ds = None
   src_ds = None

# .............................................................................
def scaleAndConvertLayer(srcFn, dstFn, outputFormat='GTiff', scaleMax=100, 
                         scaleMin=0, noDataVal=127, lyrMin=None, lyrMax=None,
                         dataType="int"):
   """
   @summary: Scales a layer and converts it to the specified format
   @note: We did this for MaxEnt layers so that they could be compared directly
             to openModeller output
   @param srcFn: The filename of the source file
   @param dstFn: The filename of the destination file
   @param outputFormat: The GDAL format to convert to
   @param scaleMax: The maximum scale value for the new layer
   @param scaleMin: The minimum scale value for the new layer
   @param noDataVal: The no data value to use for the new layer
   @param dataType: The data type for the resulting raster
   @raise Division by zero: If the min and max value are the same, there will 
                               be division by zero
   """
   if dataType.lower() == "int":
      npType = numpy.uint8
      gdalType = gdal.GDT_Byte
   else:
      raise Exception, "Unknown data type"
   
   
   src_ds = gdal.Open(srcFn)
   band = src_ds.GetRasterBand(1)
   band.GetStatistics(0,1)
   if lyrMin is None:
      min = band.GetMinimum()
   else:
      min = lyrMin
   
   if lyrMax is None:
      max = band.GetMaximum()
   else:
      max = lyrMax

   ndVal = band.GetNoDataValue()

   def scaleFn(x):
      if x == ndVal:
         return noDataVal
      else:
         return (scaleMax-scaleMin)*((x-min) / (max-min)) + scaleMin
   
   data = src_ds.ReadAsArray(0, 0, src_ds.RasterXSize, src_ds.RasterYSize)
   
   data = numpy.vectorize(scaleFn)(data)
   
   data = data.astype(npType)
   driver = gdal.GetDriverByName(outputFormat)
   dst_ds = driver.Create(dstFn, src_ds.RasterXSize, src_ds.RasterYSize, 1, gdalType)
   
   dst_ds.GetRasterBand(1).WriteArray(data)
   dst_ds.GetRasterBand(1).SetNoDataValue(noDataVal)
   dst_ds.GetRasterBand(1).ComputeStatistics(True)
   
   dst_ds.SetProjection(src_ds.GetProjection())
   dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
   
   driver = None
   dst_ds = None
   src_ds = None

# .............................................................................
def getAndStoreShapefile(sfUrl, outputPath):
   """
   @summary: Retrieve and store a shapefile in the indicated directory
   @note: This should be moved into the layer manager but that may cascade
   @param sfUrl: The url of the shapefile to retrieve
   @param outputPath: The directory to store the files in
   """
   fn = None
   if not os.path.exists(outputPath):
      os.mkdir(outputPath)
      
   # Get the zip file
   content = ''.join(urllib2.urlopen(sfUrl).readlines())
   content = StringIO(content)
   content.seek(0)
   # Extract it
   with zipfile.ZipFile(content, allowZip64=True) as z:
      for name in z.namelist():
         if name.endswith('shp'):
            fn = os.path.join(outputPath, name)
         z.extract(name, outputPath)

   return fn
   
# .............................................................................
def processASCIILayer(layerUrl, filename):
   """
   @summary: This method will download an ASCII grid and process it so that it
                will play nicely with Maxent.  By that, I mean that the headers 
                will be adjusted.  The headers often include floating point 
                numbers that have several decimal digits (to a point where they
                are specifying decimeters).  These will be truncated.  Other
                modifications will be to ensure that a NODATA_value is included
                and make sure that cellsize is used instead of dx and dy.
   @param layerUrl: The URL where the layer can be found
   @param filename: The location to save the file
   """
   
   # ....................................
   def _processFloatHeader(headerRow, numDigits=4):
      """
      @summary: This method will process a header row and truncate a floating 
                   point value if necessary
      @param headerRow: This is a string in the format 
                           "{header name}   {header value}"
      @param numDigits: (optional) Truncate a decimal after this many places
      """
      # Split out header name and value (replace tabs with spaces and use 
      #    regular expression to split
      header, value = re.split(r' +', headerRow.replace('\t', ' '))
      # Truncate the value by finding the decimal (if it exists) and adding numDigits places
      truncatedValue = value[:value.find('.')+numDigits+1] if value.find('.') >= 0 else value
      
      return "%s     %s\n" % (header, truncatedValue)
   
   
   content = urllib2.urlopen(layerUrl).readlines()
   
   cont = True
   ndLine = "NODATA_value    -9999\n" # Initialize in cas missing
   i = 0
   
   while cont: # Continue reading headers
      if content[i].startswith('ncols'):
         ncolsLine = content[i] # Should be integer and thus fine
      elif content[i].startswith('nrows'):
         nrowsLine = content[i]
         numberOfRows = int(nrowsLine.split('nrows')[1].strip())
         if len(content) < numberOfRows: # Only an approximate check
            raise Exception, "There are not enough rows in this layer"
      elif content[i].startswith('xllcorner'):
         # Need to truncate floats
         xllLine = _processFloatHeader(content[i])
      elif content[i].startswith('yllcorner'):
         yllLine = _processFloatHeader(content[i])
      elif content[i].startswith('cellsize'):
         cellsizeLine = _processFloatHeader(content[i])
      elif content[i].startswith('dx'):
         cellsizeLine = _processFloatHeader(content[i]).replace('dx', 'cellsize')
      elif content[i].startswith('dy'):
         pass # We are going to use dx for consistency
      elif content[i].startswith('NODATA_value'):
         ndLine = content[i]
      else: # Data line
         cont = False
      if cont:
         i += 1 # Only increment if we processed a header row

   f = open(filename, 'w')

   # Write out processed header
   #   note: Will fail if any of the required elements are missing
         
   f.write(ncolsLine)
   f.write(nrowsLine)
   f.write(xllLine)
   f.write(yllLine)
   f.write(cellsizeLine)
   f.write(ndLine)
   
   # Write out data
   f.writelines(content[i:])
   
   f.close()

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
@todo: Add convert tool to config
"""
from hashlib import md5, sha256
from mx.DateTime import gmt
import numpy
import os
from osgeo import gdal
import re
import shutil
import sqlite3
from StringIO import StringIO
import subprocess
from time import sleep
import urllib2
import zipfile

from LmCommon.common.lmconstants import (JobStatus, OutputFormat,
                                         SHAPEFILE_EXTENSIONS)
from LmCompute.common.lmconstants import (INPUT_LAYER_DIR, LayerAttributes, 
                                          LayerFormat, LayerStatus, 
                                          RETRIEVED_LAYER_DIR)
from LmCompute.common.localconstants import TEMPORARY_FILE_PATH
from LmCompute.common.lmObj import LmException
from LmCompute.plugins.sdm.maxent.localconstants import (JAVA_CMD, ME_CMD,
                                                         CONVERT_TOOL)

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
   def _executeDbFunction(self, cmd):
      """
      @summary: Executes a database command
      """
      rows = None
      with self.con:
         cur = self.con.cursor()
         cur.execute(cmd)
            
         rows = cur.fetchall()
      return rows
   
   # .................................
   def _queryLayer(self, layerId, layerFormat):
      """
      @summary: Query the layers table for information about the layer
      @return: Status and a layer if it exists, or None
      """
      status = None
      lyr = None
      cmd = "SELECT {lyrIdAtt}, {fpAtt}, {fTypeAtt}, {statAtt}, {createAtt}, {touchAtt} FROM layers WHERE layerid = '{layerId}'".format(
               lyrIdAtt=LayerAttributes.LAYER_ID, 
               fpAtt=LayerAttributes.FILE_PATH, 
               ftypeAtt=LayerAttributes.FILE_TYPE,
               statAtt=LayerAttributes.STATUS,
               createAtt=LayerAttributes.CREATE_TIME,
               touchAtt=LayerAttributes.TOUCH_TIME,
               layerId=layerId)

      rows = self._executeDbFunction(cmd)
      
      if len(rows) == 0:
         lyr = None
         status = LayerStatus.ABSENT
      else:
         for row in rows:
            lyr = {
                   LayerAttributes.LAYER_ID: row[0],
                   LayerAttributes.FILE_PATH: row[1],
                   LayerAttributes.FILE_TYPE: row[2],
                   LayerAttributes.STATUS: row[3],
                   LayerAttributes.CREATE_TIME: row[4],
                   LayerAttributes.TOUCH_TIME: row[5]
                  }
            status = lyr[LayerAttributes.STATUS]
            if lyr[LayerAttributes.FILE_TYPE]:
               status = lyr[LayerAttributes.STATUS]
               break # We found what we wanted
            elif lyr[LayerAttributes.FILE_TYPE] == LayerFormat.GTIFF:
               status = LayerStatus.TIFF_AVAILABLE
               lyr = None # Don't send back the wrong layer
            else:
               # Could be ASCII Grid when we're looking for MXE, just move on
               pass

      return status, lyr
      
   # .................................
   def _convertLayer(self, layerId, layerFormat):
      status, lyr = self._queryLayer(layerId, LayerFormat.GTIFF)
      if status not in [LayerStatus.SEEDED, LayerStatus.STORED]:
         lyr = self._waitOnLayer(layerId, LayerFormat.GTIFF)

      if lyr is not None:
         newFn = self._getFilePath(layerId, layerFormat)
         self._insertLayer(layerId, layerFormat, newFn, LayerStatus.RETRIEVING)
         if layerFormat == LayerFormat.ASCII:
            convertTiffToAscii(lyr[LayerAttributes.FILE_PATH], newFn)
         elif layerFormat == LayerFormat.MXE:
            aStatus, aLyr = self._queryLayer(layerId, LayerFormat.ASCII)
            
            if aStatus not in [LayerStatus.SEEDED, LayerStatus.STORED]:
               ascFn = aLyr[LayerAttributes.FILE_PATH]
            elif aStatus == LayerStatus.ABSENT:
               ascFn = self._getFilePath(layerId, LayerFormat.ASCII)
               convertTiffToAscii(lyr[LayerAttributes.FILE_PATH], ascFn)
            else:
               # Note, if it takes to long to write the layer, an excpetion 
               #    will be thrown that will bubble up from here
               aLyr = self._waitOnLayer(layerId, LayerFormat.ASCII)
               ascFn = aLyr[LayerAttributes.FILE_PATH]
            convertAsciisToMxes([(ascFn, newFn)])
         else:
            raise LmException(JobStatus.DB_LAYER_ERROR, 
                              "Don't know how to convert to: %s" % layerFormat)
         self._updateLayerStatus(layerId, layerFormat, LayerStatus.STORED)
      else:
         raise LmException(JobStatus.DB_LAYER_READ_ERROR,
                           "GeoTIFF is missing for: %s" % layerId)
   
   # .................................
   def _findOldLayers(self, lastTouchedBefore):
      cmd = "SELECT {lyrIdAtt}, {lyrFmtAtt} FROM layers WHERE {touchAtt} < {touchTime} AND {statusAtt} != {seededStatus}".format(
               lyrIdAtt=LayerAttributes.LAYER_ID, 
               lyrFmtAtt=LayerAttributes.FILE_TYPE,
               touchAtt=LayerAttributes.TOUCH_TIME,
               touchTime=lastTouchedBefore,
               statusAtt=LayerAttributes.STATUS,
               seededStatus=LayerStatus.SEEDED)
      rows = self._executeDbFunction(cmd)
      return rows

   # .................................
   def _insertLayer(self, layerId, layerFormat, filePath, status):
      nowMjd = gmt().mjd
      cmd = "INSERT INTO layers VALUES ('{layerId}', '{filePath}', {fileType}, {status}, {createTime}, {touchTime})".format(
             layerId=layerId, filePath=filePath, fileType=layerFormat,
             status=status, createTime=nowMjd, touchTime=nowMjd)
      rows = self._executeDbFunction(cmd)
   
   # .................................
   def _touchLayer(self, layerId, layerFormat):
      cmd = "UPDATE layers SET {touchAtt} = {rightNow} WHERE {lyrIdAtt} = '{layerId}' AND {lyrFmtAtt} = '{lyrFmt}'".format(
               touchAtt=LayerAttributes.TOUCH_TIME,
               rightNow=gmt().mjd,
               lyrIdAtt=LayerAttributes.LAYER_ID,
               layerId=layerId,
               lyrFmtAtt=LayerAttributes.FILE_TYPE,
               lyrFmt=layerFormat)
      rows = self._executeDbFunction(cmd)
   
   # .................................
   def _updateLayerStatus(self, layerId, layerFormat, status):
      cmd = "UPDATE layers SET {statAtt} = {status}, {touchAtt} = {rightNow} WHERE {lyrIdAtt} = '{layerId}' AND {lyrFmtAtt} = '{lyrFmt}'".format(
               statAtt=LayerAttributes.STATUS,
               status=status,
               touchAtt=LayerAttributes.TOUCH_TIME,
               rightNow=gmt().mjd,
               lyrIdAtt=LayerAttributes.LAYER_ID,
               layerId=layerId,
               lyrFmtAtt=LayerAttributes.FILE_TYPE,
               lyrFmt=layerFormat)
      rows = self._executeDbFunction(cmd)
   
   # .................................
   def _waitOnLayer(self, layerId, layerFormat, layerUrl):
      
      status, lyr = self._queryLayer(layerId, layerFormat)
      
      # IF layer url is none, get from existing layer
      waitTime = 0
      
      while status == LayerStatus.RETRIEVING and waitTime < TIMEOUT:
         sleep(WAIT_SECONDS)
         waitTime += WAIT_SECONDS
         status, lyr = self._queryLayer(layerId, layerFormat)
         
      if waitTime >= TIMEOUT:
         # Layer took too long to write
         # Delete the existing record.  
         self._deleteLayer(layerId, layerFormat)
         if layerUrl is not None:
            # Try to retrieve again
            lyr = self._retrieveLayer(layerId, layerFormat, layerUrl)
         else:
            # Fail if we don't have the URL.  This would happen if it is coming
            #    from a convert call and the TIFF download failed
            raise LmException(JobStatus.IO_LAYER_WRITE_ERROR, 
                    "Layer took too long to write and now replacement URL available.")
      return lyr
         
   # .................................
   def _getFilePath(self, layerId, layerFormat):
      if layerFormat == LayerFormat.GTIFF:
         lyrExt = OutputFormat.GTIFF
      elif layerFormat == LayerFormat.ASCII:
         lyrExt = OutputFormat.ASCII
      elif layerFormat == LayerFormat.MXE:
         lyrExt = OutputFormat.MXE
      elif layerFormat == LayerFormat.SHAPE:
         lyrExt = OutputFormat.SHAPE
      else:
         raise LmException(JobStatus.IO_LAYER_ERROR, 
                           "Unknown file type: %s" % layerFormat)
      filename = os.path.join(self.lyrBasePath, RETRIEVED_LAYER_DIR, '%s%s' % (layerId, lyrExt))
      return filename
   
   # .................................
   def _retrieveLayer(self, layerId, layerFormat, layerUrl):
      # Have to determine file name to retrieve (shapefile or tiff)
      if layerFormat == LayerFormat.SHAPE:
         lyrPath = self._getFilePath(layerId, layerFormat)
         rLyrFormat = LayerFormat.SHAPE
      else:
         lyrPath = self._getFilePath(layerId, LayerFormat.GTIFF)
         rLyrFormat = LayerFormat.GTIFF
         
      # Insert layer into db
      self._insertLayer(layerId, rLyrFormat, lyrPath, LayerStatus.RETRIEVING)

      # Retrieve content
      lyrCnt = urllib2.urlopen(layerUrl)
      
      if layerFormat == LayerFormat.SHAPE:
         outDir = os.path.split(lyrPath)[0]
         content = StringIO(lyrCnt)
         content.seek(0)
         with zipfile.ZipFile(content, allowZip64=True) as z:
            for name in z.namelist():
               z.extract(name, outDir)
      else:
         with open(lyrPath, 'w') as outF:
            outF.write(lyrCnt)

      # Validate content
      if not self.validateLayer(layerId, lyrPath):
         raise LmException(JobStatus.IO_LAYER_ERROR, 
                           "Layer content does not match SHA256")
      # Update db
      self._updateLayerStatus(layerId, rLyrFormat, LayerStatus.STORED)
      
      # Convert if necessary
      if layerFormat in [LayerFormat.ASCII, LayerFormat.MXE]:
         self._convertLayer(layerId, layerFormat)

      status, lyr = self._queryLayer(layerId, layerFormat)

      return lyr
   
   # .................................
   def validateLayer(self, validValue, layerFn):
      """
      @summary: Checks that a stored layer has the correct SHA-256 sum.  We can
                   change this validation methodology here if we want
      @param validValue: The valid SHA256sum
      @param layerFn: The file to check the SHA256sum against
      @todo: Consider providing a direct variable for comparison, such as 
                layer content.  This could be useful if we are comparing a 
                download from a URL
      """
      with open(layerFn, 'r') as inF:
         layerCnt = inF.read()
      cmpVal = sha256(layerCnt).hexdigest()
      if cmpVal != validValue:
         return False
      else:
         return True
      
   # .................................
   def getLayerFilename(self, layerId, layerFormat, layerUrl=None):
      """
      @summary: Gets the path to the file specified by the layer id and desired 
                   format
      """
      status, lyr = self._queryLayer(layerId, layerFormat)
      
      if status in [LayerStatus.SEEDED, LayerStatus.STORED]:
         # Nothing to do
         pass
      elif status == LayerStatus.TIFF_AVAILABLE:
         # Need to convert
         lyr = self._convertLayer(layerId, layerFormat)
      elif status == LayerStatus.RETRIEVING:
         lyr = self._waitOnLayer(layerId, layerFormat, layerUrl)
      elif status == LayerStatus.ABSENT:
         lyr = self._retrieveLayer(layerId, layerFormat, layerUrl)
      else:
         raise LmException(JobStatus.IO_LAYER_ERROR, 
                           "Unknown layer status: %s" % status)
      if lyr is not None: # Should never be None, but just in case
         fn = lyr['filepath']
         self._touchLayer(layerId, layerFormat)
      else:
         raise LmException(JobStatus.IO_LAYER_ERROR, 
                           "Layer is unexpectedly None")
      return fn
   
   # .................................
   def purgeLayers(self, lastTouched):
      """
      @summary: Purge layers from the database that were last touched before 
                   the parameter
      @param lastTouched: Purge (non-seeded) layers that were last touched 
                             before this time (MJD format)
      """
      rows = self._findOldLayers(lastTouched)
      for row in rows:
         print "Deleting layer:", row[0]
         self._deleteLayer(row[0], row[1])
   
   # .................................
   def seedLayers(self, layerTups, makeASCIIs=True, makeMXEs=True):
      """
      @summary: Seeds the layer database with a list of layers that are already
                   stored on the local system.  This prevents extra downloads
                   of data when it is already present
      @param layerTups: A list of tuples (layer identifier, file path to TIFF)
      @param makeASCIIs: Should ASCII files be generated (for Maxent)
      @param makeMXEs: Should MXE files be generated (for Maxent)
      """
      print "Seeding GeoTiffs"
      for layerId, layerPath in layerTups:
         if self.validateLayer(layerId, layerPath):
            self._insertLayer(layerId, LayerFormat.GTIFF, layerPath, 
                              LayerStatus.SEEDED)
         else:
            raise Exception, "SHA256 of %s did not match %s" % (
                                                            layerPath, layerId)
      print "Done seeding GeoTiffs"
      
      # ASCII Grids
      if makeASCIIs:
         mxeTups = []
         print "Seeding ASCII layers"
         for layerId, layerPath in layerTups:
            ascFn = self._getFilePath(layerId, LayerFormat.ASCII)
            mxeFn = self._getFilePath(layerId, LayerFormat.MXE)
            
            mxeTups.append((ascFn, mxeFn))
            
            convertTiffToAscii(layerPath, ascFn)
            self._insertLayer(layerId, LayerFormat.ASCII, ascFn, 
                              LayerStatus.SEEDED)
         print "Done converting ASCIIs"
         # Only make MXEs if we make ASCIIs
         if makeMXEs:
            print "Seeding MXEs"
            convertAsciisToMxes(mxeTups)
            for layerId, _ in layerTups:
               self._insertLayer(layerId, LayerFormat.MXE, 
                                 self._getFilePath(layerId, LayerFormat.MXE), 
                                 LayerStatus.SEEDED)
            print "Done seeding MXEs"
         
   # .................................
   def _createLayerDb(self):
      """
      @summary: Attempts to create a layers database table
      """
      try:
         self.con.execute("CREATE TABLE layers({lyrIdAtt} TEXT, {fpAtt} TEXT, {ftypeAtt} INT, {statAtt} INT, {createAtt} REAL, {touchAtt} REAL, PRIMARY KEY ({lyrIdAtt}, {ftypeAtt}))".format(
               lyrIdAtt=LayerAttributes.LAYER_ID, 
               fpAtt=LayerAttributes.FILE_PATH, 
               ftypeAtt=LayerAttributes.FILE_TYPE,
               statAtt=LayerAttributes.STATUS,
               createAtt=LayerAttributes.CREATE_TIME,
               touchAtt=LayerAttributes.TOUCH_TIME))
      except:
         pass

   # .................................
   def _deleteLayer(self, layerId, layerFormat):
      """
      @summary: Deletes a layer from the database
      @param hostname: The name of the layer host
      @param uPath: The url path (between the host and parameters)
      @param key: The key for the layer
      @note: This could be useful if the layer failed to store
      """
      cmd = "DELETE FROM layers WHERE {lyrIdAtt} = '{lyrId}' AND {fTypeAtt} = {fType}".format(
                  lyrIdAtt=LayerAttributes.LAYER_ID,
                  lyrId=layerId,
                  fTypeAtt=LayerAttributes.FILE_TYPE,
                  fType=LayerFormat)
      self._executeDbFunction(cmd)
      
      lyrFn = self._getFilePath(layerId, layerFormat)
      filesToDelete = [lyrFn]
      
      # GeoTiffs may have extra metadata file
      if layerFormat == LayerFormat.GTIFF:
         filesToDelete.append('%s%s' % (os.path.splitext(lyrFn)[0], '.aux.xml'))
      # Shapefiles have several files
      elif layerFormat == LayerFormat.SHAPE:
         fnBase = os.path.splitext(lyrFn)
         for ext in SHAPEFILE_EXTENSIONS:
            filesToDelete.append('%s%s' % (fnBase, ext))

      for fn in filesToDelete:
         if os.path.exists(fn):
            os.remove(fn)

# .............................................................................
def convertAndModifyAsciiToTiff(ascFn, tiffFn, scale=None, multiplier=None,
                                noDataVal=127, dataType='int'):
   """
   @summary: Converts an ASCII file into a GeoTiff.  This function will, 
                optionally, modify the data while converting by scaling the 
                outputs or multiplying
   @param ascFn: The file name of the existing ASCII grid to convert
   @param tiffFn: The file path for the new Tiff file
   @param scale: If None, don't do anything.  To use, provide a tuple in the 
                    form (scaleMin, scaleMax)
   @param multiplier: If None, don't do anything.  If provided, multiply all
                         data values in the grid by this number
   @param noDataVal: The no data value to use for the new layer
   @param dataType: The data type for the resulting raster
   """
   if dataType.lower() == "int":
      npType = numpy.uint8
      gdalType = gdal.GDT_Byte
   else:
      raise Exception, "Unknown data type"
    
   src_ds = gdal.Open(ascFn)
   band = src_ds.GetRasterBand(1)
   band.GetStatistics(0,1)
   
   ndVal = band.GetNoDataValue()

   data = src_ds.ReadAsArray(0, 0, src_ds.RasterXSize, src_ds.RasterYSize)

   # If scale
   if scale is not None:
      scaleMin, scaleMax = scale
      lyrMin = band.GetMinimum()
      lyrMax = band.GetMaximum()
   
      def scaleFn(x):
         if x == ndVal:
            return noDataVal
         else:
            return (scaleMax - scaleMin)*((x-lyrMin) / (lyrMax-lyrMin)) + scaleMin
   
      data = numpy.vectorize(scaleFn)(data)
   
   # If multiply
   elif multiplier is not None:
      def multFn(x):
         if x == ndVal:
            return noDataVal
         else:
            return multiplier * x
         
      data = numpy.vectorize(multFn)(data)
   
   data = data.astype(npType)
   driver = gdal.GetDriverByName('GTiff')
   dst_ds = driver.Create(tiffFn, src_ds.RasterXSize, src_ds.RasterYSize, 1, 
                          gdalType)
    
   dst_ds.GetRasterBand(1).WriteArray(data)
   dst_ds.GetRasterBand(1).SetNoDataValue(noDataVal)
   dst_ds.GetRasterBand(1).ComputeStatistics(True)
    
   dst_ds.SetProjection(src_ds.GetProjection())
   dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
    
   driver = None
   dst_ds = None
   src_ds = None

# .............................................................................
def convertAsciisToMxes(fnTups):
   """
   @summary: Converts a list of ASCII grids into a Maxent indexed environmental 
                layers
   @param fnTups: A list of tuples of (ASCII file name, new MXE file name)
   """
   # Get temporary directories
   t = md5(str(gmt())).hexdigest() # Should be roughly unique
   
   inDir = os.path.join(TEMPORARY_FILE_PATH, 'asciis-%s' % t)
   outDir = os.path.join(TEMPORARY_FILE_PATH, 'mxes-%s' % t)

   # Just in case we happen to collide
   while os.path.exists(inDir):
      t = md5(str(gmt())).hexdigest() # Should be roughly unique
   
      inDir = os.path.join(TEMPORARY_FILE_PATH, 'asciis-%s' % t)
      outDir = os.path.join(TEMPORARY_FILE_PATH, 'mxes-%s' % t)
   
   os.makedirs(inDir)
   os.makedirs(outDir)
   
   # Sym link all of the ASCII grids in the input directory
   for asciiFn, _ in fnTups:
      baseName = os.path.basename(asciiFn)
      os.symlink(asciiFn, os.path.join(inDir, baseName))
      
   # Run Maxent converter
   meConvertCmd = "{javaCmd} {meCmd} {convertTool} -t {inDir} asc {outDir} mxe".format(
                     javaCmd=JAVA_CMD, meCmd=ME_CMD, convertTool=CONVERT_TOOL,
                     inDir=inDir, outDir=outDir)
   p = subprocess.Popen(meConvertCmd, shell=True)
   
   while p.poll() is not None:
      print "Waiting for layer conversion (asc to mxe) to finish..."
      sleep(WAIT_SECONDS)
   
   # Move all of the MXEs to the correct location
   for asciiFn, mxeFn in fnTups:
      # We used the ASCII base name.  This is probably the same as the MXE with
      #    a different extension, but just in case
      baseName = os.path.basename(asciiFn)
      tmpMxeFn = '%s%s' % (os.path.splitext(baseName)[0], OutputFormat.MXE)
      os.rename(tmpMxeFn, mxeFn)
      
   # Remove temporary directories
   # TODO: Make this safer 
   shutil.rmtree(inDir)
   shutil.rmtree(outDir)

# .............................................................................
def convertTiffToAscii(tiffFn, asciiFn, headerPrecision=4):
   """
   @summary: Converts an existing GeoTIFF file into an ASCII grid
   @param tiffFn: The path to an existing GeoTIFF file
   @param asccFn: The output path for the new ASCII grid
   @param headerPrecision: The number of decimal places to keep in the ASCII  
                              grid headers.  Setting to None skips.
   @note: Headers must match exactly for Maxent so truncating them eliminates
             floating point differences
   @todo: Evaluate if this can all be done with GDAL.  
   """
   # ....................................
   def _processFloatHeader(headerRow, numDigits):
      """
      @summary: This method will process a header row and truncate a floating 
                   point value if necessary
      @param headerRow: This is a string in the format 
                           "{header name}   {header value}"
      @param numDigits: Truncate a decimal after this many places, keep all if 
                           this is None
      """
      # Split out header name and value (replace tabs with spaces and use 
      #    regular expression to split
      header, value = re.split(r' +', headerRow.replace('\t', ' '))
      # Truncate the value by finding the decimal (if it exists) and adding numDigits places
      truncatedValue = value[:value.find('.')+numDigits+1] if value.find('.') >= 0 else value
      return "%s     %s\n" % (header, truncatedValue)
   
   # Use GDAL to generate ASCII Grid 
   drv = gdal.GetDriverByName('AAIGrid')
   ds_in = gdal.Open(tiffFn)
   ds_out = drv.CreateCopy(asciiFn, ds_in)
   ds_in = None
   ds_out = None   
   
   # Now go back and modify the output if necessary
   # Note, this will fail if any of the required headers are missing
   if headerPrecision is not None:
      output = [] # Lines to output back to file
      cont = True
      
      with open(asciiFn, 'r') as ascIn:
         for line in ascIn:
            if cont:
               if line.lower().startswith('ncols'):
                  nColsLine = line
               elif line.lower().startswith('nrows'):  
                  nRowsLine = line
               elif line.lower().startswith('xllcorner'):
                  xllLine = _processFloatHeader(line, numDigits=headerPrecision)
               elif line.lower().startswith('yllcorner'):
                  yllLine = _processFloatHeader(line, numDigits=headerPrecision)
               elif line.lower().startswith('cellsize'):
                  cellsizeLine = _processFloatHeader(line, numDigits=headerPrecision)
               elif line.lower().startswith('dx'):
                  cellsizeLine = _processFloatHeader(line, numDigits=headerPrecision).replace('dx', 'cellsize')
               elif line.lower().startswith('dy'):
                  pass
               elif line.lower().startswith('nodata_value'):
                  ndLine = line
               else: # Data line
                  cont = False
                  output.append(nColsLine)
                  output.append(nRowsLine)
                  output.append(xllLine)
                  output.append(yllLine)
                  output.append(cellsizeLine)
                  output.append(ndLine)
                  output.append(line)
            else:
               output.append(line)
      # Rewrite ASCII Grid
      with open(asciiFn, 'w') as ascOut:
         ascOut.writelines(output)


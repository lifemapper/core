"""
@summary: Module containing compute environment layer management code
@author: CJ Grady
@status: beta
@version: 4.0.0

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
@todo: Add convert tool to config
@todo: Use verify module
@todo: Skip if exists

@todo: Alphabetize
"""
from hashlib import md5
from mx.DateTime import gmt
import numpy
import os
from osgeo import gdal
import re
import shutil
import subprocess
from time import sleep

from LmCommon.common.lmconstants import (LMFormat)
from LmCompute.common.lmconstants import (CONVERT_JAVA_CMD, CONVERT_TOOL, 
                                          ME_CMD, TEMPORARY_FILE_PATH)

WAIT_SECONDS = 30

## .............................................................................
#class LayerManager(object):
#   """
#   @summary: Manages the storage of layers on the file system through sqlite
#   """
#   # .................................
#   def __init__(self, dataDir):
#      dbFile = os.path.join(dataDir, ENV_LAYER_DIR, INPUT_LAYER_DB)
#      self.lyrBasePath = os.path.join(dataDir, ENV_LAYER_DIR)
#      createDb = False
#      if not os.path.exists(dbFile):
#         createDb = True
#      self.con = sqlite3.connect(dbFile, timeout=DB_TIMEOUT, isolation_level=None)
#      if createDb:
#         print("Database does not exist, creating...")
#         self._createMetadataTable()
#         self._createLayerDb()
   
   ## .................................
   #def seedLayers(self, layerTups, makeASCIIs=True, makeMXEs=True):
   #   """
   #   @summary: Seeds the layer database with a list of layers that are already
   #                stored on the local system.  This prevents extra downloads
   #                of data when it is already present
   #   @param layerTups: A list of tuples (layer identifier, file path to TIFF)
   #   @param makeASCIIs: Should ASCII files be generated (for Maxent)
   #   @param makeMXEs: Should MXE files be generated (for Maxent)
   #   """
   #   print "Seeding GeoTiffs"
   #   for layerId, layerPath in layerTups:
   #      # Check for existing layer
   #      if self._queryLayer(layerId, LayerFormat.GTIFF)[1] is None:
   #         if verifyHash(layerId, dlocation=layerPath):
   #            self._insertLayer(layerId, LayerFormat.GTIFF, layerPath, 
   #                              LayerStatus.SEEDED)
   #         else:
   #            raise Exception, "Identifier of %s did not match %s" % (
   #                                                            layerPath, layerId)
   #   print "Done seeding GeoTiffs"
   #   
   #   # ASCII Grids
   #   if makeASCIIs:
   #      mxeTups = []
   #      mxeLayers = [] # Identifiers for MXE layers we create
   #      print "Seeding ASCII layers"
   #      for layerId, layerPath in layerTups:
   #         
   #         # Generate desired ASCII and MXE file names
   #         basename = os.path.splitext(layerPath)[0]
   #         ascFn = '%s%s' % (basename, LMFormat.ASCII.ext)
   #         mxeFn = '%s%s' % (basename, LMFormat.MXE.ext)
   #         
   #         # Query to see if ASCII is already inserted
   #         ascStatus, _ = self._queryLayer(layerId, LayerFormat.ASCII)
   #         # Query to see if MXE is already inserted
   #         mxeStatus, _ = self._queryLayer(layerId, LayerFormat.MXE)
   #
   #         if ascStatus in (LayerStatus.ABSENT, LayerStatus.TIFF_AVAILABLE):
   #            if not os.path.exists(ascFn): # Only create if file does not exist
   #               convertTiffToAscii(layerPath, ascFn)
   #            # Insert into DB if not there
   #            self._insertLayer(layerId, LayerFormat.ASCII, ascFn, 
   #                           LayerStatus.SEEDED)
   #         
   #         if mxeStatus in (LayerStatus.ABSENT, LayerStatus.TIFF_AVAILABLE):
   #            # Only set MXE to generate if file does not exist
   #            if not os.path.exists(mxeFn):
   #               mxeTups.append((ascFn, mxeFn))
   #            # Set to insert into db if not present in db
   #            mxeLayers.append((layerId, mxeFn))
   #         
   #      print "Done converting ASCIIs"
   #      # Only make MXEs if we make ASCIIs
   #      if makeMXEs:
   #         print "Seeding MXEs"
   #         convertAsciisToMxes(mxeTups)
   #         for layerId, mxeFn in mxeLayers:
   #            self._insertLayer(layerId, LayerFormat.MXE, mxeFn, 
   #                              LayerStatus.SEEDED)
   #         print "Done seeding MXEs"
         
# .............................................................................
def processLayersJSON(layerJSON, symDir=None):
   """
   @summary: Process layer JSON and return a list of file names and 
                a mask filename
   @param layerJSON: A JSON object with an entry for layers (list) and a 
                        mask.  Each layer should be an object with an 
                        identifier and / or url
   @param layerFormat: The format for the returned layer file names
   @param symDir: If provided, symbolically link the layers in this 
                     directory
   @note: Assumes that layerJSON is an object with layers and mask
   @todo: Use constants
   """
   layers = []
   for lyrObj in layerJSON['layers']:
      #lyrId = None
      #if lyrObj.has_key('identifier'):
      #   lyrId = lyrObj['identifier']
      
      layers.append(lyrObj['path'])
      #layers.append(self.getLayerFilename(lyrId, layerFormat, lyrUrl))
   
   #TODO: Do this with constants
   lyrExt = os.path.splitext(layers[0])[1]

   if symDir is not None:
      newLayers = []
      for i in range(len(layers)):
         newFn = os.path.join(symDir, "layer{}{}".format(i, lyrExt))
         os.symlink(layers[i], newFn)
         newLayers.append(newFn)
      return newLayers
   else:
      return layers

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
                     javaCmd=CONVERT_JAVA_CMD, meCmd=ME_CMD, 
                     convertTool=CONVERT_TOOL, inDir=inDir, outDir=outDir)
   p = subprocess.Popen(meConvertCmd, shell=True)
   
   while p.poll() is None:
      print "Waiting for layer conversion (asc to mxe) to finish..."
      sleep(WAIT_SECONDS)
   
   # Move all of the MXEs to the correct location
   for asciiFn, mxeFn in fnTups:
      
      #TODO: Add a try except here
      try:
         # We used the ASCII base name.  This is probably the same as the MXE 
         #    with a different extension, but just in case
         baseName = os.path.basename(asciiFn)
         tmpMxeFn = os.path.join(outDir, 
                     '%s%s' % (os.path.splitext(baseName)[0], LMFormat.MXE.ext))
         shutil.copy(tmpMxeFn, mxeFn)
      except Exception, e:
         print "Failed to rename layer: %s -> %s" % (tmpMxeFn, mxeFn)
         print str(e)
      
   # Remove temporary directories
   # TODO: Make this safer 
   shutil.rmtree(inDir)
   shutil.rmtree(outDir)

# .............................................................................
def convertLayersInDirectory(layerDir):
   """
   @summary: Converts all layers in directory from tiffs to asciis and mxes
   @param layerDir: The directory to traverse through looking for layers to
                       convert
   """
   mxeTups = []
   for myDir, _ , files in os.walk(layerDir):
      for fn in files:
         tiffFn = os.path.join(myDir, fn)
         basename, ext = os.path.splitext(tiffFn)
         if ext.lower() == LMFormat.GTIFF.ext:
            asciiFn = '{}{}'.format(basename, LMFormat.ASCII.ext)
            mxeFn = '{}{}'.format(basename, LMFormat.MXE.ext)
            
            if not os.path.exists(asciiFn):
               print 'Converting: {}'.format(tiffFn)
               convertTiffToAscii(tiffFn, asciiFn)
               
            if not os.path.exists(mxeFn):
               mxeTups.append((asciiFn, mxeFn))
   
   if len(mxeTups) > 0:
      print "Converting ASCIIs to MXEs"
      convertAsciisToMxes(mxeTups)
         

# .............................................................................
def convertTiffToAscii(tiffFn, asciiFn):
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
   # Use GDAL to generate ASCII Grid 
   drv = gdal.GetDriverByName('AAIGrid')
   ds_in = gdal.Open(tiffFn)

   # MXE creation fails if we don't have a NODATA value, so add one if missing
   if ds_in.GetRasterBand(1).GetNoDataValue() is None:
      ds_in.GetRasterBand(1).SetNoDataValue(-999)

   options = ['FORCE_CELLSIZE=True']
   ds_out = drv.CreateCopy(asciiFn, ds_in, 0, options)
   #ds_out = drv.CreateCopy(asciiFn, ds_in)
   ds_in = None
   ds_out = None   
   
   # Now go back and modify the output if necessary
   # Note, this will fail if any of the required headers are missing
   output = [] # Lines to output back to file
   cont = True
   
   
   # Get header information from tiff file instead of reading ascii for it
   ds = gdal.Open(tiffFn)
   band = ds.GetRasterBand(1)

   leftX, xres, _, uly, _, yres = ds.GetGeoTransform()
   
   
   
   leftY = uly + (ds.RasterYSize * yres)
   
   nColsLine = 'ncols   {}\n'.format(ds.RasterXSize)
   nRowsLine = 'nrows   {}\n'.format(ds.RasterYSize)
   xllLine = 'xllcorner   {}\n'.format(leftX)
   yllLine = 'yllcorner   {}\n'.format(leftY)
   cellsizeLine = 'cellsize   {}\n'.format(xres)
   ndLine = 'NODATA_value   {}\n'.format(int(band.GetNoDataValue()))
   
   
   with open(asciiFn, 'r') as ascIn:
      for line in ascIn:
         if cont:
            if line.lower().startswith('ncols'):
               pass
            elif line.lower().startswith('nrows'):  
               pass
            elif line.lower().startswith('xllcorner'):
               pass
            elif line.lower().startswith('yllcorner'):
               pass
            elif line.lower().startswith('cellsize'):
               pass
            elif line.lower().startswith('dx'):
               pass
            elif line.lower().startswith('dy'):
               pass
            elif line.lower().startswith('nodata_value'):
               #ndLine = line
               pass
            else: # Data line
               cont = False
               output.append(nColsLine)
               output.append(nRowsLine)
               output.append(xllLine)
               output.append(yllLine)
               output.append(cellsizeLine)
               if ndLine is not None:
                  output.append(ndLine)
               output.append(line)
         else:
            output.append(line)
   # Rewrite ASCII Grid
   with open(asciiFn, 'w') as ascOut:
      for line in output:
         ascOut.write(line)


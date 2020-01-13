"""
@summary: Module containing compute environment layer management code
@author: CJ Grady
@status: beta
@version: 4.0.0

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
@todo: Add convert tool to config
@todo: Use verify module
@todo: Skip if exists

@todo: Alphabetize
"""
import numpy
import os
from osgeo import gdal
import subprocess
from time import sleep

from LmCommon.common.lmconstants import (LMFormat, DEFAULT_NODATA)
from LmCompute.common.lmconstants import (CONVERT_JAVA_CMD, CONVERT_TOOL, 
                                                        ME_CMD)

WAIT_SECONDS = 30

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
    @param noDataVal: The no data value to use for the new value-adjusted layer
    @param dataType: The data type for the resulting raster
    """
    if dataType.lower() == "int":
        npType = numpy.uint8
        gdalType = gdal.GDT_Byte
    else:
        raise Exception("Unknown data type")
     
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
def convertAsciisToMxes(lyrDir):
    """
    @summary: Converts a list of ASCII grids into a Maxent indexed environmental 
                     layers
    @param lyrDir: A directory containing ASCII grids that should be converted
                            to MXEs
    """
    # Run Maxent converter
    meConvertCmd = "{javaCmd} {meCmd} {convertTool} -t {inDir} asc {outDir} mxe".format(
                            javaCmd=CONVERT_JAVA_CMD, meCmd=ME_CMD, 
                            convertTool=CONVERT_TOOL, inDir=lyrDir, outDir=lyrDir)
    p = subprocess.Popen(meConvertCmd, shell=True)
    
    while p.poll() is None:
        print("Waiting for layer conversion (asc to mxe) to finish...")
        sleep(WAIT_SECONDS)
    
# .............................................................................
def convertLayersInDirectory(layerDir):
    """
    @summary: Converts all layers in directory from tiffs to asciis and mxes
    @param layerDir: The directory to traverse through looking for layers to
                              convert
    """
    mxeDirs = set([])
    for myDir, _ , files in os.walk(layerDir):
        for fn in files:
            tiffFn = os.path.join(myDir, fn)
            basename, ext = os.path.splitext(tiffFn)
            if ext.lower() == LMFormat.GTIFF.ext:
                asciiFn = '{}{}'.format(basename, LMFormat.ASCII.ext)
                mxeFn = '{}{}'.format(basename, LMFormat.MXE.ext)
                
                if not os.path.exists(asciiFn):
                    print('Converting: {}'.format(tiffFn))
                    convertTiffToAscii(tiffFn, asciiFn)
                    
                if not os.path.exists(mxeFn):
                    mxeDirs.add(myDir)

    for lyrDir in mxeDirs:
        print('Converting ASCIIs in {} to MXEs'.format(lyrDir))
        convertAsciisToMxes(lyrDir)


# .............................................................................
def convertTiffToAscii(tiffFn, asciiFn, headerPrecision=6):
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
    # Get header information from tiff file
    leftX, xres, _, uly, _, yres = ds_in.GetGeoTransform()
    
    leftY = uly + (ds_in.RasterYSize * yres)
    cols = ds_in.RasterXSize
    rows = ds_in.RasterYSize
    # Force a NODATA value if missing from TIFF before copying to ASCII 
    nodata = ds_in.GetRasterBand(1).GetNoDataValue()
    if nodata is None:
        ds_in.GetRasterBand(1).SetNoDataValue(DEFAULT_NODATA)
        nodata = DEFAULT_NODATA
    # If header precision is not None, round vlaues
    if headerPrecision is not None:
        leftX = round(leftX, headerPrecision)
        leftY = round(leftY, headerPrecision)
        xres = round(xres, headerPrecision)
    
    options = ['FORCE_CELLSIZE=True']
    ds_out = drv.CreateCopy(asciiFn, ds_in, 0, options)
    ds_in = None
    ds_out = None    
        
    # Rewrite  ASCII header with tiff info
    output = [] 
    output.append('ncols    {}\n'.format(cols))
    output.append('nrows    {}\n'.format(rows))
    output.append('xllcorner    {}\n'.format(leftX))
    output.append('yllcorner    {}\n'.format(leftY))
    output.append('cellsize    {}\n'.format(xres))
    output.append('NODATA_value    {}\n'.format(int(nodata)))
    pastHeader = False
    with open(asciiFn, 'r') as ascIn:
        for line in ascIn:
            lowline = line.lower()
            if (not pastHeader and (
                 lowline.startswith('ncols') or
                 lowline.startswith('nrows') or
                 lowline.startswith('xllcorner') or
                 lowline.startswith('yllcorner') or
                 lowline.startswith('cellsize') or
                 lowline.startswith('dx') or
                 lowline.startswith('dy') or 
                 lowline.startswith('nodata_value'))):  
                pass
            else:
                pastHeader = True
                output.append(line)
    # Rewrite ASCII Grid
    with open(asciiFn, 'w') as ascOut:
        for line in output:
            ascOut.write(line)

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
    for lyrObj in layerJSON['layer']:
        #lyrId = None
        #if lyrObj.has_key('identifier'):
        #    lyrId = lyrObj['identifier']
        
        layers.append(lyrObj['path'])
        #layers.append(self.getLayerFilename(lyrId, layerFormat, lyrUrl))
    
    #TODO: Do this with constants
    lyrExt = os.path.splitext(layers[0])[1]

    if symDir is not None:
        newLayers = []
        for i in range(len(layers)):
            newFn = os.path.join(symDir, "layer{}{}".format(i, lyrExt))
            if not os.path.exists(newFn):
                os.symlink(layers[i], newFn)
            newLayers.append(newFn)
        return newLayers
    else:
        return layers

"""
from hashlib import md5
from mx.DateTime import gmt
import numpy
import os
from osgeo import gdal
import shutil
import subprocess
from time import sleep

from LmCommon.common.lmconstants import (LMFormat)
from LmCompute.common.lmconstants import (CONVERT_JAVA_CMD, CONVERT_TOOL, 
                                                        ME_CMD, TEMPORARY_FILE_PATH)

WAIT_SECONDS = 30


tiffFn='/share/lm/data/layers/30sec-CONUS/worldclim1.4/alt.tif'
basename, ext = os.path.splitext(tiffFn)
asciiFn = '{}{}'.format(basename, LMFormat.ASCII.ext)


drv = gdal.GetDriverByName('AAIGrid')
ds_in = gdal.Open(tiffFn)
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

nColsLine = 'ncols    {}\n'.format(ds.RasterXSize)
nRowsLine = 'nrows    {}\n'.format(ds.RasterYSize)
xllLine = 'xllcorner    {}\n'.format(leftX)
yllLine = 'yllcorner    {}\n'.format(leftY)
cellsizeLine = 'cellsize    {}\n'.format(xres)
ndLine = 'NODATA_value    {}\n'.format(int(band.GetNoDataValue()))


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


"""
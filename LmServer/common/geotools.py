"""
From http://perrygeo.googlecode.com/svn/trunk/gis-bin/flip_raster.py
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
   import numpy
except:
   print('Unable to import numpy')
   
import os
from osgeo import gdal
from osgeo import gdalconst

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import DEFAULT_NODATA

# ............................................................................
class GeoFileInfo(LMObject):
   """
   @summary: Class for getting information from a raster dataset readable
             by GDAL.
   """
   def __init__(self, dlocation, varpattern=None, updateable=False):
      """
      @param dlocation: dataset location, interpretable by GDAL
      @param varpattern: string to match for the final portion of a subdataset
             name, for those datasets where the data of interest is one of 
             multiple subdatasets.
      @param updateable: False if open in Read-Only mode, True if writeable. 
      """
      # Used for geotools (checking point values)
      self.scalef = 1.0
      self._cscanline = None
      self._band = None
      self._dataset = None
      self._min = None
      self._max = None
      self._mean = None
      self._stddev = None
      
      # General dataset info
      self.dlocation = dlocation
      fname = os.path.basename(self.dlocation)
      basename, ext = os.path.splitext(fname)
      self.name = basename
      self.variable = basename
      # filled in when dataset is opened below
      self.gdalFormat = None
   
      try:
         self.openDataSet(varpattern, updateable)
      except LMError as e:
         raise e
      except Exception as e:
         raise LMError(['Unable to open raster file %s' % self.dlocation, str(e)])
      else:
         if self._dataset is None:
            raise LMError(['No dataset for file %s' % self.dlocation, str(e)])
         self._band = self._dataset.GetRasterBand(1)
         self.srs = self._dataset.GetProjection()
         self.bands = self._dataset.RasterCount
         self.xsize = self._dataset.RasterXSize
         self.ysize = self._dataset.RasterYSize
         self.geoTransform = self._dataset.GetGeoTransform()
   
         # GDAL data type
         self.gdalBandType = self._band.DataType
         self.nodata = self._band.GetNoDataValue()
         
         self.ulx = self.geoTransform[0]
         self.xPixelSize = self.geoTransform[1]
         self.uly = self.geoTransform[3]
         self.yPixelSize = self.geoTransform[5]
   
         self.lrx = self.ulx + self.xPixelSize * self.xsize
         self.lry = self.uly + self.yPixelSize * self.ysize

# ...............................................
   def openDataSet(self, varpattern=None, updateable=False):
      """
      @summary: Open (or re-open) the dataset.
      @param varpattern: string to match for the final portion of a subdataset
             name, for those datasets where the data of interest is one of 
             multiple subdatasets.
      @param updateable: False if open in Read-Only mode, True if writeable. 
      """
      try:
         if updateable:
            self._dataset = None
            self._band = None
            self._dataset = gdal.Open(str(self.dlocation), gdalconst.GA_Update)
         elif self._dataset is None:
            self._dataset = gdal.Open(str(self.dlocation), gdalconst.GA_ReadOnly)
      except Exception as e:
         print(('Exception raised trying to open layer=%s\n%s'
               % (self.dlocation, str(e))))
         raise LMError(['Unable to open %s' % self.dlocation])
      else:
         if self._dataset is None:
            raise LMError(['Unable to open %s' % self.dlocation])
         
         drv = self._dataset.GetDriver()
         self.gdalFormat = drv.GetDescription()
                  
      self.units = self._dataset.GetMetadataItem('UNITS')
      # Subdatasets for NIES IPCC AR4 netcdf data
      # Resets dlocation, _dataset, variable, units, description
      self._checkSubDatasets(varpattern)
   
# .............................................
   def writeWktSRS(self, srs):
      """
      @summary: Write a new SRS to the dataset
      @param srs: spatial reference system in well-known-text (wkt) 
                  to write to the dataset. 
      """
      try:
         self.openDataSet(updateable=True)
         self._dataset.SetProjection( srs )
         self._dataset.FlushCache()
         self._dataset = None
         self.srs = srs
      except Exception as e:
         raise LMError(['Unable to write SRS info to file', srs, 
                        self.dlocation, str(e)])
   
# .............................................
   def copySRS(self, fname):
      """
      @summary: Write a new SRS, copied from another dataset, to this dataset.
      @param fname: Filename for dataset from which to copy spatial reference 
                system. 
      """
      newsrs = GeoFileInfo.getSRSAsWkt(fname)
      self.writeWktProjection(newsrs)
      
# .............................................
   def copyWithoutProjection(self, outfname, format='GTiff'):
      """
      @summary: Copy this dataset with no projection information.
      @param outfname: Filename to write this dataset to.
      @param format: GDAL-writeable raster format to use for new dataset. 
                     http://www.gdal.org/formats_list.html
      """
      self.openDataSet()
      driver = gdal.GetDriverByName(format)
      
      outds = driver.Create(outfname, self.xsize, self.ysize, 1, self.gdalBandType)
      if outds is None:
         print('Creation failed for %s from band %d of %s' % (outfname, 1, 
                                                              self.dlocation))
         return 0 
      
      outds.SetGeoTransform(self.geoTransform)
      outband = outds.GetRasterBand(1)
      outband.SetNoDataValue(self.nodata)
      outds.SetProjection('')
      rstArray = self._dataset.ReadAsArray()
      outband.WriteArray( rstArray )

      # Close new dataset to flush to disk
      outds.FlushCache()
      outds = None

# .............................................
   def _checkSubDatasets(self, varpattern):
      try:
         subdatasets = self._dataset.GetSubDatasets()
      except:
         pass
      else:
         found = False
         for subname, subdesc in subdatasets:
            if (varpattern is None and not (subname.endswith('bounds'))
                or
                (varpattern is not None and subname.endswith(varpattern))):
               # Replace enclosing dataset values
               self.variable = subname.split(':')[2]
               self.dlocation = subname
               self.description = subdesc
               found = True
               break
               
         if found:
            # Replace enclosing dataset with subdataset
            self._dataset = None
            self._dataset = gdal.Open(self.dlocation, gdalconst.GA_ReadOnly)
            # Replace units of subdataset
            self.units = self._dataset.GetMetadataItem(self.variable+'#units')
            


# .............................................
   def _cycleRow(self, scanline, arrtype, left, center, right):
      """
      @summary: Shift the values in a row to the right, so that the first 
                column in the row is shifted to the center.  Used for data in
                which a row begins with 0 degree longitude and ends with 360 
                degrees longitude (instead of -180 to +180) 
      @param scanline: Original row to shift.
      @param arrtype: Numpy datatype for scanline values
      @param left: Leftmost column index
      @param center: Center column index
      @param right: Rightmost column index
      """
      newline = numpy.empty((self.xsize), dtype=arrtype)
      c = 0
      for col in range(center, right):
         newline[c] = scanline[col]
         c += 1
      for col in range(left, center):
         newline[c] = scanline[col]
         c += 1
      return newline
   
   # ............................................................................
   def _getNumpyType(self, othertype):
      if othertype == gdalconst.GDT_Float32:
         arrtype = numpy.float32
      return arrtype
   
# .............................................
   def getArray(self, bandnum, doFlip=False, doShift=False):
      """
      @summary: Read the dataset into numpy array  
      @param bandnum: The band number to read.
      @param doFlip: True if data begins at the southern edge of the region
      @param doShift: True if the leftmost edge of the data should be shifted 
             to the center (and right half shifted around to the beginning) 
      """
      if 'numpy' in dir():
         inds = gdal.Open(self.dlocation, gdalconst.GA_ReadOnly)
         inband = inds.GetRasterBand(bandnum)
         arrtype = self._getNumpyType(self.gdalBandType)
         outArr = numpy.empty([self.ysize, self.xsize], dtype=arrtype)
         
         for row in range(self.ysize):
            scanline = inband.ReadAsArray(0, row, self.xsize, 1, self.xsize, 1)
            
            if doShift:
               scanline = self._cycleRow(scanline, arrtype, 0, self.xsize/2, 
                                         self.xsize)
            if doFlip:
               newrow = self.ysize-row-1
            else:
               newrow = row
   
            outArr[newrow] = scanline
   
         inds = None   
         return outArr
      else:
         raise LMError('numpy missing - unable to getArray')
      
# .............................................
   def copyDataset(self, bandnum, outfname, format='GTiff', kwargs={}):
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
      driver = gdal.GetDriverByName(format)
      metadata = driver.GetMetadata()
      if not (gdal.DCAP_CREATECOPY in metadata 
                and metadata[gdal.DCAP_CREATECOPY] == 'YES'):
         raise LMError(currargs='Driver %s does not support CreateCopy() method.' 
                       % format)
      inds = gdal.Open( self.dlocation )
      try:
         outds = driver.CreateCopy(outfname, inds, 0, **kwargs)
      except Exception as e:
         raise LMError(currargs='Creation failed for %s from band %d of %s (%s)'
                                % (outfname, bandnum, self.dlocation, str(e)))
      if outds is None:
         raise LMError(currargs='Creation failed for %s from band %d of %s)'
                                % (outfname, bandnum, self.dlocation))
      
      # Close new dataset to flush to disk
      outds = None
      inds = None

      
# ...............................................
# .............................................
   def writeBand(self, bandnum, outfname, format='GTiff', doFlip=False, 
                doShift=False, nodata=None, srs=None):
      """
      @summary: Write the dataset into a new file, line by line.  
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
      driver = gdal.GetDriverByName(format)
      metadata = driver.GetMetadata()
      if not (gdal.DCAP_CREATE in metadata 
              and metadata[gdal.DCAP_CREATE] == 'YES'):
         raise LMError(currargs='Driver %s does not support Create() method.' 
                                % format)
         
      outds = driver.Create(outfname, self.xsize, self.ysize, 1, self.gdalBandType)
      if outds is None:
         raise LMError('Creation failed for %s from band %d of %s'
                        % (outfname, bandnum, self.dlocation))
      
      outds.SetGeoTransform(self.geoTransform)
      
      inds = gdal.Open(self.dlocation, gdalconst.GA_ReadOnly)
      inband = inds.GetRasterBand(bandnum)
      outband = outds.GetRasterBand(1)
      
      if nodata is None:
         nodata = inband.GetNoDataValue()
      if nodata is not None:
         outband.SetNoDataValue(nodata)
      if srs is None:
         srs = self.srs
      outds.SetProjection(srs)
      
      for row in range(self.ysize):
         scanline = inband.ReadAsArray(0, row, self.xsize, 1, self.xsize, 1)
         
         if doShift:
            arrType = self._getNumpyType(self.gdalBandType)
            scanline = self._cycleRow(scanline, arrType, 0, self.xsize/2, 
                                      self.xsize)

         if doFlip:
            outband.WriteArray(scanline, 0, self.ysize-row-1)
         else:
            outband.WriteArray(scanline, 0, row)
      # Close new dataset to flush to disk
      outds = None
      inds = None
      
# ...............................................
   def __unicode__(self):
      return '%s (%s)' % (self.name, self.dlocation)
   
# ...............................................
   def loadBand(self, bandnum=1):
      """
      @summary: Open the dataset and save the band to a member attribute for 
                further examination.  
      @param bandnum: The band number to read.
      """
      if self._band is None:
         if self._dataset is None:
            self.openDataSet(None)
         self._band = self._dataset.GetRasterBand(bandnum)
    
# ...............................................
   def _getStats(self, bandnum=1):
      if (self._min is None or self._max is None 
          or self._mean is None or self._stddev is None):
         self.loadBand(bandnum)
         try:
            min, max, mean, stddev = self._band.GetStatistics(False,True)
#            min, max, mean, stddev = self._band.ComputeBandStats(False)
         except Exception as e:
            print(('Exception in GeoFileInfo._getStats: band.GetStatistics %s' % str(e)))
            min, max, mean, stddev = None, None, None, None
         self._min = min
         self._max = max
         self._mean = mean
         self._stddev = stddev
      return min, max, mean, stddev
   
# ...............................................
   def getHistogram(self, bandnum=1):
      """
      @summary: Get the data values histogram, for coloring in a map.  
      @param bandnum: The band number to read.
      @return: a list of data values present in the dataset
      @note: this returns only a list, not a true histogram.  
      @note: this only works on 8-bit data.
      """
      vals = []      
      # Get histogram only for 8bit data (projections)
      if self.gdalBandType == gdalconst.GDT_Byte:
         self.loadBand(bandnum)
         hist = self._band.GetHistogram()
         for i in range(len(hist)):
            if i > 0 and i != self.nodata and hist[i] > 0:
               vals.append(i)
      else:
         print('Histogram calculated only for 8-bit data')
      return vals

   
# ...............................................
   def _getMin(self):
      if self._min is None:
         self._getStats()
      return self._min
   
   min = property(_getMin)
   
# ...............................................
   def _getMax(self):
      if self._max is None:
         self._getStats()
      return self._max
   
   max = property(_getMax)
# ...............................................
   def _getMean(self):
      if self._mean is None:
         self._getStats()
      return self._mean
   
   mean = property(_getMean)
# ...............................................
   def _getStddev(self):
      if self._stddev is None:
         self._getStats()
      return self._stddev
   
   stddev = property(_getStddev)
   
# ...............................................
   def pointInside(self, point):
      '''
      @summary: Returns true if point (x,y) is within extents
      @param point: a tuple representing a point to query for.
      '''
      ext = self.getExtents()
      if (point[0] >= ext[0] and point[0] <= ext[2]):
         if (point[1] >= ext[1] and point[1] <= ext[3]):
            return True
      return False
  
# ...............................................
   def getZvalues(self, points, missingv=DEFAULT_NODATA):
      '''
      @summary: Given a set of [[x,y], ...], returns [[x,y,z], ... ]
      @param points: A sequence of points (tuples of x and y)
      @param missingv: Value to return if the point is at a nodata cell
      '''
      #-------------------
      def pointsortfunc(p1, p2):
         '''
         Used to sort in increasing y value
         '''
         if p1[1] < p2[1]: 
            return -1
         if p1[1] > p2[1]:
            return 1
         return 0
      #-------------------
      
      ppos = 0
      self.loadBand()
      if len(points) > 1:
         points.sort(pointsortfunc)
      res = []
      for point in points:
         rpoint = [point[0], point[1]]
         if self.pointInside(rpoint):
            cxy = gxy2xy(rpoint, self.geoTransform)
            if cxy[1] != self._cscanline:
               self._cscanline = cxy[1]
               try:
                  self._scanline = self._band.ReadAsArray(0, self._cscanline, self.xsize, 1)
               except:
                  #could not create buffer
                  pass
            try:
               z = self._scanline[0,cxy[0]]
            except Exception as e:
               z = self.nodata
            if z == self.nodata:
               rpoint.append(missingv)
            else:
               #return the real value, this changes from float
#               newz = z * self.scalef
               rpoint.append(z)
         else:
            rpoint.append(missingv)
         res.append(rpoint)
      return res

# ...............................................
   def getBounds(self):
      '''
      @summary: Return a list of [minx, miny, maxx, maxy] which is the same 
                order as expected by W*S services and openlayers
      '''
      bounds = [self.ulx, self.lry, self.lrx, self.uly]
      return bounds
        
# ...............................................
   @staticmethod
   def rasterSize(datasrc):
      '''
      @summary: Return [width, height] in pixels
      '''
      datasrc = gdal.Open(str(datasrc))
      return [datasrc.RasterXSize, datasrc.RasterYSize]      

# ...............................................
   @staticmethod
   def getSRSAsWkt(filename):
      """
      @summary: Reads spatial reference system information from provided file 
      @param filename: The raster file from which to read srs information.
      @return: Projection information from GDAL
      @raise LMError: on failure to open dataset
      """
      if (filename is not None and os.path.exists(filename)):
         geoFI = GeoFileInfo(filename)
         srs = geoFI.srs
         geoFI = None
         return srs
      else:
         raise LMError(['Unable to read file %s' % filename])
    
'''
Implements some tools for visualizing points
'''
from math import floor

DEFAULT_PROJ = 'GEOGCS["WGS84", DATUM["WGS84", SPHEROID["WGS84", 6378137.0, 298.257223563]], PRIMEM["Greenwich", 0.0], UNIT["degree", 0.017453292519943295], AXIS["Longitude",EAST], AXIS["Latitude",NORTH]]'

# ...............................................
def gxy2xy(gxy,gt):
   """
   @summary: Convert geographic coordinates to pixel coordinates.
   
   Given a geographic coordinate (in the form of a two element, one
   dimensional array, [0] = x, [1] = y), and an affine transform, this
   function returns the inverse of the transform, that is, the pixel
   coordinates corresponding to the geographic coordinates.
   
   Arguments:
   @param gxy: sequence of two elements [x coordinate, y coordinate]
   @param gt: the affine transformation associated with a dataset
   @return: list of 2 elements [x pixel, y pixel] or None if the transform is 
            invalid.
   """
   xy = [0,0]
   gx = gxy[0]
   gy = gxy[1]
   
   # Determinant of affine transformation
   det = gt[1]*gt[5] - gt[4]*gt[2]

   # If the transformation is not invertable return None
   if det == 0.0:
      return None
   
   t1 = gx*gt[5] - gt[0]*gt[5] - gt[2]*gy + gt[2]*gt[3]
   
   #
   # Note:  by using floor() instead of int(x-0.5) (which truncates) the pixels
   # around the origin are not an extra half unit wide.
   #
   xy[0] = floor( t1 / det )
   t1 = gy*gt[1] - gt[1]*gt[3] - gx*gt[4] + gt[4]*gt[0]
   xy[1] = floor( t1 / det )
   xy[0] = int(xy[0])
   xy[1] = int(xy[1])
   return xy

# ...............................................
def xy2gxy(xy, gt, at='center'):
   """
   @summary: Compute geographic coordinates from pixel coordinates.
   @param xy: sequence of two elements [pixel x, pixel y]
   @param gt: the affine transformation associated with a dataset
   @param at: determines where in the pixel the transformation occurrs
         valid values are: 'bl', 'br', 'tr', 'tl', 'center'
   @return: list of two elements [geo x, geo y]
   """
   gxy = [0,0]
   at_delta = { 'bl': (0.0, 0.0),
                'br': (1.0, 0.0),
                'tr': (1.0, 1.0),
                'tl': (0.0, 1.0),
                'center': (0.5, 0.5) }
   xx = xy[0] + at_delta[at][0]
   yy = xy[1] + at_delta[at][1]
   gxy[0] = gt[0] + gt[1]*xx + gt[2]*yy
   gxy[1] = gt[3] + gt[4]*xx + gt[5]*yy
   return gxy

# ...............................................
def makeTransform(height, width, extent):
   '''
   @summary: Generates a simple geographic transform based on the provided 
             height and width in pixels, and the extent as 
             [minx, miny, maxx, maxy].
   @param height: height in pixels of the desired transform
   @param width: width in pixels of the desired transform
   @param extent: list of bounding coordinates in format[minx, miny, maxx, maxy]
   @return: a tuple of 6 elements, a geo-transform as defined by GDAL
   '''
   transform = (extent[0],
                (extent[2]-extent[0])/width,
                0.,
                extent[3],
                0.,
                -(extent[3]-extent[1])/height)
   return transform

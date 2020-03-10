"""Module containing geo tools

Note:
     From http://perrygeo.googlecode.com/svn/trunk/gis-bin/flip_raster.py
"""

import numpy
import os
from osgeo import gdal, gdalconst

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import DEFAULT_NODATA


# ............................................................................
class GeoFileInfo(LMObject):
    """Class for getting information from a raster dataset readable by GDAL."""

    # ................................
    def __init__(self, dlocation, var_pattern=None, updateable=False):
        """Constructor

        Args:
            dlocation: dataset location, interpretable by GDAL
            var_pattern: string to match for the final portion of a subdataset
                name, for those datasets where the data of interest is one of
                multiple subdatasets.
            updateable: False if open in Read-Only mode, True if writeable.
        """
        # Used for geotools (checking point values)
        self.scale_f = 1.0
        self._c_scan_line = None
        self._scan_line = None
        self._band = None
        self._dataset = None
        self._min = None
        self._max = None
        self._mean = None
        self._std_dev = None
        self.description = None
        self.units = None

        # General dataset info
        self.dlocation = dlocation
        fname = os.path.basename(self.dlocation)
        basename, _ = os.path.splitext(fname)
        self.name = basename
        self.variable = basename
        # filled in when dataset is opened below
        self.gdal_format = None

        try:
            self.open_dataset(var_pattern, updateable)
        except LMError as err:
            raise err
        except Exception as err:
            raise LMError('Unable to open file at {}'.format(dlocation), err)
        else:
            if self._dataset is None:
                raise LMError('No dataset for file {}'.format(dlocation))
            self._band = self._dataset.GetRasterBand(1)
            self.srs = self._dataset.GetProjection()
            self.bands = self._dataset.RasterCount
            self.x_size = self._dataset.RasterXSize
            self.y_size = self._dataset.RasterYSize
            self.geo_transform = self._dataset.GetGeoTransform()

            # GDAL data type
            self.gdal_band_type = self._band.DataType
            self.nodata = self._band.GetNoDataValue()

            self.ul_x = self.geo_transform[0]
            self.x_pixel_size = self.geo_transform[1]
            self.ul_y = self.geo_transform[3]
            self.y_pixel_size = self.geo_transform[5]

            self.lr_x = self.ul_x + self.x_pixel_size * self.x_size
            self.lr_y = self.ul_y + self.y_pixel_size * self.y_size

    # ................................
    def open_dataset(self, var_pattern=None, updateable=False):
        """Open (or re-open) the dataset.

        Args:
            var_pattern: String to match for the final portion of a subdataset
                name, for those datasets where the data of interest is one of
                multiple subdatasets.
            updateable: False if open in Read-Only mode, True if writeable.
        """
        try:
            if updateable:
                self._dataset = None
                self._band = None
                self._dataset = gdal.Open(
                    str(self.dlocation), gdalconst.GA_Update)
            elif self._dataset is None:
                self._dataset = gdal.Open(
                    str(self.dlocation), gdalconst.GA_ReadOnly)
        except Exception as err:
            raise LMError('Unable to open {}'.format(self.dlocation), err)
        else:
            if self._dataset is None:
                raise LMError('Unable to open {}'.format(self.dlocation))

            drv = self._dataset.GetDriver()
            self.gdal_format = drv.GetDescription()

        self.units = self._dataset.GetMetadataItem('UNITS')
        # Subdatasets for NIES IPCC AR4 netcdf data
        # Resets dlocation, _dataset, variable, units, description
        self._check_sub_datasets(var_pattern)

    # ................................
    def write_wkt_srs(self, srs):
        """Write a new SRS to the dataset

        Args:
            srs: spatial reference system in well-known-text (wkt) to write to
            the dataset.
        """
        try:
            self.open_dataset(updateable=True)
            self._dataset.SetProjection(srs)
            self._dataset.FlushCache()
            self._dataset = None
            self.srs = srs
        except Exception as err:
            raise LMError(
                'Unable to write SRS info to file', err, srs=srs,
                d_location=self.dlocation)

    # ................................
    def copy_srs(self, fname):
        """Write a new SRS, copied from another dataset, to this dataset.

        Args:
            fname: Filename for dataset from which to copy spatial reference
                system.
        """
        new_srs = GeoFileInfo.get_srs_as_wkt(fname)
        self.write_wkt_srs(new_srs)

    # ................................
    def copy_without_projection(self, out_f_name, format_='GTiff'):
        """Copy this dataset with no projection information.

        Args:
            out_f_name: Filename to write this dataset to.
            format_: GDAL-writeable raster format to use for new dataset.
                http://www.gdal.org/formats_list.html
        """
        self.open_dataset()
        driver = gdal.GetDriverByName(format_)

        out_ds = driver.Create(
            out_f_name, self.x_size, self.y_size, 1, self.gdal_band_type)
        if out_ds is None:
            print(
                'Creation failed for {} from band {} of {}'.format(
                    out_f_name, 1, self.dlocation))
            return 0

        out_ds.SetGeoTransform(self.geo_transform)
        out_band = out_ds.GetRasterBand(1)
        out_band.SetNoDataValue(self.nodata)
        out_ds.SetProjection('')
        rst_array = self._dataset.ReadAsArray()
        out_band.WriteArray(rst_array)

        # Close new dataset to flush to disk
        out_ds.FlushCache()
        out_ds = None

    # ................................
    def _check_sub_datasets(self, var_pattern):
        try:
            sub_datasets = self._dataset.GetSubDatasets()
        except Exception:
            pass
        else:
            found = False
            for sub_name, sub_desc in sub_datasets:
                if (not var_pattern and not sub_name.endswith('bounds')) or\
                        var_pattern and sub_name.endswith(var_pattern):
                    # Replace enclosing dataset values
                    self.variable = sub_name.split(':')[2]
                    self.dlocation = sub_name
                    self.description = sub_desc
                    found = True
                    break

            if found:
                # Replace enclosing dataset with subdataset
                self._dataset = None
                self._dataset = gdal.Open(
                    self.dlocation, gdalconst.GA_ReadOnly)
                # Replace units of subdataset
                self.units = self._dataset.GetMetadataItem(
                    self.variable + '#units')

    # ................................
    def _cycle_row(self, scan_line, arr_type, left, center, right):
        """Shift the values in a row

        Shift the values in a row to the right, so that the first column in the
        row is shifted to the center.  Used for data in which a row begins with
        0 degree longitude and ends with 360 degrees longitude (instead of -180
        to +180)

        Args:
            scan_line: Original row to shift.
            arr_type: Numpy datatype for scanline values
            left: Leftmost column index
            center: Center column index
            right: Rightmost column index
        """
        new_line = numpy.empty((self.x_size), dtype=arr_type)
        col_i = 0
        for col in range(center, right):
            new_line[col_i] = scan_line[col]
            col_i += 1
        for col in range(left, center):
            new_line[col_i] = scan_line[col]
            col_i += 1
        return new_line

    # ................................
    @staticmethod
    def _get_numpy_type(other_type):
        arr_type = None
        if other_type == gdalconst.GDT_Float32:
            arr_type = numpy.float32
        return arr_type

    # ................................
    def get_array(self, band_num, do_flip=False, do_shift=False):
        """Read the dataset into numpy array

        Args:
            band_num: The band number to read.
            do_flip: True if data begins at the southern edge of the region
            do_shift: True if the leftmost edge of the data should be shifted
                to the center (and right half shifted around to the beginning)
        """
        if 'numpy' in dir():
            in_ds = gdal.Open(self.dlocation, gdalconst.GA_ReadOnly)
            in_band = in_ds.GetRasterBand(band_num)
            arr_type = self._get_numpy_type(self.gdal_band_type)
            out_arr = numpy.empty((self.y_size, self.x_size), dtype=arr_type)

            for row in range(self.y_size):
                scan_line = in_band.ReadAsArray(
                    0, row, self.x_size, 1, self.x_size, 1)

                if do_shift:
                    scan_line = self._cycle_row(
                        scan_line, arr_type, 0, self.x_size / 2, self.x_size)
                if do_flip:
                    new_row = self.y_size - row - 1
                else:
                    new_row = row

                out_arr[new_row] = scan_line

            in_ds = None
            return out_arr

        raise LMError('numpy missing - unable to getArray')

    # ................................
    def copy_dataset(self, band_num, out_f_name, format_='GTiff',
                     kw_args=None):
        """Copy the dataset into a new file.

        Args:
            band_num: The band number to read.
            out_f_name: Filename to write this dataset to.
            format_: GDAL-writeable raster format to use for new dataset.
                http://www.gdal.org/formats_list.html
        """
        if kw_args is None:
            kw_args = {}
        driver = gdal.GetDriverByName(format_)
        metadata = driver.GetMetadata()
        if gdal.DCAP_CREATECOPY not in metadata.keys() and \
                metadata[gdal.DCAP_CREATECOPY] != 'YES':
            raise LMError(
                'Driver {} does not support CreateCopy() method.'.format(
                    format_))
        in_ds = gdal.Open(self.dlocation)
        try:
            out_ds = driver.CreateCopy(out_f_name, in_ds, 0, **kw_args)
        except Exception as err:
            raise LMError(
                'Creation failed for {} from band {} of {} ({})'.format(
                    out_f_name, band_num, self.dlocation, err))
        if out_ds is None:
            raise LMError(
                'Creation failed for {} from band {} of {}'.format(
                    out_f_name, band_num, self.dlocation))

        # Close new dataset to flush to disk
        out_ds = None
        in_ds = None

    # ................................
    def write_band(self, band_num, out_f_name, format_='GTiff', do_flip=False,
                   do_shift=False, nodata=None, srs=None):
        """Write the dataset into a new file, line by line.

        Args:
            band_num: The band number to read.
            out_f_name: Filename to write this dataset to.
            format_: GDAL-writeable raster format to use for new dataset.
                http://www.gdal.org/formats_list.html
            do_flip: True if data begins at the southern edge of the region
            do_shift: True if the leftmost edge of the data should be shifted
                to the center (and right half shifted around to the beginning)
            nodata: Value used to indicate nodata in the new file.
            srs: Spatial reference system to use for the data. This is only
                necessary if the dataset does not have an SRS present.  This
                will NOT project the dataset into a different projection.
        """
        driver = gdal.GetDriverByName(format_)
        metadata = driver.GetMetadata()
        if gdal.DCAP_CREATE not in metadata and \
                metadata[gdal.DCAP_CREATE] != 'YES':
            raise LMError(
                'Driver {} does not support Create() method'.format(format_))

        out_ds = driver.Create(
            out_f_name, self.x_size, self.y_size, 1, self.gdal_band_type)
        if out_ds is None:
            raise LMError(
                'Creation failed for {} from band {} of {}'.format(
                    out_f_name, band_num, self.dlocation))

        out_ds.SetGeoTransform(self.geo_transform)

        in_ds = gdal.Open(self.dlocation, gdalconst.GA_ReadOnly)
        in_band = in_ds.GetRasterBand(band_num)
        out_band = out_ds.GetRasterBand(1)

        if nodata is None:
            nodata = in_band.GetNoDataValue()
        if nodata is not None:
            out_band.SetNoDataValue(nodata)
        if srs is None:
            srs = self.srs
        out_ds.SetProjection(srs)

        for row in range(self.y_size):
            scan_line = in_band.ReadAsArray(
                0, row, self.x_size, 1, self.x_size, 1)

            if do_shift:
                arr_type = self._get_numpy_type(self.gdal_band_type)
                scan_line = self._cycle_row(
                    scan_line, arr_type, 0, self.x_size / 2, self.x_size)

            if do_flip:
                out_band.WriteArray(scan_line, 0, self.y_size - row - 1)
            else:
                out_band.WriteArray(scan_line, 0, row)
        # Close new dataset to flush to disk
        out_ds = None
        in_ds = None

    # ................................
    def __unicode__(self):
        return '%s (%s)' % (self.name, self.dlocation)

    # ................................
    def load_band(self, band_num=1):
        """Open the dataset and save the band to a member attribute

        Args:
            band_num: The band number to read.
        """
        if self._band is None:
            if self._dataset is None:
                self.open_dataset(None)
            self._band = self._dataset.GetRasterBand(band_num)

    # ................................
    def _get_stats(self, band_num=1):
        if not all(self._min, self._max, self._mean, self._std_dev):
            self.load_band(band_num=band_num)
            try:
                min_, max_, mean, stddev = self._band.GetStatistics(
                    False, True)
            except Exception as err:
                print(
                    ('Exception in _get_stats: band.GetStatistics {}'.format(
                        err)))
                min_, max_, mean, stddev = None, None, None, None
            self._min = min_
            self._max = max_
            self._mean = mean
            self._std_dev = stddev
        return min, max, mean, stddev

    # ................................
    def get_histogram(self, band_num=1):
        """Get the data values histogram, for coloring in a map.

        Args:
            band_num: The band number to read.

        Returns:
            A list of data values present in the dataset

        Note:
            - This returns only a list, not a true histogram.
            - This only works on 8-bit data.
        """
        vals = []
        # Get histogram only for 8bit data (projections)
        if self.gdal_band_type == gdalconst.GDT_Byte:
            self.load_band(band_num)
            hist = self._band.GetHistogram()
            for i, hist_val in enumerate(hist):
                if i > 0 and i != self.nodata and hist_val > 0:
                    vals.append(i)
        else:
            print('Histogram calculated only for 8-bit data')
        return vals

    # ................................
    def _get_min(self):
        if self._min is None:
            self._get_stats()
        return self._min

    min = property(_get_min)

    # ................................
    def _get_max(self):
        if self._max is None:
            self._get_stats()
        return self._max

    max = property(_get_max)

    # ................................
    def _get_mean(self):
        if self._mean is None:
            self._get_stats()
        return self._mean

    mean = property(_get_mean)

    # ................................
    def _get_std_dev(self):
        if self._std_dev is None:
            self._get_stats()
        return self._std_dev

    std_dev = property(_get_std_dev)

    # ................................
    def point_inside(self, point):
        """Returns boolean indicating if point is within extent.

        Args:
            point: A tuple representing a point.
        """
        ext = self.get_extents()
        return ext[0] <= point[0] <= ext[2] and ext[1] <= point[1] <= ext[3]

    # ................................
    def get_z_values(self, points, missing_v=DEFAULT_NODATA):
        """Return z values for (x,y) pairs

        Args:
            points: A list of (x, y) points
            missing_v: Value to return if the point is at a nodata cell
        """
        # ................................
        def point_sort_func(pt_1, pt_2):
            """Used to sort in increasing y value
            """
            if pt_1[1] < pt_2[1]:
                return -1
            if pt_1[1] > pt_2[1]:
                return 1
            return 0

        self.load_band()
        if len(points) > 1:
            points.sort(point_sort_func)
        res = []
        for point in points:
            r_point = [point[0], point[1]]
            if self.point_inside(r_point):
                c_xy = geo_coord_to_pixel_coord(r_point, self.geo_transform)
                if c_xy[1] != self._c_scan_line:
                    self._c_scan_line = c_xy[1]
                    try:
                        self._scan_line = self._band.ReadAsArray(
                            0, self._c_scan_line, self.x_size, 1)
                    except Exception:
                        # could not create buffer
                        pass
                try:
                    z_val = self._scan_line[0, c_xy[0]]
                except Exception:
                    z_val = self.nodata
                if z_val == self.nodata:
                    r_point.append(missing_v)
                else:
                    # return the real value, this changes from float
                    # newz = z * self.scalef
                    r_point.append(z_val)
            else:
                r_point.append(missing_v)
            res.append(r_point)
        return res

    # ................................
    def get_bounds(self):
        """Return list of bounds for the layer (min_x, min_y, max_x, max_y)
        """
        return [self.ul_x, self.lr_y, self.lr_x, self.ul_y]

    # ................................
    @staticmethod
    def raster_size(data_src):
        """Return [width, height] in pixels
        """
        open_ds = gdal.Open(str(data_src))
        return [open_ds.RasterXSize, open_ds.RasterYSize]

    # ................................
    @staticmethod
    def get_srs_as_wkt(filename):
        """Reads spatial reference system information from provided file

        Args:
            filename: The raster file from which to read srs information.

        Returns:
            Projection information from GDAL

        Raises:
            LMError: on failure to open dataset
        """
        if (filename is not None and os.path.exists(filename)):
            geo_fi = GeoFileInfo(filename)
            srs = geo_fi.srs
            geo_fi = None
            return srs

        raise LMError('Unable to read file {}'.format(filename))


# .............................................................................
DEFAULT_PROJ = (
    'GEOGCS["WGS84", DATUM["WGS84", SPHEROID["WGS84", 6378137.0, '
    '298.257223563]], PRIMEM["Greenwich", 0.0], UNIT["degree", '
    '0.017453292519943295], AXIS["Longitude",EAST], AXIS["Latitude",NORTH]]')


# ...............................................
def geo_coord_to_pixel_coord(geo_xy, geo_transform):
    """Convert geographic coordinates to pixel coordinats.

    Given a geographic coordinate (in the form of a two element, one
    dimensional array, [0] = x, [1] = y), and an affine transform, this
    function returns the inverse of the transform, that is, the pixel
    coordinates corresponding to the geographic coordinates.

    Args:
        geo_xy: Sequence of two elements (x, y)
        geo_transform: The affine transformationa ssociated with a dataset

    Return:
        List of (x, y) or None if the transform is invalid
    """
    g_x, g_y = geo_xy

    # Determinant of affine transformation
    det = (geo_transform[1] * geo_transform[5]) - (
        geo_transform[4] * geo_transform[2])

    # If the transformation is not invertable return None
    if det == 0.0:
        return None

    transform_1 = (g_x * geo_transform[5]) - (
        geo_transform[0] * geo_transform[5]) - (geo_transform[2] * g_y) + (
            geo_transform[2] * geo_transform[3])

    transform_2 = (g_y * geo_transform[1]) - (
        geo_transform[1] * geo_transform[3]) - (g_x * geo_transform[4]) + (
            geo_transform[4] * geo_transform[0])
    return (int(transform_1 // det), int(transform_2 // det))

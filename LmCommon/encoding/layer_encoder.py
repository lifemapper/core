"""This module contains a class for encoding spatial layers into a Matrix

The 'LayerEncoder' class uses a shapegrid to generate a base matrix structure
and then each layer is encoded as a new column (or columns) for the resulting
encoded matrix.

Todo:
    Consider if we want to support hexagonal cells, if so, we will need to
        mask the resulting data windows and possibly change the minimum
        coverage calculation.

Note:
    Data array is oriented at top left (min x, max y)
"""
import json
import os
from random import shuffle

import numpy as np
from osgeo import gdal, ogr

from lmpy import Matrix

from LmCommon.common.lmconstants import DEFAULT_NODATA, LMFormat

# DEFAULT_SCALE is the scale of the layer data array to the shapegrid cellsize
#     The number of data array cells in a (square) shapegrid cell is::
#         1.0 / DEFAULT_SCALE^2
DEFAULT_SCALE = 0.1  # Default scale for data array versus shapegrid cells

# .............................................................................
# Encoding methods


# .............................................................................
def _get_presence_absence_method(min_presence, max_presence, min_coverage,
                                 nodata):
    """Gets the function for determining presence for a data window

    Args:
        min_presence: Data cells must have a value greater than or equal to
            this to be considered present
        max_presence: Data cells must have a value less than or equal to this
            to be considered present
        min_coverage: At least the percentage of the window must be classified
            as present to consider the window present
        nodata: This values should be considered nodata
    """
    if min_coverage > 1.0:
        min_coverage = min_coverage / 100.0

    # ...............................
    def get_presence_absence(window):
        min_num = max(min_coverage * window.size, 1)
        valid_cells = np.logical_and(
            np.logical_and(window >= min_presence, window <= max_presence),
            window != nodata)
        return np.sum(valid_cells) >= min_num

    return get_presence_absence


# .............................................................................
def _get_mean_value_method(nodata):
    """Gets the function to use for determining the mean value of a data window

    Args:
        nodata: This value is assumed to be nodata in the array
    """

    # ...............................
    def get_mean(window):
        window_mean = np.nanmean(window)
        if np.isnan(window_mean):
            return nodata

        return window_mean

    return get_mean


# .............................................................................
def _get_largest_class_method(min_coverage, nodata):
    """Gets the function to use for determining the largest class

    Args:
        min_coverage: The minimum percentage of the data window that must be
            covered by the largest class.
        nodata: This value is assumed to be nodata in the array
    """
    if min_coverage > 1.0:
        min_coverage = min_coverage / 100.0

    # ...............................
    def get_largest_class(window):
        """Get largest class for numpy > 1.8
        """
        min_num = min_coverage * window.size
        largest_count = 0
        largest_class = nodata
        unique_values = np.column_stack(np.unique(window, return_counts=True))
        for class_, num in unique_values:
            if not np.isclose(class_, nodata) and num > largest_count \
                    and num > min_num:
                largest_class = class_
        return largest_class

    # ...............................
    def get_largest_class_1_8(window):
        """Get largest class for numpy 1.8
        """
        min_num = min_coverage * window.size
        largest_count = 0
        largest_class = nodata
        unique_values = np.unique(window)
        for class_ in unique_values:
            num = np.where(window == class_)[0].size
            if not np.isclose(class_, nodata) and num > largest_count \
                    and num > min_num:
                largest_class = class_
        return largest_class

    return get_largest_class_1_8


# .............................................................................
def _get_encode_hypothesis_method(hypothesis_values, min_coverage, nodata):
    """Gets the function to determine the hypothesis value for each data window

    Args:
        hypothesis_values: A list of possible hypothesis values to look for.
            Each item in the list will be treated as its own column.
        min_coverage: The minimum percentage of each data window that must be
            covered by the returned hypothesis value.
        nodata: This value is assumed to be nodata
    """
    # Build the map
    val_map = {}
    i = 0
    for val in hypothesis_values:
        contrast_values = [-1, 1]
        shuffle(contrast_values)
        try:
            # Pair of values
            val_map[val[0]] = {
                'val': contrast_values[0],
                'index': i
            }
            val_map[val[1]] = {
                'val': contrast_values[1],
                'index': i
            }
        except Exception:
            # Single value
            val_map[val] = {
                'val': contrast_values[0],
                'index': i
            }
        i += 1
        # Note: 'i' is the number of values, we'll use that later

    if min_coverage > 1.0:
        min_coverage = min_coverage / 100.0

    # ...............................
    def encode_method(window):
        """Encode method for numpy > 1.8
        """
        min_vals = int(min_coverage * window.size)
        # Set default min count to min_vals
        # Note: This will cause last one to win if they are equal, change to
        #     '>' below and set this to * (min_vals - 1) to have first one win
        counts = np.zeros((i), dtype=int) * min_vals
        ret = np.zeros((i))

        unique_values = np.column_stack(np.unique(window, return_counts=True))

        # Check unique values in window
        for val, num in unique_values:
            if not np.isclose(val, nodata) and val in list(val_map.keys()) and\
                    num >= counts[val_map[val]['index']]:
                counts[val_map[val]['index']] = num
                ret[val_map[val]['index']] = val_map[val]['val']
        return ret

    # ...............................
    def encode_method_1_8(window):
        """Encode method for Numpy 1.8
        """
        min_vals = int(min_coverage * window.size)
        # Set default min count to min_vals
        # Note: This will cause last one to win if they are equal, change to
        #     '>' below and set this to * (min_vals - 1) to have first one win
        counts = np.zeros((i), dtype=int) * min_vals
        ret = np.zeros((i))

        unique_values = np.unique(window)

        # Check unique values in window
        for val in unique_values:
            num = np.where(window == val)[0].size
            if not np.isclose(val, nodata) and val in list(val_map.keys()) and\
                    num >= counts[val_map[val]['index']]:
                counts[val_map[val]['index']] = num
                ret[val_map[val]['index']] = val_map[val]['val']
        return ret

    return encode_method_1_8


# .............................................................................
class LayerEncoder:
    """The LayerEncoder class encodes layers into matrix columns

    Attributes:
        encoded_matrix: A Matrix object with encoded layers
    """

    # ...............................
    def __init__(self, shapegrid_filename):
        # Process shapegrid
        self.shapegrid_filename = shapegrid_filename
        self._read_shapegrid(shapegrid_filename)

        self.encoded_matrix = None

    # ...............................
    def _encode_layer(self, window_func, encode_func, column_name,
                      num_columns=1):
        """Encodes the layer using the provided encoding function

        Args:
            window_func: A function that returns a window of array data for a
                provided x, y pair.
            encode_func: A function that encodes a window of array data.
            column_name: The header name to use for the column in the encoded
                matrix.
            num_columns: The number of columns that will be encoded by
                'encode_func'.  This can be non-zero if we are testing for
                multiple biogeographic hypotheses in a single vector layer for
                example.

        Returns:
            A list of column headers for the newly encoded columns
        """
        shapegrid_dataset = ogr.Open(self.shapegrid_filename)
        shapegrid_layer = shapegrid_dataset.GetLayer()

        encoded_column = np.zeros((self.num_cells, num_columns))
        if num_columns == 1:
            column_headers = [column_name]
        else:
            column_headers = [
                '{}-{}'.format(column_name, val) for val in range(num_columns)]

        row_headers = []

        i = 0

        feat = shapegrid_layer.GetNextFeature()
        while feat is not None:
            geom = feat.GetGeometryRef()
            cent = geom.Centroid()
            x_coord = cent.GetX()
            y_coord = cent.GetY()
            val = encode_func(window_func(x_coord, y_coord))
            encoded_column[i] = val
            row_headers.append((feat.GetFID(), x_coord, y_coord))
            i += 1
            feat = shapegrid_layer.GetNextFeature()

        col = Matrix(encoded_column,
                     headers={'0': row_headers,
                              '1': column_headers})

        if self.encoded_matrix is None:
            self.encoded_matrix = col
        else:
            self.encoded_matrix = Matrix.concatenate(
                [self.encoded_matrix, col], axis=1)
        # Return column headers for added columns
        return column_headers

    # ...............................
    @staticmethod
    def _get_window_function(data, layer_bbox, cell_size, num_cell_sides=4):
        """Gets a windowing function for the data.

        This function generates a function that will return a "window" of array
        data for a given (x, y) pair.

        Args:
            data: A numpy array with data for a layer
            layer_bbox: The bounding box of the layer in the map units of the
                layer.
            cell_size: Either a single value or a tuple with two values. If it
                is a single value, it will be used for both x and y cell sizes.
                If a tuple is provided, the first value will be used for the
                size of each cell in the x dimension and the second will be
                used for the size of the cell in the y dimension.
            num_cell_sides: The number of sides each shapegrid cell has::
                4 -- square
                6 -- hexagon

        Note:
            The origin (0, 0) of the data array should represent (min x, max y)
                for the layer.

        TODO(CJ): Enable hexagonal windows by masking data
        """
        # Compute bounds here to save compute time
        y_size, x_size = data.shape
        min_x, min_y, max_x, max_y = layer_bbox

        # x_res = float(max_x - min_x) / x_size

        y_range = max_y - min_y
        x_range = max_x - min_x

        try:
            # Tuple or list
            x_size_2 = cell_size[0] / 2.0
            y_size_2 = cell_size[1] / 2.0
        except TypeError:
            # Single value
            x_size_2 = y_size_2 = cell_size / 2.0

        # ...............................
        def get_rc(x_coord, y_coord):
            x_prop = (1.0 * x_coord - min_x) / x_range
            y_prop = (1.0 * y_coord - min_y) / y_range

            col = int(x_size * x_prop)
            row = y_size - int(y_size * y_prop)
            return row, col

        # ...............................
        def window_function(x_coord, y_coord):
            """Get the array window from the centroid coordinates"""
            # Note: Again, 0 row corresponds to top of map, so bigger y
            #     corresponds to lower row number
            # Upper left coorner
            uly, ulx = get_rc(x_coord - x_size_2, y_coord + y_size_2)
            # Lower right corner
            lry, lrx = get_rc(x_coord + x_size_2, y_coord - y_size_2)

            return data[max(0, uly):min(y_size, lry),
                        max(0, ulx):min(x_size, lrx)]

        return window_function

    # ...............................
    def _read_layer(self, layer_filename, resolution=None, bbox=None,
                    nodata=DEFAULT_NODATA, event_field=None):
        """Reads a layer for processing

        Args:
            layer_filename: The file path for the layer to read.
            resolution: An optional resolution to use for the input data if it
                is a vector layer.
            bbox: An optional bounding box in the form
                (min x, min y, max x, max y) to use if the layer is a vector
                layer.
            nodata: An optional nodata value to use if the layer is a vector
                layer.
            event_field: If provided, use this field as the burn value for a
                vector layer.

        Returns:
            A tuple containing a window function for returning a portion of the
            numpy array generated by the layer and the NODATA value to use with
            this layer.
        """
        # Get the file extension for the layer file name
        ext = os.path.splitext(layer_filename)[1]

        if ext == LMFormat.SHAPE.ext:
            window_func, nodata_value, events = self._read_vector_layer(
                layer_filename, resolution=resolution, bbox=bbox,
                nodata=nodata, event_field=event_field)
        else:
            window_func, nodata_value = self._read_raster_layer(layer_filename)
            events = set([])

        return (window_func, nodata_value, events)

    # ...............................
    def _read_raster_layer(self, raster_filename):
        """Reads a raster layer for processing

        Args:
            raster_filename: The file path for the raster layer.

        Returns:
            A tuple containing a window function for returning a portion of the
            numpy array generated by the layer and the NODATA value to use with
            this layer.
        """
        dataset = gdal.Open(raster_filename)
        band = dataset.GetRasterBand(1)
        layer_array = band.ReadAsArray()
        nodata = band.GetNoDataValue()

        num_y, num_x = layer_array.shape
        min_x, x_res, _, max_y, _, y_res = dataset.GetGeoTransform()
        max_x = min_x + (num_x * x_res)
        min_y = max_y + (y_res * num_y)
        layer_bbox = (min_x, min_y, max_x, max_y)
        window_func = self._get_window_function(
            layer_array, layer_bbox, self.shapegrid_resolution,
            num_cell_sides=self.shapegrid_sides)

        return (window_func, nodata)

    # ...............................
    def _read_shapegrid(self, shapegrid_filename):
        """Read the shapegrid

        Args:
            shapegrid_filename: The file location of the shapegrid
        """
        shapegrid_dataset = ogr.Open(shapegrid_filename)
        self.shapegrid_layer = shapegrid_dataset.GetLayer()
        tmp = self.shapegrid_layer.GetExtent()
        self.shapegrid_bbox = (tmp[0], tmp[2], tmp[1], tmp[3])

        self.num_cells = self.shapegrid_layer.GetFeatureCount()

        feature_0 = self.shapegrid_layer.GetFeature(0)
        geom = feature_0.GetGeometryRef()

        # Get resolution and number of sides
        geom_wkt = geom.ExportToWkt()
        boundary_points = geom_wkt.split(',')
        if len(boundary_points) == 5:
            # Square
            envelope = geom.GetEnvelope()
            self.shapegrid_resolution = (envelope[1] - envelope[0],
                                         envelope[3] - envelope[2])
        else:
            # Hexagon
            center = geom.Centroid()
            x_cent = center.GetX()
            y_cent = center.GetY()
            x_1, y_1 = boundary_points[1].split(' ')
            self.shapegrid_resolution = np.sqrt(
                (x_cent - x_1) ** 2 + (y_cent - y_1) ** 2)
        self.shapegrid_sides = len(boundary_points) - 1
        # self.shapegrid_layer.ResetReading()
        self.shapegrid_layer = None

    # ...............................
    def _read_vector_layer(self, vector_filename, resolution=None, bbox=None,
                           nodata=DEFAULT_NODATA, event_field=None):
        """Reads a vector layer for processing

        Args:
            vector_filename: The vectorfile path for the layer to read.
            resolution: An optional resolution to use for the generated data
                array for the layer.
            bbox: An optional bounding box in the form
                (min x, min y, max x, max y) to use for the vector layer.  Will
                use the shapegrid bounding box if not provided.
            nodata: An optional nodata value to use if the layer is a vector
                layer.
            event_field: An optional shapefile attribute to use as the burn
                value for each cell.  This should be numeric.

        Returns:
            A tuple containing a window function for returning a portion of the
            numpy array generated by the layer, the NODATA value to use with
            this layer, and a set of distinct events to be used for processing.
        """
        options = ['ALL_TOUCHED=TRUE']
        if event_field is not None:
            options.append('ATTRIBUTE={}'.format(event_field))

        if resolution is None:
            resolution = [DEFAULT_SCALE * i for i in self.shapegrid_resolution]
        try:
            # Tuple or list
            x_res = resolution[0]
            y_res = resolution[1]
        except TypeError:
            # Single value
            x_res = y_res = resolution

        if bbox is None:
            bbox = self.shapegrid_bbox

        min_x, min_y, max_x, max_y = bbox
        x_size = int(float(max_x - min_x) / x_res)
        y_size = int(float(max_y - min_y) / y_res)

        vector_ds = ogr.Open(vector_filename)
        vector_layer = vector_ds.GetLayer()

        raster_drv = gdal.GetDriverByName('MEM')
        raster_ds = raster_drv.Create(
            'temp', x_size, y_size, 1, gdal.GDT_Float32)
        raster_ds.SetGeoTransform((min_x, x_res, 0, max_y, 0, -1.0 * y_res))
        band = raster_ds.GetRasterBand(1)
        band.SetNoDataValue(nodata)
        band.FlushCache()
        init_ary = np.empty((y_size, x_size))
        init_ary.fill(nodata)
        band.WriteArray(init_ary)
        gdal.RasterizeLayer(raster_ds, [1], vector_layer, options=options)

        layer_array = raster_ds.ReadAsArray()
        raster_ds = None

        layer_bbox = (min_x, min_y, max_x, max_y)
        window_func = self._get_window_function(
            layer_array, layer_bbox, self.shapegrid_resolution,
            num_cell_sides=self.shapegrid_sides)

        distinct_events = list(np.unique(layer_array))
        try:
            # Go through list backwards to safely pop if needed
            for i in range(len(distinct_events) - 1, -1, -1):
                if np.isclose(distinct_events[i], nodata):
                    distinct_events.pop(i)
        except TypeError:
            # This happens if only one value
            if np.isclose(distinct_events, nodata):
                distinct_events = []
            else:
                distinct_events = [distinct_events]
        return (window_func, nodata, distinct_events)

    # ...............................
    def encode_biogeographic_hypothesis(self, layer_filename, column_name,
                                        min_coverage, resolution=None,
                                        bbox=None, nodata=DEFAULT_NODATA,
                                        event_field=None):
        """Encodes a biogeographic hypothesis layer

        Encodes a biogeographic hypothesis layer by creating a Helmert contrast
        column in the encoded matrix.

        Args:
            layer_filename: The file location of the layer to encode.
            column_name: What to name this column in the encoded matrix.
            min_coverage: The minimum percentage of each data window that must
                be covered.
            resolution: If the layer is a vector, optionally use this as the
                resolution of the data grid.
            bbox: If the layer is a vector, optionally use this bounding box
                for the data grid.
            nodata: If the layer is a vector, optionally use this as the data
                grid nodata value.
            event_field: If the layer is a vector and contains multiple
                hypotheses, use this field to separate the vector file.

        Returns:
            A list of column headers for the newly encoded columns
        """
        window_func, nodata, distinct_events = self._read_layer(
            layer_filename, resolution=resolution, bbox=bbox, nodata=nodata,
            event_field=event_field)
        if len(distinct_events) == 2:
            # Set the events to be opposite sides of same hypothesis
            distinct_events = [tuple(distinct_events)]
        encode_func = _get_encode_hypothesis_method(
            distinct_events, min_coverage, nodata)
        return self._encode_layer(
            window_func, encode_func, column_name,
            num_columns=len(distinct_events))

    # ...............................
    def encode_presence_absence(self, layer_filename, column_name,
                                min_presence, max_presence, min_coverage,
                                resolution=None, bbox=None,
                                nodata=DEFAULT_NODATA, attribute_name=None):
        """Encodes a distribution layer into a presence absence column

        Args:
            layer_filename: The file location of the layer to encode.
            column_name: What to name this column in the encoded matrix.
            min_presence: The minimum value that should be treated as presence.
            max_presence: The maximum value to be considered as present.
            min_coverage: The minimum percentage of each data window that must
                be present to consider that cell present.
            resolution: If the layer is a vector, optionally use this as the
                resolution of the data grid.
            bbox: If the layer is a vector, optionally use this bounding box
                for the data grid.
            nodata: If the layer is a vector, optionally use this as the data
                grid nodata value.
            attribute_name: If the layer is a vector, use this field to
                determine presence.

        Returns:
            A list of column headers for the newly encoded columns
        """
        window_func, nodata, _ = self._read_layer(
            layer_filename, resolution=resolution, bbox=bbox, nodata=nodata,
            event_field=attribute_name)
        encode_func = _get_presence_absence_method(
            min_presence, max_presence, min_coverage, nodata)
        return self._encode_layer(window_func, encode_func, column_name)

    # ...............................
    def encode_mean_value(self, layer_filename, column_name, resolution=None,
                          bbox=None, nodata=DEFAULT_NODATA,
                          attribute_name=None):
        """Encodes a layer based on the mean value for each data window.

        Args:
            layer_filename: The file location of the layer to encode.
            column_name: What to name this column in the encoded matrix.
            resolution: If the layer is a vector, optionally use this as the
                resolution of the data grid.
            bbox: If the layer is a vector, optionally use this bounding box
                for the data grid.
            nodata: If the layer is a vector, optionally use this as the data
                grid nodata value.
            attribute_name: If the layer is a vector, use this field to
                determine value.

        Returns:
            A list of column headers for the newly encoded columns
        """
        window_func, nodata, _ = self._read_layer(
            layer_filename, resolution=resolution, bbox=bbox, nodata=nodata,
            event_field=attribute_name)
        encode_func = _get_mean_value_method(nodata)
        return self._encode_layer(window_func, encode_func, column_name)

    # ...............................
    def encode_largest_class(self, layer_filename, column_name, min_coverage,
                             resolution=None, bbox=None, nodata=DEFAULT_NODATA,
                             attribute_name=None):
        """Encodes a layer based on the largest class in each data window.

        Args:
            layer_filename: The file location of the layer to encode.
            column_name: What to name this column in the encoded matrix.
            min_coverage: The minimum percentage of each data window that must
                be the covered by the largest class.
            resolution: If the layer is a vector, optionally use this as the
                resolution of the data grid.
            bbox: If the layer is a vector, optionally use this bounding box
                for the data grid.
            nodata: If the layer is a vector, optionally use this as the data
                grid nodata value.
            attribute_name: If the layer is a vector, use this field to
                determine largest class.

        Returns:
            A list of column headers for the newly encoded columns
        """
        window_func, nodata, _ = self._read_layer(
            layer_filename, resolution=resolution, bbox=bbox, nodata=nodata,
            event_field=attribute_name)
        encode_func = _get_largest_class_method(min_coverage, nodata)
        return self._encode_layer(window_func, encode_func, column_name)

    # ...............................
    def get_encoded_matrix(self):
        """Returns the encoded matrix

        Returns:
            The encoded matrix as a Matrix object
        """
        return self.encoded_matrix

    # ...............................
    def get_geojson(self):
        """Formats the encoded matrix as GeoJSON
        """
        ret = {
            'type': 'FeatureCollection'
        }
        features = []

        column_headers = self.encoded_matrix.get_column_headers()

        column_enum = [(j, str(k)) for j, k in enumerate(column_headers)]

        shapegrid_dataset = ogr.Open(self.shapegrid_filename)
        shapegrid_layer = shapegrid_dataset.GetLayer()

        i = 0
        feat = shapegrid_layer.GetNextFeature()
        while feat is not None:
            ft_json = json.loads(feat.ExportToJson())
            # right_hand_rule(ft_json['geometry']['coordinates'])
            # TODO(CJ): Remove this if updated library adds first id correctly
            ft_json['id'] = feat.GetFID()
            ft_json['properties'] = {
                k: self.encoded_matrix[i, j].item() for j, k in column_enum}
            features.append(ft_json)
            i += 1
            feat = shapegrid_layer.GetNextFeature()

        ret['features'] = features
        shapegrid_dataset = shapegrid_layer = None
        return ret

"""Extract environmental values
"""
import argparse
import json

import numpy as np
from osgeo import gdal, ogr

from lmpy import Matrix


# .............................................................................
def is_close(a_val, b_val, rel_tol=1e-09, abs_tol=0.0):
    """Test if two values are almost equal
    """
    return abs(a_val - b_val) <= max(
        rel_tol * max(abs(a_val), abs(b_val)), abs_tol)


# .............................................................................
def get_layer_info(layer_json_file):
    """Get layer information to use for metrics from the JSON file
    """
    with open(layer_json_file) as in_file:
        raw_layers = json.load(in_file)

    layers = []
    for lyr in raw_layers:
        layers.append((lyr['identifier'], lyr['dlocation']))
    return layers


# .............................................................................
def get_metrics(points_filename, layer_info, identifier,
                remove_duplicate_locations=True):
    """Get metrics for the data
    """
    metrics = [
        ('minimum', np.min), ('maximum', np.max), ('mean', np.mean)]
    metric_functions = [f for _, f in metrics]

    # Add an extra dimension so we can attach a header for this set and then
    #     stack this matrix with others
    metrics_data = np.zeros((len(metrics), len(layer_info), 1), dtype=np.float)

    # Get the points
    points = get_point_xys(
        points_filename, remove_duplicate_locations=remove_duplicate_locations)

    # Get matrics for layers
    for i, info in enumerate(layer_info):
        # Get metrics for a layer
        lyr_metrics = get_metrics_for_layer(
            points, info[1], metric_functions)

        # Set values in matrix
        for j, metric in enumerate(lyr_metrics):
            metrics_data[j, i, 0] = metric

    return Matrix(
        metrics_data,
        headers={
            '0': [h for h, _ in metrics],
            '1': [lyr[0] for lyr in layer_info],
            '2': [identifier]
        })


# .............................................................................
def get_metrics_for_layer(points, layer_filename, metric_functions):
    """Get layer values for each point and then generate metrics
    """
    dataset = gdal.Open(layer_filename)
    band = dataset.GetRasterBand(1)
    data = np.array(band.ReadAsArray())
    geo_transform = dataset.GetGeoTransform()
    nodata_val = band.GetNoDataValue()

    values = []

    for x_coord, y_coord in points:
        pix_x = int((x_coord - geo_transform[0]) / geo_transform[1])
        pix_y = int((y_coord - geo_transform[3]) / geo_transform[5])
        try:
            val = data[pix_y, pix_x]
            if not is_close(val, nodata_val):
                values.append(data[pix_y, pix_x])
            else:
                print(
                    'Could not append value at ({}, {}): {}'.format(
                        pix_x, pix_y, val))
        except Exception as e:
            print(
                'Could not append value at ({}, {}): {}'.format(
                    pix_x, pix_y, str(e)))

    arr = np.array(values)

    return [func(arr) for func in metric_functions]


# .............................................................................
def get_point_xys(points_filename, remove_duplicate_locations=True):
    """Get x,y pairs for each point in a shapefile
    """
    points = []

    dataset = ogr.Open(points_filename)
    lyr = dataset.GetLayer()

    for feat in lyr:
        geom = feat.GetGeometryRef()
        points.append((geom.GetX(), geom.GetY()))

    if remove_duplicate_locations:
        points = list(set(points))

    return points


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(
        description=(
            'This script extracts environmental data metrics for points'))

    parser.add_argument(
        'points_file', type=str,
        help='The file location of the points shapefile')
    parser.add_argument(
        'points_name', type=str,
        help='A name (such as the squid) to be associated with these points')
    parser.add_argument(
        'layer_json_file', type=str,
        help='JSON file containing layer information')
    parser.add_argument(
        'output_file', type=str,
        help='File location to write the output matrix')
    # TODO: Add parameters for metrics to collect
    args = parser.parse_args()

    layer_info = get_layer_info(args.layer_json_file)

    metrics = get_metrics(args.points_file, layer_info, args.points_name)

    # Write outputs
    metrics.write(args.output_file)


# .............................................................................
if __name__ == '__main__':
    main()

"""Module containing methods to build a shapegrid
"""
import math

import numpy as np
from osgeo import ogr, osr

from LmCommon.common.lmconstants import LMFormat

# Calculate this once and store as a constant instead of for every cell
SQRT_3 = math.sqrt(3)


# .............................................................................
def make_polygon_wkt_from_points(points):
    """Create a polygon WKT string from a list of points.

    Args:
        points (list of (x, y) tuples): A list of (x,y) points for each vertex
            of the polygon.

    Note:
        Points should be ordered following right hand rule
    """
    points.append(points[0])
    point_strings = ['{} {}'.format(x, y) for x, y in points]
    return 'POLYGON(({}))'.format(','.join(point_strings))


# .............................................................................
def hexagon_wkt_generator(min_x, min_y, max_x, max_y, x_res, y_res):
    """Generator producing hexagonal WKT for cells of the shapegrid

    Args:
        min_x (numeric): The minimum x value
        min_y (numeric): The minimum y value
        max_x (numeric): The maximum x value
        max_y (numeric): The maximum y value
        x_res (numeric): The x size of the cell
        y_res (numeric): The y size of the cell
    """
    y_coord = max_y
    y_row = True
    y_step = -1 * y_res * SQRT_3 / 4
    x_step = x_res * 1.5

    while y_coord > min_y:
        # Every other row needs to be shifted slightly
        x_coord = min_x if y_row else min_x + (x_res * .75)

        while x_coord < max_x:
            yield make_polygon_wkt_from_points([
                (x_coord - (x_res * .25), y_coord + (y_res * .25) * SQRT_3),
                (x_coord + (x_res * .25), y_coord + (y_res * .25) * SQRT_3),
                (x_coord + (x_res * .25), y_coord),
                (x_coord + (x_res * .25), y_coord - (y_res * .25) * SQRT_3),
                (x_coord - (x_res * .25), y_coord - (y_res * .25) * SQRT_3),
                (x_coord - (x_res * .5), y_coord)])
            x_coord += x_step
        y_row = not y_row
        y_coord += y_step


# .............................................................................
def square_wkt_generator(min_x, min_y, max_x, max_y, x_res, y_res):
    """Generator producing square WKT for cells of the shapegrid

    Args:
        min_x (numeric): The minimum x value
        min_y (numeric): The minimum y value
        max_x (numeric): The maximum x value
        max_y (numeric): The maximum y value
        x_res (numeric): The x size of the cell
        y_res (numeric): The y size of the cell
    """
    for y_coord in np.arange(max_y, min_y, -y_res):
        for x_coord in np.arange(min_x, max_x, x_res):
            yield make_polygon_wkt_from_points([
                (x_coord, y_coord),
                (x_coord + x_res, y_coord),
                (x_coord + x_res, y_coord - y_res),
                (x_coord, y_coord - y_res)])


# .............................................................................
def build_shapegrid(shapegrid_file_name, min_x, min_y, max_x, max_y, cell_size,
                    epsg_code, cell_sides, site_id='siteid', site_x='siteX',
                    site_y='siteY', cutout_wkt=None):
    """Build a shapegrid with an optional cutout

    Args:
        shapegrid_file_name (str): The location to store the resulting
            shapegrid.
        min_x (numeric): The minimum value for X of the shapegrid.
        min_y (numeric): The minimum value for Y of the shapegrid.
        max_x (numeric): The maximum value for X of the shapegrid.
        max_y (numeric): The maximum value for Y of the shapegrid.
        cell_size (numeric): The size of each cell (in units indicated by EPSG)
        epsg_code (int): The EPSG code for the new shapegrid.
        cell_sides (int): The number of sides for each cell of the shapegrid.
            4 - square cells, 6 - hexagon cells
        site_id (str): The name of the site id field for the shapefile.
        site_x (str): The name of the X field for the shapefile.
        site_y (str): The name of the Y field for the shapefile.
        cutout_wkt (None or str): WKT for an area of the shapegrid to be cut
            out.

    Returns:
        int - The number of cells in the new shapegrid
    """
    # We'll always check for intersection to reduce amount of work
    if cutout_wkt is None:
        cutout_wkt = make_polygon_wkt_from_points([
            (min_x, max_y), (max_x, max_y), (max_x, min_y), (min_x, min_y)])
    selected_poly = ogr.CreateGeometryFromWkt(cutout_wkt)

    # Just in case we decide that these can vary at some point
    x_res = y_res = cell_size

    # Initialize shapefile
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(epsg_code)

    drv = ogr.GetDriverByName(LMFormat.get_default_ogr().driver)
    data_set = drv.CreateDataSource(shapegrid_file_name)

    layer = data_set.CreateLayer(
        data_set.GetName(), geom_type=ogr.wkbPolygon, srs=target_srs)
    layer.CreateField(ogr.FieldDefn(site_id, ogr.OFTInteger))
    layer.CreateField(ogr.FieldDefn(site_x, ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn(site_y, ogr.OFTReal))

    # Set up generator
    if cell_sides == 4:
        wkt_generator = square_wkt_generator(
            min_x, min_y, max_x, max_y, x_res, y_res)
    elif cell_sides == 6:
        wkt_generator = hexagon_wkt_generator(
            min_x, min_y, max_x, max_y, x_res, y_res)
    else:
        raise Exception(
            'Cannot generate shapegrid cells with {} sides'.format(cell_sides))

    shape_id = 0
    for cell_wkt in wkt_generator:
        geom = ogr.CreateGeometryFromWkt(cell_wkt)
        geom.AssignSpatialReference(target_srs)
        centroid = geom.Centroid()
        x_center = centroid.GetX()
        y_center = centroid.GetY()
        feat = ogr.Feature(feature_def=layer.GetLayerDefn())
        feat.SetField(site_x, x_center)
        feat.SetField(site_y, y_center)
        feat.SetField(site_id, shape_id)
        # Check for intersection
        if geom.Intersection(selected_poly):
            layer.CreateFeature(feat)
            shape_id += 1
        feat.Destroy()
    data_set.Destroy()
    return shape_id

"""Tools for creating masks
"""
import math
import os

import numpy as np
from osgeo import gdal, gdalconst, ogr, osr

from LmCommon.common.lmconstants import (
    DEFAULT_EPSG, DEFAULT_NODATA, ENCODING, LMFormat)
from LmCommon.common.ready_file import ready_filename

# TODO: Move to constants probably
NUM_QUAD_SEGS = 30


# .............................................................................
def create_convex_hull_region_intersect_mask(
        occ_shp_filename, mask_path, region_layer_filename, buffer_distance,
        nodata=DEFAULT_NODATA, ascii_filename=None, tiff_filename=None):
    """Create a mask using the convex hull / region intersection method

    Args:
        occ_shp_filename: File location of occurrence set shapefile
        region_layer_filename: File location of ecoregions raster file
        nodata: A value to use for NODATA
        ascii_filename: If provided, write the mask raster as ASCII to this
            location
        tiff_filename: If provided, write the mask raster as GeoTiff to this
                                     location
    """
    pts = []

    # Read points, get their x,y and create geometry collection for convex hull
    points_ds = ogr.Open(occ_shp_filename)
    pts_lyr = points_ds.GetLayer()

    # Convex hull geometry
    geom_coll = ogr.Geometry(ogr.wkbGeometryCollection)

    for feat in pts_lyr:
        geom = feat.GetGeometryRef()
        geom_coll.AddGeometry(geom)
        pts.append((geom.GetX(), geom.GetY()))

    pts_lyr = points_ds = None

    bbox, cell_size, epsg = get_layer_dimensions(region_layer_filename)
    min_x, _, _, max_y = bbox

    region_ds = gdal.Open(region_layer_filename)
    band = region_ds.GetRasterBand(1)

    cols = region_ds.RasterXSize
    rows = region_ds.RasterYSize

    data = band.ReadAsArray(0, 0, cols, rows)

    # Get layer values for the points
    vals = set([])
    for x_coord, y_coord in pts:
        col = int((x_coord - min_x) / cell_size)
        row = int((max_y - y_coord) / cell_size)
        try:
            vals.add(data[row][col])
        except Exception:
            pass

    list_vals = list(vals)
    if len(list_vals) == 0:
        raise Exception('No intersection between points and raster')
    new_data = nodata * np.ones(data.shape, dtype=np.int8)

    convex_hull_raw = geom_coll.ConvexHull()
    buffered_convex_hull = convex_hull_raw.Buffer(
        buffer_distance, NUM_QUAD_SEGS)

    # Get convex hull array
    con_hull_data = create_convex_hull_array(
        mask_path, buffered_convex_hull, bbox, cell_size, epsg, nodata=nodata)

    # Mask the layer to only regions that points fall within
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            if data[i, j] in list_vals and con_hull_data[i, j] == 1:
                new_data[i, j] = 1

    if ascii_filename is not None:
        write_ascii(
            ascii_filename, bbox, cell_size, new_data, epsg, nodata=nodata)

    if tiff_filename is not None:
        write_tiff(
            tiff_filename, bbox, cell_size, new_data, epsg, nodata=nodata)


# .............................................................................
def create_convex_hull_array(base_path, convex_hull, bbox, cell_size, epsg,
                             nodata=DEFAULT_NODATA):
    """Create a numpy array containing the convex hull mask

    Args:
        base_path: A base file path (w/o extension) to use for temp files
        convex_hull: Convex hull geometry
        bbox: (minx, miny, maxx, maxy) tuple of raster coordinates
        cell_size: The cell size, in map units, of each cell in the raster
        epsg: The epsg code of the map projection to use for this raster
        nodata: A value to use for NODATA
    """
    # Create a shapefile for the convex hull
    # --------------------------------------
    # Note: Must encode or gdal will fail
    lyr_name = os.path.basename(base_path).encode(ENCODING)

    tmp_shp_filename = '{}_convex_hull.shp'.format(base_path)
    tmp_raster_filename = '{}_convex_hull.tif'.format(base_path)

    print(tmp_raster_filename, tmp_shp_filename)

    shp_drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)

    assert ready_filename(tmp_shp_filename, overwrite=True)

    # Create the convex hull shapefile
    out_ds = shp_drv.CreateDataSource(tmp_shp_filename)
    out_lyr = out_ds.CreateLayer(
        lyr_name.decode(ENCODING), geom_type=ogr.wkbPolygon)

    # Add an ID field
    id_field = ogr.FieldDefn('id', ogr.OFTInteger)
    out_lyr.CreateField(id_field)

    # Create the feature
    feat_defn = out_lyr.GetLayerDefn()
    feat = ogr.Feature(feat_defn)
    feat.SetGeometry(convex_hull)
    feat.SetField('id', 1)
    out_lyr.CreateFeature(feat)
    feat = None

    # Rasterize the shapefile
    # --------------------------------------
    minx, miny, maxx, maxy = bbox
    num_cols = int(math.ceil(float(maxx - minx) / cell_size))
    num_rows = int(math.ceil(float(maxy - miny) / cell_size))

    tiff_drv = gdal.GetDriverByName(LMFormat.GTIFF.driver)
    rst_ds = tiff_drv.Create(
        tmp_raster_filename, num_cols, num_rows, 1, gdalconst.GDT_Int16)
    rst_ds.SetGeoTransform([minx, cell_size, 0, maxy, 0, -cell_size])

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(int(epsg))
    rst_ds.SetProjection(srs.ExportToWkt())

    gdal.RasterizeLayer(rst_ds, [1], out_lyr, burn_values=[1])

    rst_ds.FlushCache()

    out_ds = None
    rst_ds = None

    # Get the data array
    # --------------------------------------
    rst_ds = gdal.Open(tmp_raster_filename)
    band = rst_ds.GetRasterBand(1)
    data = np.array(band.ReadAsArray())

    return data


# .............................................................................
def create_blank_mask_from_layer(
        template_layer_filename, nodata=DEFAULT_NODATA, ascii_filename=None,
        tiff_filename=None):
    """Generate a mask layer based on the input raster layer

    Args:
        template_layer_filename: Use this layer as a template for the mask
        nodata: A value to use for NODATA
        ascii_filename: If provided, write the mask raster as ASCII to this
            location
        tiff_filename: If provided, write the mask raster as GeoTiff to this
            location
    """
    bbox, cell_size, epsg = get_layer_dimensions(template_layer_filename)

    create_blank_mask(
        bbox, cell_size, epsg, nodata=nodata, ascii_filename=ascii_filename,
        tiff_filename=tiff_filename)


# .............................................................................
def create_blank_mask(bbox, cell_size, epsg, nodata=DEFAULT_NODATA,
                      ascii_filename=None, tiff_filename=None):
    """Create a blank mask raster for the specified region

    Args:
        bbox: (minx, miny, maxx, maxy) tuple of raster coordinates
        cell_size: The cell size, in map units, of each cell in the raster
        epsg: The epsg code of the map projection to use for this raster
        nodata: A value to use for NODATA
        ascii_filename: If provided, write the mask raster as ASCII to this
            location
        tiff_filename: If provided, write the mask raster as GeoTiff to this
            location
    """
    min_x, min_y, max_x, max_y = bbox
    num_cols = int(float(max_x - min_x) / cell_size)
    num_rows = int(float(max_y - min_y) / cell_size)

    data = np.ones((num_rows, num_cols), dtype=np.int8)

    if ascii_filename is not None:
        write_ascii(ascii_filename, bbox, cell_size, data, epsg, nodata=nodata)

    if tiff_filename is not None:
        write_tiff(tiff_filename, bbox, cell_size, data, epsg, nodata=nodata)


# .............................................................................
def get_layer_dimensions(template_layer_filename):
    """Get layer information

    Args:
        template_layer_filename: The template layer to get information for
    """
    dataset = gdal.Open(template_layer_filename)

    num_cols = dataset.RasterXSize
    num_rows = dataset.RasterYSize

    minx, cell_size, _, maxy, _, _ = dataset.GetGeoTransform()

    maxx = minx + (cell_size * num_cols)
    miny = maxy - (cell_size * num_rows)

    bbox = (minx, miny, maxx, maxy)

    srs = osr.SpatialReference()
    srs.ImportFromWkt(dataset.GetProjectionRef())
    # TODO: Try to find a better way to fail
    try:
        epsg = int(srs.GetAttrValue('AUTHORITY', 1))
    except Exception:
        epsg = DEFAULT_EPSG
    dataset = None
    return bbox, cell_size, epsg


# .............................................................................
def write_ascii(out_filename, bbox, cell_size, data, epsg,
                nodata=DEFAULT_NODATA, header_precision=6):
    """Write an ASCII raster layer from a numpy array

    Args:
        out_filename: The file location to write the raster to
        bbox: (minx, miny, maxx, maxy) tuple of raster coordinates
        cell_size: The cell size, in map units, of each cell in the raster
        data: The data array to use for raster content
        epsg: The epsg code of the map projection to use for this raster
        nodata: A value to use for NODATA

    Todo:
        * Probably should establish a common layer package in LmCommon
        * Needs to use the same code we use for conversions
    """
    min_x, min_y, _, _ = bbox

    (num_rows, num_cols) = data.shape

    if header_precision is not None:
        minx = round(min_x, header_precision)
        miny = round(min_y, header_precision)
        cell_size = round(cell_size, header_precision)

    ready_filename(out_filename, overwrite=True)
    with open(out_filename, 'w', encoding=ENCODING) as out_f:
        out_f.write('ncols    {}\n'.format(num_cols))
        out_f.write('nrows    {}\n'.format(num_rows))
        out_f.write('xllcorner    {}\n'.format(minx))
        out_f.write('yllcorner    {}\n'.format(miny))
        out_f.write('cellsize    {}\n'.format(cell_size))
        out_f.write('NODATA_value    {}\n'.format(nodata))

        np.savetxt(out_f, data, fmt='%i')

    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(epsg)
    spatial_ref.MorphToESRI()

    # Write .prj file
    if epsg is not None:
        prj_filename = '{}.prj'.format(os.path.splitext(out_filename)[0])
        with open(prj_filename, 'w', encoding=ENCODING) as prj_out:
            prj_out.write(spatial_ref.ExportToWkt())


# .............................................................................
def write_tiff(out_filename, bbox, cell_size, data, epsg,
               nodata=DEFAULT_NODATA):
    """Write a GeoTiff raster layer from a numpy array

    Args:
        out_filename: The file location to write the raster to
        bbox: (minx, miny, maxx, maxy) tuple of raster coordinates
        cell_size: The cell size, in map units, of each cell in the raster
        data: The data array to use for raster content
        epsg: The epsg code of the map projection to use for this raster
        nodata: A value to use for NODATA

    Todo:
        Probably should establish a common layer package in LmCommon
    """
    min_x, _, _, maxy = bbox
    (num_rows, num_cols) = data.shape

    ready_filename(out_filename, overwrite=True)

    drv = gdal.GetDriverByName(LMFormat.GTIFF.driver)
    dataset = drv.Create(
        out_filename, num_cols, num_rows, 1, gdalconst.GDT_Byte)
    dataset.SetGeoTransform([min_x, cell_size, 0, maxy, 0, -cell_size])

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    dataset.SetProjection(srs.ExportToWkt())

    out_band = dataset.GetRasterBand(1)
    out_band.WriteArray(data)
    out_band.FlushCache()
    out_band.SetNoDataValue(nodata)

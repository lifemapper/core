"""
@summary: Tools for creating masks
@author: CJ Grady
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import numpy as np
import os
from osgeo import gdal, gdalconst, ogr, osr

from LmCommon.common.lmconstants import DEFAULT_NODATA, LMFormat
from LmCommon.common.readyfile import readyFilename

# TODO: Move to constants probably
NUM_QUAD_SEGS = 30

# .............................................................................
def create_convex_hull_region_intersect_mask(occ_shp_filename, 
                                 region_layer_filename, buffer_distance,
                                 nodata=DEFAULT_NODATA, 
                                 ascii_filename=None, tiff_filename=None):
   """
   @summary: Create a mask using the convex hull / region intersection method
   @param occ_shp_filename: File location of occurrence set shapefile
   @param region_layer_filename: File location of ecoregions raster file
   @param nodata: A value to use for NODATA
   @param ascii_filename: If provided, write the mask raster as ASCII to this 
                             location
   @param tiff_filename: If provided, write the mask raster as GeoTiff to this
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
   minx, miny, maxx, maxy = bbox
   
   region_ds = gdal.Open(region_layer_filename)
   band = region_ds.GetRasterBand(1)
   
   cols = region_ds.RasterXSize
   rows = region_ds.RasterYSize
   
   data = band.ReadAsArray(0, 0, cols, rows)
   
   # Get layer values for the points
   vals = set([])
   for x,y in pts:
      col = int((x - minx) / cell_size)
      row = int((maxy - y) / cell_size)
      try:
         vals.add(data[row][col])
      except:
         pass
   
   listVals = list(vals)
   if len(listVals) == 0:
      raise Exception, 'No intersection between points and raster'
   newData = nodata * np.ones(data.shape, dtype=np.int8)
   
   convex_hull_raw = geom_coll.ConvexHull()
   buffered_convex_hull = convex_hull_raw.Buffer(buffer_distance, NUM_QUAD_SEGS)
   
   # Get convex hull array
   con_hull_data = create_convex_hull_array(
               os.path.splitext(occ_shp_filename)[0], buffered_convex_hull, 
               bbox, cell_size, epsg, nodata=nodata)
   
   # Mask the layer to only regions that points fall within
   for i in range(data.shape[0]):
      for j in range(data.shape[1]):
         if data[i,j] in listVals and con_hull_data[i,j] == 1:
            newData[i,j] = 1
   
   if ascii_filename is not None:
      write_ascii(ascii_filename, bbox, cell_size, newData, epsg, nodata=nodata)
   
   if tiff_filename is not None:
      write_tiff(tiff_filename, bbox, cell_size, newData, epsg, nodata=nodata)

# .............................................................................
def create_convex_hull_array(base_path, convex_hull, bbox, cell_size, epsg, 
                                 nodata=DEFAULT_NODATA):
   """
   @summary: Create a numpy array containing the convex hull mask
   @param base_path: A base file path (w/o extension) to use for temp files
   @param convex_hull: Convex hull geometry
   @param bbox: (minx, miny, maxx, maxy) tuple of raster coordinates
   @param cell_size: The cell size, in map units, of each cell in the raster
   @param epsg: The epsg code of the map projection to use for this raster
   @param nodata: A value to use for NODATA
   """
   # Create a shapefile for the convex hull
   # --------------------------------------
   lyr_name = os.path.basename(base_path)
   tmp_shp_filename = '{}_convex_hull.shp'.format(base_path)
   tmp_raster_filename = '{}_convex_hull.tif'.format(base_path)
   
   print tmp_raster_filename, tmp_shp_filename
   
   shp_drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
   
   assert readyFilename(tmp_shp_filename, overwrite=True)
   
   # Create the convex hull shapefile
   out_ds = shp_drv.CreateDataSource(tmp_shp_filename)
   out_lyr = out_ds.CreateLayer(lyr_name, geom_type=ogr.wkbPolygon)
   
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
   num_cols = float(maxx - minx) / cell_size
   num_rows = float(maxy - miny) / cell_size
   
   tiff_drv = gdal.GetDriverByName(LMFormat.GTIFF.driver)
   rst_ds = tiff_drv.Create(tmp_raster_filename, int(num_cols), int(num_rows), 1, 
                            gdalconst.GDT_Int16)
   rst_ds.SetGeoTransform([minx, cell_size, 0, maxy, 0, -cell_size])

   srs = osr.SpatialReference()
   srs.ImportFromEPSG(int(epsg))
   rst_ds.SetProjection(srs.ExportToWkt())
   
   gdal.RasterizeLayer(rst_ds, [1], out_lyr, burn_values=[1])#, options=options)
   
   rst_ds.FlushCache()
   
   out_ds = None
   rst_ds = None
   
   # Get the data array
   # --------------------------------------
   rst_ds = gdal.Open(tmp_raster_filename)
   band = rst_ds.GetRasterBand(1)
   data = np.array(band.ReadAsArray())
   
   # Remove temp files
   try:
      os.remove(tmp_raster_filename)
   except Exception, e:
      print str(e)
      
   try:
      os.remove(tmp_shp_filename)
   except Exception, e:
      print str(e)
   
   return data

# .............................................................................
def create_blank_mask_from_layer(template_layer_filename, 
                                 nodata=DEFAULT_NODATA, ascii_filename=None,
                                 tiff_filename=None):
   """
   @summary: Generate a mask layer based on the input raster layer
   @param template_layer_filename: Use this layer as a template for the mask
   @param nodata: A value to use for NODATA
   @param ascii_filename: If provided, write the mask raster as ASCII to this 
                             location
   @param tiff_filename: If provided, write the mask raster as GeoTiff to this
                            location
   """
   bbox, cell_size, epsg = get_layer_dimensions(template_layer_filename)

   create_blank_mask(bbox, cell_size, epsg, nodata=nodata, 
                  ascii_filename=ascii_filename, tiff_filename=tiff_filename)

# .............................................................................
def create_blank_mask(bbox, cell_size, epsg, nodata=DEFAULT_NODATA, 
                      ascii_filename=None, tiff_filename=None):
   """
   @summary: Create a blank mask raster for the specified region
   @param bbox: (minx, miny, maxx, maxy) tuple of raster coordinates
   @param cell_size: The cell size, in map units, of each cell in the raster
   @param epsg: The epsg code of the map projection to use for this raster
   @param nodata: A value to use for NODATA
   @param ascii_filename: If provided, write the mask raster as ASCII to this 
                             location
   @param tiff_filename: If provided, write the mask raster as GeoTiff to this
                            location
   """
   minx, miny, maxx, maxy = bbox
   num_cols = float(maxx - minx) / cell_size
   num_rows = float(maxy - miny) / cell_size
   
   data = np.ones((num_rows, num_cols), dtype=np.int8)
   
   if ascii_filename is not None:
      write_ascii(ascii_filename, bbox, cell_size, data, epsg, nodata=nodata)
   
   if tiff_filename is not None:
      write_tiff(tiff_filename, bbox, cell_size, data, epsg, nodata=nodata)
   
# .............................................................................
def get_layer_dimensions(template_layer_filename):
   """
   @summary: Get layer information
   @param template_layer_filename: The template layer to get information for
   """
   ds = gdal.Open(template_layer_filename)

   num_cols = ds.RasterXSize
   num_rows = ds.RasterYSize
   
   minx, cell_size, _, maxy, _, _ = ds.GetGeoTransform()
   
   maxx = minx + (cell_size * num_cols)
   miny = maxy - (cell_size * num_rows)
   
   bbox = (minx, miny, maxx, maxy)

   srs = osr.SpatialReference()
   srs.ImportFromWkt(ds.GetProjectionRef())
   epsg = int(srs.GetAttrValue('AUTHORITY', 1))
   ds = None
   
   return bbox, cell_size, epsg

# .............................................................................
def write_ascii(out_filename, bbox, cell_size, data, epsg, 
                                                      nodata=DEFAULT_NODATA):
   """
   @summary: Write an ASCII raster layer from a numpy array
   @param out_filename: The file location to write the raster to
   @param bbox: (minx, miny, maxx, maxy) tuple of raster coordinates
   @param cell_size: The cell size, in map units, of each cell in the raster
   @param data: The data array to use for raster content
   @param epsg: The epsg code of the map projection to use for this raster
   @param nodata: A value to use for NODATA
   @todo: Probably should establish a common layer package in LmCommon
   """
   minx, miny, maxx, maxy = bbox
   num_cols = float(maxx - minx) / cell_size
   num_rows = float(maxy - miny) / cell_size
   
   assert (num_rows, num_cols) == data.shape

   readyFilename(out_filename, overwrite=True)
   with open(out_filename, 'w') as outF:
      outF.write('ncols   {}\n'.format(num_cols))
      outF.write('nrows   {}\n'.format(num_rows))
      outF.write('xllcorner   {}\n'.format(minx))
      outF.write('yllcorner   {}\n'.format(miny))
      outF.write('cellsize   {}\n'.format(cell_size))
      outF.write('NODATA_value   {}\n'.format(nodata))
      
      np.savetxt(outF, data, fmt='%i')
   
   spatial_ref = osr.SpatialReference()
   spatial_ref.ImportFromEPSG(epsg)
   spatial_ref.MorphToESRI()
   
   # Write .prj file
   if epsg is not None:
      prj_filename = '{}.prj'.format(os.path.splitext(out_filename)[0])
      with open(prj_filename, 'w') as prjOut:
         prjOut.write(spatial_ref.ExportToWkt())

# .............................................................................
def write_tiff(out_filename, bbox, cell_size, data, epsg, 
                                                      nodata=DEFAULT_NODATA):
   """
   @summary: Write a GeoTiff raster layer from a numpy array
   @param out_filename: The file location to write the raster to
   @param bbox: (minx, miny, maxx, maxy) tuple of raster coordinates
   @param cell_size: The cell size, in map units, of each cell in the raster
   @param data: The data array to use for raster content
   @param epsg: The epsg code of the map projection to use for this raster
   @param nodata: A value to use for NODATA
   @todo: Probably should establish a common layer package in LmCommon
   """
   minx, miny, maxx, maxy = bbox
   num_cols = int(float(maxx - minx) / cell_size)
   num_rows = int(float(maxy - miny) / cell_size)
   
   assert (num_rows, num_cols) == data.shape
   
   readyFilename(out_filename, overwrite=True)
   
   drv = gdal.GetDriverByName(LMFormat.GTIFF.driver)
   ds = drv.Create(out_filename, num_cols, num_rows, 1, gdalconst.GDT_Byte)
   ds.SetGeoTransform([minx, cell_size, 0, maxy, 0, -cell_size])

   srs = osr.SpatialReference()
   srs.ImportFromEPSG(epsg)
   ds.SetProjection(srs.ExportToWkt())
   
   outBand = ds.GetRasterBand(1)
   outBand.WriteArray(data)
   outBand.FlushCache()
   outBand.SetNoDataValue(nodata)

# .............................................................................
# Testing code
"""
shp_filename = 'ptTestTemp/pt_64474.shp'
region_filename = 'ecoreg.tif'
out_ascii = 'convex_hull_test.asc'
out_tiff = 'convex_hull_test.tif'
import os
os.path.exists(region_filename)
os.path.exists(shp_filename)
from LmCompute.plugins.single.mask.create_mask import *
create_convex_hull_region_intersect_mask(shp_filename, region_filename, ascii_filename=out_ascii, tiff_filename=out_tiff)
"""

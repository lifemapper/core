import os
import subprocess

cutPath = "/home/jcavner/Pragma_Data/BBOX.shp"

# listtifs = commands.getoutput('ls /home/jcavner/worldclim_lwr48/future/NIES/%s/%s/*.tif' % (scen,yr))
listtifs = subprocess.getoutput('ls /home/jcavner/Pragma_Data/Current/geotiff/*.tif')
tifpaths = listtifs.split('\n')

# outpath = "/home/jcavner/Bison_WorldClim/Future/%s/%s/" % (scen,yr)
outpath = "/home/jcavner/Pragma_Data/Current/Asia/"

for tif in tifpaths:

   # get just tiff names
   fN = os.path.split(tif)[1]
   fullout = os.path.join(outpath, fN)
   # extent = "-125 50 -66 24"
   # gdalwarp -q -cutline /home/jcavner/USAdminBoundaries/US_Bison_Lwr48_singleParts.shp -crop_to_cutline -of GTiff /share/data/ClimateData/Present/worldclim/bio_9.tif /home/jcavner/worldclim_lwr48/test.tif
   # gdal_translate -projwin -90.0 90.0 180.0 -36.0 -of GTiff /share/data/ClimateData/Present/worldclim/bio_1.tif /home/jcavner/out.tif
   # gdalStr = "gdal_translate -projwin %s -of GTiff %s %s" % (extent,path,fullout)

   # /home/jcavner/Pragma_Data/alt/geotiff/alt.tif /home/jcavner/Pragma_Data/alt/geotiff/Asia/alt.tif

   # gdalStr = "gdal_translate -a_nodata -9999 -projwin 95.0 21.0 153.0 -11.0 -of GTiff %s %s" % (tif, fullout)
   # TRANSLATE WON"T WORK FOR LZW COMPRESSED TIFFS!

   gdalStr = "gdalwarp -dstnodata -9999 -q -cutline %s -crop_to_cutline -of GTiff %s %s" % (cutPath, tif, fullout)
   # print gdalStr
   resp = subprocess.getoutput(gdalStr)
   print(resp)
print("done")

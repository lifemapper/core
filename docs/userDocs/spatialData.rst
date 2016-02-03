## Raster Data:  

Geospatial data best represented in raster format generally has values at every point.  The area to be represented is split up into a grid, with each grid cell containing a value. Elevation, temperature, precipitation are examples of data generally represented as raster.

## Vector Data:  

Geospatial data best represented in vector format can be represented as points, lines, or polygons.  

**Point**: Points represent a single location on a 2 dimensional surface.  Depending on the scale, different types of data are appropriate to be represented as points. At a very small scale (think of from a very great distance), a city might be represented as a single point.  Species occurrences are usually represented as points.

**Line**: Lines are made up of multiple points, joined.  Roads and rivers are usually represented as rivers.  

**Polygon**: Polygons are made up of multiple points or lines, joined to define a discrete area.  At a larger scale (zoomed in), a city, river, or street might be represented as a polygon. Administrative boundaries, like country, state, or other geo-political units are examples of polygon data.

## Scale:

Large scale maps generally refer to maps of a small area with a lot of detail.  It is called large because where the ratio between distance on the map to distance on the ground is very large.  For example 1:5000 means that 1 unit on the map (i.e. centimeter) equals 5000 units on the ground (5000 centimeters).  This type of map might be used for a detailed map of a city.  

Small scale maps are generally used for mapping large areas.  The ratio of the distance on the map to the distance on the ground is much smaller.  For example, a map of the country of Indonesia might be at the ratio of 1:2,000,000.


## Sample Datasets:

Available on your Windows Virtual Box Desktop in the Data folder
* Elevation: alt.tif
* Country boundaries: TM_WORLD_BORDERS_SIMPL-0.3.shp
* Indonesia: IDN_adm
* Malaysia: MYS_adm 
* Philippines: PHL_adm
* Thailand: THA_adm
* Multiple levels of Bathymetry: ne_10m_bathymetry_all
* Coastline: ne_10m_coastline
* Marine areas: ne_10m_geography_marine_polys
* Geographic regions: ne_10m_geography_regions_polys
* Lakes: ne_10m_lakes
* Land masses: ne_10m_land
* Minor islands: ne_10m_minor_islands
* Oceans: ne_10m_ocean
* Populated places/cities: ne_10m_populated_places
* Reefs: ne_10m_reefs
* Rivers and Lakes: ne_10m_rivers_lake_centerlines

### Some resources: 
**World borders:**  http://thematicmapping.org/downloads/world_borders.php
**Global administrative boundaries:**  http://www.gadm.org/country 
**Natural earth data:** http://www.naturalearthdata.com/ 


## QGIS tools: 

The QGIS application has several ways to do most common operations in QGIS:  from the top menu, from the button bar, and from the shortcut on the left side of the table of contents.  The large area on the right is called the canvas.

You can modify the tools available as shortcuts by choosing the View menu item at the top of QGIS, then choosing ‘Toolbars’.  Panels will open when you begin a task for which they are appropriate, such as the ‘Identify’ function on the button bar.  If you accidentally close the listing of layers on the canvas, you can turn that on again by choosing ‘Layers’ option under Panels.

### Explore your data
 
Symbolize:
Either double click on the layer name or right click, then choose ‘Properties’.  
* The **General** tab will give you name, filename, and projection information (Coordinate Reference System, CRS)
* The **Style** tab will allow you to change symbols and colors to be displayed on the canvas

Analyze: 
The Raster and Vector menu items at the top of the application have a wide variety of tools available.  Some interesting tools under Vector are under Geoprocessing Tools.
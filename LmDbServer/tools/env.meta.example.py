"""
These constants represent metadata for Lifemapper-available climate data 
packages.  New climate data packages should be in a file named 
CLIMATE_PACKAGE.tar.gz 

@note:  
Directory pattern like 10min/worldclim1.4/  or 10min/CCSM4/2050/RCP4.5/
   SCENARIO_PACKAGE topdir/
      Baseline name/
         climate files 
   IPCC Report code/
      GCM Model code/
         Time code/
            RCP code/
         
@note:
Filename (baseline) pattern like alt.tif (altitude), bio1.tif .. bio19.tif
   lowercase layertype keys
Filename (future) pattern like cc85bi5019.tif
   <GCM shortcode><RCP shortcode>bi<2 digit timekey><2 digit bioclim number>
"""
from osgeo import gdalconst
 
# For remote data, cannot read to get this 
ENVLYR_GDALTYPE = gdalconst.GDT_Int16
ENVLYR_GDALFORMAT = 'GTiff'
EPSG = 4326
MAPUNITS = 'dd' 
# Decimal degrees for EPSG 4326
RESOLUTIONS = {'10min': 0.16667, '5min': 0.083333, '30sec': 0.0083333}
CLIMATE_KEYWORDS = ['bioclimatic variables', 'climate', 'elevation']

# Observed climate data, from www.worldclim.org 
BASELINE_DATA = {
   'WC': {
      # Name for climate scenario
      'name': 'Worldclim 1.4', 
      # Layers from Baseline which are re-used in predicted future
      'staticLayerTypes': ['ALT'],
      # Top relative directory, under CLIMATE_PACKAGE top dir, for scenario layers
      'directory': 'worldclim1.4', 
      # Year range (low, high) that these data represent
      'time': (1950, 2000), 
      # Short code for naming this scenario
      'code': 'WC', 
      # Short title for this scenario
      'title': 'Worldclim 1.4', 
      # Author of these cliamte data
      'author': ' Hijmans, R.J., S.E. Cameron, J.L. Parra, P.G. Jones and A. Jarvis, 2005. Very high resolution interpolated climate surfaces for global land areas. International Journal of Climatology 25: 1965-1978',
      # Extended description of the data  
      'description': 'WorldClim 1.4 elevation and bioclimatic variables computed from interpolated observation data collected between 1950 and 2000 (http://www.worldclim.org/), 5 min resolution',
      'keywords': ['observed', 'present'] 
   }
}
 
# Time periods available for worldclim data, AR5 future projections, 
# CMIP5 past projections, from www.worldclim.org 
TIME_PERIODS = {
   # Time step key, used in directory structure
   '2050': {  
      # Full name of time step
      'name': '2041-2060',
      # Integer for year, or None if NA 
      'startdate': 2041, 
      'enddate': 2060,
      'keywords': ['predicted', 'future']
   }, 
   '2070': {
      'name': '2061-2080',
      'startdate': 2061,
      'enddate': 2080,
      'keywords': ['predicted', 'future']
   },
   'mid': {
      'name': 'Mid Holocene (~ 6000 years ago)',
      'startdate': None,
      'enddate': None,
      'keywords': ['predicted', 'past']}, 
   'lgm': {'name': 'Last Glacial Maximium (~ 22000 years ago)',
      'startdate': None,
      'enddate': None,
      'keywords': ['predicted', 'past']},
                
}

# (previously REPORTS)
PREDICTED_DATA = {
   # Predicted future (IPCC) or past (CMIP) report code
   # Used for naming scenario code and relative paths.
   # All models may be paired with any scenarios
   # Top relative directory, under under CLIMATE_PACKAGE top dir
   # Past
   'CMIP5': {'name': 'Coupled Model Intercomparison Project Phase 5',              
             'models': {'CCSM4':
                        {'name': 'Community Climate System Model, 4.0',
                         'shortcode': 'cc',
                         'author': 'National Center for Atmospheric Research (NCAR) http://www.cesm.ucar.edu/models/ccsm4.0/'
                         }},
             'scenarios': None },
   # Future
   'AR5': {'name': 'IPCC Fifth Assessment Report (2014)', 
           'models': {'CCSM4': {'name': 'Community Climate System Model, 4.0',
                                # Short code for GCM model, for filenames 
                                'shortcode': 'cc', 
                                'author': 'National Center for Atmospheric Research (NCAR) http://www.cesm.ucar.edu/models/ccsm4.0/' }
                      },
           'scenarios': {# Representative Concentration Pathways code, 
                         'RCP4.5': {'shortcode': '45',
                                    'keywords': ['radiative forcing +4.5', 
                                                 'likely temperature increase 1.1-2.6 C']},
                         'RCP8.5': {'shortcode': '85',
                                    'keywords': ['radiative forcing +8.5', 
                                                 'likely temperature increase 2.6-4.8 C',
                                                 'likely sea level increase 0.45-0.82 m']} 
                         }
           }}

# Lifemapper-packaged data, produced by Worldclim, www.worldclim.org 
# Example: 10 minute, global data, baseline, past, future
   # past: CCSM4-lgm-10min,CCSM4-mid-10min
   # baseline: WC-10min
   # future: CCSM4-RCP8.5-2070-10min,CCSM4-RCP4.5-2070-10min,CCSM4-RCP8.5-2050-10min,CCSM4-RCP4.5-2050-10min
#
# This is the primary component of the metadata, it assembles a subset of data  
# from the previous dictionaries: BASELINE, TIME_PERIODS, and REPORTS
# 
CLIMATE_PACKAGES = {
   # Name of climate data package, base filename for tar file, metadata file, csv file 
   '10min-past-present-future': 
   { 
      # Resolution of climate data in this package
      'res': '10min', 
      # Top directory for this package of data
      'topdir': '10min', 
      # Bounding box for the layers in this package, in MAPUNITS 
      # (dd, Decimal degrees for EPSG 4326)
      'bbox': [-180, -60, 180, 90],
      'past': 
         # {REPORT: [Time,Time, ...]} 
         {'CMIP5': ['mid', 'lgm']},
      # BASELINE_DATA code
      'present': 'WC',
      'future': {
         # References key in REPORTS
         # {REPORT: [(RCP, Time), (RCP, Time) ...]} 
         'AR5': [
                 ('CCSM4', 'RCP4.5', '2050'), 
                 ('CCSM4', 'RCP4.5', '2070'),
                 ('CCSM4', 'RCP8.5', '2050'), 
                 ('CCSM4', 'RCP8.5', '2070')
                ]
                 },
      # Append to all scenario codes if needed for identification  
      # (i.e. of a region or other constraint) 
      'suffix': None
   }
}
                     

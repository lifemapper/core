"""
These constants represent metadata for Lifemapper-available climate data 
packages.  New climate data packages should be in a file named 
CLIMATE_PACKAGE.tar.gz 
@note: LAYERTYPE_DATA
Each entry should have a title, description, value units, keywords, and files
title: The title of this layer type.  Examples: Elevation, Annual Mean Temperature
description: A description of what this layer type is
valunits: What are the units for the values in these layers.  Examples:
          meters for elevation, and mm for precipitation
keywords: a tuple of keywords associated with this layer type
files: a dictionary containing 
  key = the relative path and filename
  value = list including keys defining the type of data/scenario to compile: 
       OBSERVED_PREDICTED_META top-level key ('observed' or past/future report)  
       (opt to further narrow down past/future) keys for 'models'
                                                         'times'
                                                         'alternatePredictions'
                                                         
@note:
Filename (baseline) pattern like alt.tif (altitude), bio1.tif .. bio19.tif
   lowercase layertype keys
Filename (future) pattern like cc85bi5019.tif
   <GCM shortcode><RCP shortcode>bi<2 digit timekey><2 digit bioclim number>
   
@note:
Charlie says:
Here is my attempt at arranging the metadata files for "experiment 3" which 
includes climate (bioclim), soil, and spatial layers.

I was a little unclear on how to structure the 'env.meta' file for the 
combinations we are looking. We are currently projecting across 4 different 
GCMs (he, gs, gd, ac) at one RCP (4.5), and 1 GCM (he) at 3 additional 
RCPs (2.6, 6.0, 8.5). These projections are for both 2050 and 2070.
"""

from osgeo import gdalconst
 
# For remote data, cannot read to get this 
ENVLYR_GDALTYPE = gdalconst.GDT_Int16
ENVLYR_GDALFORMAT = 'GTiff'
EPSG = 2163
# @TODO: meters!?
MAPUNITS = 'dd' 
# Decimal degrees for EPSG 2163
# @TODO: values in meters!?
RESOLUTIONS = {'10min': 0.16667, '5min': 0.083333, '30sec': 0.0083333}
ENV_KEYWORDS = ['bioclimatic variables', 'climate', 'elevation', 'soil', 
                    'spatial distance']

OBSERVED_PREDICTED_META = {
   # Top keys are the relative directory, under under CLIMATE_PACKAGE topdir
   'observed': {
      # Name for climate scenario
      'name': 'Worldclim1.4+Soil+SpatialDistance', 
      'keywords': ['observed', 'present'], 
      # Year range (low, high) that these data represent
      'times':
      {'1950-2000': {'shortcode': 'Curr'}},
      # @TODO: Fix title, author, description
      # Short title for this scenario
      'title': 'Worldclim1.4, Soil, SpatialDistance', 
      # Author of these data
      'author': ' Hijmans, R.J., S.E. Cameron, J.L. Parra, P.G. Jones and A. Jarvis, 2005. Very high resolution interpolated climate surfaces for global land areas. International Journal of Climatology 25: 1965-1978',
      # Extended description of the data  
      'description': 'WorldClim 1.4  bioclimatic variables computed from interpolated observation data collected between 1950 and 2000 (http://www.worldclim.org/), 5 min resolution',
   },
   # Past Report
   'CMIP5': {'name': 'Coupled Model Intercomparison Project Phase 5',              
             'keywords': ['predicted', 'past'],
             'times': {'Mid Holocene (~6K years ago)': {'shortcode': 'mid'}, 
                       'Last Glacial Maximium (~22K years ago)': {'shortcode': 'lgm'}},
             'models': {'CCSM4':
                        {'name': 'Community Climate System Model, 4.0',
                         'shortcode': 'cc',
                         'author': 'National Center for Atmospheric Research (NCAR) http://www.cesm.ucar.edu/models/ccsm4.0/'
                         }},
             'scenarios': None },
   # Future Report
   'AR5': { 
      # Name of climate report
      'name': 'IPCC Fifth Assessment Report (2014)', 
      'keywords': ['predicted', 'future'],
      'times':
      {'2041-2060': {'shortcode': '2050'}, 
       '2061-2080': {'shortcode': '2070'}},
      # he / HadGEM2-ES
      'models': 
      {# AKA he, long code used for scenario code and dir structure
       'HadGEM2-ES': {'name': 'Hadley Centre Global Environment Model v2 - Earth System',
                      # Short code for GCM model, for filenames 
                      'shortcode': 'he', 
                      'author': 'Collins, W.J., N. Bellouin, M. Doutriaux-Boucher, N. Gedney, T. Hinton, C. D. Jones, S. Liddicoat, G. Martin, F. OConnor, J. Rae, C. Senior, I. Totterdell, S. Woodward, T. Reichler, J. Kim, 2008: Evaluation of the HadGEM2 model. Met Office Hadley Centre Technical Note no. HCTN 74, available from Met Office, FitzRoy Road, Exeter EX1 3PB http://www.metoffice.gov.uk/publications/HCTN/index.html'
                      },
       # AKA gs
       'GISS-E2-R': {'name': 'NASA GISS GCM ModelE',
                     # Short code for GCM model, for filenames 
                     'shortcode': 'gs', 
                     'author': 'Nazarenko, L., G.A. Schmidt, R.L. Miller, N. Tausnev, M. Kelley, R. Ruedy, G.L. Russell, I. Aleinov, M. Bauer, S. Bauer, R. Bleck, V. Canuto, Y. Cheng, T.L. Clune, A.D. Del Genio, G. Faluvegi, J.E. Hansen, R.J. Healy, N.Y. Kiang, D. Koch, A.A. Lacis, A.N. LeGrande, J. Lerner, K.K. Lo, S. Menon, V. Oinas, J.P. Perlwitz, M.J. Puma, D. Rind, A. Romanou, M. Sato, D.T. Shindell, S. Sun, K. Tsigaridis, N. Unger, A. Voulgarakis, M.-S. Yao, and J. Zhang, 2015: Future climate change under RCP emission scenarios with GISS ModelE2. J. Adv. Model. Earth Syst., early on-line, doi:10.1002/2014MS000403, http://data.giss.nasa.gov/modelE/ar5/'
                     },
       # AKA ac
       'ACCESS1-0': {'name': 'ACCESS1-0 CSIRO, Commonwealth Scientific and Industrial Research Organisation, Australia',
                     # Short code for GCM model, for filenames 
                     'shortcode': 'ac', 
                     'author': 'CSIRO, Commonwealth Scientific and Industrial Research Organisation, Australia, https://confluence.csiro.au/display/ACCESS/Home'
                     },
       # AKA gd
       'GFDL-ESM2G': {'name': 'Geophysical Fluid Dynamics Laboratory - Earth System Models 2G',
                     # Short code for GCM model, for filenames 
                     'shortcode': 'gd', 
                     'author': 'Dunne, John P., et al. "GFDLs ESM2 global coupled climate-carbon Earth System Models. Part I: Physical formulation and baseline simulation characteristics." Journal of Climate 25.19 (2012): 6646-6665, http://www.gfdl.noaa.gov/earth-system-model'
                     }},      		
      'alternatePredictions': 
      {# Representative Concentration Pathways code, used in scenario code and dir structure
       'RCP2.6': {'shortcode': '26',
                  'keywords': ['radiative forcing +2.6', 
                               'likely temperature increase 0.3 to 1.7 C by 2081-2100']},         
       'RCP4.5': {'shortcode': '45',
                  'keywords': ['radiative forcing +4.5', 
                               'likely temperature increase 1.1 to 2.6 C by 2081-2100']},
       'RCP6.0': {'shortcode': '60',
                  'keywords': ['radiative forcing +6.0', 
                               'likely temperature increase 1.4 to 3.1 C by 2081-2100']},
       'RCP8.5': {'shortcode': '85',
                  'keywords': ['radiative forcing +8.5', 
                               'likely temperature increase 2.6 to 4.8 C by 2081-2100']} 
      }
}}

# This is the primary component of the metadata, it assembles a subset of data  
# from the dictionaries: OBSERVED_PREDICTED_META and LAYERTYPE_DATA
CLIMATE_PACKAGES = {
   # Name of climate data package, base filename for tar file, metadata file, csv file 
   '1km-past-present-future': 
   {# Resolution of climate data in this package
    'res': '1km', 
    # Top directory for this package of data
    'topdir': '1km', 
    # TODO: Bounding box for the layers in this package, in MAPUNITS 
    'bbox': [-180, -60, 180, 90],
    # BASELINE_DATA code
    'baseline': 'observed',
    # {REPORT: [(model, Time, RCP(opt)), (model, Time, RCP(opt)) ...]}
    'predicted': {'AR5': [('ACCESS1-0', '2041-2060', 'RCP4.5'), 
                          ('ACCESS1-0', '2061-2080', 'RCP4.5'),
                          ('GISS-E2-R', '2041-2060', 'RCP4.5'), 
                          ('GISS-E2-R', '2061-2080', 'RCP4.5'),
                          ('GFDL-ESM2G', '2041-2060', 'RCP4.5'), 
                          ('GFDL-ESM2G', '2061-2080', 'RCP4.5'),
                          ('HadGEM2-ES', '2041-2060', 'RCP2.6'), 
                          ('HadGEM2-ES', '2061-2080', 'RCP2.6'),
                          ('HadGEM2-ES', '2041-2060', 'RCP4.5'), 
                          ('HadGEM2-ES', '2061-2080', 'RCP4.5'),
                          ('HadGEM2-ES', '2041-2060', 'RCP6.0'), 
                          ('HadGEM2-ES', '2061-2080', 'RCP6.0'),
                          ('HadGEM2-ES', '2041-2060', 'RCP8.5'), 
                          ('HadGEM2-ES', '2061-2080', 'RCP8.5')]},
    'layertypes': ['bio1', 'bio7', 'bio8', 'bio12', 'bio15', 'bio18', 
                   'gmted2010.elv_mean', 't_clay', 't_gravel', 't_sand', 
                   't_silt', 't_oc', 't_ph', 't_ece', 'svd.pc1', 'svd.pc2', 
                   'svd.pc3'],
   # Append to all scenario codes if needed for identification  
   # (i.e. of a region or other constraint) 
   'suffix': None
   }
}

LAYERTYPE_META = {
   'bio1': {
      'title': 'Annual Mean Temperature',
      'description': 'Annual Mean Temperature',
      'valunits':'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'annual'),
      'files': {'clim.current.bio03.tif': ['observed'],
                'clim.ac4550.bio01.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio01.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio01.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio01.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio01.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio01.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio01.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio01.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio01.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio01.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio01.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio01.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio01.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio01.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
            },
   'bio2': {
      'title': 'Mean Diurnal Range',
      'description': 'Mean Diurnal Range (Mean of monthly (max temp - min temp))',
      'valunits':'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'range'),
      'files': {'clim.current.bio02.tif': ['observed'],
                'clim.ac4550.bio02.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio02.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio02.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio02.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio02.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio02.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio02.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio02.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio02.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio02.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio02.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio02.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio02.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio02.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
            },
   'bio3': {
      'title': 'Isothermality',
      'description': 'Isothermality (bio2/bio7) (* 100)',
      'valunits': 'dimensionless',
      'keywords': ('bioclim','temperature', 'isothermality'),
      'files': {'clim.current.bio03.tif': ['observed'],
                'clim.ac4550.bio03.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio03.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio03.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio03.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio03.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio03.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio03.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio03.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio03.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio03.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio03.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio03.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio03.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio03.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']}
            },
   'bio4': {
      'title': 'Temperature Seasonality',
      'description': 'Temperature Seasonality (standard deviation *100)',
      'valunits': 'standardDeviationTimes100',
      'keywords': ('bioclim','temperature', 'seasonality'),
      'files': {'clim.current.bio04.tif': ['observed'],
                'clim.ac4550.bio04.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio04.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio04.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio04.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio04.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio04.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio04.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio04.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio04.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio04.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio04.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio04.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio04.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio04.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
            },
   'bio5': {
      'title': 'Max Temperature of Warmest Month',
      'description': 'Max Temperature of Warmest Month',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'max', 'warmest month'),
      'files': {'clim.current.bio05.tif': ['observed'],
                'clim.ac4550.bio05.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio05.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio05.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio05.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio05.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio05.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio05.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio05.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio05.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio05.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio05.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio05.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio05.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio05.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
            },
   'bio6': {
      'title': 'Min Temperature of Coldest Month',
      'description': 'Min Temperature of Coldest Month',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','min', 'temperature', 'coldest month'),
      'files': {'clim.current.bio06.tif': ['observed'],
                'clim.ac4550.bio06.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio06.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio06.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio06.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio06.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio06.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio06.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio06.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio06.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio06.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio06.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio06.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio06.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio06.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
            },
   'bio7': {
      'title': 'Temperature Annual Range',
      'description': 'Temperature Annual Range (bio5-bio6)',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'range'),
      'files': {'clim.current.bio07.tif': ['observed'],
                'clim.ac4507.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio07.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio07.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio07.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio07.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio07.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio07.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio07.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio07.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio07.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio07.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio07.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio07.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio07.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
            },
   'bio8': {
      'title': 'Mean Temperature of Wettest Quarter',
      'description': 'Mean Temperature of Wettest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'wettest quarter'),
      'files': {'clim.current.bio08.tif': ['observed'],
                'clim.ac4550.bio08.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio08.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio08.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio08.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio08.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio08.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio08.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio08.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio08.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio08.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio08.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio08.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio08.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio08.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
            },
   'bio9': {
      'title': 'Mean Temperature of Driest Quarter',
      'description': 'Mean Temperature of Driest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'driest quarter'),
      'files': {'clim.current.bio09.tif': ['observed'],
                'clim.ac4550.bio09.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio09.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio09.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio09.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio09.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio09.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio09.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio09.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio09.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio09.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio09.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio09.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio09.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio09.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
            },
   'bio10': {
      'title': 'Mean Temperature of Warmest Quarter',
      'description': 'Mean Temperature of Warmest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'warmest quarter'),
      'files': {'clim.current.bio10.tif': ['observed'],
                'clim.ac4550.bio10.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio10.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio10.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio10.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio10.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio10.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio10.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio10.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio10.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio10.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio10.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio10.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio10.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio10.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio11': {
      'title': 'Mean Temperature of Coldest Quarter',
      'description': 'Mean Temperature of Coldest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'coldest quarter'),
      'files': {'clim.current.bio11.tif': ['observed'],
                'clim.ac4550.bio11.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio11.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio11.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio11.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio11.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio11.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio11.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio11.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio11.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio11.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio11.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio11.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio11.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio11.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio12': {
      'title': 'Annual Precipitation',
      'description': 'Annual Precipitation',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'annual'),
      'files': {'clim.current.bio12.tif': ['observed'],
                'clim.ac4550.bio12.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio12.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio12.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio12.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio12.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio12.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio12.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio12.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio12.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio12.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio12.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio12.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio12.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio12.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio13': {
      'title': 'Precipitation of Wettest Month',
      'description': 'Precipitation of Wettest Month',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'wettest month'),
      'files': {'clim.current.bio13.tif': ['observed'],
                'clim.ac4550.bio13.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio13.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio13.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio13.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio13.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio13.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio13.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio13.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio13.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio13.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio13.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio13.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio13.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio13.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio14': {
      'title': 'Precipitation of Driest Month',
      'description': 'Precipitation of Driest Month',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'driest month'),
      'files': {'clim.current.bio14.tif': ['observed'],
                'clim.ac4550.bio14.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio14.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio14.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio14.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio14.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio14.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio14.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio14.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio14.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio14.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio14.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio14.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio14.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio14.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio15': {
      'title': 'Precipitation Seasonality',
      'description': 'Precipitation Seasonality (Coefficient of Variation)',
      'valunits': 'coefficientOfVariation',
      'keywords': ('bioclim','precipitation', 'seasonality'),
      'files': {'clim.current.bio15.tif': ['observed'],
                'clim.ac4550.bio15.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio15.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio15.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio15.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio15.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio15.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio15.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio15.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio15.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio15.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio15.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio15.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio15.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio15.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio16': {
      'title': 'Precipitation of Wettest Quarter',
      'description': 'Precipitation of Wettest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'wettest quarter'),
      'files': {'clim.current.bio16.tif': ['observed'],
                'clim.ac4550.bio16.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio16.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio16.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio16.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio16.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio16.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio16.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio16.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio16.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio16.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio16.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio16.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio16.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio16.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio17': {
      'title': 'Precipitation of Driest Quarter',
      'description': 'Precipitation of Driest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'driest quarter'),
      'files': {'clim.current.bio17.tif': ['observed'],
                'clim.ac4550.bio17.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio17.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio17.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio17.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio17.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio17.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio17.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio17.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio17.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio17.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio17.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio17.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio17.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio17.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio18': {
      'title': 'Precipitation of Warmest Quarter',
      'description': 'Precipitation of Warmest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'warmest quarter'),
      'files': {'clim.current.bio18.tif': ['observed'],
                'clim.ac4550.bio18.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio18.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio18.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio18.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio18.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio18.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio18.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio18.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio18.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio18.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio18.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio18.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio18.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio18.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'bio19': {
      'title': 'Precipitation of Coldest Quarter',
      'description': 'Precipitation of Coldest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'coldest quarter'),
      'files': {'clim.current.bio19.tif': ['observed'],
                'clim.ac4550.bio19.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2041-2060'],
                'clim.ac4570.bio19.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2061-2080'],
                'clim.gs4550.bio19.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2041-2060'],
                'clim.gs4570.bio19.tif': ['AR5', 'GISS-E2-R', 'RCP4.5', '2061-2080'],
                'clim.gd4550.bio19.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2041-2060'],
                'clim.gd4570.bio19.tif': ['AR5', 'GFDL-ESM2G', 'RCP4.5', '2061-2080'],
                'clim.he2650.bio19.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2041-2060'],
                'clim.he2670.bio19.tif': ['AR5', 'HadGEM2-ES', 'RCP2.6', '2061-2080'],
                'clim.he4550.bio19.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2041-2060'],
                'clim.he4570.bio19.tif': ['AR5', 'HadGEM2-ES', 'RCP4.5', '2061-2080'],
                'clim.he6050.bio19.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2041-2060'],
                'clim.he6070.bio19.tif': ['AR5', 'HadGEM2-ES', 'RCP6.0', '2061-2080'],
                'clim.he8550.bio19.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2041-2060'],
                'clim.he8570.bio19.tif': ['AR5', 'HadGEM2-ES', 'RCP8.5', '2061-2080']},
             },
   'gmted2010.elv_mean': {
      'title': 'Global Multi-resolution Terrain Elevation Data 2010',
      'description': 'mean elevation',
      'valunits': 'm',
      'keywords': ('elevation','geospatial','environment'),
      'files': {'env.gmted2010.elev_mean.tif': ['observed', 'AR5']}
      },
   't_clay': {
      'title': 'clay fraction',
      'description': '% clay fraction',
      'valunits': 'percentage',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_clay.tif': ['observed', 'AR5']}
      },
      't_gravel': {
      'title': 'gravel content',
      'description': '% volume of gravel content',
      'valunits': 'percentage',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_gravel.tif': ['observed', 'AR5']}
      },
   't_sand': {
      'title': 'sand fraction',
      'description': '% sand fraction',
      'valunits': 'percentage',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_sand.tif': ['observed', 'AR5']}
      },   
   't_silt': {
      'title': 'silt fraction',
      'description': '% silt fraction',
      'valunits': 'percentage',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_silt.tif': ['observed', 'AR5']}
      },
   't_oc': {
      'title': 'organic carbon',
      'description': '% weight',
      'valunits': 'percentage',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_oc.tif': ['observed', 'AR5']}
   },
   't_ph': {
      'title': 'pH',
      'description': 'log[H+]',
      'valunits': 'pH',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_clay': ['observed', 'AR5']}
   },
      't_cec': {
      'title': 'cation exchange capacity',
      'description': 'cmol/kg',
      'valunits': 'cmol/kg',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_cecsoil': ['observed', 'AR5']}
   },
      't_caco3': {
      'title': 'Calcium Carbonate',
      'description': '% weight',
      'valunits': 'percentage',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_caco3.tif': ['observed', 'AR5']}
   },
      't_caso4': {
      'title': 'Gypsium',
      'description': '% weight',
      'valunits': 'percentage',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_caso4.tif': ['observed', 'AR5']}
   },
      't_ece': {
      'title': 'Salinity',
      'description': 'salt content',
      'valunits': 'dS/m',
      'keywords': ('soil','geospatial','environment'),
      'files': {'env.hwsd.t_ece.tif': ['observed', 'AR5']}
   },
   'svd.pc1': {
      'title': 'spatial distance vector 1',
      'description': 'PC1 of distance matrix',
      'valunits': 'pc',
      'keywords': ('spatial','distance'),
      'files': {'spat.svd.pc1': ['observed', 'AR5']}
   },
   'svd.pc2': {
      'title': 'spatial distance vector 2',
      'description': 'PC2 of distance matrix',
      'valunits': 'pc',
      'keywords': ('spatial','distance'),
      'files': {'spat.svd.pc2': ['observed', 'AR5']}
   },
   'svd.pc3': {
      'title': 'spatial distance vector 3',
      'description': 'PC3 of distance matrix',
      'valunits': 'pc',
      'keywords': ('spatial','distance'),
      'files': {'spat.svd.pc3': ['observed', 'AR5']}
   } 
}}

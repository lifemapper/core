from osgeo import gdalconst
# .............................................................................
# Default Overrides
# .............................................................................
# The following section contains constants, which if None or missing, 
# will default to constants in config.lmserver.ini
# ...............................................

# These must be valid ALGORITHM_CODES in the Lifemapper database; 
# Default = ALGORITHMS
#ALGORITHM_CODES = None

# Default = SCENARIO_PACKAGE_EPSG
EPSG = 4326
# Default = SCENARIO_PACKAGE_MAPUNITS IFF EPSG == SCENARIO_PACKAGE_EPSG
MAPUNITS = 'dd'

# .............................................................................
# Required
# .............................................................................
# The following section contains required constants, which describe this data 
# package
# ...............................................
# For remote data, cannot read to get this 
ENVLYR_GDALTYPE = gdalconst.GDT_Int16
ENVLYR_GDALFORMAT = 'GTiff'

RESOLUTIONS = {'10min': 0.16667, '5min': 0.083333, '30sec': 0.0083333}
CLIMATE_KEYWORDS = ['bioclimatic variables', 'climate', 'elevation']

OBSERVED_PREDICTED_META = {
   'observed': {
      # Name for climate scenario
      'name': 'Worldclim1.4+Soil+SpatialDistance', 
      'keywords': ['observed', 'present'], 
      # Year range (low, high) that these data represent
      'times': {'Curr': {'name': '1950-2000'}},
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
             'times': {'mid': {'name': 'Mid Holocene (~6K years ago)'}, 
                       'lgm': {'name': 'Last Glacial Maximium (~22K years ago)'}},
             'models': {'CCSM4':
                        {'name': 'Community Climate System Model, 4.0',
                         'shortcode': 'cc',
                         'author': 'National Center for Atmospheric Research (NCAR) http://www.cesm.ucar.edu/models/ccsm4.0/'
                         }},
             'alternatePredictions': None },
   # Future Report
   'AR5': { 
      # Name of climate report
      'name': 'IPCC Fifth Assessment Report (2014)', 
      'keywords': ['predicted', 'future'],
      'times': {'2050': {'name': '2041-2060'}, 
                '2070': {'name': '2061-2080'}},
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
                     },
       # AKA cc
       'CCSM4': {'name': 'Community Climate System Model, 4.0',
                 'shortcode': 'cc',
                 'author': 'National Center for Atmospheric Research (NCAR) http://www.cesm.ucar.edu/models/ccsm4.0/'}},            
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

CLIMATE_PACKAGES = {
   '10min-past-present-future':
   # past: CCSM4-lgm-10min,CCSM4-mid-10min
   # current: WC-10min
   # future: CCSM4-lgm-10min,CCSM4-mid-10min,CCSM4-RCP8.5-2070-10min,CCSM4-RCP4.5-2070-10min,CCSM4-RCP8.5-2050-10min,CCSM4-RCP4.5-2050-10min
      {'res': '10min',
       'bbox': [-180, -60, 180, 90],
       # BASELINE_DATA code
       'baseline': 'observed',
       # {REPORT: [(GCM, Time, RCP(opt)), (model, Time, RCP(opt)) ...]}
       'predicted': {'AR5': [('CCSM4', '2050', 'RCP4.5'),
                             ('CCSM4', '2070', 'RCP4.5'), 
                             ('CCSM4', '2050', 'RCP8.5'),
                             ('CCSM4', '2070', 'RCP8.5')],
                     'CMIP5': [('CCSM4', 'mid'),
                               ('CCSM4', 'lgm')]},
       'layertypes': ['alt', 
                      'bio1', 'bio2', 'bio3', 'bio4', 'bio5', 'bio6', 'bio7', 
                      'bio8', 'bio9', 'bio10', 'bio11', 'bio12', 'bio13', 'bio14', 
                      'bio15', 'bio16', 'bio17', 'bio18', 'bio19'],
       # Append to all scenario codes
       'suffix': None} }

LAYERTYPE_META = {
   'bio1': {
      'title': 'Annual Mean Temperature',
      'description': 'Annual Mean Temperature',
      'valunits':'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'annual'),
      'files': {'10min/worldclim1.4/bio1.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi1.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi1.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi501.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi701.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi501.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi701.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
            },
   'bio2': {
      'title': 'Mean Diurnal Range',
      'description': 'Mean Diurnal Range (Mean of monthly (max temp - min temp))',
      'valunits':'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'range'),
      'files': {'10min/worldclim1.4/bio2.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi2.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi2.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi502.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi702.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi502.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi702.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
            },
   'bio3': {
      'title': 'Isothermality',
      'description': 'Isothermality (bio2/bio7) (* 100)',
      'valunits': 'dimensionless',
      'keywords': ('bioclim','temperature', 'isothermality'),
      'files': {'10min/worldclim1.4/bio3.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi3.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi3.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi503.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi703.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi503.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi703.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']}
            },
   'bio4': {
      'title': 'Temperature Seasonality',
      'description': 'Temperature Seasonality (standard deviation *100)',
      'valunits': 'standardDeviationTimes100',
      'keywords': ('bioclim','temperature', 'seasonality'),
      'files': {'10min/worldclim1.4/bio4.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi4.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi4.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi504.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi704.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi504.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi704.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
            },
   'bio5': {
      'title': 'Max Temperature of Warmest Month',
      'description': 'Max Temperature of Warmest Month',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'max', 'warmest month'),
      'files': {'10min/worldclim1.4/bio5.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi5.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi5.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi505.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi705.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi505.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi705.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
            },
   'bio6': {
      'title': 'Min Temperature of Coldest Month',
      'description': 'Min Temperature of Coldest Month',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','min', 'temperature', 'coldest month'),
      'files': {'10min/worldclim1.4/bio6.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi6.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi6.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi506.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi706.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi506.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi706.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
            },
   'bio7': {
      'title': 'Temperature Annual Range',
      'description': 'Temperature Annual Range (bio5-bio6)',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'range'),
      'files': {'10min/worldclim1.4/bio7.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi7.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi7.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi507.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi707.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi507.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi707.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
            },
   'bio8': {
      'title': 'Mean Temperature of Wettest Quarter',
      'description': 'Mean Temperature of Wettest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'wettest quarter'),
      'files': {'10min/worldclim1.4/bio8.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi8.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi8.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi508.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi708.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi508.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi708.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
            },
   'bio9': {
      'title': 'Mean Temperature of Driest Quarter',
      'description': 'Mean Temperature of Driest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'driest quarter'),
      'files': {'10min/worldclim1.4/bio9.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi9.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi9.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi509.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi709.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi509.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi709.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
            },
   'bio10': {
      'title': 'Mean Temperature of Warmest Quarter',
      'description': 'Mean Temperature of Warmest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'warmest quarter'),
      'files': {'10min/worldclim1.4/bio10.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi10.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi10.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5010.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7010.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5010.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7010.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio11': {
      'title': 'Mean Temperature of Coldest Quarter',
      'description': 'Mean Temperature of Coldest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'coldest quarter'),
      'files': {'10min/worldclim1.4/bio11.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi11.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi11.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5011.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7011.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5011.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7011.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio12': {
      'title': 'Annual Precipitation',
      'description': 'Annual Precipitation',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'annual'),
      'files': {'10min/worldclim1.4/bio12.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi12.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi12.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5012.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7012.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5012.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7012.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio13': {
      'title': 'Precipitation of Wettest Month',
      'description': 'Precipitation of Wettest Month',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'wettest month'),
      'files': {'10min/worldclim1.4/bio13.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi13.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi13.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5013.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7013.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5013.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7013.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio14': {
      'title': 'Precipitation of Driest Month',
      'description': 'Precipitation of Driest Month',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'driest month'),
      'files': {'10min/worldclim1.4/bio14.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi14.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi14.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5014.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7014.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5014.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7014.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio15': {
      'title': 'Precipitation Seasonality',
      'description': 'Precipitation Seasonality (Coefficient of Variation)',
      'valunits': 'coefficientOfVariation',
      'keywords': ('bioclim','precipitation', 'seasonality'),
      'files': {'10min/worldclim1.4/bio15.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi15.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi15.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5015.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7015.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5015.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7015.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio16': {
      'title': 'Precipitation of Wettest Quarter',
      'description': 'Precipitation of Wettest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'wettest quarter'),
      'files': {'10min/worldclim1.4/bio16.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi16.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi16.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5016.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7016.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5016.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7016.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio17': {
      'title': 'Precipitation of Driest Quarter',
      'description': 'Precipitation of Driest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'driest quarter'),
      'files': {'10min/worldclim1.4/bio17.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi17.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi17.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5017.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7017.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5017.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7017.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio18': {
      'title': 'Precipitation of Warmest Quarter',
      'description': 'Precipitation of Warmest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'warmest quarter'),
      'files': {'10min/worldclim1.4/bio18.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi18.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi18.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5018.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7018.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5018.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7018.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'bio19': {
      'title': 'Precipitation of Coldest Quarter',
      'description': 'Precipitation of Coldest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'coldest quarter'),
      'files': {'10min/worldclim1.4/bio19.tif': ['observed'],
                '10min/CCSM4/mid/ccmidbi19.tif': ['CMIP5', 'CCSM4', 'mid'],
                '10min/CCSM4/lgm/cclgmbi19.tif': ['CMIP5', 'CCSM4', 'lgm'],
                '10min/CCSM4/2050/RCP4.5/cc45bi5019.tif': ['AR5', 'CCSM4', 'RCP4.5', '2050'],
                '10min/CCSM4/2070/RCP4.5/cc45bi7019.tif': ['AR5', 'CCSM4', 'RCP4.5', '2070'],
                '10min/CCSM4/2050/RCP8.5/cc85bi5019.tif': ['AR5', 'CCSM4', 'RCP8.5', '2050'],
                '10min/CCSM4/2070/RCP8.5/cc85bi7019.tif': ['AR5', 'CCSM4', 'RCP8.5', '2070']},
             },
   'alt': {
      'title': 'Elevation',
      'description': 'Worldclim Elevation (altitude above sea level, from SRTM, http://www2.jpl.nasa.gov/srtm/)',
      'valunits': 'meters',
      'keywords': ['elevation'],
      'files': {'10min/worldclim1.4/alt.tif': ['observed', 'AR5', 'CMIP5']}}}
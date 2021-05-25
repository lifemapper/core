"""
These constants represent metadata for one or more Lifemapper environmental
data packages.  New climate data packages should be created in a compressed
file named <scenPackageName>.tar.gz in the ENV_DATA_PATH directory.

Uncompressing the package should create <scenPackageName>.py and
<scenPackageName>.csv at the topmost level; data files should be under the
'topdir', specified in the <scenPackageName> entry of the CLIMATE_PACKAGES
dictionary. Filenames for each layertype are specified in the LAYERTYPE_DATA
dictionary, and should be their relative path and filename (under <topdir>).

@note: LAYERTYPE_DATA
Each entry should have a title, description, value units, keywords, and files
title: The title of this layer type.  Examples: Elevation, Annual Mean
    Temperature
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
"""
from osgeo import gdalconst

# User should be a dictionary with 'id' and 'email' keys.
# If this is None, data is assigned to the PUBLIC_USER
USER = None

# SPECIES_DATA is the basename, without path, of the of the .csv file
# containing and .meta file describing species data for this archive/experiment
SPECIES_DATA = None
MIN_POINT_COUNT = 20

# These must be valid ALGORITHM_CODES in the Lifemapper database;
# If this is None, data is assigned to the DEFAULT_ALGORITHMS
ALGORITHM_CODES = None

# Constants for all CLIMATE_PACKAGES below
EPSG = 4326
ENVLYR_GDALTYPE = gdalconst.GDT_Int16
ENVLYR_GDALFORMAT = 'GTiff'
MAPUNITS = 'dd'
ENV_KEYWORDS = ['bioclimatic variables', 'climate', 'elevation']

# Square 4, Hexagon 6
GRID_NUM_SIDES = 4
GRID_NAME = '10km-grid'
GRID_CELLSIZE = 10000

RESOLUTIONS = {'10min': 0.16667, '5min': 0.083333, '30sec': 0.0083333}

# Chosen package(s) in CLIMATE_PACKAGES to be pulled.  OBSERVED_PREDICTED_META
# and LAYERTYPE_META have specifics about the chosen data or predicted data
CLIMATE_PACKAGES = {
    '10min-past-present-future': {
        'res': '10min',
        'bbox': [-180, -60, 180, 90],
        # 'baseline': 'observed',
        # {REPORT: [(GCM, Time, RCP(opt)), (model, Time, RCP(opt)) ...]}
        'predicted': {'AR5': [('CCSM4', '2050', 'RCP4.5'),
                              ('CCSM4', '2070', 'RCP4.5'),
                              ('CCSM4', '2050', 'RCP8.5'),
                              ('CCSM4', '2070', 'RCP8.5')]},
        'layertypes': ['bio1', 'bio2', 'bio3', 'bio4', 'bio5', 'bio6', 'bio7',
                       'bio8', 'bio9', 'bio10', 'bio11', 'bio12', 'bio13',
                       'bio14', 'bio15', 'bio16', 'bio17', 'bio18', 'bio19',
                       'alt'],
        # Append to all scenario codes if needed for identification
        # (i.e. of a region or other constraint)
        'suffix': None}
}

# # Required keys in SDM_MASK_INPUT: name, bbox, gdaltype, gdalformat, file
SDM_MASK_INPUT = {
    'name': 'ecoreg_10min_global',
    'bbox': (-180.0, -90.0, 180.0, 83.0),
    'gdaltype': ENVLYR_GDALTYPE,
    'gdalformat': ENVLYR_GDALFORMAT,
    'file': 'ecoreg_10min_global.tif',
    'title': 'Generalized, Rasterized TNC Terrestrial Ecoregions, 10min',
    'author': 'The Nature Conservancy',
    'isCategorical': True,
    'description': (
        'Global Ecoregions, Major Habitat Types,Biogeographical Realms and '
        'The Nature Conservancy Terrestrial Assessment Units as of December '
        '14, 2009 Purpose: Developed originally by Olson, D. M. and '
        'E. Dinerstein (2002), Bailey (1995) and Environment Canada (Wiken,'
        '1986), these data layers were modified by The Nature Conservancy '
        '(TNC) to be used in its Biodiversity Planning exercises in the '
        'process known as Ecoregional Assessments. Several Ecoregions were '
        'modified from the originals by TNC staff developing the '
        'aforementioned assessments. The modifications are based on '
        'ecological, bio-physical and political rationales; most changes are '
        'noted in the accompanying documentation (attributes). Ecoregions in '
        'Canada and Mexico were modified mainly at the border with US '
        'territory, where TNC modified-Bailey (1995) ecoregions crossed over '
        'the country boundaries and the Olson, D. M. and E. Dinerstein (2002) '
        'and (Wiken, 1986) were replaced where the TNC modified-Bailey (1995) '
        'overlayed them. This layer was split from the terrestrial '
        'ecoregional assessment layer in June 2008.'),
    'keywords': [
        'Terrestrial Ecoregions', 'Major Habitat Types',
        'Biogeographic Realms', 'TNC', 'World', 'Global'],
    'url': 'http://maps.tnc.org',
    'citation': (
        'Olson, D. M. and E. Dinerstein. 2002. The Global 200: Priority '
        'ecoregions for global conservation. (PDF file) Annals of the',
        'Missouri Botanical Garden 89:125-126. -The Nature Conservancy, USDA '
        'Forest Service and U.S. Geological Survey, based on Bailey, Robert G.'
        ' 1995. Description of the ecoregions of the United States (2nd ed.). '
        'Misc. Pub. No. 1391, Map scale 1:7,500,000. USDA Forest Service. '
        '108pp. -The Nature Conservancy (2003), based on Wiken, E.B.'
        '(compiler). 1986. Terrestrial ecozones of Canada. Ecological Land '
        'Classification Series No. 19. Environment Canada, Hull, Que. '
        '26 pp. + map.')
    }

# Metadata for possible data - ONLY that specified by 'baseline' and
#    'predicted' in the chosen package in CLIMATE_PACKAGES will be pulled.
OBSERVED_PREDICTED_META = {
    # Top keys are 'baseline' for modeling scenario, or GCM report (past CMIP,
    #     future IPCC)
    'baseline': {
        # append res and suffix for scenariococe
        'code': 'observed',
        'name': 'Worldclim 1.4',
        'keywords': ['observed', 'present'],
        'times': {'Curr': {'name': '1950-2000'}},
        'title': 'Worldclim 1.4',
        'author': (
            'Hijmans, R.J., S.E. Cameron, J.L. Parra, P.G. Jones and '
            'A. Jarvis, 2005. Very high resolution interpolated climate '
            'surfaces for global land areas. International Journal of '
            'Climatology 25: 1965-1978'),
        'description': (
            'WorldClim 1.4 elevation and bioclimatic variables computed from '
            'interpolated observation data collected between 1950 and 2000 '
            '(http://www.worldclim.org/)')
    },
    # Past Report
    'CMIP5': {
        'name': 'Coupled Model Intercomparison Project Phase 5',
        'keywords': ['predicted', 'past'],
        'times': {
            'mid': {
                'name': 'Mid Holocene (~6K years ago)'
                },
            'lgm': {'name': 'Last Glacial Maximium (~22K years ago)'}
            },
        'models': {
            'CCSM4': {
                'name': 'Community Climate System Model, 4.0',
                'shortcode': 'cc',
                'author': (
                    'National Center for Atmospheric Research (NCAR) '
                    'http://www.cesm.ucar.edu/models/ccsm4.0/')
                }
            },
        'alternatePredictions': None
        },
    # Future Report
    'AR5': {
        # Name of climate report
        'name': 'IPCC Fifth Assessment Report (2014)',
        'keywords': ['predicted', 'future'],
        'times': {
            '2050': {'name': '2041-2060'},
            '2070': {'name': '2061-2080'}
            },
        # Below are optional models, only those specified in
        'models':
        {
            'CCSM4': {
                'name': 'Community Climate System Model, 4.0',
                'shortcode': 'cc',
                'author': (
                    'National Center for Atmospheric Research (NCAR) '
                    'http://www.cesm.ucar.edu/models/ccsm4.0/')
                },
            'HadGEM2-ES': {
                'name': (
                    'Hadley Centre Global Environment Model '
                    'v2 - Earth System'),
                # Short code for GCM model, for filenames
                'shortcode': 'he',
                'author': (
                    'Collins, W.J., N. Bellouin, M. Doutriaux-Boucher, '
                    'N. Gedney, T. Hinton, C. D. Jones, S. Liddicoat, G. '
                    'Martin, F. OConnor, J. Rae, C. Senior, I. Totterdell, S.'
                    ' Woodward, T. Reichler, J. Kim, 2008: Evaluation of the '
                    'HadGEM2 model. Met Office Hadley Centre Technical Note '
                    'no. HCTN 74, available from Met Office, FitzRoy Road, '
                    'Exeter EX1 3PB '
                    'http://www.metoffice.gov.uk/publications/HCTN/index.html'
                    )
                },
            'GISS-E2-R': {
                'name': 'NASA GISS GCM ModelE',
                # Short code for GCM model, for filenames
                'shortcode': 'gs',
                'author': (
                    'Nazarenko, L., G.A. Schmidt, R.L. Miller, N. Tausnev, '
                    'M. Kelley, R. Ruedy, G.L. Russell, I. Aleinov, M. Bauer, '
                    'S. Bauer, R. Bleck, V. Canuto, Y. Cheng, T.L. Clune, A.D.'
                    ' Del Genio, G. Faluvegi, J.E. Hansen, R.J. Healy, N.Y. '
                    'Kiang, D. Koch, A.A. Lacis, A.N. LeGrande, J. '
                    'Lerner, K.K. Lo, S. Menon, V. Oinas, J.P. Perlwitz, M.J. '
                    'Puma, D. Rind, A. Romanou, M. Sato, D.T. Shindell, S. '
                    'Sun, K. Tsigaridis, N. Unger, A. Voulgarakis, M.-S. Yao, '
                    'and J. Zhang, 2015: Future climate change under RCP '
                    'emission scenarios with GISS ModelE2. J. Adv. Model. '
                    'Earth Syst., early on-line, doi:10.1002/2014MS000403, '
                    'http://data.giss.nasa.gov/modelE/ar5/')
                },
            'ACCESS1-0': {
                'name': (
                    'ACCESS1-0 CSIRO, Commonwealth Scientific and Industrial '
                    'Research Organisation, Australia'),
                # Short code for GCM model, for filenames
                'shortcode': 'ac',
                'author': (
                    'CSIRO, Commonwealth Scientific and Industrial Research '
                    'Organisation, Australia, '
                    'https://confluence.csiro.au/display/ACCESS/Home')
                },
            'GFDL-ESM2G': {
                'name': (
                    'Geophysical Fluid Dynamics Laboratory - Earth System '
                    'Models 2G'),
                # Short code for GCM model, for filenames
                'shortcode': 'gd',
                'author': (
                    'Dunne, John P., et al. "GFDLs ESM2 global coupled '
                    'climate-carbon Earth System Models. Part I: Physical '
                    'formulation and baseline simulation characteristics." '
                    'Journal of Climate 25.19 (2012): 6646-6665, '
                    'http://www.gfdl.noaa.gov/earth-system-model')
                }
            },
        'alternatePredictions': {
            # Representative Concentration Pathways code, used in scenario
            #     code and dir structure
            'RCP2.6': {
                'shortcode': '26',
                'keywords': [
                    'radiative forcing +2.6',
                    'likely temperature increase 0.3 to 1.7 C by 2081-2100']
                },
            'RCP4.5': {
                'shortcode': '45',
                'keywords': [
                    'radiative forcing +4.5',
                    'likely temperature increase 1.1 to 2.6 C by 2081-2100']
                },
            'RCP6.0': {
                'shortcode': '60',
                'keywords': [
                    'radiative forcing +6.0',
                    'likely temperature increase 1.4 to 3.1 C by 2081-2100']
                },
            'RCP8.5': {
                'shortcode': '85',
                'keywords': [
                    'radiative forcing +8.5',
                    'likely temperature increase 2.6 to 4.8 C by 2081-2100']
                }
            }
        }
    }

# Metadata for possible data - ONLY that specified by 'layertypes' in the
# chosen package in CLIMATE_PACKAGES will be pulled.
# Files contain relative file path, plus a list of either:
#     * the baseline code, or
#     * for GCM predictions, a combination of the
#        Report, Model, Times, and AlternatePrediction
LAYERTYPE_META = {
    'bio1': {
        'title': 'Annual Mean Temperature',
        'description': 'Annual Mean Temperature',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'temperature', 'mean', 'annual'),
        'files': {
            'worldclim1.4/bio1.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi501.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi701.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi501.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi701.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio2': {
        'title': 'Mean Diurnal Range',
        'description':
            'Mean Diurnal Range (Mean of monthly (max temp - min temp))',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'temperature', 'range'),
        'files': {
            'worldclim1.4/bio2.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi502.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi702.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi502.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi702.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio3': {
        'title': 'Isothermality',
        'description': 'Isothermality (bio2/bio7) (* 100)',
        'valunits': 'dimensionless',
        'keywords': ('bioclim', 'temperature', 'isothermality'),
        'files': {
            'worldclim1.4/bio03.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi503.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi703.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi503.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi703.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']}
        },
    'bio4': {
        'title': 'Temperature Seasonality',
        'description': 'Temperature Seasonality (standard deviation *100)',
        'valunits': 'standardDeviationTimes100',
        'keywords': ('bioclim', 'temperature', 'seasonality'),
        'files': {
            'worldclim1.4/bio04.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi504.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi704.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi504.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi704.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio5': {
        'title': 'Max Temperature of Warmest Month',
        'description': 'Max Temperature of Warmest Month',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'temperature', 'max', 'warmest month'),
        'files': {
            'worldclim1.4/bio05.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi505.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi705.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi505.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi705.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio6': {
        'title': 'Min Temperature of Coldest Month',
        'description': 'Min Temperature of Coldest Month',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'min', 'temperature', 'coldest month'),
        'files': {
            'worldclim1.4/bio06.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi506.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi706.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi506.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi706.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio7': {
        'title': 'Temperature Annual Range',
        'description': 'Temperature Annual Range (bio5-bio6)',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'temperature', 'range'),
        'files': {
            'worldclim1.4/bio07.tif': ['observed'],
            'clim.ac4507.tif': ['AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi707.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi507.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi707.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio8': {
        'title': 'Mean Temperature of Wettest Quarter',
        'description': 'Mean Temperature of Wettest Quarter',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'temperature', 'mean', 'wettest quarter'),
        'files': {
            'worldclim1.4/bio08.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi508.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi708.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi508.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi708.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio9': {
        'title': 'Mean Temperature of Driest Quarter',
        'description': 'Mean Temperature of Driest Quarter',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'temperature', 'mean', 'driest quarter'),
        'files': {
            'worldclim1.4/bio09.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi509.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi709.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi509.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi709.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio10': {
        'title': 'Mean Temperature of Warmest Quarter',
        'description': 'Mean Temperature of Warmest Quarter',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'temperature', 'mean', 'warmest quarter'),
        'files': {
            'worldclim1.4/bio10.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5010.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7010.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5010.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7010.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio11': {
        'title': 'Mean Temperature of Coldest Quarter',
        'description': 'Mean Temperature of Coldest Quarter',
        'valunits': 'degreesCelsiusTimes10',
        'keywords': ('bioclim', 'temperature', 'mean', 'coldest quarter'),
        'files': {
            'worldclim1.4/bio11.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5011.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7011.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5011.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7011.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio12': {
        'title': 'Annual Precipitation',
        'description': 'Annual Precipitation',
        'valunits': 'mm',
        'keywords': ('bioclim', 'precipitation', 'annual'),
        'files': {
            'worldclim1.4/bio12.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5012.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7012.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5012.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7012.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio13': {
        'title': 'Precipitation of Wettest Month',
        'description': 'Precipitation of Wettest Month',
        'valunits': 'mm',
        'keywords': ('bioclim', 'precipitation', 'wettest month'),
        'files': {
            'worldclim1.4/bio13.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5013.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7013.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5013.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7013.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio14': {
        'title': 'Precipitation of Driest Month',
        'description': 'Precipitation of Driest Month',
        'valunits': 'mm',
        'keywords': ('bioclim', 'precipitation', 'driest month'),
        'files': {
            'worldclim1.4/bio14.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5014.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7014.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5014.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7014.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio15': {
        'title': 'Precipitation Seasonality',
        'description': 'Precipitation Seasonality (Coefficient of Variation)',
        'valunits': 'coefficientOfVariation',
        'keywords': ('bioclim', 'precipitation', 'seasonality'),
        'files': {
            'worldclim1.4/bio15.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5015.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7015.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5015.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7015.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio16': {
        'title': 'Precipitation of Wettest Quarter',
        'description': 'Precipitation of Wettest Quarter',
        'valunits': 'mm',
        'keywords': ('bioclim', 'precipitation', 'wettest quarter'),
        'files': {
            'worldclim1.4/bio16.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5016.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7016.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5016.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7016.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio17': {
        'title': 'Precipitation of Driest Quarter',
        'description': 'Precipitation of Driest Quarter',
        'valunits': 'mm',
        'keywords': ('bioclim', 'precipitation', 'driest quarter'),
        'files': {
            'worldclim1.4/bio17.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5017.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7017.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5017.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7017.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio18': {
        'title': 'Precipitation of Warmest Quarter',
        'description': 'Precipitation of Warmest Quarter',
        'valunits': 'mm',
        'keywords': ('bioclim', 'precipitation', 'warmest quarter'),
        'files': {
            'worldclim1.4/bio18.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5018.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7018.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5018.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7018.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'bio19': {
        'title': 'Precipitation of Coldest Quarter',
        'description': 'Precipitation of Coldest Quarter',
        'valunits': 'mm',
        'keywords': ('bioclim', 'precipitation', 'coldest quarter'),
        'files': {
            'worldclim1.4/bio19.tif': ['observed'],
            'CCSM4/2050/RCP4.5/cc45bi5019.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2050'],
            'CCSM4/2070/RCP4.5/cc45bi7019.tif': [
                'AR5', 'ACCESS1-0', 'RCP4.5', '2070'],
            'CCSM4/2050/RCP8.5/cc85bi5019.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2050'],
            'CCSM4/2070/RCP8.5/cc85bi7019.tif': [
                'AR5', 'HadGEM2-ES', 'RCP8.5', '2070']},
        },
    'alt': {
        'title': 'Worldclim 1.4 Elevation',
        'description': 'mean elevation',
        'valunits': 'm',
        'keywords': ['elevation'],
        'files': {'worldclim1.4/alt.tif': ['observed', 'AR5']}
        }
    }

"""
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research
 
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

"""
These constants represent metadata for Lifemapper-available climate data 
packages.  New climate data packages should be in a file named 
CLIMATE_PACKAGE.tar.gz with a 
"""
from osgeo import gdalconst
 
# For remote data, cannot read to get this 
ENVLYR_GDALTYPE = gdalconst.GDT_Int16
ENVLYR_GDALFORMAT = 'GTiff'
 
# Decimal degrees for EPSG 4326
RESOLUTIONS = {'10min': 0.16667, '5min': 0.083333, '30sec': 0.0083333}
CLIMATE_KEYWORDS = ['bioclimatic variables', 'climate', 'elevation']

# Observed climate data, from www.worldclim.org 
BASELINE_DATA = {
   'WC':
      {'name': 'Worldclim 1.4',
       # Layers from Baseline which are re-used in predicted future
       'staticLayerTypes': ['ALT'],
       'directory': 'worldclim1.4',
       'time': (1950, 2000),
       'code': 'WC',
       'title': 'Worldclim 1.4', 
       'author': ' Hijmans, R.J., S.E. Cameron, J.L. Parra, P.G. Jones and A. Jarvis, 2005. Very high resolution interpolated climate surfaces for global land areas. International Journal of Climatology 25: 1965-1978', 
       'description': 'WorldClim 1.4 elevation and bioclimatic variables computed from interpolated observation data collected between 1950 and 2000 (http://www.worldclim.org/), 5 min resolution',    # Add time
       'keywords': ['observed', 'present']}}
 
# Time periods available for worldclim data, AR5 future projections, 
# CMIP5 past projections, from www.worldclim.org 
TIME_PERIODS = {'mid': {'name': 'Mid Holocene (~ 6000 years ago)',
                         'startdate': None,
                         'enddate': None,
                        'keywords': ['predicted', 'past']}, 
                'lgm': {'name': 'Last Glacial Maximium (~ 22000 years ago)',
                         'startdate': None,
                         'enddate': None,
                        'keywords': ['predicted', 'past']},
                '2050': {'name': '2041-2060',
                         'startdate': 2041,
                         'enddate': 2060,
                         'keywords': ['predicted', 'future']}, 
                '2070': {'name': '2061-2080',
                         'startdate': 2061,
                         'enddate': 2080,
                         'keywords': ['predicted', 'future']} }
REPORTS = {
   # Past
   'CMIP5': {'name': 'Coupled Model Intercomparison Project Phase 5',              
             'model': {'code': 'CCSM4',
                       'name': 'Community Climate System Model, 4.0',
                       'shortcode': 'cc',
                       'author': 'National Center for Atmospheric Research (NCAR) http://www.cesm.ucar.edu/models/ccsm4.0/'},
           'scenarios': None },
   # Future
   'AR5': {'name': 'IPCC Fifth Assessment Report (2013)',
           'model': {'code': 'CCSM4',
                      'name': 'Community Climate System Model, 4.0',
                      'shortcode': 'cc',
                      'author': 'National Center for Atmospheric Research (NCAR) http://www.cesm.ucar.edu/models/ccsm4.0/'},
           'scenarios': 
           {# Representative Concentration Pathways
            # Filename pattern like 10min/CCSM4/2050/RCP8.5/cc85bi5019.tif: 
            #   res/mdlcode/timecode/rcpcode/mdlshort|rcpshort|bi|yr|var#|.tif
            'RCP2.6': {'shortcode': '26',
                       'keywords': ['radiative forcing +2.6',
                                 'likely temperature increase 0.3-1.7 C', ]
                   },
            'RCP4.5': {'shortcode': '45',
                       'keywords': ['radiative forcing +4.5', 
                                 'likely temperature increase 1.1-2.6 C',]
                   },
            'RCP6.0': {'shortcode': '60',
                       'keywords': ['radiative forcing +6.0',
                                 'likely temperature increase 1.4-3.1 C', 
                                 ]
                   },
            'RCP8.5': {'shortcode': '85',
                       'keywords': ['radiative forcing +8.5', 
                                 'likely temperature increase 2.6-4.8 C',
                                 'likely sea level increase 0.45-0.82 m'] } } } }

# Lifemapper-packaged data, produced by Worldclim, www.worldclim.org 
CLIMATE_PACKAGES = {
   '30sec-present-future-CONUS':
   # Continental United States, for BISON
   # current: WC-30sec-CONUS
   # future: CCSM4-RCP4.5-2050-30sec-CONUS,CCSM4-RCP4.5-2070-30sec-CONUS,CCSM4-RCP8.5-2050-30sec-CONUS,CCSM4-RCP8.5-2070-30sec-CONUS
      {'res': '30sec',
       'topdir': '30sec-CONUS',
       'bbox': [-125, 24, -66, 50],
       'helpme': None,
       # {REPORT: [Time,Time, ...]} 
       'past': {},
       # Name only
       'present': 'WC',
       # {REPORT: [(Scenario,Time), (Scenario,Time) ...]} 
       'future': {'AR5': [('RCP4.5', '2050'), ('RCP4.5', '2070'),
                          ('RCP8.5', '2050'), ('RCP8.5', '2070')]},
       # Append to all scenario codes
       'suffix': 'CONUS'},
   '30sec-present-future-SEA':
   # Southeast Asia, for PRAGMA
   # current: WC-30sec-SEA
   # future: CCSM4-RCP4.5-2050-30sec-SEA,CCSM4-RCP4.5-2070-30sec-SEA,CCSM4-RCP8.5-2050-30sec-SEA,CCSM4-RCP8.5-2070-30sec-SEA
      {'res': '30sec',
       'topdir': '30sec-SEA',
       'bbox': [95, -11, 153, 21],
       'helpme': None,
       # {REPORT: [Time,Time, ...]} 
       'past': {},
       # Name only
       'present': 'WC',
       # {REPORT: [(Scenario,Time), (Scenario,Time) ...]} 
       'future': {'AR5': [('RCP4.5', '2050'), ('RCP4.5', '2070'),
                          ('RCP8.5', '2050'), ('RCP8.5', '2070')]},
       # Append to all scenario codes
       'suffix': 'SEA'},
   '10min-past-present-future':
   # past: CCSM4-lgm-10min,CCSM4-mid-10min
   # current: WC-10min
   # future: CCSM4-lgm-10min,CCSM4-mid-10min,CCSM4-RCP8.5-2070-10min,CCSM4-RCP4.5-2070-10min,CCSM4-RCP8.5-2050-10min,CCSM4-RCP4.5-2050-10min
      {'res': '10min',
       'topdir': '10min',
       'bbox': [-180, -60, 180, 90],
       'helpme': None,
       # {REPORT: [Time,Time, ...]} 
       'past': {'CMIP5': ['mid', 'lgm']},
       # Name only (same as BASELINE_DATA 'code')
       'present': 'WC',
       # {REPORT: [(Scenario,Time), (Scenario,Time) ...]} 
       'future': {'AR5': [('RCP4.5', '2050'), ('RCP4.5', '2070'),
                          ('RCP8.5', '2050'), ('RCP8.5', '2070')]},
       # Append to all scenario codes
       'suffix': None},
   '5min-past-present-future':
   # past: CCSM4-lgm-5min,CCSM4-mid-5min
   # current: WC-5min
   # future: CCSM4-lgm-5min,CCSM4-mid-5min,CCSM4-RCP8.5-2070-5min,CCSM4-RCP4.5-2070-5min,CCSM4-RCP8.5-2050-5min,CCSM4-RCP4.5-2050-5min
      {'res': '5min',
       'topdir': '5min',
       'bbox': [-180, -60, 180, 90],
       'helpme': None,
       # {REPORT: [Time,Time, ...]} 
       'past': {'CMIP5': ['mid', 'lgm']},
       # Name only (same as BASELINE_DATA 'code')
       'present': 'WC',
       # {REPORT: [(Scenario,Time), (Scenario,Time) ...]} 
       'future': {'AR5': [('RCP4.5', '2050'), ('RCP4.5', '2070'),
                          ('RCP8.5', '2050'), ('RCP8.5', '2070')]},
       # Append to all scenario codes
       'suffix': None} }
                     
# Bioclimatic variable descriptions, originally defined by ANUCLIM, 
# created for these data by www.worldclim.org
LAYERTYPE_DATA = {'ALT': {'title': 'Elevation',
                          'description': 'Worldclim Elevation (altitude above sea level, from SRTM, http://www2.jpl.nasa.gov/srtm/)',
                          'valunits': 'meters',
                          'keywords': ['elevation']},
                  'BIO1': {'title': 'Annual Mean Temperature',
                           'description': 'Annual Mean Temperature',
                           'valunits':'degreesCelsiusTimes10',
                           'keywords': ('temperature', 'mean', 'annual')},
                  'BIO2': {'title': 'Mean Diurnal Range',
                           'description': 'Mean Diurnal Range (Mean of monthly (max temp - min temp))',
                           'valunits':'degreesCelsiusTimes10',
                           'keywords': ('temperature', 'range')},
                  'BIO3': {'title': 'Isothermality',
                           'description': 'Isothermality (BIO2/BIO7) (* 100)',
                           'valunits': 'dimensionless',
                           'keywords': ('temperature', 'isothermality')},
                  'BIO4': {'title': 'Temperature Seasonality',
                           'description': 'Temperature Seasonality (standard deviation *100)',
                           'valunits': 'standardDeviationTimes100',
                           'keywords': ('temperature', 'seasonality')},
                  'BIO5': {'title': 'Max Temperature of Warmest Month',
                           'description': 'Max Temperature of Warmest Month',
                           'valunits': 'degreesCelsiusTimes10',
                           'keywords': ('temperature', 'max', 'warmest month')},
                  'BIO6': {'title': 'Min Temperature of Coldest Month',
                           'description': 'Min Temperature of Coldest Month',
                           'valunits': 'degreesCelsiusTimes10',
                           'keywords': ('min', 'temperature', 'coldest month')},
                  'BIO7': {'title': 'Temperature Annual Range',
                           'description': 'Temperature Annual Range (BIO5-BIO6)',
                           'valunits': 'degreesCelsiusTimes10',
                           'keywords': ('temperature', 'range')},
                  'BIO8': {'title': 'Mean Temperature of Wettest Quarter',
                           'description': 'Mean Temperature of Wettest Quarter',
                           'valunits': 'degreesCelsiusTimes10',
                           'keywords': ('temperature', 'mean', 'wettest quarter')},
                  'BIO9': {'title': 'Mean Temperature of Driest Quarter',
                           'description': 'Mean Temperature of Driest Quarter',
                           'valunits': 'degreesCelsiusTimes10',
                           'keywords': ('temperature', 'mean', 'driest quarter')},
                  'BIO10': {'title': 'Mean Temperature of Warmest Quarter',
                           'description': 'Mean Temperature of Warmest Quarter',
                           'valunits': 'degreesCelsiusTimes10',
                           'keywords': ('temperature', 'mean', 'warmest quarter')},
                  'BIO11': {'title': 'Mean Temperature of Coldest Quarter',
                           'description': 'Mean Temperature of Coldest Quarter',
                           'valunits': 'degreesCelsiusTimes10',
                           'keywords': ('temperature', 'mean', 'coldest quarter')},
                  'BIO12': {'title': 'Annual Precipitation',
                           'description': 'Annual Precipitation',
                           'valunits': 'mm',
                           'keywords': ('precipitation', 'annual')},
                  'BIO13': {'title': 'Precipitation of Wettest Month',
                           'description': 'Precipitation of Wettest Month',
                           'valunits': 'mm',
                           'keywords': ('precipitation', 'wettest month')},
                  'BIO14': {'title': 'Precipitation of Driest Month',
                           'description': 'Precipitation of Driest Month',
                           'valunits': 'mm',
                           'keywords': ('precipitation', 'driest month')},
                  'BIO15': {'title': 'Precipitation Seasonality',
                           'description': 'Precipitation Seasonality (Coefficient of Variation)',
                           'valunits': 'coefficientOfVariation',
                           'keywords': ('precipitation', 'seasonality')},
                  'BIO16': {'title': 'Precipitation of Wettest Quarter',
                           'description': 'Precipitation of Wettest Quarter',
                           'valunits': 'mm',
                           'keywords': ('precipitation', 'wettest quarter')},
                  'BIO17': {'title': 'Precipitation of Driest Quarter',
                           'description': 'Precipitation of Driest Quarter',
                           'valunits': 'mm',
                           'keywords': ('precipitation', 'driest quarter')},
                  'BIO18': {'title': 'Precipitation of Warmest Quarter',
                           'description': 'Precipitation of Warmest Quarter',
                           'valunits': 'mm',
                           'keywords': ('precipitation', 'warmest quarter')},
                  'BIO19': {'title': 'Precipitation of Coldest Quarter',
                           'description': 'Precipitation of Coldest Quarter',
                           'valunits': 'mm',
                           'keywords': ('precipitation', 'coldest quarter')} }

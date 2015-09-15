""" 
    Module to store parameters used for testing multiple modules

    @status: alpha
    @author: Aimee Stewart
"""
# created by envreader.py
ENV_OUTPUT_FILENAME = 'envoutput.txt'

SCENARIO_DATA = {'CRU_CL_2.0':
                 {'datatype': 'ASCII GRID',
                  'extension': '.asc',
                  'files':
                 ('meanDiurnal', 
                 'meanDiurnalOverCoolestM', 
                 'meanDiurnalOverWarmestM', 
                 'meanFrostDays', 
                 'meanPrecip', 
                 'meanPrecipOverCoolestM', 
                 'meanPrecipOverCoolestQ', 
                 'meanPrecipOverDriestM', 
                 'meanPrecipOverDriestQ', 
                 'meanPrecipOverFrostFreeM', 
                 'meanPrecipOverWarmestM', 
                 'meanPrecipOverWarmestQ', 
                 'meanPrecipOverWettestM', 
                 'meanPrecipOverWettestQ', 
                 'meanTemp', 
                 'meanTempOverCoolestM', 
                 'meanTempOverCoolestQ', 
                 'meanTempOverFrostFreeM', 
                 'meanTempOverWarmestM', 
                 'meanTempOverWarmestQ', 
                 'meanWindSpeed', 
                 'stdevMeanPrecip', 
                 'stdevMeanTemp')},
                 
                 'hadley_a2_1961to1990':
                 {'datatype': 'ASCII GRID',
                  'extension': '.asc',
                  'files':
                 ('ftr_ann_rng_tmp', 
                      'ftr_hi_tmp_wm_mo', 
                      'ftr_lo_tmp_cool_mo', 
                      'ftr_mean_pcp', 
                      'ftr_mean_pcp_cool_mo', 
                      'ftr_mean_pcp_cool_qtr', 
                      'ftr_mean_pcp_dry_mo', 
                      'ftr_mean_pcp_dry_qtr', 
                      'ftr_mean_pcp_wet_mo', 
                      'ftr_mean_pcp_wet_qtr', 
                      'ftr_mean_pcp_wm_mo', 
                      'ftr_mean_pcp_wm_qtr', 
                      'ftr_mean_rad', 
                      'ftr_mean_rad_cool_mo', 
                      'ftr_mean_rad_cool_qtr', 
                      'ftr_mean_rad_dry_mo', 
                      'ftr_mean_rad_dry_qtr', 
                      'ftr_mean_rad_wet_mo', 
                      'ftr_mean_rad_wet_qtr', 
                      'ftr_mean_rad_wm_mo', 
                      'ftr_mean_rad_wm_qtr', 
                      'ftr_mean_stdev_pcp', 
                      'ftr_mean_stdev_tmp', 
                      'ftr_mean_tmp', 
                      'ftr_mean_tmp_cool_mo', 
                      'ftr_mean_tmp_cool_qtr', 
                      'ftr_mean_tmp_wm_mo', 
                      'ftr_mean_tmp_wm_qtr')},
       'CRU_CL_1.0':
                 {'datatype': 'ASCII GRID',
                  'extension': '.asc',
                  'files':
                 ('curr_ann_rng_tmp',
                         'curr_hi_tmp_wm_mo',
                         'curr_lo_tmp_cool_mo',
                         'curr_mean_diur',
                         'curr_mean_diur_cool_mo',
                         'curr_mean_diur_wm_mo',
                         'curr_mean_frst_dys',
                         'curr_mean_pcp',
                         'curr_mean_pcp_cool_mo',
                         'curr_mean_pcp_cool_qtr',
                         'curr_mean_pcp_dry_mo',
                         'curr_mean_pcp_dry_qtr',
                         'curr_mean_pcp_wet_mo',
                         'curr_mean_pcp_wet_qtr',
                         'curr_mean_pcp_wm_mo',
                         'curr_mean_pcp_wm_qtr',
                         'curr_mean_rad',
                         'curr_mean_rad_cool_mo',
                         'curr_mean_rad_cool_qtr',
                         'curr_mean_rad_dry_mo',
                         'curr_mean_rad_dry_qtr',
                         'curr_mean_rad_wet_mo',
                         'curr_mean_rad_wet_qtr',
                         'curr_mean_rad_wm_mo',
                         'curr_mean_rad_wm_qtr',
                         'curr_mean_stdev_pcp',
                         'curr_mean_stdev_tmp',
                         'curr_mean_tmp',
                         'curr_mean_tmp_cool_mo',
                         'curr_mean_tmp_cool_qtr',
                         'curr_mean_tmp_wm_mo',
                         'curr_mean_tmp_wm_qtr',
                         'curr_num_mo_above0')}}


# ============================================================================
# Test Class Parameters
# ============================================================================    
class WMS100:
   """ Class for WMS 1.0.0 GetCapabilities sample data """ 
   svcurl = 'http://129.237.201.132/OGC/wms/1.0.0/capabilities_1_0_0.xml'
   svctitle = 'Acme Corp. Map Server'
   svcname = 'GetMap'
   svcver = '1.0.0'
   svckeys = set(['bird', 'roadrunner', 'ambush'])
   svclyrs = 9
   parsetsts = [('Alignment test grid', 'keyword', set(['graticule', 'test']))]
   inherittsts = [('Alignment test grid', 'srs', set(['EPSG:4326'])), 
                  ('Roads at 1:1M scale', 'srs', set(['EPSG:4326', 'EPSG:26986'])), 
                  ('Forecast cloud cover', 'srs', set(['EPSG:4326'])), 
                  ('Forecast temperature', 'bbox', ('-180', '-90', '180', '90'))]
   svcData = None
        
#.............................................................................
class WMS107:
   """ Class for WMS 1.0.7 GetCapabilities sample data """
   svcurl = 'http://129.237.201.132/OGC/wms/1.0.7/capabilities_1_0_7.xml'
   svctitle = 'Acme Corp. Map Server'
   svcname = 'GetMap'
   svcver = '1.0.7'
   svckeys = set(['bird', 'roadrunner', 'ambush'])
   svclyrs = 9 
   parsetsts = [('Roads at 1:1M scale', 'keyword', set(['road', 'transportation', 'atlas']))]
   inherittsts = [('Roads at 1:1M scale', 'bbox', ('-71.634696', '41.754149', '-70.789798', '42.908459')), 
                  ('Acme Corp. Map Server', 'srs', set(['EPSG:4326'])), 
                  ('Roads and Rivers', 'srs', set(['EPSG:4326', 'EPSG:26986'])), 
                  ('Roads at 1:1M scale', 'srs', set(['EPSG:4326', 'EPSG:26986'])), 
                  ('Forecast cloud cover', 'srs', set(['EPSG:4326'])), 
                  ('Forecast temperature', 'bbox', ('-180', '-90', '180', '90'))]
   svcData = None
        
#.............................................................................
class WMS110:
   """ Class for WMS 1.1.0 GetCapabilities sample data """
   # These files refer to dtd-s from a defunct website, changed to 
   # modified local files with local dtd-s
   svcurl = 'http://129.237.201.132/OGC/wms/1.1.0/capabilities_1_1_0.xml'
   svctitle = 'Acme Corp. Map Server'
   svcname = 'OGC:WMS'
   svcver = '1.1.0'
   svckeys = set(['bird', 'roadrunner', 'ambush'])
   svclyrs = 10
   parsetsts = [('Roads at 1:1M scale', 'keyword', set(['road', 'transportation', 'atlas']))]
   inherittsts = [('Roads at 1:1M scale', 'bbox', ('-71.63', '41.75', '-70.78', '42.90')), 
                  ('Acme Corp. Map Server', 'srs', set(['EPSG:4326'])), 
                  ('Roads and Rivers', 'srs', set(['EPSG:4326', 'EPSG:26986'])), 
                  ('Roads at 1:1M scale', 'srs', set(['EPSG:4326', 'EPSG:26986'])), 
                  ('Forecast cloud cover', 'srs', set(['EPSG:4326'])), 
                  ('Forecast temperature', 'bbox', ('-180', '-90', '180', '90'))]
   svcData = None
       
#.............................................................................
class WMS111:            
   """ Class for WMS 1.1.1 GetCapabilities sample data """
   svcurl = 'http://129.237.201.132/OGC/wms/1.1.1/capabilities_1_1_1.xml'
   svctitle = 'Acme Corp. Map Server'
   svcname = 'OGC:WMS'
   svcver = '1.1.1'
   svckeys = set(['bird', 'roadrunner', 'ambush'])
   svclyrs = 10
   parsetsts = [('Roads at 1:1M scale', 'keyword', set(['road', 'transportation', 'atlas']))]
   inherittsts = [('Roads at 1:1M scale', 'bbox', ('-71.63', '41.75', '-70.78', '42.90')), 
                  ('Acme Corp. Map Server', 'srs', set(['EPSG:4326'])), 
                  ('Roads and Rivers', 'srs', set(['EPSG:4326', 'EPSG:26986'])), 
                  ('Roads at 1:1M scale', 'srs', set(['EPSG:4326', 'EPSG:26986'])), 
                  ('Forecast cloud cover', 'srs', set(['EPSG:4326'])), 
                  ('Forecast temperature', 'bbox', ('-180', '-90', '180', '90'))]
   svcData = None     
   
#.............................................................................
class WMS130:            
   """ Class for WMS 1.3.0 GetCapabilities sample data """
   svcurl = 'http://129.237.201.132/OGC/wms/1.3.0/capabilities_1_3_0.xml'
   svctitle = 'Acme Corp. Map Server'
   svcname = 'WMS'
   svcver = '1.3.0'
   svckeys = set(['bird', 'roadrunner', 'ambush'])
   svclyrs = 10
   parsetsts = [('Roads at 1:1M scale', 'keyword', set(['road', 'transportation', 'atlas']))]
   inherittsts = [('Roads at 1:1M scale', 'bbox', ('-71.63', '41.75', '-70.78', '42.90')), 
                  ('Acme Corp. Map Server', 'srs', set(['CRS:84'])), 
                  ('Roads and Rivers', 'srs', set(['CRS:84', 'EPSG:26986'])), 
                  ('Roads at 1:1M scale', 'srs', set(['CRS:84', 'EPSG:26986'])), 
                  ('Forecast cloud cover', 'srs', set(['CRS:84'])), 
                  ('Forecast temperature', 'bbox', ('-180', '-90', '180', '90'))]
   svcData = None
            
#.............................................................................
#    No sample data available for WFS 1.0.0
            
#.............................................................................
class WFS110:            
   """ Class for WFS 1.1.0 GetCapabilities sample data """
   # Online version at http://schemas.opengis.net has an error - this is corrected here
   svcurl = 'http://129.237.201.132/OGC/wfs/1.1.0/examples/WFS_Capabilities_Sample.xml'
   svctitle = 'OGC Member WFS'
   svcname = 'WFS'
   svcver = '1.1.0'
   svcsrss = set(['EPSG:4326', 'EPSG:32100', 'EPSG:32101', 'EPSG:32102'])
   svckeys = set(['FGDC', 'NSDI', 'Framework Data Layer', 'BlueOx'])
   svcfeattypes = 1
   svcData = None
            
#.............................................................................
#   http://iceds.ge.ucl.ac.uk/cgi-bin/icedswcs?SERVICE=WCS&REQUEST=GetCapabilities
class WCS100:            
   """ Class for WCS 1.0.0 GetCapabilities sample data """
   svcurl = 'http://129.237.201.132/OGC/wcs/1.0.0/examples/iceds_wcs_example.xml'
   svcbase = 'http://iceds.ge.ucl.ac.uk/cgi-bin/icedswcs'
   svcname = 'ICEDSWCS'
   svctype = 'WCS'
   svcver = '1.0.0'
   svckeys = set(['ICEDS', 'WCS', 'LANDSAT', 'SRTM'])
   covname = 'landsat'
   covkeys = set(['JPL', 'WCS', 'LANDSAT 5','ICEDS', 'LANDSAT', 'SRTM'])
   covbbox = (-30, -35, 65, 80)
   svcData = None
            
# .............................................................................
# GetCoverage: 
#   http://motherlode.ucar.edu:8080/thredds/wcs/galeon/testdata/RUC.nc?request=GetCapabilities&version=1.0.0&service=WCS
# DescribeCoverage:
#   http://webapps.datafed.net/ogc.wsfl?VERSION=1.0.0&SERVICE=WCS&REQUEST=DescribeCoverage&Coverage=AIRNOW.pmfine
class WCS100b:            
   """ Class for WCS 1.0.0 GetCapabilities sample data """
   svcurl ='http://129.237.201.132/OGC/wcs/1.0.0/examples/ucar_wcs_example.xml'
   svcbase = 'http://motherlode.ucar.edu:8080/thredds/wcs/galeon/testdata/RUC.nc'
   svcname = '/data/thredds/galeon/RUC.nc'
   svctype = 'WCS'
   svcver = '1.0.0'
   svckeys = set([])
   svcfeattypes = 1
   covname = 'Temperature_layer_between_two_pressure_difference_from_ground'
   covbbox = (-153.57137910046424, 11.747958797813363, -48.67045857136577, 57.47373334674363)
   covkeys = set([])
   svcData = None 

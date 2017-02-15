from osgeo import gdalconst

# Coordinate Reference System, see http://www.epsg.org/ or 
#    http://spatialreference.org/ref/epsg/
EPSG = 4326
# Mapunits must be appropriate for EPSG
# Legal values are 'feet', 'inches', 'kilometers', 'meters', 'miles', 'dd', 'ds'
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

# Descriptive string with numerical value (in mapunits) of data resolution
#     This document only requires values for the data being described
RESOLUTIONS = {'30sec': 0.0083333}
CLIMATE_KEYWORDS = ['bioclimatic variables', 'climate', 'elevation', 'land cover', 'soil', 'topography']

# The following describe the different scenarios that can be assembled with 
#     these data.  
# Predicted scenarios can be assembled with all combinations of 
#     Report x model (GCM) x alternatePrediction x time.  
OBSERVED_PREDICTED_META = {
   'observed': {
      # Name for climate scenario
      'name': 'BIOCLIM+GTOPO+ISRIC_SoilGrids1km+Land_Cover', 
      'keywords': ['observed', 'present'], 
      # Year range (low, high) that these data represent
      'times': {'Curr': {'name': '1950-2000'}},
      # Short title for this scenario
      'title': 'Worldclim1.4, GTOPO, ISRIC SoilGrids1km, Global 1-km Consensus Land Cover', 
      # Author of these data
      'author': ' Hijmans, R.J., S.E. Cameron, J.L. Parra, P.G. Jones and A. Jarvis, 2005. Very high resolution interpolated climate surfaces for global land areas. International Journal of Climatology 25: 1965-1978; Hengl T, de Jesus JM, MacMillan RA, Batjes NH, Heuvelink GBM, et al. (2014) SoilGrids1km — Global Soil Information Based on Automated Mapping. PLoS ONE 9(8): e105992. doi:10.1371/journal.pone.0105992; Tuanmu, M.-N. and W. Jetz. 2014. A global 1-km consensus land-cover product for biodiversity and ecosystem modeling. Global Ecology and Biogeography 23(9): 1031-1045.',
      # Extended description of the data  
      'description': 'WorldClim 1.4  bioclimatic variables computed from interpolated observation data collected between 1950 and 2000 (http://www.worldclim.org/), 30 sec resolution; ISRIC SoilGrids 1km, 30 sec resolution;  Land cover 1km, 30 sec resolution;  Global 1-km Consensus Land Cover, 30 sec resolution.',
   },
}

# The following describe the scenarios in one or more CLIMATE_PACKAGES (one 
#     `10min-past-present-future` in this example)
# Predicted scenarios used by a CLIMATE_PACKAGE are described by combinations of 
#     Report x model (GCM) x alternatePrediction x time.  Not all 
#     combinations possible may be included.  In this example, only the 
#     CCSM4 GCM model of the AR5 (future) and CMIP5 (past) is used.
CLIMATE_PACKAGES = {
   '10min-past-present-future':
   # past: CCSM4-lgm-10min,CCSM4-mid-10min
   # current: WC-10min
   # future: CCSM4-lgm-10min,CCSM4-mid-10min,CCSM4-RCP8.5-2070-10min,CCSM4-RCP4.5-2070-10min,CCSM4-RCP8.5-2050-10min,CCSM4-RCP4.5-2050-10min
      {'res': '10min',
       'bbox': [-180, -60, 180, 90],
       # BASELINE_DATA code - baseline data will be used for SDM modeling and projecting
       'baseline': 'observed',
       # predicted data will be used for SDM projecting
       # {REPORT: [(GCM, Time, RCP(opt)), (model, Time, RCP(opt)) ...]}
       # Layertype codes are defined in LAYERTYPE_META
       'layertypes': ['alt', 'slope', 'aspect',
                      'landcover_needle', 'landcover_everbroad', 'landcover_decidbroad', 'landcover_mixed', 'landcover_shrub', 'landcover_herb',
                      'ph', 'carbon', 'sand', 'fragment', 'bulkdensity', 'clay', 'silt',
                      'bio1', 'bio2', 'bio3', 'bio4', 'bio5', 'bio6', 'bio7', 'bio8', 'bio9', 'bio10', 'bio11', 'bio12', 'bio13', 'bio14', 'bio15', 'bio16', 'bio17', 'bio18', 'bio19'],
       # Append to all scenario codes
       'suffix': None} }

# Each of these layertypes describes one or more layers, listed in the 'files'  
# section 
LAYERTYPE_META = {
   'bio1': {
      'title': 'Annual Mean Temperature',
      'description': 'Annual Mean Temperature',
      'valunits':'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'annual'),
      # The key for each file is a relative path (starting with the top level of 
      #     the directory tree of the data package)
      # The value is a list of keywords identifying the scenario this layer
      #     is a part of.  The first keyword is `observed` or predicted `report`, 
      #     the first key in OBSERVED_PREDICTED_META
      # For predicted reports, codes for model, alternatePrediction, and time
      #     are also specified.
      'files': {'BIOCLIM_1.tif': ['observed']},
            },
   'bio2': {
      'title': 'Mean Diurnal Range',
      'description': 'Mean Diurnal Range (Mean of monthly (max temp - min temp))',
      'valunits':'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'range'),
      'files': {'BIOCLIM_2.tif': ['observed']},
            },
   'bio3': {
      'title': 'Isothermality',
      'description': 'Isothermality (bio2/bio7) (* 100)',
      'valunits': 'dimensionless',
      'keywords': ('bioclim','temperature', 'isothermality'),
      'files': {'BIOCLIM_3.tif': ['observed']},
            },
   'bio4': {
      'title': 'Temperature Seasonality',
      'description': 'Temperature Seasonality (standard deviation *100)',
      'valunits': 'standardDeviationTimes100',
      'keywords': ('bioclim','temperature', 'seasonality'),
      'files': {'BIOCLIM_4.tif': ['observed']},
            },
   'bio5': {
      'title': 'Max Temperature of Warmest Month',
      'description': 'Max Temperature of Warmest Month',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'max', 'warmest month'),
      'files': {'BIOCLIM_5.tif': ['observed']},
            },
   'bio6': {
      'title': 'Min Temperature of Coldest Month',
      'description': 'Min Temperature of Coldest Month',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','min', 'temperature', 'coldest month'),
      'files': {'BIOCLIM_6.tif': ['observed']},
            },
   'bio7': {
      'title': 'Temperature Annual Range',
      'description': 'Temperature Annual Range (bio5-bio6)',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'range'),
      'files': {'BIOCLIM_7.tif': ['observed']},
            },
   'bio8': {
      'title': 'Mean Temperature of Wettest Quarter',
      'description': 'Mean Temperature of Wettest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'wettest quarter'),
      'files': {'BIOCLIM_8.tif': ['observed']},
            },
   'bio9': {
      'title': 'Mean Temperature of Driest Quarter',
      'description': 'Mean Temperature of Driest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'driest quarter'),
      'files': {'BIOCLIM_9.tif': ['observed']},
            },
   'bio10': {
      'title': 'Mean Temperature of Warmest Quarter',
      'description': 'Mean Temperature of Warmest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'warmest quarter'),
      'files': {'BIOCLIM_10.tif': ['observed']},
             },
   'bio11': {
      'title': 'Mean Temperature of Coldest Quarter',
      'description': 'Mean Temperature of Coldest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'coldest quarter'),
      'files': {'BIOCLIM_11.tif': ['observed']},
             },
   'bio12': {
      'title': 'Annual Precipitation',
      'description': 'Annual Precipitation',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'annual'),
      'files': {'BIOCLIM_12.tif': ['observed']},
             },
   'bio13': {
      'title': 'Precipitation of Wettest Month',
      'description': 'Precipitation of Wettest Month',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'wettest month'),
      'files': {'BIOCLIM_13.tif': ['observed']},
             },
   'bio14': {
      'title': 'Precipitation of Driest Month',
      'description': 'Precipitation of Driest Month',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'driest month'),
      'files': {'BIOCLIM_14.tif': ['observed']},
             },
   'bio15': {
      'title': 'Precipitation Seasonality',
      'description': 'Precipitation Seasonality (Coefficient of Variation)',
      'valunits': 'coefficientOfVariation',
      'keywords': ('bioclim','precipitation', 'seasonality'),
      'files': {'BIOCLIM_15.tif': ['observed']},
             },
   'bio16': {
      'title': 'Precipitation of Wettest Quarter',
      'description': 'Precipitation of Wettest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'wettest quarter'),
      'files': {'BIOCLIM_16.tif': ['observed']},
             },
   'bio17': {
      'title': 'Precipitation of Driest Quarter',
      'description': 'Precipitation of Driest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'driest quarter'),
      'files': {'BIOCLIM_17.tif': ['observed']},
             },
   'bio18': {
      'title': 'Precipitation of Warmest Quarter',
      'description': 'Precipitation of Warmest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'warmest quarter'),
      'files': {'BIOCLIM_18.tif': ['observed']},
             },
   'bio19': {
      'title': 'Precipitation of Coldest Quarter',
      'description': 'Precipitation of Coldest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'coldest quarter'),
      'files': {'BIOCLIM_19.tif': ['observed']},
             },
   'alt': {
      'title': 'Elevation',
      'description': 'GTOPO Elevation (altitude above sea level, from USGS, https://lta.cr.usgs.gov/GTOPO30)',
      'valunits': 'meters',
      'keywords': ['elevation'],
      'files': {'GTOPO30_ELEVATION.tif': ['observed']},
                },
   'slope': {
      'title': 'Slope',
      'description': 'Calculated in qGIS from GTOPO Elevation (altitude above sea level, from USGS, https://lta.cr.usgs.gov/GTOPO30)',
      'valunits': 'degrees from horizontal',
      'keywords': ['slope'],
      'files': {'GTOPO30_SLOPE.tif': ['observed']},
                },
   'aspect': {
      'title': 'Aspect',
      'description': 'Calculated in qGIS from GTOPO Elevation (altitude above sea level, from USGS, https://lta.cr.usgs.gov/GTOPO30)',
      'valunits': 'degrees from north',
      'keywords': ['aspect'],
      'files': {'GTOPO30_ASPECT.tif': ['observed']},
                },
   'landcover_needle': {
      'title': 'Land cover: Evergreen/Deciduous Needleleaf Trees',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and W. Jetz. 2014. A global 1-km consensus land-cover product for biodiversity and ecosystem modeling. Global Ecology and Biogeography 23(9): 1031-1045; http://www.earthenv.org/landcover',
      'valunits': 'percent',
      'keywords': ['landcover', 'needle leaf'],
      'files': {'LandCover_1_Needleleaf.tif': ['observed']},
                },
   'landcover_everbroad': {
      'title': 'Land cover: Evergreen Broadleaf Trees',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and W. Jetz. 2014. A global 1-km consensus land-cover product for biodiversity and ecosystem modeling. Global Ecology and Biogeography 23(9): 1031-1045; http://www.earthenv.org/landcover',
      'valunits': 'percent',
      'keywords': ['landcover', 'evergreen broadleaf'],
      'files': {'LandCover_2_Evergreenbroadleaf.tif': ['observed']},
                },
   'landcover_needle': {
      'title': 'Land cover: Deciduous Broadleaf Trees',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and W. Jetz. 2014. A global 1-km consensus land-cover product for biodiversity and ecosystem modeling. Global Ecology and Biogeography 23(9): 1031-1045; http://www.earthenv.org/landcover',
      'valunits': 'percent',
      'keywords': ['landcover', 'deciduous broad leaf'],
      'files': {'LandCover_3_Deciduousbroadleaf.tif': ['observed']},
                },
   'landcover_needle': {
      'title': 'Land cover: Mixed/Other Trees',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and W. Jetz. 2014. A global 1-km consensus land-cover product for biodiversity and ecosystem modeling. Global Ecology and Biogeography 23(9): 1031-1045; http://www.earthenv.org/landcover',
      'valunits': 'percent',
      'keywords': ['landcover', 'other'],
      'files': {'LandCover_4_Mixedtrees.tif': ['observed']},
                },
   'landcover_needle': {
      'title': 'Land cover: Shrubs',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and W. Jetz. 2014. A global 1-km consensus land-cover product for biodiversity and ecosystem modeling. Global Ecology and Biogeography 23(9): 1031-1045; http://www.earthenv.org/landcover',
      'valunits': 'percent',
      'keywords': ['landcover', 'shrub'],
      'files': {'LandCover_5_Shrubs.tif': ['observed']},
                },
   'landcover_needle': {
      'title': 'Land cover: Herbaceous Vegetation',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and W. Jetz. 2014. A global 1-km consensus land-cover product for biodiversity and ecosystem modeling. Global Ecology and Biogeography 23(9): 1031-1045; http://www.earthenv.org/landcover',
      'valunits': 'percent',
      'keywords': ['landcover', 'herbaceous'],
      'files': {'LandCover_6_Needleleaf.tif': ['observed']},
                },
   'ph': {
      'title': 'SoilGrids 1km: Soil pH x 10 in H2O (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths 2.5, 10, 22.5, 45 cm; data from ISRIC – World Soil Information, 2013. SoilGrids: an automated system for global soil mapping. Available for download at http://soilgrids1km.isric.org.',
      'valunits': 'pH * 10',
      'keywords': ['soil', 'pH'],
      'files': {'ISRICSOILGRIDS_average_phx10.tif': ['observed']},
                },
   'carbon': {
      'title': 'SoilGrids 1km: Soil organic carbon content (fine earth fraction) in permilles (mean estimate) ',
      'description': 'Average calculated in qGIS of layers for core depths 2.5, 10, 22.5, 45 cm; data from ISRIC – World Soil Information, 2013. SoilGrids: an automated system for global soil mapping. Available for download at http://soilgrids1km.isric.org.',
      'valunits': 'permilles (parts per thousand)',
      'keywords': ['soil', 'carbon'],
      'files': {'ISRICSOILGRIDS_average_soilorganiccarboncontent.tif': ['observed']},
                },
   'sand': {
      'title': 'SoilGrids 1km: Soil texture fraction sand in percent (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths 2.5, 10, 22.5, 45 cm; data from ISRIC – World Soil Information, 2013. SoilGrids: an automated system for global soil mapping. Available for download at http://soilgrids1km.isric.org.',
      'valunits': 'percent',
      'keywords': ['soil', 'sand'],
      'files': {'ISRICSOILGRIDS_average_sandpercent': ['observed']},
                },
   'fragment': {
      'title': 'SoilGrids 1km: Coarse fragments volumetric in percent (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths 2.5, 10, 22.5, 45 cm; data from ISRIC – World Soil Information, 2013. SoilGrids: an automated system for global soil mapping. Available for download at http://soilgrids1km.isric.org.',
      'valunits': 'percent (volume per volume)',
      'keywords': ['soil', 'coarse fragment'],
      'files': {'ISRICSOILGRIDS_average_coarsefragmentpercent.tif': ['observed']},
                },
   'bulkdensity': {
      'title': 'SoilGrids 1km: Bulk density in kg / cubic-meter (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths 2.5, 10, 22.5, 45 cm; data from ISRIC – World Soil Information, 2013. SoilGrids: an automated system for global soil mapping. Available for download at http://soilgrids1km.isric.org.',
      'valunits': 'kg/m^3',
      'keywords': ['soil', 'bulk density'],
      'files': {'ISRICSOILGRIDS_average_bulkdensity.tif': ['observed']},
                },
   'clay': {
      'title': 'SoilGrids 1km: Soil texture fraction clay in percent (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths 2.5, 10, 22.5, 45 cm; data from ISRIC – World Soil Information, 2013. SoilGrids: an automated system for global soil mapping. Available for download at http://soilgrids1km.isric.org.',
      'valunits': 'percent',
      'keywords': ['soil', 'clay'],
      'files': {'ISRICSOILGRIDS_average_claypercent.tif': ['observed']},
                },
   'silt': {
      'title': 'SoilGrids 1km: Soil texture fraction silt in percent (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths 2.5, 10, 22.5, 45 cm; data from ISRIC – World Soil Information, 2013. SoilGrids: an automated system for global soil mapping. Available for download at http://soilgrids1km.isric.org.',
      'valunits': 'percent',
      'keywords': ['soil', 'silt'],
      'files': {'ISRICSOILGRIDS_average_siltpercent.tif': ['observed']},
                },
		}
	
      

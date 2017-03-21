from osgeo import gdalconst
      'keywords': ('bioclim','temperature', 'mean', 'annual'),
      'description': 'Mean Diurnal Range (Mean of monthly (max temp - min temp))',
      'valunits':'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'range'),
      'files': {'ryan/BIOCLIM_2.tif': ['observed']}},
   'bio3': {
      'description': 'Isothermality (bio2/bio7) (* 100)',
      'valunits': 'dimensionless',
      'keywords': ('bioclim','temperature', 'isothermality'),
      'files': {'ryan/BIOCLIM_3.tif': ['observed']}},
   'bio4': {
      'title': 'Temperature Seasonality',
      'description': 'Temperature Seasonality (standard deviation *100)',
      'valunits': 'standardDeviationTimes100',
      'keywords': ('bioclim','temperature', 'seasonality'),
      'files': {'ryan/BIOCLIM_4.tif': ['observed']}},
   'bio5': {
      'title': 'Max Temperature of Warmest Month',
      'description': 'Max Temperature of Warmest Month',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'max', 'warmest month'),
      'files': {'ryan/BIOCLIM_5.tif': ['observed']}},
   'bio6': {
      'title': 'Min Temperature of Coldest Month',
      'description': 'Min Temperature of Coldest Month',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','min', 'temperature', 'coldest month'),
      'files': {'ryan/BIOCLIM_6.tif': ['observed']}},
   'bio7': {
      'title': 'Temperature Annual Range',
      'description': 'Temperature Annual Range (bio5-bio6)',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'range'),
      'files': {'ryan/BIOCLIM_7.tif': ['observed']}},
   'bio8': {
      'title': 'Mean Temperature of Wettest Quarter',
      'description': 'Mean Temperature of Wettest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'wettest quarter'),
      'files': {'ryan/BIOCLIM_8.tif': ['observed']}},
   'bio9': {
      'title': 'Mean Temperature of Driest Quarter',
      'description': 'Mean Temperature of Driest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'driest quarter'),
      'files': {'ryan/BIOCLIM_9.tif': ['observed']}},
   'bio10': {
      'title': 'Mean Temperature of Warmest Quarter',
      'description': 'Mean Temperature of Warmest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'warmest quarter'),
      'files': {'ryan/BIOCLIM_10.tif': ['observed']}},
   'bio11': {
      'title': 'Mean Temperature of Coldest Quarter',
      'description': 'Mean Temperature of Coldest Quarter',
      'valunits': 'degreesCelsiusTimes10',
      'keywords': ('bioclim','temperature', 'mean', 'coldest quarter'),
      'files': {'ryan/BIOCLIM_11.tif': ['observed']}},
   'bio12': {
      'title': 'Annual Precipitation',
      'description': 'Annual Precipitation',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'annual'),
      'files': {'ryan/BIOCLIM_12.tif': ['observed']}},
   'bio13': {
      'title': 'Precipitation of Wettest Month',
      'description': 'Precipitation of Wettest Month',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'wettest month'),
      'files': {'ryan/BIOCLIM_13.tif': ['observed']}},
   'bio14': {
      'title': 'Precipitation of Driest Month',
      'description': 'Precipitation of Driest Month',
      'valunits': 'mm',
      'keywords': ('ryan/bioclim','precipitation', 'driest month'),
      'files': {'ryan/BIOCLIM_14.tif': ['observed']},},
   'bio15': {
      'title': 'Precipitation Seasonality',
      'description': 'Precipitation Seasonality (Coefficient of Variation)',
      'valunits': 'coefficientOfVariation',
      'keywords': ('bioclim','precipitation', 'seasonality'),
      'files': {'ryan/BIOCLIM_15.tif': ['observed']}},
   'bio16': {
      'title': 'Precipitation of Wettest Quarter',
      'description': 'Precipitation of Wettest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'wettest quarter'),
      'files': {'ryan/BIOCLIM_16.tif': ['observed']}},
   'bio17': {
      'title': 'Precipitation of Driest Quarter',
      'description': 'Precipitation of Driest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'driest quarter'),
      'files': {'ryan/BIOCLIM_17.tif': ['observed']}},
   'bio18': {
      'title': 'Precipitation of Warmest Quarter',
      'description': 'Precipitation of Warmest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'warmest quarter'),
      'files': {'ryan/BIOCLIM_18.tif': ['observed']}},
   'bio19': {
      'title': 'Precipitation of Coldest Quarter',
      'description': 'Precipitation of Coldest Quarter',
      'valunits': 'mm',
      'keywords': ('bioclim','precipitation', 'coldest quarter'),
      'files': {'ryan/BIOCLIM_19.tif': ['observed']}},
   'alt': {
      'title': 'Elevation',
      'description': 'GTOPO Elevation (altitude above sea level, from USGS, ' + 
      'valunits': 'meters',
      'keywords': ['elevation'],
      'files': {'ryan/GTOPO30_ELEVATION.tif': ['observed']},
                },
   'slope': {
      'title': 'Slope',
      'description': 'Calculated in qGIS from GTOPO Elevation (altitude ' + 
      'valunits': 'degrees from horizontal',
      'keywords': ['slope'],
      'files': {'ryan/GTOPO30_SLOPE.tif': ['observed']},
                },
   'aspect': {
      'title': 'Aspect',
      'description': 'Calculated in qGIS from GTOPO Elevation (altitude ' + 
      'valunits': 'degrees from north',
      'keywords': ['aspect'],
      'files': {'ryan/GTOPO30_ASPECT.tif': ['observed']},
                },
   'landcover_needle': {
      'title': 'Land cover: Evergreen/Deciduous Needleleaf Trees',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and  ' + 
      'valunits': 'percent',
      'keywords': ['landcover', 'needle leaf'],
      'files': {'ryan/LandCover_1_Needleleaf.tif': ['observed']},
                },
   'landcover_everbroad': {
      'title': 'Land cover: Evergreen Broadleaf Trees',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and ' + 
      'valunits': 'percent',
      'keywords': ['landcover', 'evergreen broadleaf'],
      'files': {'ryan/LandCover_2_Evergreenbroadleaf.tif': ['observed']},
                },
   'landcover_decidbroad': {
      'title': 'Land cover: Deciduous Broadleaf Trees',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and ' + 
      'valunits': 'percent',
      'keywords': ['landcover', 'deciduous broad leaf'],
      'files': {'ryan/LandCover_3_Deciduousbroadleaf.tif': ['observed']},
                },
   'landcover_mixed': {
      'title': 'Land cover: Mixed/Other Trees',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and ' + 
      'keywords': ['landcover', 'other'],
      'files': {'ryan/LandCover_4_Mixedtrees.tif': ['observed']},
                },
   'landcover_shrub': {
      'title': 'Land cover: Shrubs',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and ' + 
      'keywords': ['landcover', 'shrub'],
      'files': {'ryan/LandCover_5_Shrubs.tif': ['observed']},
                },
   'landcover_herb': {
      'title': 'Land cover: Herbaceous Vegetation',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and ' + 
      'keywords': ['landcover', 'herbaceous'],
      'files': {'ryan/LandCover_6_Needleleaf.tif': ['observed']},
                },
   'ph': {
      'title': 'SoilGrids 1km: Soil pH x 10 in H2O (mean estimate)',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and ' + 
      'keywords': ['soil', 'pH'],
      'files': {'ryan/ISRICSOILGRIDS_average_phx10.tif': ['observed']},
                },
   'carbon': {
      'title': 'SoilGrids 1km: Soil organic carbon content (fine earth ' + 
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and ' + 
      'valunits': 'permilles (parts per thousand)',
      'keywords': ['soil', 'carbon'],
      'files': {'ryan/ISRICSOILGRIDS_average_soilorganiccarboncontent.tif': ['observed']},
                },
   'sand': {
      'title': 'SoilGrids 1km: Soil texture fraction sand in percent (mean estimate)',
      'description': 'Global 1-km Consensus Land Cover: Tuanmu, M.-N. and ' + 
      'keywords': ['soil', 'sand'],
      'files': {'ryan/ISRICSOILGRIDS_average_sandpercent': ['observed']},
                },
   'fragment': {
      'title': 'SoilGrids 1km: Coarse fragments volumetric in percent (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths ' + 
      'keywords': ['soil', 'coarse fragment'],
      'files': {'ryan/ISRICSOILGRIDS_average_coarsefragmentpercent.tif': ['observed']},
                },
   'bulkdensity': {
      'title': 'SoilGrids 1km: Bulk density in kg / cubic-meter (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths ' + 
      'keywords': ['soil', 'bulk density'],
      'files': {'ryan/ISRICSOILGRIDS_average_bulkdensity.tif': ['observed']},
                },
   'clay': {
      'title': 'SoilGrids 1km: Soil texture fraction clay in percent (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths ' + 
      'keywords': ['soil', 'clay'],
      'files': {'ryan/ISRICSOILGRIDS_average_claypercent.tif': ['observed']},
                },
   'silt': {
      'title': 'SoilGrids 1km: Soil texture fraction silt in percent (mean estimate)',
      'description': 'Average calculated in qGIS of layers for core depths ' + 
      'keywords': ['soil', 'silt'],
      'files': {'ryan/ISRICSOILGRIDS_average_siltpercent.tif': ['observed']},
                },
		}
      
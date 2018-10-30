
.. highlight:: rest

Publicly available data packages
==================================
.. contents::  


BOOM data (workflow)
---------------------

Contents
~~~~~~~~
For a boom archive data package <boompkg>:
#. Package is a gzipped tarball (<boompkg>.tar.gz extension)
#. Package contains the following files, all at the top level of the tarball
   * contents file is named <boompkg>.contents
     this file names all other files with keywords
   * boom parameter file is named <boompkg>.params
   * SPECIES_PACKAGE: occurrence data, a csv file compressed into a gzipped 
     tarball.  The value should not include the extension '.tar.gz'
   * BOOM_PARAMETER_FILE: a ini file containing parameters for a BOOM workflow
   * BIOGEO_PACKAGE: optional gzipped tarball containing one or more shapefiles
     to be used as biogeographic hypotheses in MCPA analyses. The value should 
     not include the extension '.tar.gz'
   * TREE_FILE: optional nexus formatted tree, to be used as a phylogenetic 
     tree in MCPA analyses. 

Using
~~~~~~~~
#. Download as part of bash script 
   * /opt/lifemapper/rocks/bin/getBoomPackageForUser   
#. Catalog along with bash script
   * /opt/lifemapper/rocks/bin/catalogBoomJob


Data available on yeti (data packages are .tar.gz files)
~~~~~~~~~~~~~~~~~~~~~~
#. heuchera_boom_global_10min 
  * heuchera_boom_global_10min.contents 
  * heuchera_boom_global_10min.params (scenpkg = sax_layers_10min)
  * heuchera.tar.gz
  * heuchera.nex
  * global_hypotheses-2.0.tar.gz
  
#. heuchera_boom_na_10min
  * heuchera_boom_na_10min.contents 
  * heuchera_boom_na_10min.params (scenpkg = obs12_na_10min)
  * heuchera.tar.gz
  * heuchera.nex
  * na_hypotheses-2.0.tar.gz

#. heuchera_boom_global_30sec
  * heuchera_boom_global_30sec.contents
  * heuchera_boom_global_30sec.params (scenpkg = biotaphy12global)
  * heuchera.tar.gz
  * heuchera.nex
  * global_hypotheses-2.0.tar.gz
  
#. sax_boom_global_10min
  * sax_boom_global_10min.contents
  * sax_boom_global_10min.params  (scenpkg = sax_layers_10min)
  * saxifragales.tar.gz
  * saxifragales.nex
  * global_hypotheses-2.0.tar.gz
  
#. sax_boom_conus_30sec
  * sax_boom_conus_30sec.contents
  * sax_boom_conus_30sec.params  (scenpkg = biotaphyCONUS12)
  * saxifragales.tar.gz
  * saxifragales.nex
  * na_hypotheses-2.0.tar.gz


Scenario data 
-------------

Contents
~~~~~~~~
For package named <scenpkg>:
#. Package is a gzipped tarball (<scenpkg>.tar.gz extension)
#. Package contains metadata file at the top level of the tarball
   * metadata file is named <scenpkg>.py
#. Package contains ecoregion file
   * ecoregion is a raster file with the same resolution as the layer files 
   * ecoregion metadata is in the metadata file 
#. Package contains a directory at the top level of the tarball contents, 
   containing raster data files for all scenarios.  
   * directory is named <scenpkg>
   * directory may contain subdirectories
#. Metadata file <scenpkg>.py contains: 
   * version (of metadata) corresponding to 
     LmDbServer.tools.catalogScenPkg.SPFiller version
   * package and scenario metadata
   * layer metadata for 'layertypes'.  Each layertype will point to a separate 
     file, and relative file path, for every scenario it is included in.

Using
~~~~~~~~
#. Data will be pulled and transformed as part of BOOM data processing above
   * getBoomPackageForUser will download and transform the layers
   * catalogBoomJob will catalog the data for the workflow user
#. Iff you want to pull it separately    
   *  Download and transform data with 
      * /opt/lifemapper/rocks/bin/getScenarioPackageForUser

Data available on yeti (all version 2.0)
~~~~~~~~~~~~~~~~~~~~~~
#. 10min-past-present-future
  * 10min
  * global extent
  * 19 bioclim variables plus elevation
  * 10min ecoregion
  * present scenario (worldclim)
  * 2 past scenarios (CMIP: last glacial maximum ~22K years ago, mid Holocene ~6K years ago)
  * 4 future scenarios (IPCC AR5, GCM: CCSM4, RCPs: 4.5, 8.5, times: 2050, 2070)
  
#. sax_layers_10min
  * 10min
  * global extent (-180, -56, 180, 90)
  * 10min ecoregion
  * present scenario
     * 4 bioclim variables 
     * 2 GTOPO
     * 4 soils 
     * 2 landcover
     
#. obs12_na_10min
  * 10min
  * na extent (-178.3333, -7.1667, -12.3333, 83.6667)
  * 10min ecoregion
  * present scenario
     * 4 bioclim variables 
     * 2 GTOPO
     * 4 soils 
     * 2 landcover

#. biotaphy12global
  * 30second
  * global extent (-180, -56, 180, 90)
  * 30sec ecoregion
  * present scenario
     * 4 bioclim variables 
     * 2 GTOPO
     * 4 soils 
     * 2 landcover

#. biotaphy12conus
  * 30second
  * continental US extent (-125, 25, -66, 50)
  * 30sec ecoregion
  * present scenario
     * 4 bioclim variables 
     * 2 GTOPO
     * 4 soils 
     * 2 landcover

#. biotaphy35global
  * 30second
  * global extent (-180, -56, 180, 90)
  * 30sec ecoregion
  * present scenario
     * 19 bioclim variables 
     * 3 GTOPO
     * 7 soils 
     * 6 landcover

#. biotaphy35conus
  * 30second
  * continental US extent (-125, 25, -66, 50)
  * 30sec ecoregion
  * present scenario
     * 19 bioclim variables 
     * 3 GTOPO
     * 7 soils 
     * 6 landcover


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


Data available on yeti
~~~~~~~~~~~~~~~~~~~~~~
# 


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

Data available on yeti
~~~~~~~~~~~~~~~~~~~~~~
# 10min-past-present-future
  * version 2.0
  * present scenario (worldclim)
  * 2 past scenarios (CMIP: last glacial maximum ~22K years ago, mid Holocene ~6K years ago)
  * 4 future scenarios (IPCC AR5, GCM: CCSM4, RCPs: 4.5, 8.5, times: 2050, 2070)
  * 10min
  * global extent
  * 19 bioclim variables plus elevation
  
 

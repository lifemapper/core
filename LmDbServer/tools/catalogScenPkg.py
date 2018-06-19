"""
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research
 
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
import ConfigParser
import json
import mx.DateTime
import os
import time
import types

from LmBackend.command.boom import BoomerCommand
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_EPSG, DEFAULT_MAPUNITS, 
      DEFAULT_POST_USER, JobStatus, LMFormat, MatrixType, ProcessType, 
      SERVER_BOOM_HEADING, SERVER_SDM_ALGORITHM_HEADING_PREFIX, 
      SERVER_SDM_MASK_HEADING_PREFIX, SERVER_DEFAULT_HEADING_POSTFIX, 
      SERVER_PIPELINE_HEADING)
from LmCommon.common.readyfile import readyFilename

from LmDbServer.common.lmconstants import (SpeciesDatasource, TAXONOMIC_SOURCE, 
                                           TNCMetadata)
from LmDbServer.common.localconstants import (GBIF_PROVIDER_FILENAME, 
                                              GBIF_TAXONOMY_FILENAME)

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, ARCHIVE_KEYWORD, 
                           ENV_DATA_PATH, DEFAULT_EMAIL_POSTFIX, GGRIM_KEYWORD,
                           GPAM_KEYWORD, LMFileType, Priority, 
                           PUBLIC_ARCHIVE_NAME)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree


CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)

# .............................................................................
class SPFiller(LMObject):
   """
   @summary 
   Class to: 
     1) populate a Lifemapper database with scenario package for a BOOM archive
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, spMetaFname, userId):
      """
      @summary Constructor for BOOMFiller class.
      """
      super(SPFiller, self).__init__()
      self.name = self.__class__.__name__.lower()
      self.spMetaFname = spMetaFname
      self.userId = userId
      if self.userId != PUBLIC_USER:
         if self.userEmail is None:
            self.userEmail = '{}{}'.format(self.usr, DEFAULT_EMAIL_POSTFIX)
            
      # Get database
      try:
         self.scribe = self._getDb()
      except: 
         raise
      self.open()
      
   # ...............................................
   def open(self):
      success = self.scribe.openConnections()
      if not success: 
         raise LMError('Failed to open database')

      # ...............................................
   def close(self):
      self.scribe.closeConnections()

   # ...............................................
   @property
   def logFilename(self):
      try:
         fname = self.scribe.log.baseFilename
      except:
         fname = None
      return fname

# ...............................................
   def _warnPermissions(self):
      if not isCorrectUser():
         print("""
               When not running this {} as `lmwriter`, make sure to fix
               permissions on the newly created shapegrid {}
               """.format(self.name, self.gridname))
         
   # ...............................................
   def _getDb(self):
      import logging
      loglevel = logging.INFO
      # Logfile
      secs = time.time()
      timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
      logname = '{}.{}'.format(self.name, timestamp)
      logger = ScriptLogger(logname, level=loglevel)
      # DB connection
      scribe = BorgScribe(logger)
      return scribe

   # ...............................................
   def _createMaskLayer(self, pkgMeta, maskMeta):
      """
      @summary Assembles layer metadata for input to optional 
               pre-processing SDM Mask step identified in scenario package 
               metadata. 
      @note: Only the 'hull_region_intersect' method is currently available.
      """
      # Required keys in SDM_MASK_INPUT: name, bbox, gdaltype, gdalformat, file
      lyrmeta = {
         Vector.META_IS_CATEGORICAL: self._getOptionalMetadata(maskMeta, 'iscategorical'), 
         ServiceObject.META_TITLE: self._getOptionalMetadata(maskMeta, 'title'), 
         ServiceObject.META_AUTHOR: self._getOptionalMetadata(maskMeta, 'author'), 
         ServiceObject.META_DESCRIPTION: self._getOptionalMetadata(maskMeta, 'description'),
         ServiceObject.META_KEYWORDS: self._getOptionalMetadata(maskMeta, 'keywords'),
         ServiceObject.META_CITATION: self._getOptionalMetadata(maskMeta, 'citation')}
      # required
      try:
         dloc = os.path.join(ENV_DATA_PATH, maskMeta['file'])
      except KeyError:
         raise LMError(currargs='Missing `file` key in SDM_MASK_META in scenPkg metadata')
      else:
         if not os.path.exists(dloc):
            print('Missing local data {}'.format(dloc))
 
      try:
         masklyr = Raster(maskMeta['name'], self.usr, 
                          pkgMeta['epsg'], 
                          mapunits=pkgMeta['mapunits'],  
                          resolution=maskMeta['res'][1], 
                          dlocation=dloc, metadata=lyrmeta, 
                          dataFormat=maskMeta['gdalformat'], 
                          gdalType=maskMeta['gdaltype'], 
                          bbox=maskMeta['region'],
                          modTime=mx.DateTime.gmt().mjd)
      except KeyError:
         raise LMError(currargs='Missing one of: name, res, region, gdaltype, ' + 
                       'gdalformat in SDM_MASK_META in scenPkg metadata')

      return masklyr

   # ...............................................
   def addUser(self):
      """
      @summary Adds provided userid to the database
      """
      user = LMUser(self.userId, self.userEmail, self.userEmail, 
                    modTime=mx.DateTime.gmt().mjd)
      self.scribe.log.info('  Find or insert user {} ...'.format(self.userId))
      updatedUser = self.scribe.findOrInsertUser(user)
      # If exists, found by unique Id or Email, update values
      self.userId = updatedUser.userid
      self.userEmail = updatedUser.email
   
   # ...............................................
   def createScenPackage(self):
      SPMETA = self._findScenPkgMetadata()
      pkgMeta = SPMETA.CLIMATE_PACKAGES[self.scenPackageName]
      # TODO: Put optional masklayer into every Scenario
      try:
         maskMeta = SPMETA.SDM_MASK_META
      except:
         masklyr = None
      else:
         masklyr = self._createMaskLayer(pkgMeta, maskMeta)

      self.scribe.log.info('  Read ScenPackage {} metadata ...'.format(self.scenPackageName))
      scenPkg = ScenPackage(self.scenPackageName, self.usr, 
                            epsgcode=pkgMeta['epsg'],
                            mapunits=pkgMeta['mapunits'],
                            modTime=mx.DateTime.gmt().mjd)
      
      # Current
      bscenCode = pkgMeta['baseline']
      bscen = self._createScenario(pkgMeta, bscenCode, SPMETA.SCENARIO_META,
                                   SPMETA.LAYERTYPE_META)
      self.scribe.log.info('     Assembled base scenario {}'.format(bscenCode))
      allScens = {bscenCode: bscen}

      # Predicted Past and Future
      for pscenCode in pkgMeta['predicted']:
         pscen = self._createScenario(pkgMeta, pscenCode, SPMETA.SCENARIO_META,
                                      SPMETA.LAYERTYPE_META)
         allScens[pscenCode] = pscen
 
      self.scribe.log.info('     Assembled predicted scenarios {}'.format(allScens.keys()))
      for scen in allScens.values():
         scenPkg.addScenario(scen)      
      scenPkg.resetBBox()
      
      return (scenPkg, masklyr)
   
   # .......................
   # ...............................................
   def _getbioName(self, code, res, gcm=None, tm=None, altpred=None, 
                   lyrtype=None, suffix=None, isTitle=False):
      sep = '-'
      if isTitle: 
         sep = ', '
      name = code
      if lyrtype is not None:
         name = sep.join((lyrtype, name))
      for descriptor in (gcm, altpred, tm, res, suffix):
         if descriptor is not None:
            name = sep.join((name, descriptor))
      return name
    
   # ...............................................
   def _getOptionalMetadata(self, metaDict, key):
      """
      @summary Assembles layer metadata for mask
      """
      val = None
      try:
         val = metaDict[key]
      except:
         pass
      return val


   # ...............................................
   def _getScenLayers(self, pkgMeta, scenMeta, lyrtypeMeta):
      """
      @summary Assembles layer metadata for a single layerset
      """
      currtime = mx.DateTime.gmt().mjd
      layers = []
      staticLayers = {}
      dateCode = scenMeta['times'].keys()[0]
      res_name = scenMeta['res'][0]
      res_val = scenMeta['res'][1]
#       resolution = scenMeta['res']
      region = scenMeta['region']
      for envcode in pkgMeta['layertypes']:
         ltmeta = lyrtypeMeta[envcode]
         envKeywords = [k for k in scenMeta['keywords']]
         relfname, isStatic = self._findFileFor(ltmeta, scenMeta['code'], 
                                           gcm=None, tm=None, altPred=None)
         lyrname = self._getbioName(scenMeta['code'], res_name, 
                                    lyrtype=envcode, suffix=pkgMeta['suffix'])
         lyrmeta = {'title': ' '.join((scenMeta['code'], ltmeta['title'])),
                    'description': ' '.join((scenMeta['code'], ltmeta['description']))}
         envmeta = {'title': ltmeta['title'],
                    'description': ltmeta['description'],
                    'keywords': envKeywords.extend(ltmeta['keywords'])}
         dloc = os.path.join(ENV_DATA_PATH, relfname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
         envlyr = EnvLayer(lyrname, self.usr, pkgMeta['epsg'], 
                           dlocation=dloc, 
                           lyrMetadata=lyrmeta,
                           dataFormat=pkgMeta['gdalformat'], 
                           gdalType=pkgMeta['gdaltype'],
                           valUnits=ltmeta['valunits'],
                           mapunits=pkgMeta['mapunits'], 
                           resolution=res_val, 
                           bbox=region, 
                           modTime=currtime, 
                           envCode=envcode, 
                           dateCode=dateCode,
                           envMetadata=envmeta,
                           envModTime=currtime)
         layers.append(envlyr)
         if isStatic:
            staticLayers[envcode] = envlyr
      return layers, staticLayers         

   # ...............................................
   def _createScenario(self, pkgMeta, scenCode, scenMeta, lyrtypeMeta):
      """
      @summary Assemble Worldclim/bioclim scenario
      """
      baseScenCode = pkgMeta['baseline']
      res_name = scenMeta['res'][0]
      res_val = scenMeta['res'][1]
      # there should only be one
      scencode = self._getbioName(baseScenCode, res_name, suffix=pkgMeta['suffix'])
      lyrs, staticLayers = self._getBaselineLayers(pkgMeta, scenMeta, 
                                              lyrtypeMeta)
      scenmeta = {ServiceObject.META_TITLE: scenMeta['title'], 
                  ServiceObject.META_AUTHOR: scenMeta['author'], 
                  ServiceObject.META_DESCRIPTION: scenMeta['description'], 
                  ServiceObject.META_KEYWORDS: scenMeta['keywords']}
      scen = Scenario(scencode, self.usr, pkgMeta['epsg'], 
                      metadata=scenmeta, 
                      units=pkgMeta['mapunits'], 
                      res=res_val, 
                      dateCode=scenMeta['date'],
                      bbox=scenMeta['region'], 
                      modTime=mx.DateTime.gmt().mjd,  
                      layers=lyrs)
      return scen, staticLayers
   
   # ...............................................
   def addPackageScenariosLayers(self, scenPkg):
      """
      @summary Add scenPackage, scenario and layer metadata to database, and 
               update the scenPkg attribute with newly inserted objects
      """
      # Insert empty ScenPackage
      updatedScenPkg = self.scribe.findOrInsertScenPackage(scenPkg)
      updatedScens = []
      for scode, scen in scenPkg.scenarios.iteritems():
         self.scribe.log.info('Insert scenario {}'.format(scode))
         # Insert Scenario and its Layers
         newscen = self.scribe.findOrInsertScenario(scen, 
                                             scenPkgId=updatedScenPkg.getId())
         updatedScens.append(newscen)
      updatedScenPkg.setScenarios(updatedScens)
   
   # ...............................................
   def addMaskLayer(self, masklyr):
      updatedMask = None
      if masklyr is not None:
         updatedMask = self.scribe.findOrInsertLayer(masklyr)
      return updatedMask
   
   # ...............................................
   def _findScenPkgMetadata(self):
      scenPackageMetaFilename = os.path.join(ENV_DATA_PATH, 
                     '{}{}'.format(self.scenPackageName, LMFormat.PYTHON.ext))      
      if not os.path.exists(scenPackageMetaFilename):
         raise LMError(currargs='Climate metadata {} does not exist'
                       .format(scenPackageMetaFilename))
      # TODO: change to importlib on python 2.7 --> 3.3+  
      try:
         import imp
         SPMETA = imp.load_source('currentmetadata', scenPackageMetaFilename)
      except Exception, e:
         raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                       .format(scenPackageMetaFilename, e))
      return SPMETA

# ...............................................
def catalogScenPackage(spMetaFname, userid, userEmail):
   """
   @summary: Initialize an empty Lifemapper database and archive
   """
   filler = SPFiller(spMetaFname, userid, userEmail)

   # If email is None, a dummy email will be created
   filler.addUser()

   scenPkg, masklyr = filler.createScenPackage()

   # This updates the scenPkg with db objects for other operations
   updatedScenPkg = filler.addPackageScenariosLayers(scenPkg)
   
   updatedMask = filler.addMaskLayer(masklyr)
         
   
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('--scen_package_meta', default=None,
            help=('Metadata file for Scenario package to be cataloged in the database.'))
   parser.add_argument('--user_id', default=None,
            help=('User authorized for the scenario package'))
   parser.add_argument('--user_email', default=None,
            help=('User email'))
   args = parser.parse_args()
   scenpkg_meta_file = args.scen_pkg
   user_id = args.user_id
   user_email = args.user_email
   
   if not os.path.exists(scenpkg_meta_file):
      # if using package name, look in default location)
      scenpkg_meta_file = os.path.join(ENV_DATA_PATH, scenpkg_meta_file + '.py')
      if not os.path.exists(scenpkg_meta_file):
         print ('Missing Scenario Package metadata file {}'.format(scenpkg_meta_file))
         exit(-1)    
   
   print('Running catalogScenPkg with scenpkg_meta_file = {}, userid = {}, email = {}'
         .format(scenpkg_meta_file, user_id, user_email))
   catalogScenPackage(scenpkg_meta_file, user_id, user_email)


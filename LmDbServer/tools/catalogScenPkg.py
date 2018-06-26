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
import mx.DateTime
import os
import time

from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.lmconstants import (LMFormat, DEFAULT_POST_USER, 
                                         DEFAULT_EPSG, DEFAULT_MAPUNITS) 

from LmDbServer.common.lmconstants import TNCMetadata, TAXONOMIC_SOURCE

from LmServer.common.lmconstants import Algorithms
from LmServer.common.lmconstants import (ENV_DATA_PATH, DEFAULT_EMAIL_POSTFIX)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.scenario import Scenario, ScenPackage

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
   def __init__(self, spMetaFname, userId, email=None):
      """
      @summary Constructor for BOOMFiller class.
      """
      super(SPFiller, self).__init__()
      self.name = self.__class__.__name__.lower()
      self.spMetaFname = spMetaFname
      self.userId = userId
      self.userEmail = email
      if self.userId != PUBLIC_USER:
         if self.userEmail is None:
            self.userEmail = '{}{}'.format(self.userId, DEFAULT_EMAIL_POSTFIX)
      self.spMeta = self._findScenPkgMetadata()
            
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
         masklyr = Raster(maskMeta['name'], self.userId, 
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
   def addUser(self, userid, email):
      """
      @summary Adds or finds PUBLIC_USER, DEFAULT_POST_USER and USER arguments 
               in the database
      """
      currtime = mx.DateTime.gmt().mjd
      # Nothing changes if these are already present
      user = LMUser(userid, email, email, modTime=currtime)
      self.scribe.log.info('  Find or insert user {} ...'.format(userid))
      thisUser = self.scribe.findOrInsertUser(user)
      # If exists, found by unique Id or Email, update values
      return thisUser.userid

#       
   # .............................
   def addTNCEcoregions(self):
      meta = {Vector.META_IS_CATEGORICAL: TNCMetadata.isCategorical, 
              ServiceObject.META_TITLE: TNCMetadata.title, 
              ServiceObject.META_AUTHOR: TNCMetadata.author, 
              ServiceObject.META_DESCRIPTION: TNCMetadata.description,
              ServiceObject.META_KEYWORDS: TNCMetadata.keywords,
              ServiceObject.META_CITATION: TNCMetadata.citation,
              }
      dloc = os.path.join(ENV_DATA_PATH, 
                          TNCMetadata.filename + LMFormat.getDefaultOGR().ext)
      ecoregions = Vector(TNCMetadata.title, PUBLIC_USER, DEFAULT_EPSG, 
                          ident=None, dlocation=dloc, 
                          metadata=meta, dataFormat=LMFormat.getDefaultOGR().driver, 
                          ogrType=TNCMetadata.ogrType,
                          valAttribute=TNCMetadata.valAttribute, 
                          mapunits=DEFAULT_MAPUNITS, bbox=TNCMetadata.bbox,
                          modTime=mx.DateTime.gmt().mjd)
      updatedEcoregions = self.scribe.findOrInsertLayer(ecoregions)
      return updatedEcoregions

   # ...............................................
   def addAlgorithms(self):
      """
      @summary Adds algorithms to the database from the algorithm dictionary
      """
      algs = []
      for alginfo in Algorithms.implemented():
         meta = {'name': alginfo.name, 
                 'isDiscreteOutput': alginfo.isDiscreteOutput,
                 'outputFormat': alginfo.outputFormat,
                 'acceptsCategoricalMaps': alginfo.acceptsCategoricalMaps}
         alg = Algorithm(alginfo.code, metadata=meta)
         self.scribe.log.info('  Insert algorithm {} ...'.format(alginfo.code))
         algid = self.scribe.findOrInsertAlgorithm(alg)
         algs.append(algid)
   
   # ...............................................
   def createScenPackage(self, spName):
      pkgMeta = self.spMeta.CLIMATE_PACKAGES[spName]
      lyrMeta = self.spMeta.LAYERTYPE_META
      # TODO: Put optional masklayer into every Scenario
      try:
         maskMeta = self.spMeta.SDM_MASK_META
      except:
         masklyr = None
      else:
         masklyr = self._createMaskLayer(pkgMeta, maskMeta)

      self.scribe.log.info('  Read ScenPackage {} metadata ...'.format(spName))
      scenPkg = ScenPackage(spName, self.userId, 
                            epsgcode=pkgMeta['epsg'],
                            mapunits=pkgMeta['mapunits'],
                            modTime=mx.DateTime.gmt().mjd)
      
      # Current
      baseCode = pkgMeta['baseline']
      scenMeta = self.spMeta.SCENARIO_META[baseCode]
      bscen = self._createScenario(pkgMeta, baseCode, scenMeta, lyrMeta)
      self.scribe.log.info('     Assembled base scenario {}'.format(baseCode))
      allScens = {baseCode: bscen}

      # Predicted Past and Future
      for predCode in pkgMeta['predicted']:
         scenMeta = self.spMeta.SCENARIO_META[predCode]
         pscen = self._createScenario(pkgMeta, predCode, scenMeta, lyrMeta)
         allScens[predCode] = pscen
 
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
   def _getScenLayers(self, pkgMeta, scenCode, scenMeta, lyrMeta):
      """
      @summary Assembles layer metadata for a single layerset
      """
      currtime = mx.DateTime.gmt().mjd
      layers = []
      staticLayers = {}
      dateCode = scenMeta['date']
      res_name = scenMeta['res'][0]
      res_val = scenMeta['res'][1]
      scenKeywords = [k for k in scenMeta['keywords']]
      region = scenMeta['region']

      for envcode in pkgMeta['layertypes']:
         ltmeta = lyrMeta[envcode]
         lyrKeywords = [k for k in ltmeta['keywords']]
         lyrKeywords.extend(scenKeywords)
         relfname = ltmeta['files'][scenCode]
         lyrname = '{}_{}'.format(envcode, scenCode)
         lyrmeta = {'title': ' '.join((ltmeta['title'], scenCode)),
                    'description': ' '.join((scenCode, ltmeta['description']))}
         envmeta = {'title': ltmeta['title'],
                    'description': ltmeta['description'],
                    'keywords': lyrKeywords}
         dloc = os.path.join(ENV_DATA_PATH, relfname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
         envlyr = EnvLayer(lyrname, self.userId, pkgMeta['epsg'], 
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
      return layers         

   # ...............................................
   def _createScenario(self, pkgMeta, scenCode, scenMeta, lyrMeta):
      """
      @summary Assemble Worldclim/bioclim scenario
      """
      res_val = scenMeta['res'][1]
      lyrs = self._getScenLayers(pkgMeta, scenCode, scenMeta, lyrMeta)
      scenmeta = {ServiceObject.META_TITLE: scenMeta['name'], 
                  ServiceObject.META_AUTHOR: scenMeta['author'], 
                  ServiceObject.META_DESCRIPTION: scenMeta['description'], 
                  ServiceObject.META_KEYWORDS: scenMeta['keywords']}
      scen = Scenario(scenCode, self.userId, pkgMeta['epsg'], 
                      metadata=scenmeta, 
                      units=pkgMeta['mapunits'], 
                      res=res_val, 
                      dateCode=scenMeta['date'],
                      bbox=scenMeta['region'], 
                      modTime=mx.DateTime.gmt().mjd,  
                      layers=lyrs)
      return scen
   
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
      if not os.path.exists(self.spMetaFname):
         raise LMError(currargs='Climate metadata {} does not exist'
                       .format(self.spMetaFname))
      # TODO: change to importlib on python 2.7 --> 3.3+  
      try:
         import imp
         SPMETA = imp.load_source('currentmetadata', self.spMetaFname)
      except Exception, e:
         raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                       .format(self.spMetaFname, e))
      return SPMETA


   # ...............................................
   def addDefaults(self):
      """
      @summary Inserts or locates PUBLIC_USER, DEFAULT_POST_USER, 
               TAXONOMIC_SOURCE, ALGORITHMS, and TNC_ECOREGIONS in the database
      """
      currtime = mx.DateTime.gmt().mjd
      
      #Adds or finds PUBLIC_USER, DEFAULT_POST_USER 
      # Nothing changes if these are already present
      _ = self.addUser(PUBLIC_USER, 
                       '{}{}'.format(PUBLIC_USER, DEFAULT_EMAIL_POSTFIX))
      _ = self.addUser(DEFAULT_POST_USER, 
                       '{}{}'.format(DEFAULT_POST_USER, DEFAULT_EMAIL_POSTFIX))
      # Insert all taxonomic sources for now
      self.scribe.log.info('  Insert taxonomy metadata ...')
      for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
         taxSourceId = self.scribe.findOrInsertTaxonSource(taxInfo['name'],
                                                             taxInfo['url'])
      # Insert all algorithms 
      self.scribe.log.info('  Insert Algorithms ...')
      self.addAlgorithms()
   
      # Insert all algorithms 
      self.scribe.log.info('  Insert TNC Ecoregions ...')
      self.addTNCEcoregions()
      

# ...............................................
def catalogScenPackages(spMetaFname, userId, userEmail):
   """
   @summary: Initialize an empty Lifemapper database and archive
   """
   filler = SPFiller(spMetaFname, userId, userEmail)
   
   filler.addDefaults()
   # If email is not provided, a dummy email will be created
   # ARCHIVE_USER and DEFAULT_POST_USER will be added if they are missing 
   # (i.e. this is the first time this script has been run)
      # If exists, found by unique Id or Email, update values
   filler.userId = filler.addUser(filler.userId, filler.userEmail)
   
   updatedMask = None
   for spName in filler.spMeta.CLIMATE_PACKAGES.keys():
      filler.scribe.log.info('Creating scenario package {}'.format(spName))
      scenPkg, masklyr = filler.createScenPackage(spName)
      
      # Only one Mask is included per scenario package
      if updatedMask is None:
         filler.scribe.log.info('Adding mask layer {}'.format(masklyr.name))
         updatedMask = filler.addMaskLayer(masklyr)
      
      updatedScenPkg = filler.addPackageScenariosLayers(scenPkg)
      
      if (updatedScenPkg is not None 
          and updatedScenPkg.getId() is not None
          and updatedScenPkg.name == spName
          and updatedScenPkg.getUserId() == filler.userId):
         filler.scribe.log.info('Successfully added scenario package {} for user {}'
                                .format(spName, filler.userId))
   
   
   
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
   scenpkg_meta_file = args.scen_package_meta
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
   catalogScenPackages(scenpkg_meta_file, user_id, user_email)

"""
find . -name "*.in" -exec sed -i s%@LMHOME@%/opt/lifemapper%g {} \;



find . -name "*.in" -exec sed -i \
        -e 's%@LMHOME@%/opt/lifemapper%g' \
        -e 's%@LMSCRATCHDISK@%/state/partition1/lmscratch%g' \
        -e 's%@PYTHONVER@%python2.7%g' \
        -e 's%@PYBIN@%/opt/python/bin%g' \
        -e 's%@PKGROOT@%/opt/lifemapper%g' \
        -e 's%@UNIXSOCKET@%$(UNIXSOCKET)%g' \
        -e 's%@SCENARIO_PACKAGE@%$(SCENARIO_PACKAGE)%g' \
        -e 's%@ENV_DATA_DIR@%layers%g' \
        -e 's%@DATADIR_SERVER@%/share/lmserver/data%g' \
        -e 's%@DATADIR_SHARED@%/share/lm/data%g' \
        -e 's%@LMURL@%http://yeti.lifemapper.org/dl%g' \
        -e 's%@TARBALL_POSTFIX@%tar.gz%g' \
        -e 's%@LMCLIENT@%sdm%g' \
{} \;

import mx.DateTime
import os
import time

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import LMFormat
from LmServer.common.lmconstants import (ENV_DATA_PATH, DEFAULT_EMAIL_POSTFIX)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.scenario import Scenario, ScenPackage
from LmDbServer.tools.catalogScenPkg import *

spMetaFname = '/share/lm/data/layers/sax_layers_10min.py'
userid = 'aimee2'
userEmail = None

filler = SPFiller(spMetaFname, userid, userEmail)
filler.addUser()

spName = filler.spMeta.CLIMATE_PACKAGES.keys()[0]
scenPkg, masklyr = filler.createScenPackage(spName)

scode = scenPkg.scenarios.keys()[0]
scen = scenPkg.scenarios.values()[0]
                      
updatedMask = filler.addMaskLayer(masklyr)
updatedScenPkg = filler.addPackageScenariosLayers(scenPkg)

existingPkg = filler.scribe.getScenPackage(userId=userid, scenPkgName=spName, fillLayers=True)
"""

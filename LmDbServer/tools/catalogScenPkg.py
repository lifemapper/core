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

from LmBackend.command.server import CatalogScenarioPackageCommand
from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.lmconstants import JobStatus

from LmServer.common.lmconstants import (Priority, ENV_DATA_PATH, 
                                         DEFAULT_EMAIL_POSTFIX)
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.processchain import MFChain
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
   def __init__(self, spMetaFname, userId, email=None, logname=None, scribe=None):
      """
      @summary Constructor for BOOMFiller class.
      """
      super(SPFiller, self).__init__()
      self.name = self.__class__.__name__.lower()
      if not os.path.exists(spMetaFname):
         raise LMError(currargs='Climate metadata {} does not exist'
                       .format(spMetaFname))
      spBasename, _ = os.path.splitext(os.path.basename(spMetaFname))
      
      # TODO: change to importlib on python 2.7 --> 3.3+  
      try:
         import imp
         self.spMeta  = imp.load_source('currentmetadata', spMetaFname)
      except Exception, e:
         raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                       .format(spMetaFname, e))

      self.spMetaFname = spMetaFname
      self.userId = userId
      self.userEmail = email
      if self.userEmail is None:
         self.userEmail = '{}{}'.format(self.userId, DEFAULT_EMAIL_POSTFIX)
      
      # Logfile
      if logname is None:
         logname = '{}.{}.{}'.format(self.name, spBasename, userId)
      self.logname = logname
      
      self.scribe = scribe
      
   # ...............................................
   def initializeMe(self):
      if not (self.scribe and self.scribe.isOpen()):
         # Get database
         try:
            self.scribe = self._getDb(self.logname)
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
   def _getDb(self, logname):
      import logging
      logger = ScriptLogger(logname, level=logging.INFO)
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
            raise LMError('Missing local data {}'.format(dloc))
 
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
   def addUser(self):
      """
      @summary Adds or finds PUBLIC_USER, DEFAULT_POST_USER and USER arguments 
               in the database
      """
      currtime = mx.DateTime.gmt().mjd
      # Nothing changes if these are already present
      user = LMUser(self.userId, self.userEmail, modTime=currtime)
      self.scribe.log.info('  Find or insert user {} ...'.format(self.userId))
      thisUser = self.scribe.findOrInsertUser(user)
      # If exists, found by unique Id or Email, update values
      return thisUser.userid
   
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
      dateCode = scenMeta['date']
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
            raise LMError('Missing local data {}'.format(dloc))
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
   def catalogScenPackages(self):
      """
      @summary: Initialize an empty Lifemapper database and archive
      """
      updatedScenPkg = None
      try:
         self.initializeMe()
         # If exists, found by unique Id or Email, update values
         userId = self.addUser()
         
         updatedMask = None
         for spName in self.spMeta.CLIMATE_PACKAGES.keys():
            self.scribe.log.info('Creating scenario package {}'.format(spName))
            scenPkg, masklyr = self.createScenPackage(spName)
            
            # Only one Mask is included per scenario package
            if updatedMask is None:
               self.scribe.log.info('Adding mask layer {}'.format(masklyr.name))
               updatedMask = self.addMaskLayer(masklyr)
               if updatedMask.getDLocation() != masklyr.getDLocation():
                  raise LMError('''Returned existing layer name {} for user {} with 
                                   filename {}, not expected filename {}'''
                                   .format(masklyr.name, self.userId, 
                                           updatedMask.getDLocation(), 
                                           masklyr.getDLocation()))
            updatedScenPkg = self.addPackageScenariosLayers(scenPkg)
            if (updatedScenPkg is not None 
                and updatedScenPkg.getId() is not None
                and updatedScenPkg.name == spName
                and updatedScenPkg.getUserId() == self.userId):
               self.scribe.log.info('Successfully added scenario package {} for user {}'
                                      .format(spName, self.userId))
      finally:
         self.close()
         
      return updatedScenPkg 
      
   
   # ...............................................
   def createCatalogScenPkgMF(self):
      """
      @summary: Create a Makeflow to initiate Boomer with inputs assembled 
                and configFile written by BOOMFiller.initBoom.
      """
      scriptname, _ = os.path.splitext(os.path.basename(__file__))
      spname, _ = os.path.splitext(os.path.basename(self.spMetaFname))
      meta = {MFChain.META_CREATED_BY: scriptname,
              MFChain.META_DESCRIPTION: 'Catalog scenario task for user {} scenpkg {}'
      .format(self.userId, spname)}
      try:
         self.initializeMe()
         newMFC = MFChain(self.userId, priority=Priority.HIGH, 
                          metadata=meta, status=JobStatus.GENERAL, 
                          statusModTime=mx.DateTime.gmt().mjd)
         mfChain = self.scribe.insertMFChain(newMFC, None)
      
         # Create a rule from the MF and Arf file creation
         spCmd = CatalogScenarioPackageCommand(self.spMetaFname, self.userId)
      
         mfChain.addCommands([spCmd.getMakeflowRule(local=True)])
         mfChain.write()
         mfChain.updateStatus(JobStatus.INITIALIZE)
         self.scribe.updateObject(mfChain)
      finally:
         self.close()
      return mfChain

   
# ...............................................
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   # Required
   parser.add_argument('user_id', type=str,
            help=('User authorized for the scenario package'))
   parser.add_argument('scen_package_meta', type=str,
            help=('Metadata file for Scenario package to be cataloged in the database.'))
   
   # Optional
   parser.add_argument('--user_email', type=str, default=None,
            help=('User email'))
   parser.add_argument('--logname', type=str, default=None,
            help=('Basename of the logfile, without extension'))
   parser.add_argument('--init_makeflow', type=bool, default=False,
            help=("""Create a Makeflow task to add this scenario package of 
                     environmental data for this user"""))
   
   args = parser.parse_args()
   user_id = args.user_id
   scen_package_meta = args.scen_package_meta
   logname = args.logname
   user_email = args.user_email
   initMakeflow = args.init_makeflow
   
   if logname is None:
      import time
      scriptname, _ = os.path.splitext(os.path.basename(__file__))
      secs = time.time()
      timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
      logname = '{}.{}'.format(scriptname, timestamp)

   
   # scen_package_meta may be full pathname or in ENV_DATA_PATH dir
   if not os.path.exists(scen_package_meta):
      # if using package name, look in default location)
      scen_package_meta = os.path.join(ENV_DATA_PATH, scen_package_meta + '.py')
      if not os.path.exists(scen_package_meta):
         print ('Missing Scenario Package metadata file {}'.format(scen_package_meta))
         exit(-1)
      else:
         print('Running script with scen_package_meta: {}, userid: {}, email: {}, logbasename: {}'
               .format(scen_package_meta, user_id, user_email, logname))

      filler = SPFiller(scen_package_meta, user_id, email=user_email, 
                        logname=logname)
      filler.initializeMe()
      
      if initMakeflow:
         filler.createCatalogScenPkgMF()
      else:
         filler.catalogScenPackages()
   
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

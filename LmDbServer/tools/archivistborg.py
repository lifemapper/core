"""
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import argparse
import mx.DateTime as dt
import os, sys, time

from LmBackend.common.daemon import Daemon
from LmDbServer.common.lmconstants import (BOOM_PID_FILE, TAXONOMIC_SOURCE)
from LmDbServer.pipeline.boomborg import BisonBoom, GBIFBoom, iDigBioBoom, UserBoom
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import DEFAULT_CONFIG
from LmServer.common.log import ScriptLogger

# .............................................................................
class Archivist(Daemon):
   # .............................
   def __init__(self, pidfile, log=None, envSource=None, speciesSource=None):      
      Daemon.__init__(self, pidfile, log=log)
      self.name = self.__class__.__name__.lower()
      if envSource == DEFAULT_CONFIG:
         from LmDbServer.common.localconstants import SCENARIO_PACKAGE
         envSource = SCENARIO_PACKAGE
      self.envSource = envSource
      self.speciesSource = speciesSource
                        
   # .............................
   @staticmethod
   def getArchiveSpecificConfig(envSource=None):
      from LmCommon.common.config import Config
      from LmCommon.common.lmconstants import BISON_MIN_POINT_COUNT, OutputFormat
      from LmServer.common.lmconstants import ENV_DATA_PATH, SPECIES_DATA_PATH
      fileList = []
      if envSource is not None:
         cfgfile = os.path.join(ENV_DATA_PATH, '{}.ini'.format(envSource))
         if os.path.exists(cfgfile):
            fileList.append(cfgfile)
      cfg = Config(fns=fileList)
      
      _CONFIG_HEADING = "LmServer - pipeline"
      _ENV_CONFIG_HEADING = "LmServer - environment"
   
      user = cfg.get(_ENV_CONFIG_HEADING, 'ARCHIVE_USER')
      datasource = cfg.get(_ENV_CONFIG_HEADING, 'DATASOURCE')
      # Data Archive Pipeline
      algorithms = cfg.getlist(_CONFIG_HEADING, 'DEFAULT_ALGORITHMS')
      mdlScen = cfg.get(_CONFIG_HEADING, 'DEFAULT_MODEL_SCENARIO')
      prjScens = cfg.getlist(_CONFIG_HEADING, 'DEFAULT_PROJECTION_SCENARIOS')
      epsg = cfg.getint(_CONFIG_HEADING, 'DEFAULT_EPSG')
      gridname = cfg.get(_CONFIG_HEADING, 'DEFAULT_GRID_NAME')
      minPoints = cfg.getint(_CONFIG_HEADING, 'POINT_COUNT_MIN')
      # Expiration date for retrieved species data 
      speciesExpYear = cfg.getint(_CONFIG_HEADING, 'SPECIES_EXP_YEAR')
      speciesExpMonth = cfg.getint(_CONFIG_HEADING, 'SPECIES_EXP_MONTH')
      speciesExpDay = cfg.getint(_CONFIG_HEADING, 'SPECIES_EXP_DAY')
   
      # User data   
      userOccData = cfg.get(_CONFIG_HEADING, 'USER_OCCURRENCE_DATA')
      userOccCSV = os.path.join(SPECIES_DATA_PATH, userOccData + OutputFormat.CSV)
      userOccMeta = os.path.join(SPECIES_DATA_PATH, userOccData + OutputFormat.METADATA)
      
      # Bison data
      bisonTsn = Config().get(_CONFIG_HEADING, 'TSN_FILENAME')
      bisonTsnFile = os.path.join(SPECIES_DATA_PATH, bisonTsn)
      if datasource == 'BISON':
         minPoints = BISON_MIN_POINT_COUNT
         
      # iDigBio data
      idigTaxonids = Config().get(_CONFIG_HEADING, 'IDIG_FILENAME')
      idigTaxonidsFile = os.path.join(SPECIES_DATA_PATH, idigTaxonids)
      
      # GBIF data
      gbifTax = cfg.get(_CONFIG_HEADING, 'TAXONOMY_FILENAME')
      gbifTaxFile = os.path.join(SPECIES_DATA_PATH, gbifTax)
      gbifOcc = cfg.get(_CONFIG_HEADING, 'OCCURRENCE_FILENAME')
      gbifOccFile = os.path.join(SPECIES_DATA_PATH, gbifOcc)
      gbifProv = cfg.get(_CONFIG_HEADING, 'PROVIDER_FILENAME')
      gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
         
      return (user, datasource, algorithms, minPoints, mdlScen, prjScens, epsg, 
              gridname, userOccCSV, userOccMeta, bisonTsnFile, idigTaxonidsFile, 
              gbifTaxFile, gbifOccFile, gbifProvFile, 
              speciesExpYear, speciesExpMonth, speciesExpDay)  

   # .............................
   def initialize(self):
      """
      @summary: Creates objects (OccurrenceSets, SMDModels, SDMProjections, 
                and MatrixColumns (intersections with the default grid)) 
                and job requests for their calculation.
      @note: The argument to this script/daemon contains variables to override 
             installed defaults
      """
      (user, datasource, algorithms, minPoints, mdlScen, prjScens, epsg, 
       gridname, userOccCSV, userOccMeta, bisonTsnFile, idigTaxonidsFile, 
       gbifTaxFile, gbifOccFile, gbifProvFile, speciesExpYear, speciesExpMonth, 
       speciesExpDay) = self.getArchiveSpecificConfig()

      expdate = dt.DateTime(speciesExpYear, speciesExpMonth, speciesExpDay).mjd 
      try:
         taxname = TAXONOMIC_SOURCE[datasource]['name']
      except:
         taxname = None
      try:
         if datasource == 'BISON':
            self.boomer = BisonBoom(user, epsg, algorithms, mdlScen, prjScens,
                            bisonTsnFile, expdate, 
                            taxonSourceName=taxname, mdlMask=None, prjMask=None, 
                            minPointCount=minPoints, 
                            intersectGrid=gridname, log=self.log)
            
         elif datasource == 'GBIF':
            self.boomer = GBIFBoom(user, epsg, algorithms, mdlScen, prjScens,
                            gbifOccFile, expdate, taxonSourceName=taxname,
                            providerListFile=gbifProvFile,
                            mdlMask=None, prjMask=None, 
                            minPointCount=minPoints,  
                            intersectGrid=gridname, log=self.log)
            
         elif datasource == 'IDIGBIO':
            self.boomer = iDigBioBoom(user, epsg, algorithms, mdlScen, prjScens, 
                            idigTaxonidsFile, expdate, taxonSourceName=taxname,
                            mdlMask=None, prjMask=None, 
                            minPointCount=minPoints, 
                            intersectGrid=gridname, log=self.log)
   
         else:
            self.boomer = UserBoom(user, epsg, algorithms, mdlScen, prjScens, 
                            userOccCSV, userOccMeta, expdate, 
                            mdlMask=None, prjMask=None, 
                            minPointCount=minPoints, 
                            intersectGrid=gridname, log=self.log)
      except Exception, e:
         raise LMError(currargs='Failed to initialize Archivist ({})'.format(e))
      
   # .............................
   def run(self):
      try:
         self.boomer.moveToStart()
         self.log.debug('Starting boomer at location {} ... '
                        .format(self.boomer.currRecnum))
         while self.keepRunning:
            try:
               self.log.info('Next species ...')
               self.boomer.chainOne()
               if self.keepRunning:
                  self.keepRunning = not self.boomer.complete
            except:
               self.log.info('Saving next start {} ...'
                             .format(self.boomer.nextStart))
               self.boomer.saveNextStart()
               raise
            else:
               time.sleep(10)
      finally:
         self.boomer.close()
      self.log.debug('Stopped Archivist')
    
   # .............................
   def onUpdate(self):
      self.log.debug("Update signal caught!")
       
   # .............................
   def onShutdown(self):
      self.boomer.saveNextStart()
      self.boomer.close()
      self.log.debug("Shutdown signal caught!")
      Daemon.onShutdown(self)

def isCorrectUser():
   """ find current user """
   import subprocess
   cmd = "/usr/bin/whoami"
   info, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE).communicate()
   usr = info.split()[0]
   if usr == 'lmwriter':
      return True
   return False

# .............................................................................
if __name__ == "__main__":
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-e', '--env_source', default=DEFAULT_CONFIG,
            help=('Input config file should exist in the ENV_DATA_PATH ' +
                  'directory and be named with the arg value and .ini extension'))
   parser.add_argument('-s', '--species_source', default='User',
            help=('Species source will be \'User\' for user-supplied CSV data, ' +
                  '\'GBIF\' for GBIF-provided CSV data sorted by taxon id, ' +
                  '\'IDIGBIO\' for a list of GBIF accepted taxon ids suitable ' +
                  'for querying the iDigBio API'))

   args = parser.parse_args()
   envSource = args.env_source
   speciesSource = args.species_source
   if os.path.exists(BOOM_PID_FILE):
      pid = open(BOOM_PID_FILE).read().strip()
   else:
      pid = os.getpid()
      
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)
     
   secs = time.time()
   tuple = time.localtime(secs)
   timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", tuple))
   logger = ScriptLogger('archivist.{}'.format(timestamp))
   boomer = Archivist(BOOM_PID_FILE, log=logger, envSource=envSource, 
                      speciesSource=speciesSource)
     
   if len(sys.argv) == 2:
      if sys.argv[1].lower() == 'start':
         boomer.start()
      elif sys.argv[1].lower() == 'stop':
         boomer.stop()
      elif sys.argv[1].lower() == 'restart':
         boomer.restart()
      elif sys.argv[1].lower() == 'status':
         boomer.status()
      else:
         print("Unknown command: {}".format(sys.argv[1].lower()))
         sys.exit(2)
   else:
      print("usage: {} start|stop|update".format(sys.argv[0]))
      sys.exit(2)

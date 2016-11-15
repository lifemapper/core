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
import argparse
import mx.DateTime as dt
import os, sys, time

from LmBackend.common.daemon import Daemon
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import BISON_MIN_POINT_COUNT, DEFAULT_EPSG
from LmDbServer.common.lmconstants import (BOOM_PID_FILE, BISON_TSN_FILE, 
         GBIF_DUMP_FILE, PROVIDER_DUMP_FILE, IDIGBIO_FILE, TAXONOMIC_SOURCE)
from LmDbServer.pipeline.boomborg import BisonBoom, GBIFBoom, iDigBioBoom, UserBoom
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import ENV_DATA_PATH, SPECIES_DATA_PATH
from LmServer.common.localconstants import DATASOURCE
from LmServer.common.log import ScriptLogger

# .............................................................................
def getArchiveParameters(envPackageName=None):
   _ENV_HEADING = "LmServer - environment"
   _PIPELINE_HEADING = "LmServer - pipeline"
   configFiles = []
   if envPackageName != defaultConfiguration:
      # If there was a Override 
      SERVER_CONFIG_FILENAME = os.getenv('LIFEMAPPER_SERVER_CONFIG_FILE') 
      configPath = os.path.split(SERVER_CONFIG_FILENAME)
      boomFname = os.path.join(configPath, envPackageName + '.ini')
      configFiles.append(boomFname)
   cfg = Config(fns=configFiles)

   ARCHIVE_USER = cfg.get(_ENV_HEADING, 'ARCHIVE_USER')
   POINT_COUNT_MIN = cfg.getint(_PIPELINE_HEADING, 'POINT_COUNT_MIN')
   POINT_COUNT_MAX = cfg.getint(_PIPELINE_HEADING, 'POINT_COUNT_MAX')
   DEFAULT_ALGORITHMS = cfg.getlist(_PIPELINE_HEADING, 'DEFAULT_ALGORITHMS')
   DEFAULT_MODEL_SCENARIO = cfg.get(_PIPELINE_HEADING, 'DEFAULT_MODEL_SCENARIO')
   DEFAULT_PROJECTION_SCENARIOS = cfg.getlist(_PIPELINE_HEADING, 
                                                   'DEFAULT_PROJECTION_SCENARIOS')
   try:
      DEFAULT_EPSG = cfg.get(_PIPELINE_HEADING, 'DEFAULT_EPSG')
   except:
      pass
   SCENARIO_PACKAGE = cfg.get(_PIPELINE_HEADING, 'SCENARIO_PACKAGE')
   DEFAULT_GRID_NAME = cfg.get(_PIPELINE_HEADING, 'DEFAULT_GRID_NAME')
   DEFAULT_GRID_CELLSIZE = cfg.get(_PIPELINE_HEADING, 'DEFAULT_GRID_CELLSIZE')
   USER_OCCURRENCE_CSV_FILENAME = cfg.get(_PIPELINE_HEADING, 'USER_OCCURRENCE_CSV')
   USER_OCCURRENCE_META_FILENAME = cfg.get(_PIPELINE_HEADING, 'USER_OCCURRENCE_META')
   USER_OCCURRENCE_CSV = os.path.join(SPECIES_DATA_PATH, USER_OCCURRENCE_CSV_FILENAME)
   USER_OCCURRENCE_META = os.path.join(SPECIES_DATA_PATH, USER_OCCURRENCE_META_FILENAME)
   SPECIES_EXP_YEAR = cfg.getint(_PIPELINE_HEADING, 'SPECIES_EXP_YEAR')
   SPECIES_EXP_MONTH = cfg.getint(_PIPELINE_HEADING, 'SPECIES_EXP_MONTH')
   SPECIES_EXP_DAY = cfg.getint(_PIPELINE_HEADING, 'SPECIES_EXP_DAY')
   
   return (ARCHIVE_USER, POINT_COUNT_MIN, POINT_COUNT_MAX, DEFAULT_ALGORITHMS, 
           DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, SCENARIO_PACKAGE, 
           DEFAULT_GRID_NAME, DEFAULT_GRID_CELLSIZE, 
           USER_OCCURRENCE_CSV, USER_OCCURRENCE_META, 
           SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, SPECIES_EXP_DAY)

# .............................................................................
class Archivist(Daemon):
   # .............................
   def initialize(self):
      """
      @summary: Creates objects (OccurrenceSets, SMDModels, SDMProjections, 
                and MatrixColumns (intersections with the default grid)) 
                and job requests for their calculation.
      @note: The argument to this script/daemon contains variables to override 
             installed defaults
      """
      self.name = self.__class__.__name__.lower()
      expdate = dt.DateTime(SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, 
                                     SPECIES_EXP_DAY).mjd
      taxname = TAXONOMIC_SOURCE[DATASOURCE]['name']
      try:
         if DATASOURCE == 'BISON':
            self.boomer = BisonBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            BISON_TSN_FILE, expdate, 
                            taxonSourceName=taxname, mdlMask=None, prjMask=None, 
                            minPointCount=BISON_MIN_POINT_COUNT, 
                            intersectGrid=DEFAULT_GRID_NAME, log=self.log)
            
         elif DATASOURCE == 'GBIF':
            self.boomer = GBIFBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            GBIF_DUMP_FILE, expdate, taxonSourceName=taxname,
                            providerListFile=PROVIDER_DUMP_FILE,
                            mdlMask=None, prjMask=None, 
                            minPointCount=POINT_COUNT_MIN,  
                            intersectGrid=DEFAULT_GRID_NAME, log=self.log)
            
         elif DATASOURCE == 'IDIGBIO':
            self.boomer = iDigBioBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            IDIGBIO_FILE, expdate, taxonSourceName=taxname,
                            mdlMask=None, prjMask=None, 
                            minPointCount=POINT_COUNT_MIN, 
                            intersectGrid=DEFAULT_GRID_NAME, log=self.log)
   
         else:
            self.boomer = UserBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            USER_OCCURRENCE_CSV, USER_OCCURRENCE_META, expdate, 
                            mdlMask=None, prjMask=None, 
                            minPointCount=POINT_COUNT_MIN, 
                            intersectGrid=DEFAULT_GRID_NAME, log=self.log)
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
   defaultConfiguration = 'config'
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper boom archive ' +
                         'with the configured input data package named.'))
   parser.add_argument('-m', '--metadata', default=defaultConfiguration,
            help=('Metadata file should exist in the {} '.format(ENV_DATA_PATH) +
                  'directory and be named with the arg value and .py extension'))

   args = parser.parse_args()
   envPackageName = args.metadata
   
   (ARCHIVE_USER, POINT_COUNT_MIN, POINT_COUNT_MAX, DEFAULT_ALGORITHMS, 
    DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, SCENARIO_PACKAGE, 
    DEFAULT_GRID_NAME, DEFAULT_GRID_CELLSIZE, 
    USER_OCCURRENCE_CSV, USER_OCCURRENCE_META, 
    SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, SPECIES_EXP_DAY) = \
       getArchiveParameters(envPackageName=envPackageName)
       
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
   boomer = Archivist(BOOM_PID_FILE, log=logger)
     
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

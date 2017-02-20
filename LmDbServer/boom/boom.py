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
from LmCommon.common.lmconstants import OutputFormat
from LmDbServer.common.lmconstants import (BOOM_PID_FILE, TAXONOMIC_SOURCE, 
                                           SpeciesDatasource)
from LmDbServer.boom.sdmchainer import (BisonChainer, GBIFChainer, 
                                        iDigBioChainer, UserChainer)
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.base.lmobj import LMError
# from LmServer.common.localconstants import ARCHIVE_USER
from LmServer.common.log import ScriptLogger

# .............................................................................
class Archivist(Daemon):
   # .............................
   def __init__(self, pidfile, userId, archiveName, log=None):      
      Daemon.__init__(self, pidfile, log=log)
      self.name = self.__class__.__name__.lower()
      self.userId = userId
      self.archiveName = archiveName
                        
   # .............................
   @staticmethod
   def getArchiveSpecificConfig(userId=None, archiveName=None):
      from LmCommon.common.config import Config
      from LmServer.common.lmconstants import SPECIES_DATA_PATH
      
      fileList = []
      earl = EarlJr()
      pth = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
      archiveConfigFile = os.path.join(pth, 
                                 '{}{}'.format(archiveName, OutputFormat.CONFIG))
      print 'Config file at {}'.format(archiveConfigFile)
      if os.path.exists(archiveConfigFile):
         fileList.append(archiveConfigFile)
      cfg = Config(fns=fileList)
      
      _ENV_CONFIG_HEADING = "LmServer - environment"
      _PIPELINE_CONFIG_HEADING = "LmServer - pipeline"
   
      userCfg = cfg.get(_ENV_CONFIG_HEADING, 'ARCHIVE_USER')
      archiveNameCfg = cfg.get(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_NAME')
      if userId is None:
         userId = userCfg
      if archiveName is None:
         archiveName = archiveNameCfg

      try:
         datasource = cfg.get(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_DATASOURCE')
      except:
         datasource = cfg.get(_ENV_CONFIG_HEADING, 'DATASOURCE')

      algorithms = cfg.getlist(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_ALGORITHMS')
      mdlScen = cfg.get(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_MODEL_SCENARIO')
      prjScens = cfg.getlist(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_PROJECTION_SCENARIOS')
      epsg = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_EPSG')
      gridname = cfg.get(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_GRID_NAME')
      minPoints = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_POINT_COUNT_MIN')
      # Expiration date for retrieved species data 
      speciesExpYear = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_SPECIES_EXP_YEAR')
      speciesExpMonth = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_SPECIES_EXP_MONTH')
      speciesExpDay = cfg.getint(_PIPELINE_CONFIG_HEADING, 'ARCHIVE_SPECIES_EXP_DAY')
   
      # User data  
      userOccCSV = userOccMeta = userOccDelimiter = None 
      if SpeciesDatasource.isUser(datasource):
         userOccData = cfg.get(_PIPELINE_CONFIG_HEADING, 
                               'ARCHIVE_USER_OCCURRENCE_DATA')
         userOccDelimiter = cfg.get(_PIPELINE_CONFIG_HEADING, 
                               'ARCHIVE_USER_OCCURRENCE_DATA_DELIMITER')
         userOccCSV = os.path.join(pth, userOccData + OutputFormat.CSV)
         userOccMeta = os.path.join(pth, userOccData + OutputFormat.METADATA)
      
      # Bison data
      bisonTsn = Config().get(_PIPELINE_CONFIG_HEADING, 'BISON_TSN_FILENAME')
      bisonTsnFile = os.path.join(SPECIES_DATA_PATH, bisonTsn)
         
      # iDigBio data
      idigTaxonids = Config().get(_PIPELINE_CONFIG_HEADING, 'IDIG_FILENAME')
      idigTaxonidsFile = os.path.join(SPECIES_DATA_PATH, idigTaxonids)
      
      # GBIF data
      gbifTax = cfg.get(_PIPELINE_CONFIG_HEADING, 'GBIF_TAXONOMY_FILENAME')
      gbifTaxFile = os.path.join(SPECIES_DATA_PATH, gbifTax)
      gbifOcc = cfg.get(_PIPELINE_CONFIG_HEADING, 'GBIF_OCCURRENCE_FILENAME')
      gbifOccFile = os.path.join(SPECIES_DATA_PATH, gbifOcc)
      gbifProv = cfg.get(_PIPELINE_CONFIG_HEADING, 'GBIF_PROVIDER_FILENAME')
      gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
         
      return (userId, archiveName, datasource, algorithms, minPoints, 
              mdlScen, prjScens, epsg, gridname, 
              userOccCSV, userOccDelimiter, userOccMeta, 
              bisonTsnFile, idigTaxonidsFile, 
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
      (userId, archiveName, datasource, algorithms, minPoints, mdlScen, prjScens, 
       epsg, gridname, userOccCSV, userOccDelimiter, userOccMeta, 
       bisonTsnFile, idigTaxonidsFile, gbifTaxFile, gbifOccFile, gbifProvFile, 
       speciesExpYear, speciesExpMonth, 
       speciesExpDay) = self.getArchiveSpecificConfig(self.userId, self.archiveName)
      # Reset attributes in case they were not sent, relying on defaults
      self.userId = userId
      self.archiveName = archiveName
      
      expdate = dt.DateTime(speciesExpYear, speciesExpMonth, speciesExpDay).mjd 
      try:
         taxname = TAXONOMIC_SOURCE[datasource]['name']
      except:
         taxname = None
      try:
         if datasource == 'BISON':
            self.boomer = BisonChainer(self.archiveName, self.userId, epsg, 
                                    algorithms, mdlScen, prjScens, bisonTsnFile, 
                                    expdate, 
                                    taxonSourceName=taxname, mdlMask=None, 
                                    prjMask=None, 
                                    minPointCount=minPoints, 
                                    intersectGrid=gridname, log=self.log)
            
         elif datasource == 'GBIF':
            self.boomer = GBIFChainer(self.archiveName, self.userId, epsg, 
                                   algorithms, mdlScen, prjScens, gbifOccFile, 
                                   expdate, 
                                   taxonSourceName=taxname,
                                   providerListFile=gbifProvFile,
                                   mdlMask=None, prjMask=None, 
                                   minPointCount=minPoints,  
                                   intersectGrid=gridname, log=self.log)
            
         elif datasource == 'IDIGBIO':
            self.boomer = iDigBioChainer(self.archiveName, self.userId, epsg, 
                                      algorithms, mdlScen, prjScens, 
                                      idigTaxonidsFile, expdate, 
                                      taxonSourceName=taxname,
                                      mdlMask=None, prjMask=None, 
                                      minPointCount=minPoints, 
                                      intersectGrid=gridname, log=self.log)
   
         else:
            self.boomer = UserChainer(self.archiveName, self.userId, epsg, 
                                   algorithms, mdlScen, prjScens, 
                                   userOccCSV, userOccMeta, expdate, 
                                   userOccDelimiter=userOccDelimiter,
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

   # .............................
   @staticmethod
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
   if not Archivist.isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)

   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-n', '--archive_name', default=None,
            help=('Name for the existing archive, gridset, and grid created for ' +
                  'these data.  This name was created in initBoom.'))
   parser.add_argument('-u', '--user', default=None,
            help=('Owner of this archive this archive. The default is the '
                  'configured ARCHIVE_USER.'))
   parser.add_argument('cmd', choices=['start', 'stop', 'restart'],
              help="The action that should be performed by the Boomer daemon")

   """
   $PYTHON LmDbServer/boom/boom.py --help
   
   $PYTHON LmDbServer/boom/boom.py  --archive_name "Heuchera archive" --user ryan start
   $PYTHON LmDbServer/boom/boom.py --archive_name "Aimee test archive"  --user aimee start
   """
   args = parser.parse_args()
   archiveName = args.archive_name
   if archiveName is not None:
      archiveName = archiveName.replace(' ', '_')
   userId = args.user
   cmd = args.cmd.lower()
   
   if os.path.exists(BOOM_PID_FILE):
      pid = open(BOOM_PID_FILE).read().strip()
   else:
      pid = os.getpid()
           
   secs = time.time()
   tuple = time.localtime(secs)
   timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", tuple))
   logger = ScriptLogger('archivist.{}'.format(timestamp))
   boomer = Archivist(BOOM_PID_FILE, userId, archiveName, log=logger)
     
   if cmd == 'start':
      boomer.start()
   elif cmd == 'stop':
      boomer.stop()
   elif cmd == 'restart':
      boomer.restart()
   elif cmd == 'status':
      boomer.status()
   else:
      print("Unknown command: {}".format(cmd))
      sys.exit(2)
#    else:
#       print("usage: {} start|stop|update".format(sys.argv[0]))
#       sys.exit(2)
"""
from LmDbServer.boom.boom import Archivist
from LmServer.common.log import ScriptLogger
from LmBackend.common.occparse import OccDataParser

name = "Heuchera archive"
name = name.replace(' ', '_')
usr = 'ryan'
log = ScriptLogger('testingboom')

(userId, archiveName, datasource, algorithms, minPoints, 
 mdlScen, prjScens, epsg, gridname, userOccCSV, userOccDelimiter, userOccMeta, 
 bisonTsnFile, idigTaxonidsFile, 
 gbifTaxFile, gbifOccFile, gbifProvFile, 
 speciesExpYear, speciesExpMonth, 
 speciesExpDay) = Archivist.getArchiveSpecificConfig(userId=usr, archiveName=name)
 
occParser = OccDataParser(log, userOccCSV, userOccMeta, delimiter=',')

"""
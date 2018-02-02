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
import argparse
import logging
import sys
import time
import traceback

from LmBackend.common.daemon import Daemon
from LmDbServer.common.lmconstants import BOOM_PID_FILE
from LmDbServer.boom.boomer import Boomer
from LmServer.base.utilities import isCorrectUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmconstants import LMFileType, PUBLIC_ARCHIVE_NAME
from LmServer.common.log import ScriptLogger

SPUD_LIMIT = 100

# .............................................................................
class DaBoom(Daemon):
   """
   Class to run the Boomer as a Daemon process
   """
   # .............................
   def __init__(self, pidfile, configFname, assemblePams=True, priority=None):
      # Logfile
      secs = time.time()
      timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
      logname = '{}.{}'.format(self.__class__.__name__.lower(), timestamp)
      log = ScriptLogger(logname, level=logging.INFO)

      Daemon.__init__(self, pidfile, log=log)
      self.boomer = Boomer(configFname, assemblePams=assemblePams, log=log)

   # .............................
   def initialize(self):
      self.boomer.initializeMe()
      
   # .............................
   def run(self):
      print('Running daBoom with configFname = {}'.format(self.boomer.configFname))
      try:
         while self.boomer.keepWalken:
            self.boomer.processSpud()
      except Exception, e:
         self.log.debug('Exception {} on potato'.format(str(e)))         
         tb = traceback.format_exc()
         self.log.error("An error occurred")
         self.log.error(str(e))
         self.log.error(tb)
      finally:
         self.log.debug('Daboom finally stopping')
         self.onShutdown()
                
   # .............................
   def onUpdate(self):
      self.log.info('Update signal caught!')
       
   # .............................
   def onShutdown(self):
      self.log.info('Shutdown!')
      # Stop walken the archive and saveNextStart
      self.boomer.close()
      Daemon.onShutdown(self)
      
   # ...............................................
   @property
   def logFilename(self):
      try:
         fname = self.log.baseFilename
      except:
         fname = None
      return fname
   


# .............................................................................
if __name__ == "__main__":
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)
   earl = EarlJr()
   defaultConfigFile = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                           objCode=PUBLIC_ARCHIVE_NAME, 
                                           usr=PUBLIC_USER)   
#    pth = earl.createDataPath(PUBLIC_USER, LMFileType.BOOM_CONFIG)
#    defaultConfigFile = os.path.join(pth, '{}{}'.format(PUBLIC_ARCHIVE_NAME, 
#                                                        LMFormat.CONFIG.ext))
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-', '--config_file', default=defaultConfigFile,
            help=('Configuration file for the archive, gridset, and grid ' +
                  'to be created from these data.'))
   parser.add_argument('cmd', choices=['start', 'stop', 'restart'],
              help="The action that should be performed by the Boom daemon")

   args = parser.parse_args()
   configFname = args.config_file
   cmd = args.cmd.lower()
   
   boomer = DaBoom(BOOM_PID_FILE, configFname)
   
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
      
"""
import logging
import mx.DateTime as dt
import os, sys, time

from LmBackend.common.daemon import Daemon
from LmCommon.common.apiquery import BisonAPI, GbifAPI
from LmCommon.common.lmconstants import LMFormat
from LmDbServer.common.lmconstants import BOOM_PID_FILE, TAXONOMIC_SOURCE
from LmDbServer.boom.boomer import Boomer
from LmServer.base.utilities import isCorrectUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmconstants import LMFileType, PUBLIC_ARCHIVE_NAME
from LmServer.common.log import ScriptLogger
from LmServer.tools.cwalken import ChristopherWalken
from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (LM_NAMESPACE, LMFormat, 
                                         ProcessType, JobStatus)
from LmServer.base.layer2 import Vector, _LayerParameters
from LmServer.base.serviceobject2 import ProcessObject
from LmServer.common.lmconstants import (DEFAULT_WMS_FORMAT, 
                  OccurrenceFieldNames, ID_PLACEHOLDER, LMFileType, 
                  LMServiceType, ProcessTool)
from LmServer.common.localconstants import POINT_COUNT_MAX
from LmBackend.common.cmd import MfRule
from LmServer.common.lmconstants import SnippetOperations
from LmServer.common.lmconstants import (LMFileType, SPECIES_DATA_PATH,
                                         Priority)
from LmServer.legion.processchain import MFChain
from LmServer.tools.occwoc import *
from LmCommon.common.occparse import OccDataParser
from LmCommon.common.lmconstants import (ENCODING, OFTInteger, OFTReal, 
                                         OFTString, LMFormat)

SPUD_LIMIT = 100

configFname = '/share/lm/data/archive/tester/gbif500ktest.ini'
configFname = '/share/lm/data/archive/biotaphytest/dirty_plants.ini'
configFname = '/share/lm/data/archive/biona/biotaphy_global_plants.ini'

log = ScriptLogger('debug_dabomb', level=logging.INFO)

boomer = Boomer(configFname, assemblePams=True, log=log)
boomer._scribe.openConnections()
boomer.christopher = ChristopherWalken(boomer.configFname,
                                              scribe=boomer._scribe)
chris = boomer.christopher

spud, potatoInputs = chris.startWalken()
boomer.keepWalken = not chris.complete
# TODO: Master process for occurrence only? SDM only? 
if boomer.assemblePams and spud:
   boomer._addRuleToMasterPotatoHead(spud, prefix='spud')
   spudArf = spud.getArfFilename(
                    arfDir=boomer.masterPotato.getRelativeDirectory(), 
                    prefix='spud')
   boomer.spudArfFnames.append(spudArf)
   squid = spud.mfMetadata[MFChain.META_SQUID]
   if potatoInputs:
      for scencode, (pc, triagePotatoFile) in boomer.potatoes.iteritems():
         pavFname = potatoInputs[scencode]
         triagePotatoFile.write('{}: {}\n'.format(squid, pavFname))
   if len(boomer.spudArfFnames) >= SPUD_LIMIT:
      boomer.rotatePotatoes()


chris.moreDataToProcess = False
userId = chris._getBoomOrDefault('ARCHIVE_USER')
archiveName = chris._getBoomOrDefault('ARCHIVE_NAME')
archivePriority = chris._getBoomOrDefault('ARCHIVE_PRIORITY')
earl = EarlJr()
boompath = earl.createDataPath(userId, LMFileType.BOOM_CONFIG)
epsg = chris._getBoomOrDefault('SCENARIO_PACKAGE_EPSG')
useGBIFTaxonIds = False
datasource = chris._getBoomOrDefault('DATASOURCE')
taxonSourceName = TAXONOMIC_SOURCE[datasource]['name']
expDate = dt.DateTime(chris._getBoomOrDefault('SPECIES_EXP_YEAR'), 
                      chris._getBoomOrDefault('SPECIES_EXP_MONTH'), 
                      chris._getBoomOrDefault('SPECIES_EXP_DAY')).mjd

#if datasource == SpeciesDatasource.IDIGBIO:
useGBIFTaxonIds = True
occDelimiter = chris._getBoomOrDefault('IDIG_OCCURRENCE_DATA_DELIMITER') 
occname = chris._getBoomOrDefault('IDIG_OCCURRENCE_DATA')
occInstalled = os.path.join(SPECIES_DATA_PATH, occname)
occUser = os.path.join(boompath, occname)
occCSV = None
if os.path.exists(occInstalled + LMFormat.CSV.ext):
   occBasename = occInstalled + LMFormat.CSV.ext
elif os.path.exists(occUser + LMFormat.CSV.ext):
   occBasename = occUser
   
   
# Biotaphy data, individual files, metadata in filenames
occData = chris._getBoomOrDefault('USER_OCCURRENCE_DATA')
occDelimiter = chris._getBoomOrDefault('USER_OCCURRENCE_DATA_DELIMITER') 
occDir= os.path.join(boompath, occData)
occMeta = os.path.join(boompath, occData + LMFormat.METADATA.ext)
dirContentsFname = os.path.join(boompath, occData + LMFormat.TXT.ext)
weaponOfChoice = TinyBubblesWoC(chris._scribe, userId, archiveName, 
                          epsg, expDate, occDir, occMeta, occDelimiter,
                          dirContentsFname, 
                          taxonSourceName=taxonSourceName, 
                          logger=chris.log)

occCSV = occBasename + LMFormat.CSV.ext   
occMeta = occBasename + LMFormat.METADATA.ext
weaponOfChoice = UserWoC(chris._scribe, userId, archiveName, 
                                  epsg, expDate, occCSV, occMeta, 
                                  occDelimiter, logger=chris.log, 
                                  processType=ProcessType.USER_TAXA_OCCURRENCE,
                                  useGBIFTaxonomy=useGBIFTaxonIds,
                                  taxonSourceName=taxonSourceName)
woc = weaponOfChoice
bubbleFname = woc._getNextFilename()
occ = woc.getOne()

woc.occParser = OccDataParser(woc.log, woc._userOccCSV, 
                                        woc._userOccMeta, 
                                        delimiter=woc._delimiter, 
                                        pullChunks=True)
op = woc.occParser 

fieldmeta, metadataFname, doMatchHeader = op.readMetadata(op.metadataFname)
header = None
fieldNames = []
fieldTypes = []
filters = {}
idIdx = xIdx = yIdx = ptIdx = groupByIdx = nameIdx = None
idxdict = fieldmeta
for idx, vals in idxdict.iteritems():
   shortname = idx
   ogrtype = role = acceptedVals = None
   if vals is not None:
      shortname = vals['name']
      ogrtype = vals['type']
      try:
         acceptedVals = idxdict[idx]['acceptedVals']
      except:
         pass
      else:
         if ogrtype == OFTString:
            acceptedVals = [val.lower() for val in acceptedVals]
      try:
         role = idxdict[idx]['role'].lower()
      except:
         pass
      else:
         if role == OccDataParser.FIELD_ROLE_IDENTIFIER:
            idIdx = idx
         elif role == OccDataParser.FIELD_ROLE_LONGITUDE:
            xIdx = idx
         elif role == OccDataParser.FIELD_ROLE_LATITUDE:
            yIdx = idx
         elif role == OccDataParser.FIELD_ROLE_GEOPOINT:
            ptIdx = idx
         elif role == OccDataParser.FIELD_ROLE_TAXANAME:
            nameIdx = idx
         if role == OccDataParser.FIELD_ROLE_GROUPBY:
            groupByIdx = idx

# weaponOfChoice = chris._getOccWeaponOfChoice(userId, archiveName, epsg, 
#                                                   boompath)

(chris.userId, 
 chris.archiveName, 
 chris.priority, 
 chris.boompath, 
 self.weaponOfChoice, 
 self.epsg, 
 self.minPoints, 
 self.algs, 
 self.mdlScen, 
 self.prjScens, 
 self.sdmMaskInputLayer, 
 self.boomGridset, 
 self.intersectParams, 
 self.assemblePams) = self._getConfiguredObjects()
# One Global PAM for each scenario
if self.assemblePams:
   for prjscen in self.prjScens:
      self.globalPAMs[prjscen.code] = self.boomGridset.getPAMForCodes(
                     prjscen.gcmCode, prjscen.altpredCode, prjscen.dateCode)
      
                                              
boomer.christopher.initializeMe()                                   
boomer.initializeMe()
boomer.keepWalken
chris = boomer.christopher
woc = boomer.christopher.weaponOfChoice
op = boomer.christopher.weaponOfChoice.occParser

occ = woc.getOne()

dataChunk, taxonKey, taxonName = op.pullCurrentChunk()
(rankStr, scinameStr, canonicalStr, acceptedKey, acceptedStr, 
             nubKey, taxStatus, kingdomStr, phylumStr, classStr, orderStr, 
             familyStr, genusStr, speciesStr, genusKey, speciesKey, 
             loglines) = GbifAPI.getTaxonomy(taxonKey)
print 'taxStatus={}, match={}, acceptedKey={} taxonKey={} speciesKey={} rankStr={}'.format(
  taxStatus, str(acceptedKey==taxonKey), acceptedKey, taxonKey, speciesKey, rankStr)
print
# 
# sciName = woc._getInsertSciNameForGBIFSpeciesKey(taxonKey, None)
# sciName = woc._scribe.findOrInsertTaxon(taxonSourceId=woc._taxonSourceId, 
#                                                taxonKey=taxonKey)
# 
# if op.useGBIFTaxonomy:
#    sciName = woc._getInsertSciNameForGBIFSpeciesKey(taxonKey, None)

boomer.processSpud()

$PYTHON LmDbServer/boom/daboom.py --config_file=/share/lm/data/archive/biotaphy/biotaphy_boom.ini start

LmBackend.common.lmobj.LMError\n\n\nFailed to initialize Chris with config /share/lm/data/archive/biotaphy/biotaphy_boom.ini
"""
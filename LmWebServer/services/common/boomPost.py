"""
@summary: This module processes BOOM style HTTP POST requests
@author: CJ Grady
@version: 1.0
@status: alpha
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
@todo: Process grids
"""
from ConfigParser import ConfigParser
import os
import random

from LmCommon.common.lmconstants import SERVER_BOOM_HEADING, LMFormat
from LmDbServer.boom.boominput import ArchiveFiller
from LmServer.common.lmconstants import TEMP_PATH

# .............................................................................
class BoomPoster(object):
   """
   @summary: This class processes BOOM-style POST requests and produces a
                BOOM input config file to be used for creating a BOOM.
   """
   # ................................
   def __init__(self, userId, userEmail, archiveName, reqJson):
      """
      @todo: Make this more generic
      """
      self.config = ConfigParser()
      self.config.add_section(SERVER_BOOM_HEADING)
      self.config.set(SERVER_BOOM_HEADING, 'ARCHIVE_USER', userId)
      self.config.set(SERVER_BOOM_HEADING, 'ARCHIVE_USER_EMAIL', userEmail)
      self.config.set(SERVER_BOOM_HEADING, 'ARCHIVE_NAME', archiveName)
      
      if reqJson.has_key('algorithms'):
         self._processAlgorithms(reqJson['algorithms'])
         
      if reqJson.has_key('occurrenceSets'):
         self._processOccurrenceSets(reqJson['occurrenceSets'])
         
      if reqJson.has_key('modelScenario'):
         self._processModelScenario(reqJson['modelScenario'])
         
      if reqJson.has_key('projectionScenarios'):
         self._processProjectionScenarios(reqJson['projectionScenarios'])

   # ................................
   def initBoom(self):
      """
      @summary: Write the config file
      """
      filename = self._getTempFilename(LMFormat.CONFIG.ext)
         
      with open(filename, 'w') as configOutF:
         self.config.write(configOutF)
      
      filler = ArchiveFiller(configFname=filename)
      filler.initBoom()
      filler.close()
         
   # ................................
   def _getTempFilename(self, ext):
      """
      @summary: Return a temp file name
      """
      return os.path.join(TEMP_PATH, 'file_{}{}'.format(
         random.randint(0, 100000), ext))
   
   # ................................
   def _processAlgorithms(self, algoJson):
      """
      @summary: Process algorithms in request
      @todo: Process parameters
      """
      algos = []
      for algoSection in algoJson:
         algos.append(algoSection['code'])
      self.config.set(SERVER_BOOM_HEADING, 'ALGORITHMS', ', '.join(algos))
   
   # ................................
   def _processOccurrenceSets(self, occSetJson):
      """
      @summary: Process occurrence sets in request
      @todo: GBIF, iDigBio, Bison, etc
      """
      occIds = []
      
      for occSection in occSetJson:
         if occSection.has_key('occurrenceSetId'):
            occIds.append(occSection['occurrenceSetId'])
            
      if len(occIds) > 0:
         occFname = self._getTempFilename(LMFormat.CSV.ext)
         with open(occFname, 'w') as outF:
            for occId in occIds:
               outF.write('{}\n'.format(occId))
         self.config.set(SERVER_BOOM_HEADING, 'OCCURRENCE_ID_FILENAME', 
                         occFname)
   
   # ................................
   def _processModelScenario(self, scnJson):
      """
      """
      scnCode = scnJson['scenarioCode']
      self.config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MODEL_SCENARIO', 
                      scnCode)
   
   # ................................
   def _processProjectionScenarios(self, scnsJson):
      """
      @todo: Process layers package
      """
      prjScnCodes = []
      for scn in scnsJson:
         prjScnCodes.append(scn['scenarioCode'])
      
      self.config.set(SERVER_BOOM_HEADING, 
                      'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', ','.join(prjScnCodes))
   

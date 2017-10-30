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
@todo: Constants instead of strings
"""
from ConfigParser import ConfigParser
import json
import os
import random

from LmCommon.common.lmconstants import SERVER_BOOM_HEADING, LMFormat
#from LmDbServer.boom.boominput import ArchiveFiller
from LmDbServer.boom.initboom import initBoom
from LmServer.common.lmconstants import TEMP_PATH, Priority

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
      self.config.set(SERVER_BOOM_HEADING, 'ARCHIVE_PRIORITY', Priority.REQUESTED)
      
      
      # Check for old parameters for backwards compatibility until Ben updates
      if reqJson.has_key('algorithms') and \
           reqJson.has_key('occurrenceSets') and \
           reqJson.has_key('modelScenario') and \
           reqJson.has_key('projectionScenarios'):
         # Old app did not allow for global PAMs
         self.config.set(SERVER_BOOM_HEADING, 'ASSEMBLE_PAMS', False)
         
         if reqJson.has_key('algorithms'):
            self._old_processAlgorithms(reqJson['algorithms'])
            
         if reqJson.has_key('occurrenceSets'):
            self._old_processOccurrenceSets(reqJson['occurrenceSets'])
            
         if reqJson.has_key('modelScenario'):
            self._old_processModelScenario(reqJson['modelScenario'])
            
         if reqJson.has_key('projectionScenarios'):
            self._old_processProjectionScenarios(reqJson['projectionScenarios'])
      else:
         
         # NOTE: For this next round of boom services, we are still only 
         #          one group of occurrences, one shapegrid, one scenario 
         #          package, etc.  The schema is farther along in making things
         #          more generic, but the service will be more restrictive 
         #          until we have legitimate use-cases for the flexibility.
         
         # Look for occurrence set specification at top level
         if reqJson.has_key('occurrence'):
            self._processOccurrenceSets(reqJson['occurrence'])
         
         # Look for scenario package information at top level
         if reqJson.has_key('scenarioPackage'):
            self._processScenarioPackage(reqJson['scenarioPackage'])
            
         # Look for shapegrid information
         if reqJson.has_key('shapegrid'):
            self._processShapegrid(reqJson['shapegrid'])
            
         
         
         
         # Global PAMs (globalPam)
         # MCPAs (mcpa)
         # Occurrence sets (occurrence)
         # PAM stats (pamStats)
         # Scenario packages (scenarioPackage)
         # SDMs (sdm)
         # Shapegrid (shapegrid)
         # Tree (tree)
         
         
         
         pass
       
      
      # Look for new stuff
      
      

   # ................................
   def initBoom(self):
      """
      @summary: Write the config file
      """
      filename = self._getTempFilename(LMFormat.CONFIG.ext)
         
      with open(filename, 'w') as configOutF:
         self.config.write(configOutF)
      
      gridset = initBoom(filename, isInitial=False)
      
      return gridset
         
   # ................................
   def _getTempFilename(self, ext):
      """
      @summary: Return a temp file name
      """
      return os.path.join(TEMP_PATH, 'file_{}{}'.format(
         random.randint(0, 100000), ext))
   
   # ................................
   def _old_processAlgorithms(self, algoJson):
      """
      @summary: Process algorithms in request
      """
      i = 0
      for algo in algoJson:
         algoSection = 'ALGORITHM - {}'.format(i)
         self.config.add_section(algoSection)
         self.config.set(algoSection, 'CODE', algo['code'])
         for param in algo['parameters'].keys():
            self.config.set(algoSection, param.lower(), algo['parameters'][param])
         i += 1
   
   # ................................
   def _old_processOccurrenceSets(self, occSetJson):
      """
      @summary: Process occurrence sets in request
      @todo: GBIF, iDigBio, Bison, etc
      """
      occIds = []
      
      for occSection in occSetJson:
         if occSection.has_key('occurrenceSetId'):
            occIds.append(occSection['occurrenceSetId'])
         elif occSection.has_key('occurrenceData'):
            # Process user csv upload
            # Write the CSV data
            occFname = self._getTempFilename(LMFormat.CSV.ext)
            with open(occFname, 'w') as outF:
               outF.write(occSection['occurrenceData'])
            # Set the config
            self.config.set(SERVER_BOOM_HEADING, 'USER_OCCURRENCE_DATA')
            # Look for metadata
            metaFname = '{}{}'.format(os.path.splitext(occFname), 
                                      LMFormat.METADATA.ext)
            if occSection.has_key('occurrenceMeta'):
               with open(metaFname, 'w') as metaOut:
                  json.dump(occSection['occurrenceMeta'], metaOut, indent=3)
               
               
      if len(occIds) > 0:
         occFname = self._getTempFilename(LMFormat.CSV.ext)
         with open(occFname, 'w') as outF:
            for occId in occIds:
               outF.write('{}\n'.format(occId))
         self.config.set(SERVER_BOOM_HEADING, 'OCCURRENCE_ID_FILENAME', 
                         occFname)
         self.config.set(SERVER_BOOM_HEADING, 'DATASOURCE', 'EXISTING')
   
   # ................................
   def _old_processModelScenario(self, scnJson):
      """
      """
      scnCode = scnJson['scenarioCode']
      self.config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MODEL_SCENARIO', 
                      scnCode)
   
   # ................................
   def _old_processProjectionScenarios(self, scnsJson):
      """
      @todo: Process layers package
      """
      prjScnCodes = []
      for scn in scnsJson:
         prjScnCodes.append(scn['scenarioCode'])
      
      self.config.set(SERVER_BOOM_HEADING, 
                      'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', ','.join(prjScnCodes))
   

"""
@summary: This module processes BOOM style HTTP POST requests
@author: CJ Grady
@version: 1.0
@status: alpha
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
@todo: Process grids
@todo: Constants instead of strings
@todo: Testing
"""
import cherrypy
from ConfigParser import ConfigParser
import json
from mx.DateTime import gmt
import os
import random

from LmCommon.common.lmconstants import (LMFormat, SERVER_BOOM_HEADING, 
                                         SERVER_SDM_MASK_HEADING_PREFIX,
   HTTPStatus)
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
   def __init__(self, userId, userEmail, reqJson):
      """
      @todo: Make this more generic
      """
      self.config = ConfigParser()
      self.config.add_section(SERVER_BOOM_HEADING)
      self.config.set(SERVER_BOOM_HEADING, 'ARCHIVE_USER', userId)
      self.config.set(SERVER_BOOM_HEADING, 'ARCHIVE_USER_EMAIL', userEmail)
      self.config.set(SERVER_BOOM_HEADING, 'ARCHIVE_PRIORITY', Priority.REQUESTED)
      
      # Look for an archive name
      if reqJson.has_key('archive_name'):
         archiveName = reqJson['archive_name']
      else:
         archiveName = '{}_{}'.format(userId, gmt().mjd)
      self.config.set(SERVER_BOOM_HEADING, 'ARCHIVE_NAME', archiveName)
      
      # Check for old parameters for backwards compatibility until Ben updates
      if reqJson.has_key('algorithms') and \
           reqJson.has_key('occurrenceSets') and \
           reqJson.has_key('modelScenario') and \
           reqJson.has_key('projectionScenarios'):
         # Old app did not allow for global PAMs
         self.config.set(SERVER_BOOM_HEADING, 'ASSEMBLE_PAMS', False)
         
         if reqJson.has_key('algorithms'):
            self._old_process_algorithms(reqJson['algorithms'])
            
         if reqJson.has_key('occurrenceSets'):
            self._old_process_occurrence_sets(reqJson['occurrenceSets'])
            
         if reqJson.has_key('modelScenario'):
            self._old_process_model_scenario(reqJson['modelScenario'])
            
         if reqJson.has_key('projectionScenarios'):
            self._old_process_projection_scenarios(reqJson['projectionScenarios'])
      else:
         
         # NOTE: For this version, we will follow what is available from the 
         #          .ini file and not let it get too fancy / complicated.  We
         #          can add functionality later to connect different parts as 
         #          needed but for now they will either be present or not
         
         # Look for occurrence set specification at top level
         occSec = self._get_json_section(reqJson, 'occurrence')
         if occSec:
            self._process_occurrence_sets(occSec)
         
         # Look for scenario package information at top level
         scnSec = self._get_json_section(reqJson, 'scenario_package')
         if scnSec:
            self._process_scenario_package(scnSec)
            
         # Look for global pam information
         globalPamSec = self._get_json_section(reqJson, 'global_pam')
         if globalPamSec:
            self._process_global_pam(globalPamSec)

         # Look for tree information
         treeSec = self._get_json_section(reqJson, 'tree')
         if treeSec:
            self._process_tree(treeSec)
            
         # Look for SDM options (masks / scaling / etc)
         sdmSec = self._get_json_section(reqJson, 'sdm')
         if sdmSec:
            self._process_sdm(sdmSec)
         
         # PAM stats
         pamStatsSec = self._get_json_section(reqJson, 'pam_stats')
         if pamStatsSec:
            self._process_pam_stats(pamStatsSec)
            
         # MCPA
         mcpaSec = self._get_json_section(reqJson, 'mcpa')
         if mcpaSec:
            self._process_mcpa(mcpaSec)
         
         
         # TODO: Masks
         # TODO: Pre / post processing (scaling)
         # TODO: Randomizations

      
   
   # ................................
   def _get_json_section(self, jsonDoc, sectionKey):
      """
      @summary: This function attempts to retrieve a section from a JSON 
                   document in a case-insensitive way
      @param jsonDoc: The json document to search
      @param sectionKey: The section key to return
      @return: The json section in the document or None if not found
      """
      searchKey = sectionKey.replace(' ', '').replace('_', '').lower()
      for key in jsonDoc.keys():
         if key.lower().replace(' ', '').replace('_', '') == searchKey:
            return jsonDoc[key]
      return None

   # ................................
   def init_boom(self):
      """
      @summary: Write the config file
      """
      filename = self._get_temp_filename(LMFormat.CONFIG.ext)
         
      with open(filename, 'w') as configOutF:
         self.config.write(configOutF)
      
      gridset = initBoom(filename, isInitial=False)
      
      return gridset
         
   # ................................
   def _get_temp_filename(self, ext):
      """
      @summary: Return a temp file name
      """
      return os.path.join(TEMP_PATH, 'file_{}{}'.format(
         random.randint(0, 100000), ext))
   
   # ................................
   def _old_process_algorithms(self, algoJson):
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
   def _old_process_occurrence_sets(self, occSetJson):
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
            occFname = self._get_temp_filename(LMFormat.CSV.ext)
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
         occFname = self._get_temp_filename(LMFormat.CSV.ext)
         with open(occFname, 'w') as outF:
            for occId in occIds:
               outF.write('{}\n'.format(occId))
         self.config.set(SERVER_BOOM_HEADING, 'OCCURRENCE_ID_FILENAME', 
                         occFname)
         self.config.set(SERVER_BOOM_HEADING, 'DATASOURCE', 'EXISTING')
   
   # ................................
   def _old_process_model_scenario(self, scnJson):
      """
      """
      scnCode = scnJson['scenarioCode']
      self.config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_MODEL_SCENARIO', 
                      scnCode)
   
   # ................................
   def _old_process_projection_scenarios(self, scnsJson):
      """
      @todo: Process layers package
      """
      prjScnCodes = []
      for scn in scnsJson:
         prjScnCodes.append(scn['scenarioCode'])
      
      self.config.set(SERVER_BOOM_HEADING, 
                      'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', ','.join(prjScnCodes))
   
   # ................................
   def _process_global_pam(self, globalPamJson):
      """
      @summary: Process global pam information from request including shapegrid
                   and intersect parameters
      @param globalPamJson: JSON chunk of global pam information
      @note: This version is somewhat limited.  Must provide shapegrid 
                parameters and only one set of intersect parameters
      @todo: Shapegrid epsg?
      @todo: Shapegrid map units?
      @todo: Expand to other intersect methods
      """
      self.config.set(SERVER_BOOM_HEADING, 'ASSEMBLE_PAMS', True)
      
      # Process shapegrid
      sg = globalPamJson['shapegrid']
      sgBBox = [sg['minx'], sg['miny'], sg['maxx'], sg['maxy']]
      self.config.set(SERVER_BOOM_HEADING, 'GRID_BBOX', sgBBox)
      sgName = sg['name']
      self.config.set(SERVER_BOOM_HEADING, 'GRID_NAME', sgName)
      #sgEpsg = sg['epsg']
      sgNumSides = sg['cell_sides']
      self.config.set(SERVER_BOOM_HEADING, 'GRID_NUM_SIDES', sgNumSides)
      #sgMapUnits = sg['map_units']
      sgRes = sg['resolution']
      self.config.set(SERVER_BOOM_HEADING, 'GRID_CELLSIZE', sgRes)
      
      # Process intersect parameters
      intParams = globalPamJson['intersect_parameters']
      minPresence = intParams['min_presence']
      maxPresence = intParams['max_presence']
      intValue = intParams['value_name']
      minPercent = intParams['min_percent']
      
      self.config.set(SERVER_BOOM_HEADING, 'INTERSECT_FILTERSTRING', None)
      self.config.set(SERVER_BOOM_HEADING, 'INTERSECT_VALNAME', intValue)
      self.config.set(SERVER_BOOM_HEADING, 'INTERSECT_MINPERCENT', minPercent)
      self.config.set(SERVER_BOOM_HEADING, 'INTERSECT_MINPRESENCE', minPresence)
      self.config.set(SERVER_BOOM_HEADING, 'INTERSECT_MAXPRESENCE', maxPresence)
   
   # ................................
   def _process_mcpa(self, mcpaJson):
      """
      @summary: Process MCPA information from the request
      @param mcpaJson: JSON chunk of MCPA information
      """
      self.config.set(SERVER_BOOM_HEADING, 'BIOGEO_HYPOTHESES', 
                      mcpaJson['hypotheses_package_name'])

   # ................................
   def _process_occurrence_sets(self, occJson):
      """
      @summary: For this version, process occurrence sets specified by existing
                   identifiers or by processing an (previously) uploaded CSV
      @param occJson: JSON chunk of occurrence information
      @todo: Handle user csv points
      """
      if occJson.has_key('occurrence_ids'):
         occFname = self._get_temp_filename(LMFormat.CSV.ext)
         with open(occFname, 'w') as outF:
            for occId in occJson['occurrence_ids']:
               outF.write('{}\n'.format(occId))
         self.config.set(SERVER_BOOM_HEADING, 'OCCURRENCE_ID_FILENAME', 
                         occFname)
         self.config.set(SERVER_BOOM_HEADING, 'DATASOURCE', 'EXISTING')
      else:
         pointsFilename = occJson['points_filename']
         #TODO: Full file path?
         self.config.set(SERVER_BOOM_HEADING, 'DATASOURCE', 'USER')
         self.config.set(SERVER_BOOM_HEADING, 'USER_OCCURRENCE_DATA', 
                         pointsFilename)
         self.config.set(SERVER_BOOM_HEADING, 'USER_OCCURRENCE_DATA_DELIMITER', 
                         ',')
         if occJson.has_key('point_count_min'):
            self.config.set(SERVER_BOOM_HEADING, 'POINT_COUNT_MIN', 
                            occJson['point_count_min'])
   
   # ................................
   def _process_pam_stats(self, pamStatsJson):
      """
      @summary: Process PAM stats information
      @param pamStatsJson: JSON chunk indicating how to process PAM stats
      @note: This version just computes them or doesn't
      """
      try:
         shouldCompute = int(pamStatsJson['compute_pam_stats'])
      except:
         shouldCompute = 0

      self.config.set(SERVER_BOOM_HEADING, 'COMPUTE_PAM_STATS', shouldCompute)
   
   # ................................
   def _process_scenario_package(self, scnsJson):
      """
      @summary: Process scenario information from the request
      @param scnsJson: JSON chunk of scenario information
      @todo: Process scenario ids?
      """
      if scnsJson.has_key('scenario_package_filename'):
         self.config.set(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE', 
                         scnsJson['scenario_package_filename'])
      else:
         self.config.set(SERVER_BOOM_HEADING, 
                         'SCENARIO_PACKAGE_MODEL_SCENARIO', 
                         scnsJson['model_scenario']['scenario_code'])
         prjScnCodes = []
         for scn in scnsJson['projection_scenario']:
            prjScnCodes.append(scn['scenario_code'])
         self.config.set(SERVER_BOOM_HEADING, 
                         'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', 
                         ','.join(prjScnCodes))
   
   # ................................
   def _process_sdm(self, sdmJson):
      """
      @summary: Process SDM information in the request
      @param sdmJson: JSON chunk of SDM configuration options
      @note: This version only handles algorithms and masks
      @todo: Add scaling here
      """
      # Algorithms
      i = 0
      for algo in sdmJson['algorithm']:
         algoSection = 'ALGORITHM - {}'.format(i)
         self.config.add_section(algoSection)
         self.config.set(algoSection, 'CODE', algo['code'])
         for param in algo['parameters'].keys():
            self.config.set(algoSection, param.lower(), algo['parameters'][param])
         i += 1

      # Masks
      HULL_REGION_KEY = 'hull_region_intersect_mask'
      if sdmJson.has_key(HULL_REGION_KEY) and sdmJson[HULL_REGION_KEY] is not None:
         try:
            bufferVal = sdmJson[HULL_REGION_KEY]['buffer']
            region = sdmJson[HULL_REGION_KEY]['region']
            
            self.config.add_section(SERVER_SDM_MASK_HEADING_PREFIX)
            self.config.set(SERVER_SDM_MASK_HEADING_PREFIX, 'CODE', 
                            'hull_region_intersect')
            self.config.set(SERVER_SDM_MASK_HEADING_PREFIX, 'BUFFER', bufferVal)
            self.config.set(SERVER_SDM_MASK_HEADING_PREFIX, 'REGION', region)
            # Set the model and scenario mask options
            #TODO: Take this out later
            self.config.set(SERVER_BOOM_HEADING, 'MODEL_MASK_NAME', region)
            self.config.set(SERVER_BOOM_HEADING, 'PROJECTION_MASK_NAME', region)
         except KeyError, ke:
            raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST, 
                                     'Missing key: {}'.format(str(ke)))
   
   # ................................
   def _process_tree(self, treeJson):
      """
      @summary: Process the tree information from the request
      @param treeJson: JSON chunk with tree information
      @note: This version only allows a tree to be specified by file name
      """
      self.config.set(SERVER_BOOM_HEADING, 'TREE', treeJson['tree_file_name'])
   

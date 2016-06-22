"""
@summary Module that contains the Model class
@author Aimee Stewart
@status Status: alpha
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import JobStatus
from LmServer.base.atom import Atom
from LmServer.base.serviceobject import ServiceObject, ProcessObject
from LmServer.common.lmconstants import LMFileType, LMServiceType, LMServiceModule
from LmServer.common.localconstants import ARCHIVE_USER

# .............................................................................
class SDMModel(ServiceObject, ProcessObject):
   """
   The Model class contains all of the information 
            that openModeller needs to create a model.  
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, priority, occurrenceSet, scenario, algorithm, 
                name=None, description=None, 
                mask=None, email=None, createTime=None, 
                status=None, statusModTime=None, ruleset=None, 
                qc=None, userId=ARCHIVE_USER, modelId=None):
      """
      @summary Constructor for the Model class
      @param priority: The run priority for the Model
      @param occurrenceSet: The occurrenceSet object used to retrieve the 
                           species points for the model.  It contains the 
                           WFS or REST service url to retrieve the points
      @param scenario: The set of input environmental layers to be used
      @param algorithm: The algorithm object with all of its parameters 
      @param mask: (optional) The SDMLayer used as a boolean mask
                           to limit the geographic area used for modeling. 
      @param status: (optional) The run status of the Model
      @param statusModTime: (optional) The last time that the status was 
                              modified
      @param ruleset: (optional) The ruleset base file name (without path)
      @param qc: (optional) Quality Control
      @param userId: string id for user who owns this experiment 
      @param modelid: (optional) The modelId is used in construction if object 
                      is to be populated from the database.
      @todo do we need createTime?
      @todo what is qc exactly?
      """
      ServiceObject.__init__(self, userId, modelId, createTime, statusModTime, 
                             LMServiceType.MODELS, moduleType=LMServiceModule.SDM)
      if name is None:
         name = occurrenceSet.displayName
      self.name = name
      self.description = description 
      self.priority = priority
      self._email = email
      self.occurrenceSet = occurrenceSet
      self._scenario = scenario
      self._algorithm = algorithm

      self._mask = mask
#       if mask is None and len(scenario.layers) > 0:
#          self._mask = scenario.layers[0]
         
      self._setModelResultFilename(ruleset)
      self._qc = qc      
      self._status = status
      if status is not None:
         if statusModTime is None:
            self._statusmodtime = mx.DateTime.utc().mjd
         else:
            self._statusmodtime = statusModTime
      else:
         self._statusmodtime = None
      
# .............................................................................
# Public methods
# .............................................................................
   def update(self, priority=None, status=None, ruleset=None, 
              qc=None, modelId=None):
      """
      @summary Updates mutable objects on the Model. Object attributes to be 
               updated are populated, those which remain the same are None.
      @param priority: (optional) The new priority for the model
      @param status: (optional) The new job status for the model
      @param ruleset: (optional) The new ruleset for the model
      @param qc: (optional) The new quality control for the model
      @param modelId: (optional) The new model id number
      """
      if priority is not None:
         self.priority = priority
      if modelId is not None:
         self.setId(modelId)
      if status is not None:
         self._status = status
         self._statusmodtime = mx.DateTime.utc().mjd
         self.modTime = self._statusmodtime
      if ruleset is not None:
         self._ruleset = ruleset
      if qc is not None:
         self._qc = qc

# ...............................................
   def getAbsolutePath(self):
      """
      @summary Gets the absolute path to the species data
      @return Path to species points
      """
      return self.occurrenceSet.getAbsolutePath()

# ...............................................
   def clearLocalMapfile(self):
      """
      @summary: Delete the mapfile containing layers associated with this 
                model's occurrenceSet
      """
      self.occurrenceSet.clearLocalMapfile()
      
# ...............................................
   def deleteLocalMapfile(self):
      """
      @summary: Delete the mapfile containing this layer
      """
      self.occurrenceSet.deleteLocalMapfile()
      
# ...............................................
   @property
   def mapFilename(self):
      return self.occurrenceSet.mapFilename
   
# ...............................................
   @property
   def mapName(self):
      return self.occurrenceSet.mapName


# ...............................................
   def clearModelFiles(self):
      reqfname = self.getModelRequestFilename()
      success, msg = self._deleteFile(reqfname)
         
      self._getModelResultFilename()
      success, msg = self._deleteFile(self.ruleset)
      self._ruleset = None
         
      statsfname = self.getModelStatisticsFilename()
      success, msg = self._deleteFile(statsfname)
      
# ...............................................
   def getScenario(self):
      """
      @summary Returns the scenario object
      @return Scenario object
      """
      return self._scenario

# ...............................................
   def getMask(self):
      """
      @summary Returns the mask (SDMLayer) object
      @return Scenario object
      """
      return self._mask

# ...............................................
   def getAlgorithm(self):
      """
      @summary Returns the algorithm object
      @return Algorithm object
      """
      return self._algorithm

# ...............................................
   def getAlgorithmParameters(self):
      """
      @summary Gets the Algorithm parameters dictionary
      @return The dictionary of algorithm parameters from the Model's Algorithm
      """
      return self._algorithm.parameters
   
# ...............................................
   def rollback(self, currtime, newPriority=None, status=None):
      """
      @summary: Rollback processing
      @param currtime: Time of status/stage modfication
      @todo: remove currtime parameter
      """
      if status == None:
         if (self.occurrenceSet._status < JobStatus.COMPLETE or 
             self.occurrenceSet._status >= JobStatus.GENERAL_ERROR):
            status = JobStatus.GENERAL
         elif self.occurrenceSet._status == JobStatus.COMPLETE:
            status = JobStatus.INITIALIZE
      self.update(status=status, priority=newPriority, ruleset='', qc='')
      self.clearModelFiles()
      self.clearLocalMapfile()
   
# .............................................................................
# Private methods
# .............................................................................
   def _getStatus(self):
      """
      @summary Gets the run status of the Model
      @return The run status of the Model
      """
      return self._status

# ...............................................
   def _getStatusModTime(self):
      """
      @summary Gets the last time the status was modified
      @return Status modification time in modified julian date format
      """
      return self._statusmodtime
   
# ...............................................
   def getModelRequestFilename(self):
      fname = self._earlJr.createFilename(LMFileType.MODEL_REQUEST, 
                           modelId=self.getId(), pth=self.getAbsolutePath(), 
                           usr=self._userId, epsg=self.occurrenceSet.epsgcode)
      return fname
   
# ...............................................
   def getModelStatisticsFilename(self):
      fname = self._earlJr.createFilename(LMFileType.MODEL_STATS, 
                           modelId=self.getId(), pth=self.getAbsolutePath(), 
                           usr=self._userId, epsg=self.occurrenceSet.epsgcode)
      return fname

# ...............................................
   @property
   def makeflowFilename(self):
      dloc = self.createLocalDLocation(makeflow=True)
      return dloc

# ...............................................
   def createLocalDLocation(self, makeflow=False):
      """
      @summary: Create filename for this layer.
      @param makeflow: If true, this indicates a makeflow document of jobs 
                       related to this object
      """
      dloc = None
      if makeflow:
         dloc = self.occurrenceSet.createLocalDLocation(makeflow=True)
      else:
         if self._algorithm.code == 'ATT_MAXENT':
            ftype = LMFileType.MODEL_ATT_RESULT
         else:
            ftype = LMFileType.MODEL_RESULT
         dloc = self._earlJr.createFilename(ftype, modelId=self.getId(), 
                           pth=self.getAbsolutePath(), usr=self._userId, 
                           epsg=self.occurrenceSet.epsgcode)
      return dloc

# ...............................................
   def _getModelResultFilename(self):
      """
      @summary: Return filename for this layer.
      @note: This also SETS the _ruleset attribute if it is None
      """
      if self._ruleset is None:
         self._setModelResultFilename()
      return self._ruleset
   
   def _setModelResultFilename(self, fname=None):
      """
      @summary: Set filename for this layer.
      @note: This may override the default location
      """
      if not fname:
         fname = self.createLocalDLocation(makeflow=False)
      self._ruleset = fname
         
   def getDLocation(self):
      return self._getModelResultFilename()
   
# ...............................................
   def _getQualityControl(self):
      """
      @summary Gets the Model quality control
      @return The Model quality control variable
      """ 
      return self._qc
   
# ...............................................
   def _getOccurrenceSetName(self):
      """
      @summary Gets the species name that is used in the Model
      @return The species name of the Model
      """
      if isinstance(self.occurrenceSet, Atom):
         occname = self.occurrenceSet.title
      else:
         occname = self.occurrenceSet.displayName
      return occname
   
# ...............................................
   def _getOccurrenceSetQuery(self):
      """
      @summary Gets the species query that is used in the Model
      @return The species query of the Model
      """
      return self.occurrenceSet.query
   
# ...............................................
   def _getAlgorithmCode(self):
      """
      @summary Gets the algorithm code
      @return Algorithm code used in the database and openModeller
      """
      return self._algorithm.code
   
# ...............................................
   def _getScenarioCode(self):
      """
      @summary Gets the scenario code
      @return The scenario code used in the database
      """
      return self._scenario.code
   
# ...............................................
   def _getBBox(self):
      """
      @summary Gets the intersection of the bounding boxes in the layers of 
                the Model
      @return The largest bounding box that is common among all of the
               environmental layers in the Scenario of the Model
      """
      return self._scenario.bbox
   
   def _getEPSG(self):
      epsgcode = None
      if self.occurrenceSet is not None:
         epsgcode = self.occurrenceSet.epsgcode
      return epsgcode
   
# ...............................................
   def _getLayers(self):
      """
      @summary Gets the environmental layers of the scenario in the Model
      @return List of environmental layers of the Model's Scenario
      """
      return self._scenario.layers

# ...............................................
   def _getEmail(self):
      return self._email

# .............................................................................
# Properties
# .............................................................................
   ## The run status of the model
   status = property(_getStatus)
   ## The last time the status of the model was updated in modified julian 
   ## date format
   statusModTime = property(_getStatusModTime)
#   ## The file name of the ruleset file
#    ruleset = property(_getRulesetFilename, _setRulesetFilename)
   ruleset = property(_getModelResultFilename)
   ## Model quality control
   qualityControl = property(_getQualityControl)
   ## Name of the species used for the Model
   pointsName = property(_getOccurrenceSetName)
   ## Species query used for the Model
   pointsQuery = property(_getOccurrenceSetQuery)
   ## The algorithm code used in the Model's Algorithm
   algorithmCode = property(_getAlgorithmCode)
   ## The scenario code used in the Model's Scenario
   scenarioCode = property(_getScenarioCode)
   ## The intersection of all of the bounding boxes of the Model's Scenario
   bbox = property(_getBBox)
   ## The email used for notification when model and all associated projections 
   ## have been completed by the pipeline.  This is used for user jobs 
   ## and anonymous jobs submitted through the website
   email = property(_getEmail)
   ## The epsgcode of the OccurrenceSet
   epsgcode = property(_getEPSG)
   ## The layers of the Model's Scenario
   # TODO: remove this?
   layers = property(_getLayers)

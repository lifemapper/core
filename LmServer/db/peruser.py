# coding=utf-8
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
import mx.DateTime
import os
import socket
from types import StringType, UnicodeType, IntType
import xml.etree.ElementTree as ET 

from LmCommon.common.lmconstants import (ENCODING, JobStatus, RandomizeMethods, 
                                         ProcessType)
from LmServer.base.lmobj import LMError, LMObject
from LmServer.db.catalog_borg import Borg
from LmServer.db.catalog_model import MAL
from LmServer.db.catalog_rad import RAD
from LmServer.db.connect import HL_NAME
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import  DbUser, JobFamily, ReferenceType
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME, 
                                 POINT_COUNT_MIN, POINT_COUNT_MAX, ARCHIVE_USER)
from LmServer.sdm.sdmexperiment import SDMExperiment

# .............................................................................
class Peruser(LMObject):
   """
   Class to peruse the Lifemapper catalog
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger, dbUser=DbUser.Pipeline):
      """
      @summary Peruser constructor
      @param logger: logger for info and error reporting 
      @param dbUser: optional database user for connection
      """
      LMObject.__init__(self)
      self.log = logger
      self.hostname = socket.gethostname().lower()
      dbHost = DB_HOSTNAME
         
      if dbUser not in HL_NAME.keys():
         raise LMError('Unknown database user {}'.format(dbUser))
            
      self._mal = MAL(logger, dbHost, CONNECTION_PORT, dbUser, HL_NAME[dbUser])
      self._rad = RAD(logger, dbHost, CONNECTION_PORT, dbUser, HL_NAME[dbUser])
               
# ............................................................................
   @property
   def isOpen(self):
      return self._mal.isOpen and self._rad.isOpen

# .............................................................................
# Public functions
# .............................................................................
   def openConnections(self):
      try:
         self._mal.open()
      except Exception, e:
         self.log.error('Failed to open MAL (user={} dbname={} host={} port={}): {}' 
                        .format(self._mal.user, self._mal.db, self._mal.host, 
                           self._mal.port, e.args))
         return False
      try:
         self._rad.open()
      except Exception, e:
         self.log.error('Failed to open RAD (user={} dbname={} host={} port={}): {}' 
                        .format(self._mal.user, self._mal.db, self._mal.host, 
                           self._mal.port, e.args))
         return False
      return True

# ...............................................
   def closeConnections(self):
      self._mal.close()
      self._rad.close()
      
# ...............................................
# Algorithm
# ...............................................
   
# ...............................................
   def countAlgorithms(self):
      count = self._mal.countAlgorithms()
      return count
   
# ...............................................
   def listAlgorithms(self, firstRecNum, maxCount, atom=True):
      """
      @summary: Return all algorithms starting at the 
                firstRecNum limited to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxCount: Maximum number of records to return 
      @return: a list of algorithm atoms or full objects
      """
      objs = self._mal.listAlgorithms(firstRecNum, maxCount, atom)
      return objs

# ...............................................
# Job/Experiment (SDM)
# ...............................................
   def getJob(self, jobFamily, jobId):
      job = None
      if jobFamily == JobFamily.SDM:
         job = self._mal.getJob(jobId)
      elif jobFamily == JobFamily.RAD:
         job = self._rad.getJob(jobId)
      return job

# ...............................................
   def getJobOfType(self, jobFamily, obj):
      job = None
      if jobFamily == JobFamily.SDM:
         job = self._mal.getJobOfType(obj)
#       elif jobFamily == JobFamily.RAD:
#          job = self._rad.getJobOfType(obj)
      return job

# ...............................................
   def getExperimentsForOccurrenceSet(self, occsetid, 
                                      status=JobStatus.COMPLETE, 
                                      userid=None):
      """
      @summary: Return all experiments created using the given occurrenceset for
                the given User.
      @param occsetid: Id for an occurrenceSet for which to return models
      @param status: default filter for completed models only, None gets all
      @param userid: optional filter by userid.  Default None gets models for 
                     all users. 
      @todo: remove userid, since occset/model has only one user; occsets are 
             copied if re-used by another user
      @return: a list of SDMExperiment objects
      """
      experiments = []
      try:
         models = self._mal.getModelsForOcc(occsetid, userid, status)
      except LMError, e:
         raise e
      except Exception, e:
         raise LMError(currargs='Failed getting models', prevargs=e.args, 
                       lineno=self.getLineno())

      for mdl in models:
         try:
            projs = self._mal.getProjectionsForModel(mdl.getId(), status)
         except LMError, e:
            raise e
         except Exception, e:
            raise LMError(currargs='Failed getting experiments', 
                          prevargs=e.args, lineno=self.getLineno())
         
         exp = SDMExperiment(mdl, projs)
         experiments.append(exp)
      return experiments

# ...............................................
   def getExperimentsForUser(self, userid, status=JobStatus.COMPLETE):
      """
      @summary: Return all experiments created using the given occurrenceset for
                the given User.
      @param userid: User owner of experiments.
      @param status: default filter for completed models only, None gets all
      @return: a list of SDMExperiment objects
      """
      experiments = []
      try:
         models = self._mal.getModelsForUser(userid, status)
      except LMError, e:
         raise e
      except Exception, e:
         raise LMError(currargs='Failed getting models', prevargs=e.args, 
                       lineno=self.getLineno())

      for mdl in models:
         try:
            projs = self._mal.getProjectionsForModel(mdl.getId(), status)
         except LMError, e:
            raise e
         except Exception, e:
            raise LMError(currargs='Failed getting experiments', 
                          prevargs=e.args, lineno=self.getLineno())
         
         exp = SDMExperiment(mdl, projs)
         experiments.append(exp)
      return experiments

# ...............................................
   def getExperimentForModel(self, modelId):
      """
      @summary: Return model and all related projections for the given modelId.
      @param modelId: The unique modelId for the model and its child projections 
      @return: List of one Model and zero or more Projections 
      """
      model = self._mal.getModelById(modelId)
      projs = self._mal.getProjectionsForModel(modelId, status=None)
      return SDMExperiment(model, projs)

# ...............................................
   def getExperimentForProjection(self, projId):
      """
      @summary: Return model and all related projections for the given projectionId.
      @param projId: The unique projectionId for the projection, its parent
                model, and sibling projections
      @return: List of one Model and zero or more Projections 
      """
      model = self._mal.getModelByProjection(projId)
      projs = self._mal.getProjectionsForModel(model.getId(), status=None)
      return SDMExperiment(model, projs)

      
# ...............................................
# Layers (SDM)
# ...............................................
   def listLayers(self, firstRecNum, maxCount, 
                  userId=ARCHIVE_USER, 
                  typecode=None,
                  beforeTime=None, 
                  afterTime=None, 
                  epsg=None,
                  isCategorical=None,
                  scenarioId=None, atom=True):
      """
      @copydoc LmServer.db.catalog_model.MAL::listLayers()
      """
      objs = self._mal.listLayers(firstRecNum, maxCount, userId, typecode, 
                                   beforeTime, afterTime, epsg, isCategorical, 
                                   scenarioId, atom)
      return objs
      
# ...............................................
   def countLayers(self, userId=ARCHIVE_USER, typecode=None, beforeTime=None, 
                   afterTime=None, epsg=None, isCategorical=None, scenarioId=None):
      """
      @copydoc LmServer.db.catalog_model.MAL::countLayers()
      """
      count = self._mal.countLayers(userId, typecode, beforeTime, afterTime, 
                                    epsg, isCategorical, scenarioId)
      return count

# ...............................................
   def getLayerTypeCode(self, typecode=None, userid=None, typeid=None):
      if typeid is not None:
         etype = self._mal.getEnvironmentalTypeById(typeid)
      elif typecode is not None and userid is not None:
         etype = self._mal.getEnvironmentalType(typecode, userid)
      else:
         raise LMError(currargs='Must provide TypeCode and UserId or an EnvironmentalTypeId')
      return etype
      
# ...............................................
   def listLayerTypeCodes(self, firstRecNum, maxCount, userId=ARCHIVE_USER, 
                          beforeTime=None, afterTime=None, atom=True):
      """
      @summary: Return all layer type codes for a user, starting at the 
                firstRecNum limited to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxCount: Maximum number of records to return 
      @param userid: userid of the desired EnvironmentalType codes
      @param beforeTime: gets only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time
             Defaults to no filter by time.
      @return: a list of Layer Type Code atoms or full objects
      """
      objs = self._mal.listLayerTypeCodes(firstRecNum, maxCount, userId, 
                                   beforeTime, afterTime, atom)
      return objs
      
# ...............................................
   def countLayerTypeCodes(self, userId=ARCHIVE_USER, 
                           beforeTime=None, afterTime=None):
      """
      @param userid: userid of the desired EnvironmentalType 
      @param beforeTime: gets only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time
             Defaults to no filter by time.
      """
      count = self._mal.countLayerTypeCodes(userId, beforeTime, afterTime)
      return count

# ...............................................
   def getLayer(self, id):
      """
      @summary: Return an EnvironmentalLayer for the given id
      @param id: id of the desired EnvironmentalLayer
      @return: a full EnvironmentalLayer object
      @note: an Atom is not enough for JOD or pipeline unless we include the URL
      """
      layer = None
      if id is not None:
         layer = self._mal.getLayer(id)
      return layer
   
# ...............................................
   def getEnvLayersByNameUserEpsg(self, name, user, epsg):
      """
      @summary: Return an EnvironmentalLayer for the given id
      @param id: id of the desired EnvironmentalLayer
      @return: a full EnvironmentalLayer object
      """
      lyrs = self._mal.getEnvLayersByNameUserEpsg(name, user, epsg)
      return lyrs

# .............................................................................
   def getEnvLayersByNameAndUser(self, name, user):
      """
      @summary: Return EnvironmentalLayer with the given name and user 
      @param name: layer name of the desired EnvironmentalLayer(s)
      @param user: userid of the desired EnvironmentalLayers
      @return: an EnvironmentalLayer object
      @note: For all users other than Archive and changeThinking, 
             user/layername/epsgcode combo is unique for every layer
      """
      lyrs = self._mal.getEnvLayersByNameUser(name, user)       
      return lyrs
   
# ...............................................
   def getLayersForOcc(self, occsetid):
      lyrs = self._mal.getMapLayersForOcc(occsetid)
      return lyrs   
      
# ...............................................
# Mapservice
# ...............................................
   def getMapServiceFromMapFilename(self, mapfilename):
      """
      @summary Assembles LayerSet specified by the mapfilename from db values
      @param mapfilename: Name of the mapserver mapfile
      @return The map service specified by the mapfilename
      @todo: delete this?
      """
      earljr = EarlJr()
      (mapname, ancillary, usr, epsg, occsetId, radexpId, bucketId, scencode) \
                   = earljr.parseMapFilename(mapfilename)
           
      if occsetId is not None:
         try:
            mapSvc = self._mal.getOccMapservice(occsetId)
         except LMError, e:
            raise 
         except Exception, e:
            raise LMError(currargs='Invalid map service for occset {}' 
                         .format(occsetId), prevargs=e.args, 
                         lineno=self.getLineno())
         
      # Scenario inherits from LayerSet
      elif scencode is not None:
         mapSvc = self.getScenario(scencode)
         
      # Ancillary mapfiles are not tied to database entries, must exist!
      elif ancillary:
         self.log.debug('Ancillary mapfile {} should exist!'.format(mapfilename))
         return None
         
      elif usr is not None:
         mapSvc = self._mal.getUserMapservice(mapname, usr, epsg)

      else:
         raise LMError(currargs='Invalid map prefix in mapname {} '.format(mapname), 
                       lineno=self.getLineno())
      
      if mapSvc is not None:
         mapSvc.setLocalMapFilename(mapfilename)
      return mapSvc

# ...............................................
# Model
# ...............................................
   def getModel(self, modelId):
      """
      @summary Method to return all of the information necessary to run an 
               openModeller experiment.
      @param modelId: The id of the model to retrieve
      @return The model specified by the id
      @todo Make sure this works
      """
      # The MAL initializes the Scenario with its code and layer guids only
      model = self._mal.getModelById(modelId)
      return model
    
# ...............................................
   def getModelsForOccurrenceSet(self, occsetid, 
                                 status=JobStatus.COMPLETE, 
                                 userid=None):
      """
      @summary: Return all models created using the given occurrenceset for
                the given User.
      @param occsetid: Id for and occurrenceSet for which to return models
      @param status: default filter for completed models only, None gets all
      @param userid: optional filter by userid.  Default None gets models for 
                     all users. 
      @return: a list of LMModel objects
      @note: JobStatus.COMPLETE for finished models
      """
      
      models = self._mal.getModelsForOcc(occsetid, userid, status)
      return models

# ...............................................
   def getUnprojectedArchiveModels(self, count, alg, modelScenario, projScenario):
      mdls = self._mal.findUnprojectedArchiveModels(count,
                                             alg.code, 
                                             modelScenario.getId(),
                                             projScenario.getId())
      return mdls

# ...............................................
   def getReadyModelsWithoutJobs(self, count, readyStat):
      mdls = self._mal.getModelsNeedingJobs(count, ARCHIVE_USER, readyStat)
      return mdls

# ...............................................
   def getReadyProjectionsWithoutJobs(self, count, readyStat, completeStat):
      mdls = self._mal.getProjectionsNeedingJobs(count, ARCHIVE_USER, 
                                                 readyStat, completeStat)
      return mdls

# ...............................................
   def getModelsByStatus(self, count, status, doFillScenario):
      mdls = self._mal.getModelsByStatus(count, status, doFillScenario=doFillScenario)
      return mdls

# ...............................................
   def getRandomModel(self, userid, status):
      model = self._mal.getRandomModel(userid, status)
      return model
   
# ...............................................
   def getUserEmail(self, userid):
      email = self._mal.getEmail(userid)
      return email
   
# ...............................................
   def listModels(self, firstRecNum, maxCount, userId=ARCHIVE_USER, 
                  displayName=None, beforeTime=None, afterTime=None, epsg=None,
                  status=None, inProcess=False, occSetId=None, algParamId=None, 
                  algCode=None, atom=True):
      """
      @summary: Return all Models starting at the firstRecNum limited to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxCount: Maximum number of records to return 
      @return: a list of Models atoms or full objects
      """
      if inProcess:
         completeStat = JobStatus.COMPLETE
      else:
         completeStat = None
      models = self._mal.listModels(firstRecNum, maxCount, userId, 
                                    displayName, beforeTime, afterTime, 
                                    epsg, status, completeStat, occSetId, 
                                    algCode, atom)
      return models
   
# ...............................................
   def countModels(self, userId=ARCHIVE_USER, displayName=None, beforeTime=None, 
                   afterTime=None, epsg=None, status=None, inProcess=False, 
                   occSetId=None, algCode=None):
      if inProcess:
         completeStat = JobStatus.COMPLETE
      else:
         completeStat = None
      count = self._mal.countModels(userId, displayName, beforeTime, afterTime, 
                                    epsg, status, completeStat, occSetId, 
                                    algCode)
      return count

# ...............................................
   def getModelRuleset(self, modelId):
      """
      @summary Method to return the xml model created by openModeller
      @param modelId: The id of the model to retrieve
      @return An ElementTree object containing the model
      """
      # The MAL initializes the Scenario with its code and layer guids only
      model = self._mal.getModelById(modelId, doFillScenario=False)
      if model.status == JobStatus.COMPLETE:
         fname = os.path.join(model.getAbsolutePath(), model.ruleset)
         try:
            modelTree = ET.parse(fname)
            root = modelTree.getroot()
            return root
         except:
            return None
      else:
         return None
          
# ...............................................
# OccurrenceSets
# ...............................................
   def findUnfinishedJoblessOccurrenceSets(self, count):
      occs = self._mal.findUnfinishedJoblessOccurrenceSets(count)
      return occs
     
# ...............................................
   def findUnmodeledOccurrenceSets(self, usr, total, primaryEnv, minNumPoints,
                                   algorithm, mdlScenario, errstat):
      """
      Finds unmodeled occurrenceSets which have been checked and possibly 
      updated since the latest GBIF update.
      """
      # No upper limit on number of points - we get a random sample for modeling
      occurrenceSetLst = self._mal.selectUnModeledOccurrenceSets(usr, total, 
                                   primaryEnv, minNumPoints, algorithm.code, 
                                   mdlScenario.getId(), errstat)
      return occurrenceSetLst

# ...............................................
   def findOccurrenceSetNamesWithProj(self, qryNamestring, maxCount):
      """
      """
      nameLst = self._mal.getOccurrenceSetNamesWithProj(qryNamestring, maxCount)
      return nameLst
   
# ...............................................
   def findOccurrenceSetNamesWithPoints(self, qryNamestring, minPoint, maxCount):
      """
      """
      nameLst = self._mal.getOccurrenceSetsWithPoints(qryNamestring, minPoint, 
                                                      maxCount)
      return nameLst
   
# ...............................................
   def getOccurrenceSetsForName(self, taxName, userid=None, defaultUserid=None):
      """
      @summary: Return all occurrenceSets with the given taxonomic name.
      @param taxName: A name associated with one or more occurrenceSets
      @return: a list of OccurrenceSet objects
      @note: returns newest first
      """
      occs = self._mal.getOccurrenceSetsForName(taxName, userid, defaultUserid)
      return occs

# ...............................................
   def getOccurrenceSetsForNameAndUser(self, taxName, usrid=ARCHIVE_USER):
      """
      @copydoc LmServer.db.catalog_model.MAL::getOccurrenceSetsForName()
      """
      occs = self._mal.getOccurrenceSetsForNameAndUser(taxName, usrid)
      return occs

# ...............................................
   def getOccurrenceSetsLikeNameAndUser(self, taxName, usrid=ARCHIVE_USER):
      """
      @copydoc LmServer.db.catalog_model.MAL::getOccurrenceSetsLikeName()
      """
      taxName = taxName.lower().capitalize()
      occs = self._mal.getOccurrenceSetsLikeNameAndUser(taxName, usrid)
      return occs

# ...............................................
   def getOccurrenceSetsForGenus(self, genusname):
      """
      @copydoc LmServer.db.catalog_model.MAL::getOccurrenceSetsForGenus()
      """
      occs = self._mal.getOccurrenceSetsForGenus(genusname)
      return occs

# ...............................................
   def getOccurrenceSetsForUser(self, userid, epsgcode=None):
      """
      @copydoc LmServer.db.catalog_model.MAL::getOccurrenceSetsForUser()
      """
      occs = self._mal.getOccurrenceSetsForUser(userid, epsgcode)
      return occs
   
# ...............................................
   def getOccurrenceSetsSubSpecies(self, count, userid):
      occs = []
      rows, idxs = self._mal.executeSelectManyFunction('lm_getFullOccurrenceSetsSubspecies',
                                                       count, userid)
      for r in rows:
         occs.append(self._mal._createOccurrenceSet(r, idxs))
      return occs
   
# ...............................................
   def getComplexNamedArchiveOccurrenceSets(self, count, userid, beforeTime):
      occs = self._mal.getComplexNamedArchiveOccurrenceSets(count, userid, 
                                       beforeTime)
      return occs
   
# ...............................................
   def getUsers(self):
      """
      @copydoc LmServer.db.catalog_model.MAL::getUsers()
      """
      users = self._mal.getUsers()
      return users
   
# ...............................................
   def listOccurrenceSets(self, firstRecNum, maxCount, minOccurrenceCount=0,
                          hasProjections=False, userId=ARCHIVE_USER, 
                          displayName=None, beforeTime=None, afterTime=None,
                          epsg=None, status=None, completestat=None, atom=True):
      """
      @copydoc LmServer.db.catalog_model.MAL::listOccurrenceSets()
      """
      occsets = self._mal.listOccurrenceSets(firstRecNum, maxCount, 
                                             minOccurrenceCount, 
                                             hasProjections, userId, 
                                             displayName, beforeTime, afterTime, 
                                             epsg, status, completestat, atom)
      return occsets
   
# ...............................................
   def getOutdatedSDMArchiveExperiments(self, count):
      sdmExps = [] 
      models = self._mal.getOutdatedModels(count, ARCHIVE_USER, 
                                           JobStatus.COMPLETE)
      for mdl in models:
         projs = self.getProjectionsForModel(mdl, None)
         sdmExps.append(SDMExperiment(mdl, projs))
      return sdmExps
   
# ...............................................
   def countOccurrenceSets(self, minOccurrenceCount=0, hasProjections=False,
                           userId=ARCHIVE_USER, displayName=None,
                           beforeTime=None, afterTime=None, epsg=None,
                           status=None, completestat=None):
      """
      @copydoc LmServer.db.catalog_model.MAL::countOccurrenceSets()
      """
      count = self._mal.countOccurrenceSets(minOccurrenceCount, hasProjections, 
                                            userId, displayName, beforeTime, 
                                            afterTime, epsg, status, completestat)
      return count

# ...............................................
   def getOccurrenceSetCount(self, queryCount):
      count = None
      if queryCount == -1:
         count = self._mal.returnStatisticValue('occSetCountExpired')
      elif queryCount == 0:
         count = self._mal.returnStatisticValue('occSetCount')
      elif queryCount == 1:
         count = self._mal.returnStatisticValue('occSetCountWithPoints')
      elif queryCount == POINT_COUNT_MIN:
         count = self._mal.returnStatisticValue('occSetCountWithMinPoints')
      return count

# ...............................................
   def getDataIdForSymbol(self, usdasymbol):
      # TODO: complete this 
      return 588602
   
# ...............................................
# Projection
# ...............................................
   def listProjections(self, firstRecNum, maxCount, userId=ARCHIVE_USER, 
                  displayName=None, beforeTime=None, afterTime=None, 
                  epsg=None, status=None, inProcess=False, occSetId=None, 
                  mdlId=None, algCode=None, scenarioId=None,
                  atom=True):
      """
      @summary: Return all projections starting at the firstRecNum 
                limited to maxCount 
      @param firstRecNum: The first record to return
      @param maxCount: Maximum number of records to return 
      @return: a list of Projection atoms or full objects
      """
      if inProcess:
         completeStat = JobStatus.COMPLETE
      else:
         completeStat = None
      objs = self._mal.listProjections(firstRecNum, maxCount, userId,
                  displayName, beforeTime, afterTime, epsg, status, completeStat, 
                  occSetId, mdlId, algCode, scenarioId, atom)
      return objs

# ...............................................
   def countProjections(self, userId=ARCHIVE_USER, displayName=None, 
                        beforeTime=None, afterTime=None, epsg=None, status=None, 
                        inProcess=False, occSetId=None, mdlId=None, 
                        algCode=None, scenarioId=None):
      if inProcess:
         completeStat = JobStatus.COMPLETE
      else:
         completeStat = None
      count = self._mal.countProjections(userId, displayName, beforeTime, 
                                         afterTime, epsg, status, completeStat, 
                                         occSetId, mdlId, algCode, scenarioId)
      return count
   
# ...............................................
   def getProjectionById(self, projId):
      """
      @summary Method to return all of the information necessary to project an
               openModeller experiment.
      @param modelId: The id of the projection to retrieve
      @return The projection specified by the id
      """
      # The MAL initializes the model Scenario with its code and metadataUrls only
      proj = self._mal.getProjectionById(projId)
      return proj

# ...............................................
   def getProjectionsForOccurrenceSet(self, occ):
      """
      @summary: Return all projections created from the given taxonomic name.
      @param occ: An occurrenceSet or id for an occurrenceSet for which to  
             return projections
      @return: a list of Projection objects
      """
      if isinstance(occ, IntType):
         projections = self._mal.getProjectionsForOcc(occ)
      else:
         projections = self._mal.getProjectionsForOcc(occ.getId())
      return projections
   
# ...............................................
   def getProjectionsForModel(self, mdl, status):
      """
      @summary: Return all projections created from the given taxonomic name.
      @param mdl: A model for which to return projections
      @param status: Status for which to filter projections.  None returns all.
      @return: a list of Projection objects
      """
      projections = self._mal.getProjectionsForModel(mdl.getId(), status)
      return projections

# ...............................................
# Scenario
# ...............................................
   def getScenario(self, code, matchingLayers=None):
      """
      @summary: Get and fill a scenario from its code or database id.  If 
                matchingLayers is given, ensure that only layers with the same
                type as layers in the matchingLayers are filled, and that the 
                requested scenario layers are in the same order as those in 
                the matchingLayers.
      @param code: The code for the scenario to return
      @param matchScenarioCode: The code for the scenario to match.
      @return: Scenario object filled with Raster objects.
      """
      if isinstance(code, IntType):
         scenario = self._mal.getScenarioById(code, matchingLayers)
      elif isinstance(code, StringType) or isinstance(code, UnicodeType):
         scenario = self._mal.getScenarioByCode(code, matchingLayers)

      return scenario
              
# .............................................................................
   def getScenariosForLayer(self, lyrid):
      """
      @summary: Return all scenarios containing layer with lyrid; do not fill
                in layers
      @param id: Database key for the layer for which to fetch scenarios.
      """
      scens = self._mal.getScenariosForLayer(lyrid)
      return scens

# ...............................................
   def getScenariosByKeyword(self, keyword):
      """
      @summary: Get and fill scenarios from with the given keyword.  
      @param keyword: A keyword related to the desired scenarios
      @return: Scenario objects filled with Raster objects.
      """
      scenarios = self._mal.getScenariosByKeyword(keyword)
      return scenarios

# ...............................................
   def getMatchingScenarios(self, scenarioid):
      """
      @summary: Get scenarios with layers matching those of the  
             given scenarioid.  Do not fill layers.  Also returns the scenario
             to be matched.
      @param scenarioid: Id of the scenario for which to find others with 
             matching layertypes.
      @return: List of scenario objects NOT filled with layers; NOT filled with
             keywords.
      """
      scenarios = self._mal.getMatchingScenarios(scenarioid)
      return scenarios

# ...............................................
   def countScenarios(self, userId=ARCHIVE_USER, beforeTime=None, afterTime=None, 
                      epsg=None, matchingId=None, kywdLst=[]):
      kywds = ','.join(kywdLst)
      count = self._mal.countScenarios(userId, beforeTime, afterTime, epsg, 
                                       matchingId, kywds)
      return count
   
# ...............................................
   def listScenarios(self, firstRecNum, maxCount, userId=ARCHIVE_USER, 
                     beforeTime=None, afterTime=None, epsg=None,
                     matchingId=None, kywdLst=[], atom=True):
      """
      @summary: Return all layers (for all scenarios) starting at the 
                firstRecNum limited to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxCount: Maximum number of records to return 
      @param beforeTime: gets only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time
             Defaults to no filter by time.
      @param matchingId: return only scenarios with layers matching the given 
             scenarioId.  This also returns the given scenario to be matched.
             Defaults to no filter by matching scenario.  
      @param kywdLst: return only scenarios whose keywords match all those 
             in this list.  Defaults to no filter by matching keywords.
      @return: a list of Scenario atoms or full objects
      """
      kywds = ','.join(kywdLst)
      objs = self._mal.listScenarios(firstRecNum, maxCount, userId, 
                                      beforeTime, afterTime, epsg, matchingId, 
                                      kywds, atom)
      return objs
   
# ...............................................
   def getScenarios(self):
      """
      @summary: Get and fill will all layers all available scenarios.  
      @return: A list of Scenario objects, each filled with Raster objects.
      """
      scenarios = []
      codes = self._mal.getScenarioCodes()
      for code in codes:
         scenarios.append(self._mal.getScenarioByCode(code, None))
      return scenarios
         
# ...............................................
   def getOccurrenceSet(self, occid, firstRecNum=0, maxCount=POINT_COUNT_MAX):
      """
      @summary: get an occurrence point, possible including its points
      @param id: the database primary key of the OccurrenceSet in the MAL
      @param firstRecNum: The starting point record to return (offset from 
             beginning of query results)
      @param maxCount: the maximum number of points to return
      """
      occset = self._mal.getOccurrenceSet(occid)
      return occset
      
# ...............................................
   def getOccurrenceSetsForScientificName(self, sciName, userId):
      """
      @summary: get a list of occurrencesets for the given scientificName, 
                possibly reading its points
      @param sciName: a ScientificName object
      @param userId: the database primary key of the LMUser
      """
      occsets = []
      if sciName is not None:
         occsets = self._mal.getOccurrenceSetsForScientificName(sciName.getId(), userId)
      return occsets

# ...............................................
# Statistics
# ...............................................
   def getStatisticsQueries(self):
      statsDict = self._mal.getStatisticsQueries()
      return statsDict

# ...............................................
   def getProgress(self, starttime=None, endtime=None, usrid=None):
      '''
      @summary: returns a dictionary of {objectTypes: {status: count}}
      @note: uses the db view lm_progress
      '''
      statusDict = {}
      for reftype in ReferenceType.sdmTypes:
         statusDict[reftype] = self._mal.getProgress(reftype, starttime, 
                                                     endtime, usrid)
      return statusDict
   
# ...............................................
   def getModeledSpeciesCount(self):
      count = self._mal.returnStatisticValue('occSetCountModeled')
      return count

# ...............................................
   def getProjectedSpeciesCount(self):
      count = self._mal.returnStatisticValue('occSetCountProjected')
      return count

# ...............................................
   def getOccurrenceStats(self):
      """
      @summary: Get occurrenceSet information for occurrenceSets with > 0 points.
      @note: Used just for lucene index creation, so return the information in tuples.
      @return: list of tuples in the form (occurrenceSetId, displayname, totalpoints, totalmodels)
      """      
      tupleLst = self._mal.getOccurrenceStats()
      return tupleLst
   
# # ...............................................
#    def countModeledSpecies(self, status, userid):
#       count = self._mal.countModeledSpecies(status, userid)
#       return count

# # ...............................................
#    def getLatestModelTime(self, status):
#       time = self._mal.getLatestModelTime(status)
#       return time
# 
# # ...............................................
#    def getLatestProjectionTime(self, status):
#       time = self._mal.getLatestProjectionTime(status)
#       return time

# ...............................................
# User
# ...............................................
   def getUser(self, userId):
      """
      @summary: get a user, including the encrypted password
      @param userId: the database primary key of the LMUser in the MAL
      @return: a LMUser object
      @note: LMUser is duplicated in RAD database, so just get this once from MAL
      """
      usr = self._mal.getUser(userId)
      return usr

# ...............................................
   def getAllUserIds(self):
      """
      @summary: get a user, including the encrypted password
      @param userId: the database primary key of the LMUser in the MAL
      @return: a LMUser object
      @note: LMUser is duplicated in RAD database, so just get this once from MAL
      """
      usrs = self._mal.getUserIds()
      return usrs

# ...............................................
   def findUser(self, userId=None, email=None):
      """
      @summary: find a user with either a matching userId or email address
      @param userId: the database primary key of the LMUser in the MAL
      @param email: the email address of the LMUser in the MAL
      @return: a LMUser object
      @note: LMUser is duplicated in RAD database, so just get this once from MAL
      """
      usr = self._mal.findUser(userId, email)
      return usr
   
# ...............................................
   def getComputeResourceByIP(self, ipAddr):
      cr = self._mal.getComputeResourceByIP(ipAddr)
      return cr

# ...............................................
   def getAllComputeResources(self):
      """
      """
      comps = self._mal.getAllComputeResources()
      return comps

# ...............................................
# Miscellaneous
# ...............................................
   def findUserForObject(self, scencode=None, occsetId=None, 
                         radexpId=None, bucketId=None, pamsumId=None, shapegridId=None):
      """
      @summary: Return a count of model and projection jobs at given status.
      """
      usr = None
      if scencode is not None: 
         usr = self._mal.getUserIdForObjId(scencode, isScenario=True)
      elif occsetId is not None:
         usr = self._mal.getUserIdForObjId(occsetId, isOccurrence=True)
      elif (radexpId is not None or bucketId is not None or 
            pamsumId is not None or shapegridId is not None):
         usr = self._rad.getUserIdForObjId(radexpId=radexpId, bucketId=bucketId, 
                                 pamsumId=pamsumId, shapegridId=shapegridId)
      return usr

# ...............................................
   def countJobsOld(self, reftypeLst, status, userIdLst=[None]):
      """
      @summary: Return a count of model and projection jobs at given status.
      @todo: Change to query on ProcessType
      """
      total = 0
      if not userIdLst:
         userIdLst = [None]
      for reftype in reftypeLst:         
         for usr in userIdLst:
            if reftype in (ReferenceType.SDMModel, ReferenceType.SDMProjection,
                     ReferenceType.SDMExperiment, ReferenceType.OccurrenceSet):
               total += self._mal.countJobs(reftype, status, usr)
            elif reftype in (ReferenceType.Bucket, ReferenceType.RADExperiment,
                             ReferenceType.OriginalPamSum, 
                             ReferenceType.RandomPamSum):
               total += self._rad.countJobs(reftype, status, usr)
            else:
               raise LMError('Unknown referenceType {}'.format(reftype))

      return total

# ...............................................
   def countJobs(self, procTypeLst, status, userIdLst=[None]):
      """
      @summary: Return a count of model and projection jobs at given status.
      @param procTypeLst: list of desired LmCommon.common.lmconstants.ProcessType 
      @param status: list of desired LmCommon.common.lmconstants.JobStatus
      @param userIdLst: list of desired userIds
      """
      total = 0
      if not userIdLst:
         userIdLst = [None]
      for ptype in procTypeLst:         
         for usr in userIdLst:
            if ProcessType.isSDM(ptype):
               total += self._mal.countJobs(ptype, status, usr)
            elif ProcessType.isRAD(ptype):
               total += self._rad.countJobs(ptype, status, usr)
            else:
               raise LMError('ProcessType {} is not implemented!'.format(ptype))
      return total

# ...............................................
   def countJobChains(self, status, userIdLst=[None]):
      """
      @summary: Return a count of model and projection jobs at given status.
      @param procTypeLst: list of desired LmCommon.common.lmconstants.ProcessType 
      @param status: list of desired LmCommon.common.lmconstants.JobStatus
      @param userIdLst: list of desired userIds
      """
      total = 0
      if not userIdLst:
         userIdLst = [None]
      for usr in userIdLst:
         total += self._mal.countJobChains(status, usr)
      return total

# ...............................................
   def _getPreviousTime(self, day, hour, minute):
      if day == 0 and hour == 0 and minute == 0:
         return None
      else:
         prevTime = (mx.DateTime.gmt() 
                     - mx.DateTime.DateTimeDelta(day, hour, minute))
         return prevTime.mjd

   # ...............................................
   def _getLatin1FromUtf8(self, str_utf8):
      try:
         str_latin1 = unicode(str_utf8, ENCODING).encode("iso-8859-1")
      except Exception, e:
         self.log.error('Failed encoding {} to {}'.format(str_utf8, ENCODING))
         
      return str_latin1
   
# .............................................................................
# RAD functions
# .............................................................................
   # ...............................................
   def listRADBuckets(self, firstRecNum, maxCount, userId,
                      beforeTime=None, afterTime=None, epsg=None, 
                      experimentId=None, experimentName=None, shapegridId=None, 
                      shapegridName=None, atom=True):
      objs = self._rad.listBuckets(firstRecNum, maxCount, userId, 
                                    beforeTime, afterTime, epsg, experimentId, 
                                    experimentName, shapegridId, shapegridName,
                                    atom)
      return objs
   
# .............................................................................
   def countRADBuckets(self, userId, beforeTime=None, afterTime=None, epsg=None,
                       experimentId=None, experimentName=None, shapegridId=None, 
                       shapegridName=None):
      total = self._rad.countBuckets(userId, beforeTime, afterTime, epsg,
                                     experimentId, experimentName, shapegridId, 
                                     shapegridName)
      return total
   
# .............................................................................
   def getRADBucket(self, usr, bucketId=None, expId=None, 
                    shpName=None, shpId=None, fillRandoms=False):
      """
      Returns a RADExperiment with specified RADBucket
      """
      if bucketId is not None:
         exp = self._rad.getExperimentWithOneBucket(bucketId)
         bkt = exp.bucketList[0]
      elif expId is not None and (shpName is not None or shpId is not None): 
         bkt = self._rad.getBucketByShape(expId, shpName=shpName, shpId=shpId)
      else:
         raise LMError(currargs='getRADBucket requires either bucketId or ' + 
                       'experimentId and shapegrid Name or Id')
      if fillRandoms:
         rpsList = self._rad.getRandomPamSums(bucketId)
         bkt.setRandomPamSums(rpsList)
      return bkt

   
   # ...............................................
   def listRADExperiments(self, firstRecNum, maxCount, userId,
                      beforeTime=None, afterTime=None, epsg=None, 
                      experimentName=None, atom=True):
      objs = self._rad.listExperiments(firstRecNum, maxCount, userId, 
                                        beforeTime, afterTime, epsg, 
                                        experimentName, atom)
      return objs
   
# .............................................................................
   def countRADExperiments(self, userId, beforeTime=None, afterTime=None, 
                            epsg=None, experimentName=None):
      total = self._rad.countExperiments(userId, beforeTime, afterTime, 
                                          epsg, experimentName)
      return total
   
# ...............................................
   def getRADExperiment(self, usr, fillIndices=True, fillLayers=False, 
                        fillRandoms=False, expid=None, expname=None):
      radexp = self._rad.getExperimentWithAllBuckets(usr, expid, expname)
      if radexp is not None:
         if fillLayers:
            self.fillRADLayersetForExperiment(radexp)
         elif fillIndices:
            self._populateLayerIndices(radexp)
         if fillRandoms:
            for bucket in radexp.bucketList:
               rpsList = self._rad.getRandomPamSums(bucket.getId())
               bucket.setRandomPamSums(rpsList)
      return radexp

# ...............................................
   def getRADExperimentWithOneBucket(self, usr, bucketId, fillIndices=True, 
                                     fillLayers=False, fillRandoms=False):
      radexp = self._rad.getExperimentWithOneBucket(bucketId)
      if radexp is not None:
         if fillLayers:
            self.fillRADLayersetForExperiment(radexp)
         elif fillIndices:
            self._populateLayerIndices(radexp)
         if fillRandoms:
            rpsList = self.getRandomPamSums(bucketId)
            radexp.bucketList[0].setRandomPamSums(rpsList)
      return radexp

   # ...............................................
   def listPamSums(self, firstRecNum, maxCount, userId,
                   beforeTime=None, afterTime=None, epsg=None, experimentId=None,
                   bucketId=None, isRandomized=False, 
                   randomMethod=RandomizeMethods.NOT_RANDOM, atom=True):
      objs = self._rad.listPamSums(firstRecNum, maxCount, userId, 
                                    beforeTime, afterTime, epsg, experimentId, 
                                    bucketId, isRandomized, randomMethod, atom)
      return objs
   
# .............................................................................
   def countPamSums(self, userId, beforeTime=None, afterTime=None, epsg=None,
                    experimentId=None, bucketId=None, isRandomized=False,
                    randomMethod=RandomizeMethods.NOT_RANDOM):
      total = self._rad.countPamSums(userId, beforeTime, afterTime, epsg,
                                     experimentId, bucketId, isRandomized, 
                                     randomMethod)
      return total
   
# .............................................................................
   def getPamSum(self, pamsumId):
      pamsum = None
      if pamsumId is not None:
         pamsum = self._rad.getPamSum(pamsumId)
      return pamsum

   # ...............................................
   def findShapeGrids(self, shpgrd):
      shpgrds = self._rad.findShapeGrids(shpgrd)
      return shpgrds
   
   # ...............................................
   def getShapeGrid(self, usr, shpid=None, shpname=None):
      shg = None
      if shpid is not None:
         shg = self._rad.getShapeGrid(shpid, usr)
      elif shpname is not None:
         shg = self._rad.getShapeGridByName(shpname, usr)

      return shg

# ...............................................
   def listShapegrids(self, firstRecNum, maxCount, 
                  userId, 
                  beforeTime=None, 
                  afterTime=None, 
                  epsg=None,
                  layerId=None,
                  layerName=None,
                  cellsides=None,
                  atom=True):
      """
      @summary: Return all layers for a user starting at the firstRecNum limited 
                to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxCount: Maximum number of records to return 
      @param beforeTime: gets only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time
             Defaults to no filter by time.
      @param layerId: gets only layers with this layerid.  Defaults
             to no filter by layerid.
      @param layerName: gets only layers with this layername for this user.  
             Defaults to no filter by layername.
      @return: a list of Layer objs
      """
      objs = self._rad.listShapegrids(firstRecNum, maxCount, userId, beforeTime, 
                                   afterTime, epsg, layerId, layerName, cellsides, atom)
      return objs
      
# ...............................................
   def countShapegrids(self, userId, beforeTime=None, afterTime=None,
                      epsg=None, lyrid=None, lyrname=None, cellsides=None):
      """
      @summary: Count all layers for a user
      @param userid: Filter by userid
      @param maxCount: Maximum number of records to return 
      @param beforeTime: gets only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time
             Defaults to no filter by time.
      @return: a count of Layers for this user
      """
      count = self._rad.countShapegrids(userId, beforeTime, afterTime, epsg, 
                                    lyrid, lyrname, cellsides)
      return count
    
# ...............................................
   def listRADLayers(self, firstRecNum, maxCount, userId, beforeTime=None,
                     afterTime=None, epsg=None, layerId=None, layerName=None,
                     atom=True):
      """
      @summary: Return all layers for a user starting at the firstRecNum limited 
                to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxCount: Maximum number of records to return 
      @param beforeTime: gets only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time
             Defaults to no filter by time.
      @param layerId: gets only layers with this layerid.  Defaults
             to no filter by layerid.
      @param layerName: gets only layers with this layername for this user.  
             Defaults to no filter by layername.
      @return: a list of Layer objs
      """
      objs = self._rad.listLayers(firstRecNum, maxCount, userId, beforeTime, 
                                  afterTime, epsg, layerId, layerName, atom)
      return objs
      
# ...............................................
   def countRADLayers(self, userId, beforeTime=None, afterTime=None,
                      epsg=None, lyrid=None, lyrname=None):
      """
      @summary: Count all layers for a user
      @param userid: Filter by userid
      @param maxCount: Maximum number of records to return 
      @param beforeTime: gets only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time
             Defaults to no filter by time.
      @return: a count of Layers for this user
      """
      count = self._rad.countLayers(userId, beforeTime, afterTime, epsg, 
                                    lyrid, lyrname)
      return count
 
   # ...............................................
   def findRADLayers(self, lyr, firstrec=None, maxcount=None, 
                     beforetime=None, aftertime=None):
      lyrs = self._rad.findExistingRADLayers(lyr, firstrec, maxcount, 
                                                   beforetime, aftertime)
      return lyrs
   
   # ...............................................
   def getRADLayer(self, lyrid=None, palyrid=None, anclyrid=None):
      lyr = None
      if palyrid is not None:
         lyr = self._rad.getPALayer(palyrid)
      elif anclyrid is not None:
         lyr = self._rad.getAncLayer(anclyrid)
      elif lyrid is not None:
         lyr = self._rad.getBaseLayer(lyrid)
      return lyr
   
# ...............................................
   def fillRADLayersetForExperiment(self, radexp, isPresenceAbsence=None):
      """
      @param radexp: RADExperiment to fill with layerset/s
      @param isPresenceAbsence: True to fill OrganismLayerset, False to fill
                                EnvironmentalLayerset, None to fill both
      """
      palyrs = None
      anclyrs = None
      if isPresenceAbsence is None:
         palyrs = self._rad.getLayers(radexp.getId(), isPresenceAbsence=True)
         anclyrs = self._rad.getLayers(radexp.getId(), isPresenceAbsence=False)
      
      elif isPresenceAbsence:
         palyrs = self._rad.getLayers(radexp.getId(), isPresenceAbsence=True)
      
      else:
         anclyrs = self._rad.getLayers(radexp.getId(), False)

      if palyrs:
         radexp.setOrgLayerset(palyrs)
      if anclyrs:
         radexp.setEnvLayerset(anclyrs)
      
# ...............................................
   def listAncillaryLayers(self, firstRecNum, maxCount, userId, 
                           beforeTime=None, afterTime=None, epsg=None, layerId=None,
                           layerName=None, ancillaryValueId=None, expId=None):
      """
      @summary: Return all layers for a user starting at the firstRecNum limited 
                to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxCount: Maximum number of records to return 
      @param beforeTime: gets only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time
             Defaults to no filter by time.
      @param layerId: gets only records with this layerid.  Defaults
             to no filter by layerid.
      @param layerName: gets only records with this layername for this 
             user.  Defaults to no filter by layername.
      @param ancillaryValueId: gets only records with this set of 
             ancillaryValues for this user. Defaults to no filter by 
             ancillaryValueId.
      @param expId: gets only records for experiment with this id
      @return: a list of AncillaryLayer objs
      """
      objs = self._rad.listAncillaryLayers(firstRecNum, maxCount, userId, 
                                            beforeTime, afterTime, epsg, layerId, 
                                            layerName, ancillaryValueId, expId)
      return objs
      
# ...............................................
   def countAncillaryLayers(self, userId, beforeTime=None, afterTime=None, 
                            epsg=None, layerId=None, layerName=None, 
                            ancillaryValueId=None, expId=None):
      """
      @summary: Count all layers for a user
      @param userid: Filter by userid
      @param beforeTime: count only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: count only records modified after or at this time
             Defaults to no filter by time.
      @param layerId: gets only records with this layerid.  Defaults to no 
             filter by layerid.
      @param layerName: gets only records with this layername for this user.
             Defaults to no filter by layername.
      @param ancillaryValueId: gets only records with this set of 
             ancillaryValues for this user. Defaults to no filter by 
             ancillaryValueId.
      @param expId: count only records for experiment with this id
      @return: a count of AncillaryLayers matching these parameters
      """
      count = self._rad.countAncillaryLayers(userId, beforeTime, afterTime, 
                                             epsg, layerId, layerName, 
                                             ancillaryValueId, expId)
      return count

   # ...............................................
   def findAncillaryLayers(self, lyr, firstrec=None, maxcount=None, 
                     beforetime=None, aftertime=None):
      lyrs = self._rad.findAncillaryLayers(lyr, firstrec, maxcount, 
                                           beforetime, aftertime)
      return lyrs
   
   # ...............................................
   def findAncillaryLayersForUser(self, usr, lyrid=None, lyrname=None):
      if lyrid is not None:
         alyrs = self._rad.findAncillaryLayersForUser(usr, lyrid)
      else:
         alyrs = self._rad.findAncillaryLayersForUser(usr, lyrname)
      return alyrs

   # ...............................................
   def findRADExperiment(self, radexp):
      """
      @note: Assumes any 
      """
      existExp = self.getRADExperiment(radexp.getUserId(), 
                                       expid=radexp.getId(),
                                       expname=radexp.name)
      return existExp

   # ...............................................
   def findRADExperiments(self, usr):
      exps = self._rad.findExperimentsForUser(usr)
      return exps
   
# ...............................................
   def _populateLayerIndices(self, radexp):
      paLyrIndices = self._rad.getLayers(radexp.getId(), indicesOnly=True, 
                                         isPresenceAbsence=True)
      radexp.orgLayerset.setLayerIndices(paLyrIndices)

      ancLyrIndices = self._rad.getLayers(radexp.getId(), indicesOnly=True, 
                                         isPresenceAbsence=False)
      radexp.envLayerset.setLayerIndices(ancLyrIndices)

# ...............................................
   def _populateLayers(self, radexp):
      paLyrs = self._rad.getLayers(radexp.getId(), indicesOnly=False, 
                                   isPresenceAbsence=True)
      radexp.setOrgLayerset(paLyrs)
      
      ancLyrs = self._rad.getLayers(radexp.getId(), indicesOnly=False, 
                                    isPresenceAbsence=False)
      radexp.setEnvLayerset(ancLyrs)

# ...............................................
   def listPresenceAbsenceLayers(self, firstRecNum, maxCount, userId, 
                           beforeTime=None, afterTime=None, epsg=None, 
                           layerId=None, layerName=None, presenceAbsenceId=None, 
                           expId=None, atom=True):
      """
      @summary: Return all layers for a user starting at the firstRecNum limited 
                to maxCount 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxCount: Maximum number of records to return 
      @param beforeTime: gets only records modified before or at this time. 
             Defaults to no filter by time.  
      @param afterTime: gets only records modified after or at this time.  
             Defaults to no filter by time.
      @param layerId: gets only records with this layerid.  Defaults to no 
             filter by layerid.
      @param layerName: gets only records with this layername for this user.
             Defaults to no filter by layername.
      @param presenceAbsenceId: gets only records with this set of 
             presenceAbsence values for this user. Defaults to no filter by 
             presenceAbsenceId.
      @param expId: gets only records for experiment with this id
      @return: a list of PresenceAbsenceLayer objs matching these parameters
      """
      objs = self._rad.listPresenceAbsenceLayers(firstRecNum, maxCount, userId, 
                                            beforeTime, afterTime, epsg, layerId, 
                                            layerName, presenceAbsenceId, expId,
                                            atom)
      return objs
      
# ...............................................
   def countPresenceAbsenceLayers(self, userId, beforeTime=None, afterTime=None, 
                                  epsg=None, layerId=None, layerName=None, 
                                  presenceAbsenceId=None, expId=None):
      """
      @summary: Count all layers for a user
      @param userid: Filter by userid
      @param beforeTime: count only records modified before or at this time.
             Defaults to no filter by time.  
      @param afterTime: count only records modified after or at this time
             Defaults to no filter by time.
      @param layerId: gets only records with this layerid.  Defaults to no 
             filter by layerid.
      @param layerName: gets only records with this layername for this user.
             Defaults to no filter by layername.
      @param presenceAbsenceId: gets only records with this set of 
             presenceAbsence values for this user. Defaults to no filter by 
             presenceAbsenceId.
      @param expId: count only records for experiment with this id
      @return: a count of PresenceAbsenceLayers matching these parameters
      """
      count = self._rad.countPresenceAbsenceLayers(userId, beforeTime, afterTime, 
                                                   epsg, layerId, layerName, 
                                                   presenceAbsenceId, expId)
      return count

   # ...............................................
   def findPresenceAbsenceLayers(self, lyr, firstrec=None, maxcount=None, 
                     beforetime=None, aftertime=None, layerId=None, 
                     layerName=None, presenceAbsenceId=None, expId=None):
      lyrs = self._rad.findPresenceAbsenceLayers(lyr, firstrec, maxcount, 
                                                 beforetime, aftertime, 
                                                 layerId, layerName, 
                                                 presenceAbsenceId, expId)
      return lyrs
   

# ...............................................
   def findTaxon(self, taxonSourceId, taxonKey):
      sciname = self._mal.findTaxon(taxonSourceId, taxonKey)
      return sciname
   
# ...............................................
   def findTaxonSource(self, taxonSourceName):
      txSourceId, url, createdate, moddate = self._mal.findTaxonSource(taxonSourceName)
      return txSourceId, url, createdate, moddate
   
   
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   # ...............................................
   
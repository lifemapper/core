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
# .............................................................................
import mx.DateTime as dt
from osgeo.ogr import wkbPoint
import os


from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (ProcessType, JobStatus, LMFormat,
                                         OutputFormat, MatrixType) 
from LmDbServer.common.lmconstants import TAXONOMIC_SOURCE
from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (LOG_PATH, LMFileType, SPECIES_DATA_PATH)
from LmServer.common.localconstants import TROUBLESHOOTERS
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.processchain import MFChain
from LmServer.legion.sdmproj import SDMProjection
from LmServer.tools.occwoc import BisonWoC, iDigBioWoC, GBIFWoC, UserWoC

# .............................................................................
class ChristopherWalken(LMObject):
   """
   Class to ChristopherWalken.
   
   [occ]  ( filename of taxonids, csv of datablocks, etc each handled differently)
   [algs/params]
   mdlscen
   [prjscens]
   
   {Occtype: type,
    Occurrencedriver: [occ, occ ...], 
    Algorithm: [algs/params]
    MdlScenario: mdlscen
    ProjScenario: [prjscens]
   }
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, user, archiveName, jsonFname=None, priority=None, logger=None):
      """
      @summary Constructor for ChristopherWalken class
      """
      super(ChristopherWalken, self).__init__()
      self.rules = []
      self.userId = user
      self.archiveName = archiveName
      self.priority = priority
      
      self.name = '{}_{}_{}'.format(user, self.__class__.__name__.lower(), 
                                    archiveName)      
      (self.log, 
       self._scribe) = self._getLoggerAndDatabase(logger=logger)

      if jsonFname is not None:
         raise LMError('JSON Walken is not yet implemented')
      else:
         (weaponOfChoice, expDate, epsg, algCodes, 
          mdlScenCode, mdlMaskName, prjScenCodes, prjMaskName, 
          gridname, intersectParams) = self.getConfigValues()
      self.occWeapon = weaponOfChoice
      self._obsoleteTime = expDate
      self.epsg = epsg
      self.intersectParams = intersectParams
      
      (self.algs, 
       self.mdlScen, 
       self.prjScens, 
       self.mdlMask, 
       self.prjMask, 
       self.intersectGrid, 
       boomGridset) = self._getInputObjects(algCodes, mdlScenCode, 
                              mdlMaskName, prjScenCodes, prjMaskName, gridname)
      self.globalPAM = boomGridset.pam
      self.boomShapegrid = boomGridset.getShapegrid()

# ...............................................
   def moveToStart(self):
      self.occWeapon.moveToStart()
      
# ...............................................
   def saveNextStart(self, fail=False):
      self.occWeapon.saveNextStart(fail=fail)

# .............................................................................
   def _getLoggerAndDatabase(self, logger=None):
      # Optionally use parent process logger
      if logger is None:
         logger = ScriptLogger(self.name)
      # Database connection
      try:
         scribe = BorgScribe(self.log)
         success = scribe.openConnections()
      except Exception, e:
         raise LMError(currargs='Exception opening database', prevargs=e.args)
      else:
         if not success:
            raise LMError(currargs='Failed to open database')
         else:
            logger.info('{} opened databases'.format(self.name))
      return logger, scribe

# .............................................................................
   def getConfigValues(self):
      fileList = []
      # If None, default to configured instance variables
      if self.userId is not None and self.archiveName is not None:
         earl = EarlJr()
         pth = earl.createDataPath(self.userId, LMFileType.BOOM_CONFIG)
         archiveConfigFile = os.path.join(pth, 
                                    '{}{}'.format(self.archiveName, OutputFormat.CONFIG))
         print 'Config file at {}'.format(archiveConfigFile)
         if os.path.exists(archiveConfigFile):
            fileList.append(archiveConfigFile)
      cfg = Config(fns=fileList)
      _ENV_HEAD = "LmServer - environment"
      _PIPELINE_HEAD = "LmServer - pipeline"
   
      if self.userId is None:
         self.userId = cfg.get(_ENV_HEAD, 'ARCHIVE_USER')
      if self.archiveName is None:
         self.archiveName = cfg.get(_PIPELINE_HEAD, 'ARCHIVE_NAME')

      taxonSourceName = mdlMaskName = prjMaskName = None
      try:
         datasource = cfg.get(_PIPELINE_HEAD, 'ARCHIVE_DATASOURCE')
      except:
         datasource = cfg.get(_ENV_HEAD, 'DATASOURCE')
      try:
         taxonSourceName = TAXONOMIC_SOURCE[datasource]['name']
      except:
         pass
         
      try:
         mdlMaskName = cfg.get(_PIPELINE_HEAD, 'ARCHIVE_MODEL_MASK_NAME')
      except:
         pass
      try:
         prjMaskName = cfg.get(_PIPELINE_HEAD, 'ARCHIVE_PROJECT_MASK_NAME')
      except:
         pass

      algorithms = cfg.getlist(_PIPELINE_HEAD, 'ARCHIVE_ALGORITHMS')
      mdlScen = cfg.get(_PIPELINE_HEAD, 'ARCHIVE_MODEL_SCENARIO')
      prjScens = cfg.getlist(_PIPELINE_HEAD, 'ARCHIVE_PROJECTION_SCENARIOS')
      epsg = cfg.getint(_PIPELINE_HEAD, 'ARCHIVE_EPSG')
      gridname = cfg.get(_PIPELINE_HEAD, 'ARCHIVE_GRID_NAME')
      minPoints = cfg.getint(_PIPELINE_HEAD, 'ARCHIVE_POINT_COUNT_MIN')
      
      intersectParams = {
         MatrixColumn.INTERSECT_PARAM_FILTER_STRING: 
            Config().get(_PIPELINE_HEAD, 'INTERSECT_FILTERSTRING'),
         MatrixColumn.INTERSECT_PARAM_VAL_NAME: 
            Config().get(_PIPELINE_HEAD, 'INTERSECT_VALNAME'),
         MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: 
            Config().getint(_PIPELINE_HEAD, 'INTERSECT_MINPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: 
            Config().getint(_PIPELINE_HEAD, 'INTERSECT_MAXPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: 
            Config().getint(_PIPELINE_HEAD, 'INTERSECT_MINPERCENT')}

      # Expiration date for retrieved species data 
      expDate = dt.DateTime(cfg.getint(_PIPELINE_HEAD, 'ARCHIVE_SPECIES_EXP_YEAR'), 
                            cfg.getint(_PIPELINE_HEAD, 'ARCHIVE_SPECIES_EXP_MONTH'), 
                            cfg.getint(_PIPELINE_HEAD, 'ARCHIVE_SPECIES_EXP_DAY')).mjd
      
      # Bison data
      if datasource == 'BISON':
         bisonTsn = Config().get(_PIPELINE_HEAD, 'BISON_TSN_FILENAME')
         bisonTsnFile = os.path.join(SPECIES_DATA_PATH, bisonTsn)
         weaponOfChoice = BisonWoC(self._scribe, self.userId, self.archiveName, 
                                   epsg, expDate, minPoints, bisonTsnFile, 
                                   taxonSourceName=taxonSourceName, 
                                   logger=self.log)
      # iDigBio data
      elif datasource == 'IDIGBIO':
         idigTaxonids = Config().get(_PIPELINE_HEAD, 'IDIG_FILENAME')
         idigTaxonidsFile = os.path.join(SPECIES_DATA_PATH, idigTaxonids)
         weaponOfChoice = iDigBioWoC(self._scribe, self.userId, self.archiveName, 
                                     epsg, expDate, minPoints, idigTaxonidsFile,
                                     taxonSourceName=taxonSourceName, 
                                     logger=self.log)
      # GBIF data
      elif datasource == 'GBIF':
         gbifTax = cfg.get(_PIPELINE_HEAD, 'GBIF_TAXONOMY_FILENAME')
         gbifTaxFile = os.path.join(SPECIES_DATA_PATH, gbifTax)
         gbifOcc = cfg.get(_PIPELINE_HEAD, 'GBIF_OCCURRENCE_FILENAME')
         gbifOccFile = os.path.join(SPECIES_DATA_PATH, gbifOcc)
         gbifProv = cfg.get(_PIPELINE_HEAD, 'GBIF_PROVIDER_FILENAME')
         gbifProvFile = os.path.join(SPECIES_DATA_PATH, gbifProv)
         weaponOfChoice = GBIFWoC(self._scribe, self.userId, self.archiveName, 
                                     epsg, expDate, minPoints, gbifOccFile,
                                     providerFname=gbifProvFile, 
                                     taxonSourceName=taxonSourceName, 
                                     logger=self.log)
      # User data, anything not above
      else:
         userOccData = cfg.get(_PIPELINE_HEAD, 
                               'ARCHIVE_USER_OCCURRENCE_DATA')
         userOccDelimiter = cfg.get(_PIPELINE_HEAD, 
                               'ARCHIVE_USER_OCCURRENCE_DATA_DELIMITER')
         userOccCSV = os.path.join(pth, userOccData + OutputFormat.CSV)
         userOccMeta = os.path.join(pth, userOccData + OutputFormat.METADATA)
         weaponOfChoice = UserWoC(self._scribe, self.userId, self.archiveName, 
                                     epsg, expDate, minPoints, userOccCSV,
                                     userOccMeta, userOccDelimiter, 
                                     logger=self.log)

      return (weaponOfChoice, expDate, epsg,  algorithms, mdlScen, mdlMaskName,
              prjScens, prjMaskName, gridname, intersectParams)  

# ...............................................
   def _getInputObjects(self, algCodes, mdlScenCode, mdlMaskName, prjScenCodes, 
                        prjMaskName, intersectGridName):
      algs = prjScens = []
      mdlScen = mdlMask = prjMask = None
      for acode in algCodes:
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         algs.append(alg)
      try:
         mdlScen = self._scribe.getScenario(mdlScenCode, user=self.userId, 
                                            fillLayers=True)
         if mdlScen is not None:
            if mdlScenCode not in prjScenCodes:
               prjScens.append(mdlScen)
            for pcode in prjScenCodes:
               scen = self._scribe.getScenario(pcode, user=self.userId, 
                                               fillLayers=True)
               if scen is not None:
                  prjScens.append(scen)
               else:
                  raise LMError('Failed to retrieve scenario {}'.format(pcode))
         else:
            raise LMError('Failed to retrieve scenario {}'.format(mdlScenCode))
         
         mdlMask = self._scribe.getLayer(userId=self.userId, 
                                         lyrName=mdlMaskName, epsg=self.epsg)
         prjMask = self._scribe.getLayer(userId=self.userId, 
                                         lyrName=prjMaskName, epsg=self.epsg)
         intersectGrid = self._scribe.getShapeGrid(userId=self.userId, 
                                    lyrName=intersectGridName, epsg=self.epsg)
         # Get existing gridset for Archive "Global PAM"
         # TODO: Global PAM created and joined in initBoom???
         tmpGS = Gridset(name=self.archiveName, shapeGrid=self.intersectGrid, 
                        epsgcode=self.epsg, userId=self.userId)
         boomGridset = self._scribe.getGridset(tmpGS, fillMatrices=True)
         if boomGridset is None or boomGridset.pam is None:
            raise LMError('Failed to retrieve Gridset or Global PAM')
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      
      return (algs, mdlScen, prjScens, mdlMask, prjMask, intersectGrid, 
              boomGridset)

   # ...............................
   def startWalken(self):
      """
      @summary: Walks a list of Lifemapper objects for computation
      """
      tmpobjs = []
      occ = self.occWeapon.getOne()
      tmpobjs.append(occ)
      currtime = dt.gmt().mjd
      # Sweep over input options
      for alg in self.algs:
         for prjscen in self.prjScens:
            prj = self._createOrResetSDMProject(occ, alg, prjscen, currtime)
            tmpobjs.append(prj)
            mtxcol = self._createOrResetIntersect(prj)
            tmpobjs.append(mtxcol)
      objs = [o for o in tmpobjs if o is not None]
      
      mfchain = self._createMakeflow(objs)

# ...............................
   
# ...............................................
   def _createOrResetIntersect(self, prj, currtime):
      """
      @summary: Initialize model, projections for inputs/algorithm.
      """
      mtxcol = None
      if prj is not None:
         # TODO: Save processType into the DB??
         if LMFormat.isGDAL(format=prj.dataFormat):
            ptype = ProcessType.INTERSECT_RASTER
         else:
            ptype = ProcessType.INTERSECT_VECTOR
   
         tmpCol = MatrixColumn(None, self.globalPAM.getId(), self.userId, 
                layer=prj, shapegrid=self.boomShapegrid, 
                intersectParams=self.intersectParams, 
                squid=prj.squid, ident=prj.ident,
                processType=ptype, metadata={}, matrixColumnId=None, 
                status=JobStatus.GENERAL, statusModTime=currtime)
         mtxcol = self._scribe.findOrInsertMatrixColumn(tmpCol)
         # Reset processType (not in db)
         mtxcol.processType = ptype
         
         if self._doReset(mtxcol.status, mtxcol.statusModTime):
            mtxcol.updateStatus(JobStatus.GENERAL, modTime=currtime)
            success = self._scribe.updateMatrixColumn(mtxcol)
      return mtxcol

# ...............................................
   def _doReset(self, status, statusModTime):
      doReset = False
      if (JobStatus.failed(status) or 
          JobStatus.waiting(status) or 
           # out-of-date
          (status == JobStatus.COMPLETE and statusModTime < self._obsoleteTime)):
         doReset = True
      return doReset

# ...............................................
   def _createOrResetSDMProject(self, occ, alg, prjscen, currtime):
      """
      @summary: Iterates through all input combinations to create or reset
                SDMProjections for the given occurrenceset.
      @param occ: OccurrenceSet for which to initialize or rollback all 
                  dependent projections
      """
      prj = None
      if occ is not None:
         tmpPrj = SDMProjection(occ, alg, self.mdlScen, prjscen, 
                        modelMask=self.mdlMask, projMask=self.projMask, 
                        dataFormat=LMFormat.GTIFF.driver,
                        status=JobStatus.GENERAL, statusModTime=currtime)
         prj = self._scribe.findOrInsertSDMProject(tmpPrj)
         if prj is not None:
            # Instead of re-pulling unchanged scenario layers, masks, update 
            # with input objects
            prj._modelScenario = self.mdlScen
            prj.setModelMask(self.mdlMask)
            prj._projScenario = prjscen
            prj.setProjMask(self.prjMask)
            # Rollback if finished (new is already at initial status)
            if self._doReset(prj.status, prj.statusModTime):
               prj.updateStatus(JobStatus.GENERAL, modTime=currtime)
               prj = self._scribe.updateSDMProject(prj)
      return prj

# ...............................................
   def _createMakeflow(self, objs):
      updatedMFChain = None
      if objs:
         meta = {MFChain.META_CREATED_BY: os.path.basename(__file__)}
         newMFC = MFChain(self.userid, priority=self.priority, 
                           metadata=meta, status=JobStatus.INITIALIZE, 
                           statusModTime=dt.gmt().mjd)
         updatedMFChain = self._scribe.insertMFChain(newMFC)

         for o in objs:
            try:
               rules = o.computeMe()
            except Exception, e:
               self.log.info('Failed on object.compute {}, ({})'.format(type(o), 
                                                                        str(e)))
            else:
               updatedMFChain.addCommands(rules)
         
         updatedMFChain.write()
         self._scribe.updateObject(updatedMFChain)
         
      return updatedMFChain

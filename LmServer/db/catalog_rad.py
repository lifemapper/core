"""
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
from types import IntType, StringType, UnicodeType

from LmCommon.common.lmconstants import InputDataType, JobStatus, JobStage, \
                                 ProcessType, RandomizeMethods
from LmCommon.common.localconstants import ARCHIVE_USER

from LmServer.base.dbpgsql import DbPostgresql
from LmServer.base.layer import Raster, Vector
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject, ProcessObject

from LmServer.common.lmconstants import ReferenceType, RAD_STORE, LMServiceModule

from LmServer.common.notifyJob import NotifyJob

from LmServer.rad.anclayer import _AncillaryValue, AncillaryRaster, AncillaryVector
from LmServer.rad.matrix import Matrix
from LmServer.rad.matrixlayerset import MatrixLayerset
from LmServer.rad.palayer import _PresenceAbsence, PresenceAbsenceRaster, PresenceAbsenceVector
from LmServer.rad.pamvim import PamSum
from LmServer.rad.radbucket import RADBucket
from LmServer.rad.radexperiment import RADExperiment
from LmServer.rad.radJob import RADIntersectJob, RADSwapJob, \
                               RADSplotchJob, RADCompressJob, \
                               RADCalculateJob
from LmServer.rad.shapegrid import ShapeGrid

# .............................................................................
class RAD(DbPostgresql):
   """
   Class to control modifications to the BGT database.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger, dbHost, dbPort, dbUser, dbKey):
      """
      @summary Constructor for RAD database connector class
      @param logger: LmLogger for debugging
      @param dbHost: hostname for database machine
      @param dbPort: port for database connection
      """
      DbPostgresql.__init__(self, logger, db=RAD_STORE, user=dbUser, 
                            password=dbKey, host=dbHost, port=dbPort)
            
# .............................................................................
# Public functions
# .............................................................................

# ...............................................
# Processes
# ...............................................

   def _moveJobData(self, job):
      if job.dataObjType == ReferenceType.Bucket:
         dbfunction = 'lm_moveBucket'
      else:
         dbfunction = 'lm_movePamSum'
      success = self.executeModifyFunction(dbfunction, 
                                           job.dataObj.getId(),
                                           job.dataObj.status, 
                                           job.dataObj.statusModTime, 
                                           job.dataObj.stage, 
                                           job.dataObj.stageModTime)
      if not success:
         raise LMError('Unable to move Job.dataObject')

# ...............................................
   def getJob(self, jobId):
      try:
         row, idxs = self.executeSelectOneFunction('lm_getJobType', jobId)
      except:
         raise LMError('Job %s not found' % str(jobId))
      objtype = row[0]
      if objtype == ReferenceType.Bucket:
         fnName = 'lm_getIntJob'
      elif objtype in (ReferenceType.OriginalPamSum,
                       ReferenceType.RandomPamSum):
         fnName = 'lm_getMtxJob'
      else:
         raise LMError('Unknown Job object type %s' % str(objtype))   
      row, idxs = self.executeSelectOneFunction(fnName, jobId)
      job = self._createRADJobNew(row, idxs)
      return job
   
# ...............................................
   def insertJobNew(self, job):
      """
      @summary: Initialize a RADJob.  If it is a:
                   RADBuildGridJob: add new shapegrid object and job
                   RADIntersectJob or RADCalculateJob: add job
                   RADCompressJob: add new or update PamSum if original, 
                                   then add job
                   RADSwapJob or RADSplotchJob: add new PamSum, add job
      @param job: RADJobNew
      @todo: use job object status to move data object? are they both uptodate? 
      """
      newPSId = None
      if job.jobData.processType == ProcessType.RAD_BUILDGRID:
         upShp = self.insertShapeGrid(job.outputObj, job.jobData.cutoutWKT)
         job.resetOutputObject(upShp)
      # New Random or Original PS
      elif (ProcessType.isRandom(job.jobData.processType) or
          (job.outputObjType == ReferenceType.OriginalPamSum and 
           job.jobData.processType == ProcessType.RAD_COMPRESS) ):
         newPSId = self.insertPamSum(job.outputObj, job.bucketId)
         job.outputObj.setId(newPSId)
         
      updatedJob = self._insertJob(job)
      return updatedJob

# ...............................................
   def _insertJob(self, job):
      doNotify = (job.email is not None)
      jobid  = self.executeInsertFunction('lm_insertJob', job.jobFamily,
                                          job.processType,
                                          job.inputType,
                                          job.outputObjType, 
                                          job.outputObj.getId(),
                                          # Compute resource not yet chosen
                                          None,
                                          doNotify,
                                          job.priority,
                                          job.status, 
                                          job.stage, 
                                          job.createTime,
                                          JobStatus.COMPLETE)
      if jobid > 0:
         job.setId(jobid)
      else:
         raise LMError(currargs='Unable to insertJob')
      return job

# ...............................................
   def getUserIdForObjId(self, radexpId=None, bucketId=None, pamsumId=None, shapegridId=None):
      """
      """
      usr = None
      row, idxs = self.executeSelectOneFunction('lm_getUserId', radexpId, 
                                                bucketId, pamsumId, shapegridId)
      if row is not None:
         usr = self._getColumnValue(row, idxs, ['userid'])
      return usr

# ...............................................
# ShapeGrids
# ...............................................
   def insertShapeGrid(self, shpgrd, cutout):
      """
      @summary: Insert a ShapeGrid into the RAD database
      @param shpgrd: The ShapeGrid to insert
      @postcondition: The RAD database contains a new record with shpgrd 
                   attributes.  The shpgrd has _dbId, _dlocation, and 
                   metadataUrl populated.
      @note: findShapeGrids should be executed first to ensure that the
             user and shapename combination are unique.
      @raise LMError: on failure to insert or update the database record. 
      """
      shpgrd.modTime = mx.DateTime.utc().mjd
      metalocation = None
      usr = shpgrd.getUserId()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_insertShapeGrid',
                                      usr,
                                      shpgrd.cellsides,
                                      shpgrd.cellsize,
                                      shpgrd.size,
                                      shpgrd.siteId,
                                      shpgrd.siteX, shpgrd.siteY,
                                      shpgrd.name,
                                      shpgrd.title,
                                      shpgrd.description,
                                      shpgrd.getDLocation(),
                                      shpgrd.ogrType,
                                      shpgrd.dataFormat,
                                      shpgrd.epsgcode,
                                      shpgrd.mapUnits,
                                      shpgrd.metadataUrl,
                                      shpgrd.modTime,
                                      shpgrd.getCSVExtentString(),
                                      shpgrd.getWktExtentString(),
                                      shpgrd.metadataUrl)
      shpgrd = self._createShapeGrid(row, idxs)
      
      #     ######################
      #     ######################
      #     #                    #
      #  ############################
      #  This needs to be done in a job
      #shpgrd.buildShape(cutout=cutout, overwrite=True)
      
      
      if (shpgrd is None or shpgrd.getParametersId() == -1):
         raise LMError(currargs='Failed to insert shapegrid for user %s' 
                       % usr, doTrace=True)
      return shpgrd
   
# ...............................................
   def findShapeGrids(self, shpgrd):
      """
      @summary Return shapegrids matching userid  AND
                 * shapegrid name OR 
                 * cellsides, cellsize, mapunits, vectorsize, 
                    epsgcode, bbox, and (if provided) xsize and ysize.
      @param shpgrds: A list of matching ShapeGrid objects
      @return: A list of ShapeGrids matching the parameters listed in summary.
      """
      shapegrids = []
      rows, idxs = self.executeSelectManyFunction('lm_findShapeGrids',
                                                shpgrd.getUserId(),
                                                shpgrd.name,
                                                shpgrd.cellsides, 
                                                shpgrd.cellsize,
                                                shpgrd.xsize,
                                                shpgrd.ysize,
                                                shpgrd.size,
                                                shpgrd.epsgcode,
                                                shpgrd.mapUnits,
                                                shpgrd.getCSVExtentString())
      for r in rows:
         sg = self._createShapeGrid(r, idxs)
         shapegrids.append(sg)
      return shapegrids

# ...............................................
   def listShapegrids(self, firstRecNum, maxCount, userId, beforeTime, afterTime, 
                      epsg, layerId, layerName, cellsides, atom):
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listShapegrids', 
                                                     firstRecNum, maxCount, userId, 
                                                     beforeTime, afterTime, epsg,
                                                     layerId, layerName, cellsides)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listShapegridObjects', 
                                                     firstRecNum, maxCount, userId, 
                                                     beforeTime, afterTime, epsg,
                                                     layerId, layerName, cellsides)
         for r in rows:
            objs.append(self._createShapeGrid(r, idxs))
      return objs
   
# ...............................................
   def countShapegrids(self, userId, beforeTime, afterTime, epsg, lyrid, lyrname,
                       cellsides):
      row, idxs = self.executeSelectOneFunction('lm_countShapegrids', userId, 
                                                beforeTime, afterTime, epsg,
                                                lyrid, lyrname, cellsides)
      return self._getCount(row)
   
# ...............................................
   def getShapeGrid(self, lyrid, usr):
      row, idxs = self.executeSelectOneFunction('lm_getShapeGridByLayerid', usr, 
                                                lyrid)
      shpgrd = self._createShapeGrid(row, idxs)
      return shpgrd

# ...............................................
   def getShapeGridByShpId(self, shgid, usr):
      row, idxs = self.executeSelectOneFunction('lm_getShapeGrid', usr, shgid)
      shpgrd = self._createShapeGrid(row, idxs)
      return shpgrd

# ...............................................
   def getShapeGridByName(self, shpname, usr):
      row, idxs = self.executeSelectOneFunction('lm_getShapeGridByName', usr, 
                                                shpname)
      shpgrd = self._createShapeGrid(row, idxs)
      return shpgrd

# ...............................................
   def renameShapeGrid(self, shpgrd, newshpname):
      success = False
      usr = shpgrd.getUserId()
      existshpgrd = self.getShapeGridByName(usr, newshpname)
      if existshpgrd is not None:
         self.log.warning('Shapegrid %s already exists for user %s with id %d' %
                          (newshpname, usr, existshpgrd.getParametersId()))
      else:
         success = self.executeModifyFunction('lm_renameShapeGrid', 
                                              shpgrd.getParametersId(), usr, newshpname)
         shpgrd.name = newshpname
      return success

# ...............................................
   def deleteShapeGrid(self, shpgrdid):
      """
      @note: uses parameterId (shapegridId)
      """
      success = self.executeModifyFunction('lm_deleteShapeGrid', shpgrdid)
      return success
      
# ...............................................
# RADExperiments
# ...............................................
   def listExperiments(self, firstRecNum, maxNum,
                  usrid, beforetime, aftertime, epsg, expname, atom):
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listExperiments', 
                                                     firstRecNum, maxNum, 
                                                     usrid, beforetime, aftertime, 
                                                     epsg, expname)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listExperimentObjects', 
                                                     firstRecNum, maxNum, 
                                                     usrid, beforetime, aftertime, 
                                                     epsg, expname)
         for r in rows:
            objs.append(self._createRADExperiment(r, idxs))
      return objs
   
# .............................................................................
   def countExperiments(self, usrid, beforetime, aftertime, epsg, expname):
      row, idxs = self.executeSelectOneFunction('lm_countExperiments', usrid, 
                                                beforetime, aftertime, epsg, 
                                                expname)
      return self._getCount(row)

# ...............................................
   def insertExperiment(self, radexp):
      currtime = mx.DateTime.utc().mjd
      expid = self.executeInsertFunction('lm_insertExperiment', 
                                         radexp.getUserId(),
                                         radexp.name,
                                         radexp.attrMatrixDLocation,
                                         radexp.attrTreeDLocation,
                                         radexp.description,
                                         radexp.email,
                                         radexp.epsgcode,
                                         ','.join(radexp.keywords),
                                         currtime,
                                         radexp.metadataUrl)
      radexp.setId(expid)
      for i in range(len(radexp.bucketList)):
         newexistingBucket = self.insertBucket(radexp.bucketList[i], expid)
         radexp.bucketList[i] = newexistingBucket 
      return expid
   
# ...............................................
   def updateExperiment(self, radexp):
      success = self.executeModifyReturnValue('lm_updateExperimentInfo', 
                                              radexp.getId(), 
                                              radexp.attrMatrixDLocation,
                                              radexp.attrTreeDLocation,
                                              radexp.description,
                                              radexp.email,
                                              ','.join(radexp.keywords),
                                              mx.DateTime.utc().mjd)
      return success
   
# ...............................................
   def deleteExperiment(self, radexpid):
      success = self.executeModifyFunction('lm_deleteExperiment', radexpid)
      return success

# ...............................................
   def getExperiment(self, usr, expid):
      row, idxs = self.executeSelectOneFunction('lm_getExperiment', expid, usr)
      radExp = self._createRADExperiment(row, idxs)
      if radExp is not None:
         bktList = self.getBuckets(usr, expid)
         for bkt in bktList:
            radExp.addBucket(bkt)
      return radExp
   
# ...............................................
   def getExperimentByName(self, usr, expname):
      row, idxs = self.executeSelectOneFunction('lm_getExperimentByName', 
                                                expname, usr)
      radExp = self._createRADExperiment(row, idxs)
      if radExp is not None:
         bktList = self.getBuckets(usr, radExp.getId())
         for bkt in bktList:
            radExp.addBucket(bkt)
      return radExp

# ...............................................
   def findExperimentsByShapeGrid(self, usr, shpgridid):
      exps = []
      rows, idxs = self.executeSelectManyFunction('lm_findExperimentsByShapeGrid',
                                                  usr, shpgridid)
      for r in rows:
         exp = self._createRADExperiment(r, idxs)
         bktList = self.getBuckets(usr, exp.getId())
         for bkt in bktList:
            exp.addBucket(bkt)
         exps.append(exp)
      return exps
   
# ...............................................
   def findExperimentsForUser(self, usr):
      exps = []
      rows, idxs = self.executeSelectManyFunction('lm_findExperimentsByUser', usr)
      for r in rows:
         exp = self._createRADExperiment(r, idxs)
         bktList = self.getBuckets(usr, exp.getId())
         for bkt in bktList:
            exp.addBucket(bkt)
         exps.append(exp)
      return exps

# ...............................................
# RADBuckets
# ...............................................
   def listBuckets(self, firstRecNum, maxNum, usrid, beforetime, aftertime, 
                   epsg, expid, expname, shpid, shpname, atom):
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listBuckets', 
                                                     firstRecNum, maxNum, 
                                                     usrid, beforetime, aftertime, 
                                                     epsg, expid, expname, shpid, 
                                                     shpname)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []         
         rows, idxs = self.executeSelectManyFunction('lm_listBucketObjects', 
                                                     firstRecNum, maxNum, 
                                                     usrid, beforetime, aftertime, 
                                                     epsg, expid, expname, shpid, 
                                                     shpname)
         for r in rows:
            objs.append(self._createBucket(r, idxs))
      return objs
   
# .............................................................................
   def countBuckets(self, usrid, beforetime, aftertime, epsg, expid, expname, shpid, shpname):
      row, idxs = self.executeSelectOneFunction('lm_countBuckets', usrid, 
                                                beforetime, aftertime, epsg, 
                                                expid, expname, shpid, shpname)
      return self._getCount(row)

# ...............................................
   def insertBucket(self, bucket, expid):
      currtime = mx.DateTime.utc().mjd
      if bucket.createTime is None:
         bucket.createTime = currtime
      if bucket.status is None:
         bucket.updateStatus(JobStatus.GENERAL, modTime=currtime, stage=JobStage.GENERAL)

      if bucket.shapegrid.getParametersId() is None:
         newexistingShapegrid = self.insertShapeGrid(bucket.shapegrid)
         bucket.shapegrid = newexistingShapegrid 
      row, idxs = self.executeInsertAndSelectOneFunction('lm_insertBucket', 
                                         expid, bucket.shapegrid.getParametersId(),
                                         bucket.status,
                                         bucket.statusModTime,
                                         bucket.stage,
                                         bucket.stageModTime, 
                                         ','.join(bucket.keywords),
                                         bucket.createTime,
                                         bucket.metadataUrl)
      newexistingBucket = self._createBucket(row, idxs)
      if newexistingBucket.pamDLocation is None:
         success = self.updateBucketInfo(newexistingBucket, None)
      return newexistingBucket
   
# ...............................................
   def updateBucketInfo(self, bucket, computeId):
      success = self.executeModifyFunction('lm_updateBucketInfo', 
                                           bucket.getId(),
                                           bucket.indicesDLocation,
                                           bucket.pamDLocation,
                                           bucket.grimDLocation,
                                           bucket.metadataUrl,
                                           bucket.status,
                                           bucket.statusModTime,
                                           bucket.stage,
                                           bucket.stageModTime,
                                           computeId)
      return success

# ...............................................
   def deleteBucket(self, bktid):
      # Also deletes associated jobs
      if success:
         success = self.executeModifyFunction('lm_deleteBucket', bktid)
      return success
      
# ...............................................
   def getExperimentWithOneBucket(self, bktid):
      row, idxs = self.executeSelectOneFunction('lm_getBucket', bktid)
      exp = self._createRADExperimentWithBucket(row, idxs)
      return exp
      
# ...............................................
   def getBucket(self, bktid):
      row, idxs = self.executeSelectOneFunction('lm_getBucket', bktid)
      bkt = self._createBucket(row, idxs)
      return bkt
   
# ...............................................
   def getBucketByShape(self, expid, shpName=None, shpId=None):
      row, idxs = self.executeSelectOneFunction('lm_getBucketByShape', 
                                                expid, shpName, shpId)
      bkt = self._createRADExperimentWithBucket(row, idxs)
      return bkt
   
# ...............................................
   def getBuckets(self, usr, expid):
      bktList = []
      rows, idxs = self.executeSelectManyFunction('lm_getExperimentBuckets', 
                                                  expid, None, usr)
      for r in rows:
         bkt = self._createBucket(r, idxs)
         bktList.append(bkt)
      return bktList
   
# ...............................................
   def getExperimentWithAllBuckets(self, usr, expid, expname):
      """
      @note: this does not fill the random PamSums
      """
      radExp = None
      rows, idxs = self.executeSelectManyFunction('lm_getExperimentBuckets', 
                                                 expid, expname, usr)
      for r in rows:
         if radExp is None:
            radExp = self._createRADExperiment(r, idxs)
         bck = self._createBucket(r, idxs)
         radExp.addBucket(bck)
      if radExp is None:
         radExp = self.getExperimentWithNoBuckets(usr, expid, expname)
      return radExp
   
# ...............................................
   def getExperimentWithNoBuckets(self, usr, expid, expname):
      if expid is not None:
         row, idxs = self.executeSelectOneFunction('lm_getExperiment', 
                                                     expid, usr)
      else:
         row, idxs = self.executeSelectOneFunction('lm_getExperimentByName', 
                                                     expname, usr)
      radExp = self._createRADExperiment(row, idxs)
      return radExp
# ...............................................
   def getRandomPamSums(self, bucketid):
      """
      @note: Returns PamSum rows
      """
      randompss = []
      rows, idxs = self.executeSelectManyFunction('lm_getRandomPamSumsForBucket',
                                                  bucketid)
      for r in rows:
         rpamsum = self._createPamSum(r, idxs)
         randompss.append(rpamsum)
      return randompss
   
# ...............................................
# Any RAD Layers
# ...............................................
   def insertPamSum(self, pamsum, bucketid):
      currtime = mx.DateTime.gmt().mjd
      if pamsum.createTime is None:
         pamsum.createTime = currtime
      if pamsum.status is None:
         pamsum.updateStatus(JobStatus.GENERAL, modTime=currtime, stage=JobStage.GENERAL)
      psid = self.executeInsertFunction('lm_insertPamSum', 
                                         bucketid, pamsum.randomMethod,
                                         pamsum.dumpRandomParametersAsString(),
                                         pamsum.status,
                                         pamsum.statusModTime,
                                         pamsum.stage,
                                         pamsum.stageModTime,
                                         pamsum.createTime,
                                         pamsum.metadataUrl)
      pamsum.setId(psid)
      return psid

# ...............................................
   def updatePamSumInfo(self, pamsum, computeId):
      success = self.executeModifyFunction('lm_updatePamsumInfo', 
                                           pamsum.getId(),
                                           pamsum.pamDLocation,
                                           pamsum.sumDLocation,
                                           pamsum.splotchPamDLocation,
                                           pamsum.splotchSitesDLocation,
                                           pamsum.metadataUrl,
                                           pamsum.status,
                                           pamsum.statusModTime,
                                           pamsum.stage,
                                           pamsum.stageModTime,
                                           computeId)
      return success

# ...............................................
   def updatePamSumStatus(self, pamsum, computeId):
      success = self.executeModifyFunction('lm_updatePamsumStatus', 
                                           pamsum.getId(),
                                           pamsum.status,
                                           pamsum.statusModTime,
                                           pamsum.stage,
                                           pamsum.stageModTime,
                                           computeId)
      return success

# ...............................................
   def getOriginalPamSumForBucket(self, bucketid):
      """
      @note: Returns PamSum row
      """
      row, idxs = self.executeSelectOneFunction('lm_getOriginalPamSumForBucket',
                                                  bucketid)
      
      pamsum = self._createPamSum(row, idxs)
      return pamsum

# ...............................................
   def getPamSum(self, pamsumid):
      """
      @note: Returns PamSum row
      """
      row, idxs = self.executeSelectOneFunction('lm_getPamSum', pamsumid)
      
      pamsum = self._createPamSum(row, idxs)
      return pamsum
   
# ...............................................
   def getPamSumWithBucketAndExperiment(self, pamsumid):
      """
      @summary: Return RADExperiment with original PamSum or (one) requested
                randomized PamSum
      """
      row, idxs = self.executeSelectOneFunction('lm_getPamSum', pamsumid)
      pamsum = self._createPamSum(row, idxs)
      exp = self.getExperimentWithOneBucket(row[idxs['bucketid']])
      # If this is original PamSum, it came with the experiment
      if pamsum.randomMethod != RandomizeMethods.NOT_RANDOM:
         exp.bucketList[0].addRandomPamSum(randomPamSum=pamsum)
      return exp
   
# ...............................................
   def deletePamSum(self, psid):
      # Also deletes associated jobs
      if success:
         success = self.executeModifyFunction('lm_deletePamSum', psid)
      return success

# .............................................................................
   def listPamSums(self, firstRecNum, maxNum, usrid, beforetime, aftertime, 
                   epsg, expid, bucketid, isRandomized, randomMethod, atom):

      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listPamSums', 
                                                     firstRecNum, maxNum, 
                                                     usrid, beforetime, aftertime, 
                                                     epsg, expid, bucketid, 
                                                     isRandomized, randomMethod)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listPamSumObjects', 
                                                     firstRecNum, maxNum, 
                                                     usrid, beforetime, aftertime, 
                                                     epsg, expid, bucketid, 
                                                     isRandomized, randomMethod)
         for r in rows:
            objs.append(self._createPamSum(r, idxs))
      return objs
   
# .............................................................................
   def countPamSums(self, usrid, beforetime, aftertime, epsg, expid, bucketid, 
                    isRandomized, randomMethod):
      row, idxs = self.executeSelectOneFunction('lm_countPamSums', usrid, 
                                                beforetime, aftertime, epsg,
                                                expid, bucketid, isRandomized, 
                                                randomMethod)
      return self._getCount(row)

# ...............................................
# Any RAD Layers
# ...............................................
   def listLayers(self, firstRecNum, maxCount, userId, beforeTime, afterTime, 
                  epsg, layerId, layerName, atom):
      if layerName is not None:
         layerName = layerName.strip()
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listLayers', 
                                                     firstRecNum, maxCount, userId, 
                                                     beforeTime, afterTime, epsg,
                                                     layerId, layerName)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = [] 
         rows, idxs = self.executeSelectManyFunction('lm_listLayerObjects', 
                                                     firstRecNum, maxCount, userId, 
                                                     beforeTime, afterTime, epsg,
                                                     layerId, layerName)
         for r in rows:
            objs.append(self._createLayer(r, idxs))
      return objs
   
# ...............................................
   def countLayers(self, userId, beforeTime, afterTime, epsg, lyrid, lyrname):
      row, idxs = self.executeSelectOneFunction('lm_countLayers', userId, 
                                                beforeTime, afterTime, epsg,
                                                lyrid, lyrname)
      return self._getCount(row)
   
# ...............................................
   def getBaseLayer(self, lyrid):
      row, idxs = self.executeSelectOneFunction('lm_getLayer', lyrid)
      lyr = self._createLayer(row, idxs)
      return lyr

# ...............................................
   def deleteBaseLayer(self, usr, lyrid):
      success = self.executeModifyFunction('lm_deleteOrphanedLayer', usr, lyrid)
      return success

# ...............................................
   def findLayer(self, lyr):
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findLayer', 
                                                          lyr.getLayerUserId(),
                                                          lyr.name,
                                                          lyr.epsgcode)
      updatedLyr = self._createLayer(row, idxs)
      return updatedLyr

# ...............................................
   def insertBaseLayer(self, lyr):
      currtime=mx.DateTime.gmt().mjd
      lyr.createTime = currtime
      lyr.modTime = currtime
      rtype, vtype = self._getSpatialLayerType(lyr)
      if rtype is None and vtype is None:
         raise LMError('GDAL or OGR data type must be provided')
      lyrid = self.executeInsertFunction('lm_insertLayer', 
                                          lyr.getLayerUserId(),
                                          lyr.name,
                                          lyr.title,
                                          lyr.description,
                                          lyr.getDLocation(),
#                                           lyr.mapPrefix,
                                          vtype,
                                          rtype,
                                          lyr.dataFormat,
                                          lyr.epsgcode,
                                          lyr.mapUnits,
                                          lyr.resolution,
                                          lyr.startDate,
                                          lyr.endDate,
                                          lyr._metalocation,
                                          lyr.createTime,
                                          lyr.modTime,
                                          lyr.getCSVExtentString(),
                                          lyr.getWktExtentString(),
                                          lyr.metadataUrl)
      if lyrid != -1:
         lyr.setLayerId(lyrid)
         updatedLyr = lyr
      else:
         raise LMError(currargs='Error on adding Layer object (Command: %s)' % 
                       str(self.lastCommands))
      return updatedLyr

# ...............................................
   def getPALayer(self, palyrid):
       row, idxs = self.executeSelectOneFunction('lm_getPALayerById', palyrid) 
       lyr = self._createPALayer(row, idxs)
       return lyr
 
# ...............................................
   def getAncLayer(self, anclyrid):
       row, idxs = self.executeSelectOneFunction('lm_getAncLayerById', anclyrid)
       lyr = self._createAncLayer(row, idxs)
       return lyr
 
# ...............................................
   def findExistingRADLayers(self, lyr, firstrec, maxcount, beforetime, aftertime):
      lyrs = []
      if isinstance(lyr, _AncillaryValue):
         lyrs = self.findAncillaryLayers(lyr, firstrec, maxcount, 
                                         beforetime, aftertime)
      elif isinstance(lyr, _PresenceAbsence):
         lyrs = self.findPresenceAbsenceLayers(lyr, firstrec, maxcount, 
                                               beforetime, aftertime)
      else:
         self.log.error('Implement get generic layer?')
         raise LMError(currargs='Layer must be AncillaryValueLayer or PresenceAbsenceLayer')
      return lyrs
         
# ...............................................
   def updateLayer(self, lyr):
      rtype, vtype = self._getSpatialLayerType(lyr)
      success = self.executeModifyFunction('lm_updateLayer',
                                      lyr.getLayerId(),
                                      lyr.getLayerUserId(),
                                      lyr.title,
                                      lyr.description,
                                      lyr.getDLocation(),
                                      lyr.epsgcode,
                                      vtype,
                                      rtype,
                                      lyr.mapUnits,
                                      lyr.resolution,
                                      lyr.startDate,
                                      lyr.endDate,
                                      lyr._metalocation,
                                      lyr.modTime,
                                      lyr.getCSVExtentString(),
                                      lyr.getWktExtentString())
      return success
# ...............................................
   def renameLayer(self, lyr, newname):
      success = self.executeModifyFunction('lm_renameLayer', lyr.getLayerId(),
                                           lyr.getLayerUserId(), newname)
      if success:
         lyr.name = newname
      else:
         self.log.info('PresenceAbsenceLayer %s already exists for user %s' %
                       (newname, lyr.getLayerUserId()))
      return success
         
# ...............................................
# AncillaryLayers
# ...............................................
   def listAncillaryLayers(self, firstRecNum, maxCount, userId, beforeTime, 
                           afterTime, epsg, layerId, layerName, ancId, expId, 
                           atom):
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listAncLayers', 
                                                     firstRecNum, maxCount, userId, 
                                                     beforeTime, afterTime, epsg,
                                                     layerId, layerName, ancId,
                                                     expId)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listAncLayerObjects', 
                                                     firstRecNum, maxCount, userId, 
                                                     beforeTime, afterTime, epsg,
                                                     layerId, layerName, ancId,
                                                     expId)
         for r in rows:
            objs.append(self._createAncLayer(r, idxs))
      return objs
   
# ...............................................
   def countAncillaryLayers(self, userId, beforeTime, afterTime, epsg, 
                            layerId, layerName, ancId, expId):
      row, idxs = self.executeSelectOneFunction('lm_countAncLayers', userId, 
                                                beforeTime, afterTime, epsg, 
                                                layerId, layerName, ancId, expId)
      return self._getCount(row)
   
# ...............................................
   def findAncillaryLayers(self, lyr, firstrec, maxcount, beforetime, aftertime):
      lyrs = []
      rtype, vtype = self._getSpatialLayerType(lyr)
      rows, idxs = self.executeSelectManyFunction('lm_findAncLayers', 
                                                  firstrec, maxcount,
                                                  lyr.name,
                                                  lyr.title,
                                                  vtype,
                                                  rtype,
                                                  lyr.epsgcode,
                                                  lyr.mapUnits,
                                                  lyr.resolution,
                                                  lyr.startDate,
                                                  lyr.endDate,
                                                  lyr.getCSVExtentString(),
                                                  lyr.getUserId(),
                                                  lyr.attrValue,
                                                  lyr.weightedMean,
                                                  lyr.largestClass,
                                                  lyr.minPercent, 
                                                  beforetime, 
                                                  aftertime)
      for row in rows:
         lyr = self._createAncLayer(row, idxs)
         lyrs.append(lyr)
      return lyrs
   
# ...............................................
   def findOrInsertAncillaryValues(self, anclyr):
      ancid = self.executeSelectManyFunction('lm_findOrInsertAncValues',
                                                  anclyr.getUserId(),
                                                  anclyr.attrValue,
                                                  anclyr.weightedMean,
                                                  anclyr.largestClass,
                                                  anclyr.minPercent)
      return ancid

# ...............................................      
   def findOrInsertPresenceAbsenceValues(self, palyr):
      paid = self.executeSelectManyFunction('lm_findOrInsertPAValues',
                                            palyr.getUserId(),
                                            palyr.attrPresence,
                                            palyr.minPresence,
                                            palyr.maxPresence,
                                            palyr.percentPresence,
                                            palyr.attrAbsence,
                                            palyr.minAbsence,
                                            palyr.maxAbsence,
                                            palyr.percentAbsence)
      return paid

# ...............................................
   def getAncillaryLayersForExperiment(self, expid):
      anclyrs = []
      rows, idxs = self.executeSelectManyFunction('lm_getAncLayersForExperiment', 
                                                  expid)
      for r in rows:
         lyr = self._createAncLayer(r, idxs)
         anclyrs.append(lyr)
      return anclyrs

# ...............................................
   def _fillExperimentWithAncillaryLayers(self, exp):
      lyrs = self.getAncillaryLayersForExperiment(exp.getId())
      exp.setEnvLayerset(lyrs)

# ...............................................
   def findAncillaryLayersForUser(self, usrid, lyrnameorid=None):
      anclyrs = [] 
      if lyrnameorid is None:
         rows, idxs = self.executeSelectManyFunction('lm_getAncLayersForUser', 
                                                     usrid)
      else:
         if isinstance(lyrnameorid, IntType):
            functionname = 'lm_getAncLayersForUserAndLayerid'
         elif isinstance(lyrnameorid, (StringType, UnicodeType)):
            functionname = 'lm_getAncLayersForUserAndLayername'
         rows, idxs = self.executeSelectManyFunction(functionname, 
                                                  usrid, lyrnameorid)
         
      for r in rows:
         lyr = self._createAncLayer(r, idxs)
         anclyrs.append(lyr)
      return anclyrs
   
# ...............................................
   def insertAncillaryValues(self, anclyr):
      ancvalid = self.executeInsertFunction('lm_insertAncValues',
                                             anclyr.getUserId(),
                                             anclyr.attrValue,
                                             anclyr.weightedMean,
                                             anclyr.largestClass,
                                             anclyr.minPercent)
      anclyr.setParametersId(ancvalid)
      return anclyr

# ...............................................
   def insertOrUpdateLayer(self, paramlyr):
      isNewLyr = False
      updatedLyr = self.findLayer(paramlyr)
      if updatedLyr is None:
         isNewLyr = True
         updatedLyr = self.insertBaseLayer(paramlyr)
      return updatedLyr, isNewLyr

# ...............................................
   def insertAncillaryLayer(self, anclyr, expid):
      """
      @note: Method returns a new object in case one or more records (layer or
             parameter values) are present in the database for this user.
      """
      updatedlyr = None
      currtime=mx.DateTime.gmt().mjd
      anclyr.createTime = currtime
      anclyr.modTime = currtime
      rtype, vtype = self._getSpatialLayerType(anclyr)
      if rtype is None:
         if vtype is not None:
            anclyr.verifyField(anclyr.getDLocation(), anclyr.dataFormat, anclyr.attrValue)
         else:
            raise LMError('GDAL or OGR data type must be provided')
      # Returns a lm_anclayer row with LayerId, AncillaryId, experimentAncLayerId.    
      row, idxs = self.executeInsertAndSelectOneFunction('lm_insertAncLayer', 
                                         anclyr.getUserId(),
                                         expid,
                                         anclyr.name,
                                         anclyr.title,
                                         anclyr.description,
                                         anclyr.getDLocation(),
                                         vtype,
                                         rtype,
                                         anclyr.dataFormat,
                                         anclyr.epsgcode,
                                         anclyr.mapUnits,
                                         anclyr.resolution,
                                         anclyr.startDate,
                                         anclyr.endDate,
                                         anclyr._metalocation,
                                         anclyr.createTime,
                                         anclyr.modTime,
                                         anclyr.getCSVExtentString(),
                                         anclyr.getWktExtentString(),
                                         anclyr.attrValue,
                                         anclyr.weightedMean,
                                         anclyr.largestClass,
                                         anclyr.minPercent,
                                         anclyr.metadataUrl)
      if not(row[idxs['layerid']] == -1 
             or row[idxs['ancillaryvalueid']] == -1 
             or row[idxs['experimentanclayerid']] == -1):
         updatedlyr = self._createAncLayer(row, idxs)
      else:
         raise LMError(currargs='Error on adding Layer object (Command: %s)' % 
                       str(self.lastCommands))
      return updatedlyr

# ...............................................
   def addAncillaryLayerToExperiment(self, anclyr, expid):
      matrixidx = self.executeModifyReturnValue('lm_addAncLayerToExperiment', 
                        anclyr.getId(), anclyr.getAncillaryId(), expid,
                        anclyr.getUserId(), ARCHIVE_USER)
      anclyr.setMatrixIndex(matrixidx)
      return anclyr


# ...............................................
   def deleteAncillaryLayer(self, anclyr, expid):
      success = self.executeModifyFunction('lm_deleteAncLayerFromExperiment',
                        anclyr.getUserId(), expid, anclyr.getId(), 
                        anclyr.getParametersId())
      return success
   
# ...............................................
   def updateAncillaryLayer(self, anclyr, expid):
      success = False
      newAncId = self.executeModifyReturnValue('lm_updateAncLayerForExperiment',
                  expid, anclyr.getId(), anclyr.getUserId(), 
                  anclyr.getParametersId(), anclyr.attrValue,
                  anclyr.weightedMean, anclyr.largestClass, anclyr.minPercent)
      if newAncId != -1: 
         anclyr.setParametersId(newAncId)
         success = True
      return success
   
# ...............................................
   def countLayersForAncillary(self, avid, usr):
      row, idxs = self.executeSelectOneFunction('lm_countLayersForAncillaryVals',
                                                usr, avid)
      count = row[0]
      return count
   
# ...............................................
   def getLayersForAncillary(self, avid, usr):
      anclyrs = []
      rows, idxs = self.executeSelectManyFunction('lm_getAncLayersForUserAndAncid',
                                                  usr, avid)
      for r in rows:
         lyr = self._createAncLayer(r, idxs)
         anclyrs.append(lyr)
      return anclyrs   

# ...............................................
# PresenceAbsenceLayers
# ...............................................
   def listPresenceAbsenceLayers(self, firstRecNum, maxCount, userId, beforeTime, 
                                 afterTime, epsg, layerId, layerName, paId, 
                                 expId, atom):
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listPALayers', 
                                                     firstRecNum, maxCount, userId, 
                                                     beforeTime, afterTime, epsg,
                                                     layerId, layerName, paId, 
                                                     expId)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listPALayerObjects', 
                                                     firstRecNum, maxCount, userId, 
                                                     beforeTime, afterTime, epsg,
                                                     layerId, layerName, paId, 
                                                     expId)
         for r in rows:
            objs.append(self._createPALayer(r, idxs))
      return objs
   
# ...............................................
   def countPresenceAbsenceLayers(self, userId, beforeTime, afterTime, 
                                  epsg, layerId, layerName, paId, expId):
      row, idxs = self.executeSelectOneFunction('lm_countPALayers', userId, 
                                                beforeTime, afterTime, epsg,
                                                layerId, layerName, paId, expId)
      return self._getCount(row)

# ...............................................
   def getPresenceAbsenceLayersForExperiment(self, expid):
      lyrs = []
      rows, idxs = self.executeSelectManyFunction('lm_getPALayersForExperiment', 
                                                  expid)
      for r in rows:
         lyr = self._createPALayer(r, idxs)
         lyrs.append(lyr)
      return lyrs

# ...............................................
   def _fillExperimentWithPresenceAbsenceLayers(self, exp):
      lyrs = self.getPresenceAbsenceLayersForExperiment(exp.getId())
      exp.setOrgLayerset(lyrs)
   
# ...............................................
   def _getPresenceAbsenceLayerset(self, expid):
      lyrset = None
      lyrlist = self.getPresenceAbsenceLayersForExperiment(expid)
      if len(lyrlist) > 0:
         if not(isinstance(lyrlist[0], _PresenceAbsence)):
            raise LMError(currargs='Layers are not type _PresenceAbsence')
         lyrset = MatrixLayerset(None, layers=lyrlist)
      return lyrset
      
# ...............................................
   def _getAncillaryLayerset(self, expid):
      lyrset = None
      lyrlist = self.getAncillaryLayersForExperiment(expid)
      if len(lyrlist) > 0:
         if not(isinstance(lyrlist[0], _AncillaryValue)):
            raise LMError(currargs='Layers are not type _AncillaryValue')
         lyrset = MatrixLayerset(None, layers=lyrlist)
      return lyrset

# ...............................................      
   def findPresenceAbsenceLayers(self, lyr, firstrec, maxcount, beforetime, aftertime):
      lyrs = []
      rtype, vtype = self._getSpatialLayerType(lyr)
      rows, idxs = self.executeSelectManyFunction('lm_findPALayers', 
                                                  firstrec, maxcount,
                                                  lyr.name,
                                                  lyr.title,
                                                  vtype,
                                                  rtype,
                                                  lyr.epsgcode,
                                                  lyr.mapUnits,
                                                  lyr.resolution,
                                                  lyr.startDate,
                                                  lyr.endDate,
                                                  lyr.getCSVExtentString(),
                                                  lyr.getUserId(),
                                                  lyr.attrPresence,
                                                  lyr.minPresence,
                                                  lyr.maxPresence,
                                                  lyr.percentPresence,
                                                  lyr.attrAbsence,
                                                  lyr.minAbsence,
                                                  lyr.maxAbsence,
                                                  lyr.percentAbsence,
                                                  beforetime, 
                                                  aftertime)
      for row in rows:
         lyr = self._createPALayer(row, idxs)
         lyrs.append(lyr)
      
      return lyrs
   
# ...............................................      
   def getPresenceAbsenceLayersForUser(self, usrid, lyrnameorid=None):
      """
      @note: Uses lm_palayer, which gets palayers for a user in experiments.
      """
      palyrs = [] 
      if lyrnameorid is None:
         rows, idxs = self.executeSelectManyFunction('lm_getPALayersForUser', usrid)
      else:
         if isinstance(lyrnameorid, IntType):
            functionname = 'lm_getPALayersForUserAndLayerid'
         elif isinstance(lyrnameorid, (StringType, UnicodeType)):
            functionname = 'getPALayersForUserAndLayername'
         
         rows, idxs = self.executeSelectManyFunction(functionname, 
                                                     usrid, lyrnameorid)
      for r in rows:
         lyr = self._createPALayer(r, idxs)
         palyrs.append(lyr)
      return palyrs

# ...............................................
   def getLayers(self, expid, indicesOnly=False, isPresenceAbsence=True):
      layers = []
      if isPresenceAbsence:
         paramfieldname = 'presenceabsenceid'
         rows, idxs = self.executeSelectManyFunction('lm_getPALayersForExperiment', 
                                                     expid)
      else:
         paramfieldname = 'ancillaryvalueid'
         rows, idxs = self.executeSelectManyFunction('lm_getAncLayersForExperiment',
                                                     expid)
         
      if indicesOnly:
         layers = {}
         for r in rows:
            matrixidx = r[idxs['matrixidx']]
            metaurl = r[idxs['metadataurl']]
            paramid =  r[idxs[paramfieldname]]
            layers[matrixidx] = (metaurl, paramid)
            
      elif isPresenceAbsence:
         for r in rows:
            lyr = self._createPALayer(r, idxs)
            layers.append(lyr)
            
      else:
         for r in rows:
            lyr = self._createAncLayer(r, idxs)
            layers.append(lyr)
            
      return layers

# ...............................................
   def insertPresenceAbsenceValues(self, palyr):
      """
      @summary: Insert PresenceAbsence values. Return the updated (or found) record.
      @note: Method returns a new object in case one or more records (layer or
             parameter values) are present in the database for this user.
      """
      paid = self.executeInsertFunction('lm_insertPAValues',
                                         palyr.getUserId(),
                                         palyr.attrPresence,
                                         palyr.minPresence,
                                         palyr.maxPresence,
                                         palyr.percentPresence,
                                         palyr.attrAbsence,
                                         palyr.minAbsence,
                                         palyr.maxAbsence,
                                         palyr.percentAbsence)
      return paid

# ...............................................
   def insertPresenceAbsenceLayer(self, palyr, expid):
      """
      @summary: Insert a Layer, PresenceAbsence values, and a Join record.  
                Return the updated (or found) record.
      @note: Method returns a new object in case one or more records (layer or
             parameter values) are present in the database for this user.
      """
      updatedlyr = None
      currtime=mx.DateTime.gmt().mjd
      palyr.createTime = currtime
      palyr.modTime = currtime
      rtype, vtype = self._getSpatialLayerType(palyr)
      if rtype is None:
         if vtype is not None:
            try:
               prsOk = palyr.verifyField(palyr.getDLocation(), palyr.dataFormat, 
                                          palyr.attrPresence)
            except LMError:
               raise 
            except Exception, e:
               raise LMError(currargs=e.args)
            
            if not prsOk:
               raise LMError('Field %s of Layer %s is not present or the wrong type' 
                             % (palyr.attrPresence, palyr.name))
            if palyr.attrAbsence is not None:
               try:
                  absOk = palyr.verifyField(palyr.getDLocation(), palyr.dataFormat, 
                                  palyr.attrAbsence)
               except LMError:
                  raise 
               except Exception, e:
                  raise LMError(currargs=e.args)

               if not absOk:
                  raise LMError('Field %s of Layer %s is not present or the wrong type' 
                             % (palyr.attrAbsence, palyr.name))
         else:
            raise LMError('GDAL or OGR data type must be provided')

      # SelectOne returns a lm_palayer row with both the new LayerId and the 
      # new PresenceAbsenceId; returns existing record if layer or PAValues
      # are already there
      row,idxs = self.executeInsertAndSelectOneFunction('lm_insertPALayer',
                                      palyr.getUserId(),
                                      expid,
                                      palyr.name,
                                      palyr.title,
                                      palyr.description,
                                      palyr.getDLocation(),
                                      vtype,
                                      rtype,
                                      palyr.dataFormat,
                                      palyr.epsgcode,
                                      palyr.mapUnits,
                                      palyr.resolution,
                                      palyr.startDate,
                                      palyr.endDate,
                                      palyr._metalocation,
                                      palyr.createTime,
                                      palyr.modTime,
                                      palyr.getCSVExtentString(),
                                      palyr.getWktExtentString(),
                                      palyr.attrPresence,
                                      palyr.minPresence,
                                      palyr.maxPresence,
                                      palyr.percentPresence,
                                      palyr.attrAbsence,
                                      palyr.minAbsence,
                                      palyr.maxAbsence,
                                      palyr.percentAbsence,
                                      palyr.metadataUrl)
      if not(row[idxs['layerid']] != -1 or row[idxs['presenceabsenceid']] != -1):
         updatedlyr = self._createPALayer(row, idxs)
      else:
         raise LMError(currargs='Error on adding Layer object (Command: %s)' % 
                       str(self.lastCommands))
      return updatedlyr
   
# ...............................................
   def addPresenceAbsenceLayerToExperiment(self, palyr, expid):
      matrixidx = self.executeModifyReturnValue('lm_addPALayerToExperiment',
                                                palyr.getId(),
                                                palyr.getParametersId(),
                                                expid, 
                                                palyr.getUserId(),
                                                ARCHIVE_USER)
      if matrixidx != -1:
         palyr.setMatrixIndex(matrixidx)
      return palyr
      
# ...............................................
   def deletePresenceAbsenceLayer(self, palyr, expid):
      success = self.executeModifyFunction('lm_deletePALayerFromExperiment',
                                           palyr.getUserId(), expid, 
                                           palyr.getId(), 
                                           palyr.getParametersId())
      return success
   
# ...............................................
   def updatePresenceAbsenceLayer(self, palyr, expid):
      success = False
      newPAId = self.executeModifyReturnValue('lm_updatePALayerForExperiment',
                                              expid, 
                                              palyr.getId(),
                                              palyr.getUserId(),
                                              palyr.getParametersId(),
                                              palyr.attrPresence,
                                              palyr.minPresence,
                                              palyr.maxPresence,
                                              palyr.percentPresence,
                                              palyr.attrAbsence,
                                              palyr.minAbsence,
                                              palyr.maxAbsence,
                                              palyr.percentAbsence)
      if newPAId != -1:
         palyr.setParametersId(newPAId)
         success = True
      
      return success
   
# ...............................................
# Private Methods
# ...............................................
   def _getSpatialLayerType(self, lyr):
      rtype = None
      vtype = None
      if isinstance(lyr, Raster):
         rtype = lyr.gdalType
      elif isinstance(lyr, Vector):
         vtype = lyr.ogrType
      return rtype, vtype

# ...............................................
   def _getLayerUser(self, row, idxs):
      return self._getColumnValue(row,idxs,['lyruserid', 'userid'])
         
# ...............................................
   def _createAncValues(self, row, idxs):
      """
      """
      av = None
      if row is not None:
         av = _AncillaryValue(self._getColumnValue(row,idxs,['matrixidx']), 
                     self._getColumnValue(row,idxs,['namevalue']), 
                     self._getColumnValue(row,idxs,['weightedmean']), 
                     self._getColumnValue(row,idxs,['largestclass']), 
                     self._getColumnValue(row,idxs,['minpercent']), 
                     self._getColumnValue(row,idxs,['ancuserid']), 
                     self._getColumnValue(row,idxs,['ancillaryvalueid']),
                     attrFilter=self._getColumnValue(row,idxs,['namefilter']),
                     valueFilter=self._getColumnValue(row,idxs,['valuefilter']))
      return av

# ...............................................
   def _createRADJobNew(self, row, idxs):
      """
      """
      job = None
      if row is not None:
         exp = self._createRADExperiment(row, idxs)
         bkt = self._createBucket(row, idxs)
         exp.addBucket(bkt)
         
         bktid = self._getColumnValue(row,idxs,['bucketid'])
         bkturl = self._getColumnValue(row,idxs,['bktmetadataurl'])
         bktstat = self._getColumnValue(row,idxs,['bktstatus'])
         bktstage = self._getColumnValue(row,idxs,['bktstage'])
         jobid = self._getColumnValue(row,idxs,['lmjobid'])
         jbfam = self._getColumnValue(row,idxs,['jobfamily'])
         objtype = self._getColumnValue(row,idxs,['referencetype'])
         objid = self._getColumnValue(row,idxs,['referenceid'])
         crid = self._getColumnValue(row,idxs,['computeresourceid'])
         prior = self._getColumnValue(row,idxs,['priority'])
         prg = self._getColumnValue(row,idxs,['progress'])
         stat = self._getColumnValue(row,idxs,['status'])
         stattime = self._getColumnValue(row,idxs,['statusmodtime'])
         stage = self._getColumnValue(row,idxs,['stage'])
         stagetime = self._getColumnValue(row,idxs,['stagemodtime'])
         donotify = self._getColumnValue(row,idxs,['donotify'])
         inputtype = self._getColumnValue(row,idxs,['reqdata'])
         processtype = self._getColumnValue(row,idxs,['reqsoftware'])
         createtime = self._getColumnValue(row,idxs,['datecreated'])
         hbtime = self._getColumnValue(row,idxs,['lastheartbeat'])
         retries = self._getColumnValue(row,idxs,['retrycount'])
         
         
      
         bkt = RADBucket(shpgrid, epsgcode=exp.epsgcode, 
                         stage=bktstage, status=bktstat,  
                         userId=exp.getUserId(), expId=exp.getId(), pamFname=pamFname,
                         bucketId=bktid, metadataUrl=bkturl, 
                         parentMetadataUrl=exp.metadataUrl)         
         exp.addBucket(bkt)
         
         if processtype == ProcessType.SMTP:
            job = NotifyJob(obj=bkt, objType=ReferenceType.Bucket,
                            parentUrl=exp.metadataUrl, 
                            jobFamily=jbfam, computeId=crid, email=exp.email, 
                            status=stat, statusModTime=stattime, 
                            priority=prior, lastHeartbeat=hbtime, 
                            createTime=createtime, jid=jobid, retryCount=retries)

         elif processtype == ProcessType.RAD_INTERSECT:
            if inputtype == InputDataType.USER_PRESENCE_ABSENCE:
               dospecies = True
               self._fillExperimentWithPresenceAbsenceLayers(exp)
            elif inputtype == InputDataType.USER_ANCILLARY:
               dospecies = False
               self._fillExperimentWithAncillaryLayers(exp)
               
            job = RADIntersectJob(exp, doSpecies=dospecies, computeId=crid, 
                                     status=stat, statusModTime=stattime, 
                                     priority=prior, lastHeartbeat=hbtime, 
                                     createTime=createtime, jid=jobid, 
                                     retryCount=retries)
         else:
            # Every other job needs a pamsum (could be original or random)
            pamsum = PamSum.initAndFillFromFile(self._getColumnValue(row,idxs,['pspamdlocation']), 
                                                bucketId=bktid, expId=exp.getId(),
                                                bucketPath=bkt.outputPath,
                                                epsgcode=exp.epsgcode,
                                                metadataUrl=self._getColumnValue(row,idxs,['psmetadataurl']), 
                                                parentMetadataUrl=exp.metadataUrl,
                                                userId=exp.getUserId(), 
                                                pamSumId=self._getColumnValue(row,idxs,['pamsumid']), 
                                                randomMethod=self._getColumnValue(row,idxs,['randommethod']),
                                                randomParameters=self._getColumnValue(row,idxs,['randomparams']),
                                                splotchFilename=self._getColumnValue(row,idxs,['splotchpamdlocation']))
            
            if processtype == ProcessType.RAD_COMPRESS:
               job = RADCompressJob(exp, pamsum, computeId=crid,  
                                       status=stat, statusModTime=stattime, 
                                       priority=prior, lastHeartbeat=hbtime, 
                                       createTime=createtime, jid=jobid,
                                       retryCount=retries)
            elif processtype == ProcessType.RAD_CALCULATE:
               job = RADCalculateJob(exp, pamsum, computeId=crid, 
                                        status=stat, statusModTime=stattime, 
                                        priority=prior, lastHeartbeat=hbtime, 
                                        createTime=createtime, jid=jobid, 
                                        retryCount=retries)
            elif processtype == ProcessType.RAD_SWAP:
               # Also need original compressed PamSum.pam for SWAP
               origPS = PamSum(None, pamFilename=self._getColumnValue(row,idxs,['opspamdlocation']), 
                               pamSumId=self._getColumnValue(row,idxs,['opspamsumid']), 
                               bucketId=bktid, expId=exp.getId(), epsgcode=exp.epsgcode,
                               bucketPath=bkt.outputPath,
                               status=self._getColumnValue(row,idxs,['opsstatus']), 
                               stage=self._getColumnValue(row,idxs,['opsstage']), 
                               randomMethod=RandomizeMethods.NOT_RANDOM)
               exp.bucketList[0].pamSum = origPS
               job = RADSwapJob(exp, pamsum, computeId=crid,  
                                   status=stat, statusModTime=stattime, 
                                   priority=prior, lastHeartbeat=hbtime, 
                                   createTime=createtime, jid=jobid,
                                   retryCount=retries)
            elif processtype == ProcessType.RAD_SPLOTCH:
               job = RADSplotchJob(exp, pamsum, computeId=crid, 
                                      status=stat, statusModTime=stattime, 
                                      priority=prior, lastHeartbeat=hbtime, 
                                      createTime=createtime, jid=jobid,
                                      retryCount=retries)
      return job

# ...............................................
   def _createAncLayer(self, row, idxs):
      """
      @note: takes an lm_anclayer record
      """
      anclyr = None
      if row is not None:
         lyr = self._createLayer(row, idxs)
         ancParam = self._createAncValues(row, idxs)
         procObj = self._createProcessObjectForLayer(row, idxs)
         if isinstance(lyr, Vector):
            anclyr = AncillaryVector.initFromParts(ancParam, lyr, procObj)
         elif isinstance(lyr, Raster):
            anclyr = AncillaryRaster.initFromParts(ancParam, lyr, procObj)
         else:
            raise LMError(currargs='Invalid class %s' % lyr.__class__.__name__)
      return anclyr
   
# ...............................................
   def _createPAValues(self, row, idxs):
      """
      @note: takes an lm_palayer record
      """
      if row is not None:
         pavalues = _PresenceAbsence(self._getColumnValue(row, idxs, ['matrixidx']),
                         self._getColumnValue(row, idxs, ['namepresence']),
                         self._getColumnValue(row, idxs, ['minpresence']), 
                         self._getColumnValue(row, idxs, ['maxpresence']), 
                         self._getColumnValue(row, idxs, ['percentpresence']), 
                         self._getColumnValue(row, idxs, ['nameabsence']), 
                         self._getColumnValue(row, idxs, ['minabsence']), 
                         self._getColumnValue(row, idxs, ['maxabsence']), 
                         self._getColumnValue(row, idxs, ['percentabsence']),
                         self._getColumnValue(row, idxs, ['pauserid']), 
                         self._getColumnValue(row, idxs, ['presenceabsenceid']),
                         attrFilter=self._getColumnValue(row, idxs, ['namefilter']),
                         valueFilter=self._getColumnValue(row, idxs, ['valuefilter']))
      return pavalues
   
# ...............................................
   def _createPALayer(self, row, idxs):
      """
      @note: takes an lm_palayer record
      """
      palyr = None
      if row is not None:         
         lyr = self._createLayer(row, idxs)
         paParam = self._createPAValues(row, idxs)
         procObj = self._createProcessObjectForLayer(row, idxs)
   
         if isinstance(lyr, Vector):
            palyr = PresenceAbsenceVector.initFromParts(paParam, lyr, procObj)
         elif isinstance(lyr, Raster):
            palyr = PresenceAbsenceRaster.initFromParts(paParam, lyr, procObj)
         else:
            raise LMError(currargs='Invalid class %s' % lyr.__class__.__name__)
      return palyr

# ...............................................
   def _createProcessObjectForLayer(self, row, idxs):
      """
      @todo: used only for PresenceAbsenceLayers, AncillaryLayers
             update to include non-layer ProcessObjects
      @note: currently, db does not hold status/stage for layers, so None 
             (will be file?)
      """
      lyr = None
      if row is not None:
         joinid = self._getColumnValue(row, idxs, 
                  ['experimentpalayerid', 'experimentanclayerid', 'layerid'])
         bktid = self._getColumnValue(row, idxs, ['bucketid'])
         stat = self._getColumnValue(row, idxs, ['status'])
         stattime = self._getColumnValue(row, idxs, ['statusmodtime'])
         stage = self._getColumnValue(row, idxs, ['stage'])
         stagetime = self._getColumnValue(row, idxs, ['stagemodtime'])
   
         procObj = ProcessObject(objId=joinid, parentId=bktid, 
                                 status=stat, statusModTime=stattime, 
                                 stage=stage, stageModTime=stagetime)
         return procObj

# ...............................................
   def _createLayer(self, row, idxs):
      """
      @note: takes an lm_anclayer, lm_palayer, lm_shapegrid or layer record 
      """
      lyr = None
      if row is not None:
         mloc = self._getColumnValue(row, idxs, ['metalocation'])
         name = self._getColumnValue(row, idxs, ['exppalayername', 'expanclayername', 'layername'])
         title = self._getColumnValue(row, idxs, ['title'])
         bbox = self._getColumnValue(row, idxs, ['bbox'])
         sDate = self._getColumnValue(row, idxs, ['startdate'])
         eDate = self._getColumnValue(row, idxs, ['enddate'])
         munits = self._getColumnValue(row, idxs, ['mapunits'])
         res = self._getColumnValue(row, idxs, ['resolution'])
         epsg = self._getColumnValue(row, idxs, ['epsgcode'])
         vtype = self._getColumnValue(row, idxs, ['ogrtype'])
         rtype = self._getColumnValue(row, idxs, ['gdaltype'])
         dataformat = self._getColumnValue(row, idxs, ['dataformat'])
         desc = self._getColumnValue(row, idxs, ['description'])
         lyrid = self._getColumnValue(row, idxs, ['layerid'])
         lyrusr = self._getColumnValue(row, idxs, ['userid', 'lyruserid'])
         dloc = self._getColumnValue(row, idxs, ['dlocation', 'lyrdlocation'])
         # Try column names for most specific (subclassed) layer types first
         soid = self._getColumnValue(row, idxs, 
                     ['experimentpalayerid', 'experimentanclayerid', 'layerid'])
         usr = self._getColumnValue(row, idxs, 
                     ['pauserid', 'ancuserid', 'lyruserid', 'userid'])
         dloc = self._getColumnValue(row, idxs, ['lyrdlocation', 'dlocation'])
         dtcreate = self._getColumnValue(row, idxs, ['lyrdatecreated', 'datecreated'])
         dtmod = self._getColumnValue(row, idxs, ['lyrdatelastmodified', 'datelastmodified'])
         murl = self._getColumnValue(row, idxs, ['metadataurl', 'lyrmetadataurl'])
   
         if vtype is not None:
            lyr = Vector(name=name, title=title, bbox=bbox, dlocation=dloc, 
                      metalocation=mloc, startDate=sDate, endDate=eDate, 
                      mapunits=munits, resolution=res, epsgcode=epsg, 
                      ogrType=vtype, ogrFormat=dataformat, description=desc, 
                      svcObjId=soid, lyrId=lyrid, lyrUserId=lyrusr, 
                      createTime=dtcreate, modTime=dtmod, metadataUrl=murl,
                      moduleType=LMServiceModule.RAD)
         elif rtype is not None:
            lyr = Raster(name=name, title=title, bbox=bbox, dlocation=dloc,
                      metalocation=mloc, startDate=sDate, endDate=eDate, 
                      mapunits=munits, resolution=res, epsgcode=epsg, 
                      gdalType=rtype, gdalFormat=dataformat, description=desc, 
                      svcObjId=soid, lyrId=lyrid, lyrUserId=lyrusr, 
                      createTime=dtcreate, modTime=dtmod, metadataUrl=murl,
                      moduleType=LMServiceModule.RAD)
      return lyr

# ...............................................
   def _createRADExperimentWithBucket(self, row, idxs):
      """
      @note: takes an lm_fullradbucket 
      """
      radexp = None
      if row is not None:
         radbuck = self._createBucket(row, idxs)
         radexp = self._createRADExperiment(row, idxs)
         radexp.addBucket(radbuck)
      return radexp

# ...............................................
   def _createBucket(self, row, idxs):
      """
      @note: Uses lm_fullradbucket or lm_bktJob for input
      """
      bkt = None
      if row is not None and row[idxs['bucketid']] is not None:
         shpgrd = self._createShapeGrid(row, idxs)
         pam = self._createMatrix(row, idxs, dlocationColumn='pamdlocation')
         grim = self._createMatrix(row, idxs, dlocationColumn='grimdlocation')
         origPS = self._createPamSum(row, idxs)
         rps = self._createRandomPamSum(row, idxs)
         bkt = RADBucket(shpgrd, fullPam=pam, fullGrim=grim, 
                   keywords=self._getColumnValue(row,idxs,['bktkeywords']),
                   pamFname=self._getColumnValue(row,idxs,['pamdlocation']), 
                   grimFname=self._getColumnValue(row,idxs,['grimdlocation']), 
                   indicesFilename=self._getColumnValue(row,idxs,['slindicesdlocation']), 
                   stage=self._getColumnValue(row,idxs,['bktstage']),
                   stageModTime=self._getColumnValue(row,idxs,['bktstagemodtime']), 
                   status=self._getColumnValue(row,idxs,['bktstatus']),
                   statusModTime=self._getColumnValue(row,idxs,['bktstatusmodtime']),
                   userId=self._getColumnValue(row,idxs,['expuserid']),
                   expId=self._getColumnValue(row,idxs,['experimentid']),
                   bucketId=self._getColumnValue(row,idxs,['bucketid']),
                   metadataUrl=self._getColumnValue(row,idxs,['bktmetadataurl']),
                   createTime=self._getColumnValue(row,idxs,['bktdatecreated']))
         if origPS is not None:
            bkt.pamSum = origPS
         if rps is not None:
            bkt.addRandomPamSum(rps)
      return bkt

# ...............................................
   def _createPamSum(self, row, idxs):
      """
      @note: Original or Standalone Pamsum
      @note: Uses  for input lm_fullradbucket or lm_pamsum 
             or lm_pamsumJobOriginal or lm_pamsumJobRandom
      """
      ps = None
      try:
         if row[idxs['pamsumid']] is not None:
            ps = PamSum.initAndFillFromFile(self._getColumnValue(row,idxs,['pspamdlocation']),
                 sumFilename=self._getColumnValue(row,idxs,['pssumdlocation']),
                 status=self._getColumnValue(row,idxs,['psstatus']),
                 statusModTime=self._getColumnValue(row,idxs,['psstatusmodtime']),
                 stage=self._getColumnValue(row,idxs,['psstage']),
                 stageModTime=self._getColumnValue(row,idxs,['psstagemodtime']),
                 createTime=self._getColumnValue(row,idxs,['psdatecreated']),
                 userId=self._getColumnValue(row,idxs,['expuserid']),
                 bucketId=self._getColumnValue(row,idxs,['bucketid']),
                 expId=self._getColumnValue(row,idxs,['experimentid']),
                 epsgcode=self._getColumnValue(row,idxs,['expepsgcode','epsgcode']),
                 metadataUrl=self._getColumnValue(row,idxs,['psmetadataurl']),
                 pamSumId=self._getColumnValue(row,idxs,['pamsumid']),
                 randomMethod=self._getColumnValue(row,idxs,['randommethod']),
                 # random-only attributes 
                 randomParameters=self._getColumnValue(row,idxs,['randomparams']), 
                 splotchFilename=self._getColumnValue(row,idxs,['splotchpamdlocation']),
                 splotchSitesFilename=self._getColumnValue(row,idxs,['splotchsitesdlocation']))
      except:
         pass
      return ps

# ...............................................
   def _createRandomPamSum(self, row, idxs):
      """
      @note: Randomized Pamsum (if Original is also present)
      @note: Uses lm_pamsumJobRandom for input
      """
      rps = None
      try:
         if row[idxs['rps_pamsumid']] is not None:
            rps = PamSum.initAndFillFromFile(self._getColumnValue(row,idxs,['rps_pamdlocation']),
                    sumFilename=self._getColumnValue(row,idxs,['rps_sumdlocation']),
                    status=self._getColumnValue(row,idxs,['rps_status']),
                    statusModTime=self._getColumnValue(row,idxs,['rps_statusmodtime']),
                    stage=self._getColumnValue(row,idxs,['rps_stage']),
                    stageModTime=self._getColumnValue(row,idxs,['rps_stagemodtime']),
                    createTime=self._getColumnValue(row,idxs,['rps_datecreated']),
                    userId=self._getColumnValue(row,idxs,['expuserid']),
                    bucketId=self._getColumnValue(row,idxs,['bucketid']),
                    expId=self._getColumnValue(row,idxs,['experimentid']),
                    epsgcode=self._getColumnValue(row,idxs,['expepsgcode', 'epsgcode']),
                    metadataUrl=self._getColumnValue(row,idxs,['rps_metadataurl']),
                    pamSumId=self._getColumnValue(row,idxs,['rps_pamsumid']),
                    randomMethod=self._getColumnValue(row,idxs,['rps_randommethod']),
                    randomParameters=self._getColumnValue(row,idxs,['rps_randomparams']), 
                    splotchFilename=self._getColumnValue(row,idxs,['rps_splotchpamdlocation']),
                    splotchSitesFilename=self._getColumnValue(row,idxs,['rps_splotchsitesdlocation']))
      except:
         pass
      return rps
   
# ...............................................
   def _createMatrix(self, row, idxs, dlocationColumn=None):
      """
      @note: Uses lm_experiment or lm_fullradbucket for input
      """
      mtx = None
      if row is not None:
         # these particular Matrices (PAM, GRIM, Splotch) are uncompressed
         isCompressed = False
         dloc = self._getColumnValue(row,idxs,[dlocationColumn])
         mtx = Matrix.initFromFile(dloc, isCompressed)
      return mtx      
      
# ...............................................
   def _createRADExperiment(self, row, idxs):
      """
      @note: Uses experiment or lm_fullradbucket for input
      """
      exp = None
      if row is not None and row[idxs['experimentid']] is not None:
         usr = self._getColumnValue(row,idxs,['expuserid', 'userid'])
         epsg = self._getColumnValue(row,idxs,['expepsgcode', 'epsgcode'])
         keywds=self._getColumnValue(row,idxs,['expkeywords', 'keywords'])
         murl=self._getColumnValue(row,idxs,['expmetadataurl', 'metadataurl'])
         ctime=self._getColumnValue(row,idxs,['expdatecreated', 'datecreated'])
         mtime=self._getColumnValue(row,idxs,['expdatelastmodified', 'datelastmodified'])
         desc=self._getColumnValue(row,idxs,['expdescription', 'description'])
            
         exp = RADExperiment(usr, 
                 self._getColumnValue(row,idxs,['expname']), epsg, 
                 attrMatrixFilename=self._getColumnValue(row,idxs,['attrmatrixdlocation']), 
                 attrTreeFilename=self._getColumnValue(row,idxs,['attrtreedlocation']), 
                 email=self._getColumnValue(row,idxs,['email']),
                 metadataUrl=murl, keywords=keywds, description=desc,
                 expId=self._getColumnValue(row,idxs,['experimentid']), 
                 createTime=ctime, modTime=mtime)
      return exp

# ...............................................
   def _createShapeGrid(self, row, idxs):
      """
      @note: takes lm_shapegrid, lm_fullradbucket, lm_intjob, lm_mtxjob record
      """
      shg = None
      if row is not None:
         shg = ShapeGrid(self._getColumnValue(row,idxs,['layername']) , 
                   self._getColumnValue(row,idxs,['cellsides']), 
                   self._getColumnValue(row,idxs,['cellsize']), 
                   self._getColumnValue(row,idxs,['mapunits']), 
                   self._getColumnValue(row,idxs,['epsgcode']), 
                   self._getColumnValue(row,idxs,['bbox']),
                   dlocation=self._getColumnValue(row,idxs,['lyrdlocation']), 
                   siteId=self._getColumnValue(row,idxs,['idattribute']),
                   siteX=self._getColumnValue(row,idxs,['xattribute']),
                   siteY=self._getColumnValue(row,idxs,['yattribute']),
                   size=self._getColumnValue(row,idxs,['vsize']), 
                   userId=self._getColumnValue(row,idxs,['lyruserid','userid']),
                   layerId=self._getColumnValue(row,idxs,['layerid']),
                   shapegridId=self._getColumnValue(row,idxs,['shapegridid']),
                   bucketId=self._getColumnValue(row,idxs,['bucketid']), 
                   status=self._getColumnValue(row,idxs,['shpstatus', 'status']), 
                   statusModTime=self._getColumnValue(row,idxs,['shpstatusmodtime', 'statusmodtime']),
                   createTime=self._getColumnValue(row,idxs,['lyrdatecreated']), 
                   modTime=self._getColumnValue(row,idxs,['lyrdatelastmodified']), 
                   metadataUrl=self._getColumnValue(row,idxs,['lyrmetadataurl','metadataurl'])) 
      return shg
      

# .............................................................................
   def rollbackRADJobs(self, queuedStatus):
      """
      @summary Reset Buckets, PamSums (JobStatus.PULL_REQUESTED) that were not 
               dispatched for whatever reason.  Set back to INITIALIZE, since
               we know they are ready.
      @param queuedStatus: status for jobs waiting to be pulled.
      @note: lm_resetJobs(double, int, int) changes status and returns int
      """
      cnt = self.executeModifyReturnValue('lm_resetJobs', 
                                          mx.DateTime.utc().mjd, 
                                          queuedStatus,
                                          JobStatus.INITIALIZE)
      self.log.debug('Reset %d queued (%d) jobs to status (%d)' 
                     % (cnt, queuedStatus, JobStatus.INITIALIZE))
      return cnt
   
# ...............................................
   def rollbackExperiment(self, radexp, rollbackStatus, rollbackStage):
      bucketRollbackCount = self.executeModifyReturnValue('lm_rollbackExperiment', 
                                              radexp.getId(), 
                                              rollbackStatus, rollbackStage,
                                              mx.DateTime.utc().mjd)
      return bucketRollbackCount

# ...............................................
   def rollbackBucket(self, bucket, rollbackStatus, rollbackStage):
      pamsumDeleteCount = self.executeModifyReturnValue('lm_rollbackBucket', 
                                              bucket.getId(), 
                                              rollbackStatus, rollbackStage,
                                              mx.DateTime.utc().mjd)
      return pamsumDeleteCount
   
# ...............................................
   def pullJobs(self, count, processType, startStat, endStat, usr, 
                inputType, computeIP):
      jobs = []
      if count == 0: 
         return jobs
      currtime = mx.DateTime.gmt().mjd
      rows = None
      if processType == ProcessType.RAD_INTERSECT:
         rows, idxs = self.executeSelectAndModifyManyFunction('lm_pullBucketJobs',
                                                              count,
                                                              processType,
                                                              startStat,
                                                              endStat,
                                                              usr, inputType, 
                                                              currtime,
                                                              computeIP)
# -- Note: Original or Splotch PAM for COMPRESS or Original PAM for SPLOTCH; 
# --       Compressed Original or Random PAM for CALCULATE or Compressed Original for SWAP
      elif (ProcessType.isRandom(processType) or 
            processType == ProcessType.RAD_COMPRESS or
            processType == ProcessType.RAD_CALCULATE): 
         rows, idxs = self.executeSelectAndModifyManyFunction('lm_pullMatrixJobs',
                                                              count,
                                                              processType,
                                                              startStat,
                                                              endStat,
                                                              usr, inputType, 
                                                              currtime,
                                                              computeIP)
      elif processType == ProcessType.RAD_BUILDGRID:
         rows, idxs = self.executeSelectAndModifyManyFunction('lm_pullGridJobs',
                                                              count,
                                                              processType,
                                                              startStat,
                                                              endStat,
                                                              usr, 
                                                              currtime,
                                                              computeIP)
      elif processType == ProcessType.SMTP:
         rows, idxs = self.executeSelectAndModifyManyFunction('lm_pullMessageJobs',
                                                              count,
                                                              processType,
                                                              startStat,
                                                              endStat,
                                                              usr, 
                                                              currtime,
                                                              computeIP)
      if rows:
         for r in rows:
            job = self._createRADJobNew(r, idxs)
            if job is not None:
               jobs.append(job)
      return jobs
         
# ...............................................
   def rollbackLifelessJobs(self, giveupTime, pulledStat, initStat, completeStat):
      currtime = mx.DateTime.gmt().mjd
      count = self.executeModifyReturnValue('lm_resetLifelessJobs', giveupTime, 
                                            currtime, pulledStat, initStat, 
                                            completeStat)
      return count
   
# ...............................................
   def moveAllDependentJobs(self, completeStat, errorStat, notReadyStat, 
                            readyStat, currtime):
      # depends on Bucket:
      completeStage = JobStage.INTERSECT
      depOPSStage = JobStage.COMPRESS 
      depRPSStage = JobStage.SPLOTCH
      total = self.executeModifyReturnValue('lm_updateAllBucketDependentJobs',
                                              completeStage, completeStat, 
                                              errorStat, depOPSStage, 
                                              depRPSStage, notReadyStat, 
                                              readyStat, currtime)
      # depends on OriginalPamSum:
      # Can SWAP after OriginalPamSum completes COMPRESS or CALCULATE
      dependencies = ((JobStage.COMPRESS, JobStage.CALCULATE, JobStage.SWAP),
                      (JobStage.CALCULATE, None, JobStage.SWAP))
      for (completeStage, depOPSStage, depRPSStage) in dependencies:

         count = self.executeModifyReturnValue('lm_updateAllOPSDependentJobs',
                                              completeStage, completeStat, 
                                              errorStat, depOPSStage, 
                                              depRPSStage, notReadyStat, 
                                              readyStat, currtime)
         total += count
      # depends on RandomPamSum:
      dependencies = ((JobStage.SPLOTCH, JobStage.COMPRESS), 
                      (JobStage.COMPRESS, JobStage.CALCULATE),
                      (JobStage.SWAP, JobStage.CALCULATE))
      for (completeStage, depRPSStage) in dependencies:
         count = self.executeModifyReturnValue('lm_updateAllRPSDependentJobs',
                                              completeStage, completeStat, 
                                              errorStat, depRPSStage, 
                                              notReadyStat, readyStat, 
                                              currtime)
         total += count
      return count

# ...............................................
   def moveDependentJobs(self, radjob, completeStat, errorStat, notReadyStat, 
                         readyStat, currtime):
      count = 0
      if radjob.status == completeStat or radjob.status >= errorStat:           
         if radjob.outputObjType == ReferenceType.Bucket:
            completeStage = JobStage.INTERSECT
            depOPSStage = JobStage.COMPRESS 
            depRPSStage = JobStage.SPLOTCH
            count = self.executeModifyReturnValue('lm_updateBucketDependentJobs',
                                                 radjob.outputObj.getId(), 
                                                 completeStage, completeStat, 
                                                 errorStat, depOPSStage, 
                                                 depRPSStage, notReadyStat, 
                                                 readyStat, currtime)
         elif radjob.outputObjType == ReferenceType.OriginalPamSum:
            # depends on OriginalPamSum:
            # Can SWAP after OriginalPamSum completes COMPRESS or CALCULATE
            dependencies = ((JobStage.COMPRESS, JobStage.CALCULATE, JobStage.SWAP),
                            (JobStage.CALCULATE, None, JobStage.SWAP))
            for (completeStage, depOPSStage, depRPSStage) in dependencies:
               count = self.executeModifyReturnValue('lm_updateOPSDependentJobs',
                                                    radjob.outputObj.getId(), 
                                                    completeStage, completeStat, 
                                                    errorStat, depOPSStage, 
                                                    depRPSStage, notReadyStat, 
                                                    readyStat, currtime)
         elif radjob.outputObjType == ReferenceType.RandomPamSum:
            dependencies = ((JobStage.SPLOTCH, JobStage.COMPRESS), 
                            (JobStage.COMPRESS, JobStage.CALCULATE),
                            (JobStage.SWAP, JobStage.CALCULATE))
            for (completeStage, depRPSStage) in dependencies:
               count = self.executeModifyReturnValue('lm_updateRPSDependentJobs',
                                                    radjob.outputObj.getId(), 
                                                    completeStage, completeStat, 
                                                    errorStat, depRPSStage, 
                                                    notReadyStat, readyStat, 
                                                    currtime)
      return count

# ...............................................
   def deleteRADJob(self, jobid):
      success = self.executeModifyFunction('lm_deleteJob', jobid)
      return success
   
   def deleteRADJobsForBucket(self, bktid):
      jobcount = self.executeModifyReturnValue('lm_deleteJobsForBucket', bktid)
      return jobcount
   
   def deleteRADJobsForExperiment(self, expid):
      jobcount = self.executeModifyReturnValue('lm_deleteJobsForExperiment', expid)
      return jobcount

# # .............................................................................
#    def countJobsOld(self, reftype, status, userId):      
#       """
#       @summary: Return the number of jobs fitting the given filter conditions
#       @param userId: include only jobs with this userid
#       @param status: include only jobs with this status
#       @return: number of jobs fitting the given filter conditions
#       """
#       if reftype == ReferenceType.Bucket:
#          fnname = 'lm_countIntJobs'
#       elif reftype in (ReferenceType.OriginalPamSum, ReferenceType.RandomPamSum):
#          fnname = 'lm_countMtxJobs'
#       elif reftype == ReferenceType.RADExperiment:
#          fnname = 'lm_countMsgJobs'
#       else:
#          raise LMError('Unknown ReferenceType %s' % str(reftype))
#       
#       row, idxs = self.executeSelectOneFunction(fnname, userId, status, 
#                                                 reftype)
#       return self._getCount(row)

# .............................................................................
   def countJobs(self, proctype, status, userId):      
      """
      @summary: Return the number of jobs fitting the given filter conditions
      @param proctype: include only jobs with this 
                       LmCommon.common.lmconstants.ProcessType  
      @param userId: include only jobs with this userid
      @param status: include only jobs with this status
      @return: number of jobs fitting the given filter conditions
      """
      if proctype == ProcessType.RAD_BUILDGRID:
         fnname = 'lm_countGrdJobs'
      elif proctype == ProcessType.RAD_INTERSECT:
         fnname = 'lm_countIntJobs'
      elif (ProcessType.isRandom(proctype) or 
            proctype in (ProcessType.RAD_COMPRESS, ProcessType.RAD_CALCULATE)):
         fnname = 'lm_countMtxJobs'
      elif proctype == ProcessType.SMTP:
         fnname = 'lm_countMsgJobs'
      else:
         return 0
      row, idxs = self.executeSelectOneFunction(fnname, userId, status, proctype)
      return self._getCount(row)

# ...............................................
   def updateRADJob(self, radjob, errorstat, incrementRetry):
      """
      @note: Operates on a complete _Job object. 
      @summary Updates or deletes an LmJob record, then updates a 
               RAD Bucket or PamSum with filenames and the current stage, 
               status and modification times, then moves all dependent jobs
      @param radjob: job to update
      @param incrementRetry: If this job has just been pulled by a 
                             ComputeResource, increment the number of tries.  
      @return True on success; False on failure
      """
      # Just in case we only have JobId
      if isinstance(radjob, IntType):
         radjob = self.getJob(radjob)

      # Update or delete job
      if radjob.status == JobStatus.COMPLETE or radjob.status >= errorstat:
         jsuccess = self.executeModifyFunction('lm_deleteJob', radjob.getId())
      else:
         jsuccess = self.executeModifyFunction('lm_updateJob', radjob.getId(), 
                                               radjob.computeResourceId, 
                                               radjob.status, 
                                               radjob.statusModTime, 
                                               incrementRetry)
      # Update object
      if jsuccess:
         try:
            osuccess = self.updateBucketInfo(radjob.outputObj, radjob.computeResourceId)
         except:
            osuccess = self.updatePamSumInfo(radjob.outputObj, radjob.computeResourceId)
      else:
         raise LMError('Unable to update RADJob %s' % str(radjob.getId()))

#       # Move dependent jobs and objects
#       if radjob.processType != ProcessType.SMTP:
#          dsuccess = self.moveDependentJobs(radjob)

      return osuccess, jsuccess
   
# ...............................................
   def updateJobAndObjectStatus(self, jobid, computeIP, status, progress,
                                incrementRetry):
      """
      @summary Updates the status on a job and its corresponding object
      @param jobid: The job record to update
      @param computeName: The name of the ComputeResource computing the job
      @param status: The JobStatus
      @param progress: Percent complete
      @param incrementRetry: If this job has just been pulled by a 
                             ComputeResource, increment the number of tries.  
      @note: This updates compute info: compute resource, progress, retryCount,  
                                        status, modtime, lastheartbeat.
      """
      success = self.executeModifyFunction('lm_updateJobAndObjLite', jobid, 
                                           computeIP, status, progress, 
                                           incrementRetry, mx.DateTime.gmt().mjd)
      return success

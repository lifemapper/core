"""
@summary: 
@author: CJ Grady
@version: 
@status: alpha

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
from mx.DateTime import gmt
import os
from time import sleep


from LmClient.lmClientLib import LMClient
from LmCommon.common.lmconstants import JobStage, JobStatus
from LmServer.common.lmconstants import TEST_DATA_PATH
from LmServer.common.localconstants import TROUBLESHOOTERS
from LmServer.common.log import ConsoleLogger
from LmServer.db.scribe import Scribe

# TODO:  CJ:  change to TEST_DATA_PATH
# SAMPLE_DATA_PATH = os.path.join(APP_PATH, "test", "sampleData")
SLEEP_INTERVAL = 30
MAX_INTERSECT_TIME = 3600
MAX_RANDOMIZE_TIME = 3600
USER_ID = "unitTest"
USER_PWD = "unitTest"

# .............................................................................
def testExperiment():
   """
   @summary: Performs an end-to-end test of a RAD experiment
   """
   cl = LMClient(USER_ID, USER_PWD)
   scribe = Scribe(ConsoleLogger())
   scribe.openConnections()
   
   print "Posting experiment"
   
   expName = "Test RAD experiment %s" % gmt().mjd
   epsgCode = 4326

   # Post an experiment
   expResp = cl.rad.postExperiment(expName, epsgCode)
   expId = expResp.id
   print "Experiment id:", expId


   print "Posting layers"
   # layer 1 - sdm layer > .5
   lyr1Name = "Gulo gulo %s" % gmt().mjd
   lyr1Fn = os.path.join(TEST_DATA_PATH, "Gulo gulo Projection 2512508.tif")
   
   # layer 2 - some other raster
   lyr2Name = "Rana areolata %s" % gmt().mjd
   lyr2Fn = os.path.join(TEST_DATA_PATH, "Rana areolata Projection 2515034.tif")
   
   # layer 4 - vector layer 1-2
   lyr4Name = "Peromyscus maniculatus %s" % gmt().mjd
   lyr4Fn = os.path.join(TEST_DATA_PATH, "Peromyscus_maniculatus.shp")
   
   # layer 5 - vector layer
   lyr5Name = "Procyon lotor %s" % gmt().mjd
   lyr5Fn = os.path.join(TEST_DATA_PATH, "Procyon_lotor.shp")
   
   # layer 6 - vector layer
   lyr6Name = "Tamiasciurus hudsonicus %s" % gmt().mjd
   lyr6Fn = os.path.join(TEST_DATA_PATH, "Tamiasciurus_hudsonicus.shp")
   
   # Post layers
   lyr1Resp = cl.rad.postRaster(lyr1Name, filename=lyr1Fn, epsgCode=epsgCode, dataFormat='AAIGrid')
   lyr2Resp = cl.rad.postRaster(lyr2Name, filename=lyr2Fn, epsgCode=epsgCode, dataFormat='AAIGrid')

   lyr4Resp = cl.rad.postVector(lyr4Name, filename=lyr4Fn, epsgCode=epsgCode)
   lyr5Resp = cl.rad.postVector(lyr5Name, filename=lyr5Fn, epsgCode=epsgCode)
   lyr6Resp = cl.rad.postVector(lyr6Name, filename=lyr6Fn, epsgCode=epsgCode)
   
   lyr1Id = lyr1Resp.id
   lyr2Id = lyr2Resp.id
   lyr4Id = lyr4Resp.id
   lyr5Id = lyr5Resp.id
   lyr6Id = lyr6Resp.id
   
   print "Layer 1 id:", lyr1Id
   print "Layer 2 id:", lyr2Id
   print "Layer 4 id:", lyr4Id
   print "Layer 5 id:", lyr5Id
   print "Layer 6 id:", lyr6Id
   
   # Add PA Layers
   cl.rad.addPALayer(expId, lyr1Id, 'pixel', '.5', '1.0', '25')
   cl.rad.addPALayer(expId, lyr2Id, 'pixel', '.5', '1.0', '25')
   
   cl.rad.addPALayer(expId, lyr4Id, 'PRESENCE', '1', '2', '25')
   cl.rad.addPALayer(expId, lyr5Id, 'PRESENCE', '1', '2', '25')
   cl.rad.addPALayer(expId, lyr6Id, 'PRESENCE', '1', '2', '25')

   # Add a bucket
   bkt1Resp = cl.rad.addBucket(expId, "testShp1%s" % gmt().mjd, "square", "2", "dd", epsgCode, '-180.0, 0.0, 0.0, 90.0')
   bkt2Resp = cl.rad.addBucket(expId, "testShp2%s" % gmt().mjd, "hexagon", "1", "dd", epsgCode, '-180.0, 0.0, 0.0, 90.0')

   buckets = cl.rad.listBuckets(expId, fullObjects=True)

   bkt1Id = buckets[0].id
   bkt2Id = buckets[1].id

   print "Buckets"
   print "bucket 1 id:", bkt1Id
   print "bucket 2 id:", bkt2Id
   
   # Intersect
   cl.rad.intersectBucket(expId, bkt1Id)
   cl.rad.intersectBucket(expId, bkt2Id)
   
   # Wait for intersects / compress / calculate to finish
   bkt1 = cl.rad.getBucket(expId, bkt1Id)
   bkt2 = cl.rad.getBucket(expId, bkt2Id)
   sleep(SLEEP_INTERVAL)
   intersectTime = SLEEP_INTERVAL
   
   bkt1Finished = bkt1.status >= JobStatus.GENERAL_ERROR or (bkt1.status >= JobStatus.COMPLETE and bkt1.stage == JobStage.CALCULATE)
   bkt2Finished = bkt2.status >= JobStatus.GENERAL_ERROR or (bkt2.status >= JobStatus.COMPLETE and bkt2.stage == JobStage.CALCULATE)
   while not (bkt1Finished and bkt2Finished) and intersectTime <= MAX_INTERSECT_TIME:
      sleep(SLEEP_INTERVAL)
      if not bkt1Finished:
         bkt1 = cl.rad.getBucket(expId, bkt1Id)
         bkt1Finished = bkt1.status >= JobStatus.GENERAL_ERROR or (bkt1.status >= JobStatus.COMPLETE and bkt1.stage == JobStage.CALCULATE)
         if bkt1.status >= JobStatus.GENERAL_ERROR:
            raise Exception, "Bucket %s failed with status %s" % (bkt1Id, bkt1.status)
      if not bkt2Finished:
         bkt2 = cl.rad.getBucket(expId, bkt2Id)
         bkt2Finished = bkt2.status >= JobStatus.GENERAL_ERROR or (bkt2.status >= JobStatus.COMPLETE and bkt2.stage == JobStage.CALCULATE)
         if bkt2.status >= JobStatus.GENERAL_ERROR:
            raise Exception, "Bucket %s failed with status %s" % (bkt2Id, bkt2.status)
      intersectTime += MAX_INTERSECT_TIME
   
   if intersectTime > MAX_INTERSECT_TIME:
      raise Exception, "Took too long to perform intersect operation"
   
   print "Intersects have completed"
   
   # Randomize
   print "Request randomizations"
   cl.rad.randomizeBucket(expId, bkt1Id, method='swap')
   cl.rad.randomizeBucket(expId, bkt1Id, method='splotch')
   cl.rad.randomizeBucket(expId, bkt2Id, method='swap')
   cl.rad.randomizeBucket(expId, bkt2Id, method='splotch')

   # Get all pam sums
   psAtoms1 = cl.rad.listPamSums(expId, bkt1Id)
   psAtoms2 = cl.rad.listPamSums(expId, bkt2Id)
   
   psIds = []
   for psAtom in psAtoms1:
      psIds.append((bkt1Id, psAtom.id))
   for psAtom in psAtoms2:
      psIds.append((bkt2Id, psAtom.id))
   
   print "Pam Sum Ids:", psIds
   
   sleep(SLEEP_INTERVAL)
   randomizeTime = SLEEP_INTERVAL
   
   # Wait for randomize / calculates to finish
   while len(psIds) > 0 and randomizeTime <= MAX_RANDOMIZE_TIME:
      newPsIds = []
      for bId, psId in psIds:
         ps = cl.rad.getPamSum(expId, bId, psId)
         if not (ps.status >= JobStatus.GENERAL_ERROR or (ps.status >= JobStatus.COMPLETE and ps.stage == JobStatus.CALCULATE)):
            if ps.status >= JobStatus.GENERAL_ERROR:
               raise Exception, "Pam Sum %s failed with status %s" % (psId, ps.status)
            newPsIds.append((bId, psId))
      psIds = newPsIds
      randomizeTime += SLEEP_INTERVAL
      sleep(SLEEP_INTERVAL)
   
   if randomizeTime > MAX_RANDOMIZE_TIME:
      raise Exception, "Took too long to perform randomize operations"   
   
   print "Random pam sums finished"
   # Check stats
   # Clean up
   print "Attempting to clean up"
   radExp = scribe.getRADExperiment(USER_ID, fillLayers=True, fillRandoms=True, expid=expId)
   scribe.deleteRADExperiment(radExp)
   scribe.closeConnections()

# .............................................................................
if __name__ == "__main__":
   try:
      testExperiment()
   except Exception, e:
      from LmServer.notifications.email import EmailNotifier
      msg = "End to end test of RAD experiment failed: %s" % str(e)
      notifier = EmailNotifier()
      notifier.sendMessage(TROUBLESHOOTERS, "RAD experiment test failed", msg)

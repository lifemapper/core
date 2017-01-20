"""
@summary: This performs an end to end test for SDM experiments
@author: CJ Grady
@version: 1.1
@status: beta

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
from random import choice, randint
from time import sleep

from LmClient.lmClientLib import LMClient
from LmCommon.common.lmconstants import JobStatus
from LmServer.common.localconstants import TROUBLESHOOTERS
from LmServer.common.log import ConsoleLogger
from LmServer.db.scribe import Scribe

# MAX_MODEL_TIME = 7200
# MAX_PROJECTION_TIME = 7200
SLEEP_INTERVAL = 30

# .............................................................................
def testExperiment():
   """
   @summary: Tests SDM experiment submission to retrieval of completed
   """
   cl = LMClient('unitTest', 'unitTest')
   scribe = Scribe(ConsoleLogger())
   scribe.openConnections()
   
   # Pick an algorithm
   algoCode = choice(cl.sdm.algos).code
   print algoCode

   # Pick an occurrence set
   # Should also try non-complete occurrence sets and uploads
   numOccurrences = scribe.countOccurrenceSets(minOccurrenceCount=50, 
                                               status=JobStatus.COMPLETE)
   occId = scribe.listOccurrenceSets(randint(0, numOccurrences), 1, 
                                     minOccurrenceCount=50, 
                                     status=JobStatus.COMPLETE)[0].id
   print occId

   # Pick a model scenario
   mdlScnId = choice(cl.sdm.listScenarios(public=True, epsgCode=4326, 
                                          keyword=['observed'])).id
   print mdlScnId
   
   
   # Determine time required
   
   mdlScn = cl.sdm.getScenario(mdlScnId)
   
   if float(mdlScn.resolution) < 2.0:
      # High resolution
      MAX_MODEL_TIME = 86400
      MAX_PROJECTION_TIME = 86400
   else:
      # Low resolution
      MAX_MODEL_TIME = 7200
      MAX_PROJECTION_TIME = 7200
   
   
   # Projection scenarios
   prjScnIds = [prj.id for prj in cl.sdm.listScenarios(public=True, 
                                                       epsgCode=4326, 
                                                    matchingScenario=mdlScnId)]
   print prjScnIds

   # Post an experiment
   expObj = cl.sdm.postExperiment(algoCode, mdlScnId, occId, prjScns=prjScnIds, 
                                  name="End to end test %s" % gmt().mjd, 
                                  description="SDM end to end test %s" % gmt().mjd)
   expId = expObj.id
   print expObj.id

   # Wait for it to complete
   sleep(SLEEP_INTERVAL)
   modelTime = SLEEP_INTERVAL
   exp = cl.sdm.getExperiment(expId)
   while int(exp.model.status) < JobStatus.COMPLETE and modelTime <= MAX_MODEL_TIME:
      sleep(SLEEP_INTERVAL)
      exp = cl.sdm.getExperiment(expId)
      print "Experiment status: %s" % exp.model.status, modelTime
      modelTime += SLEEP_INTERVAL
   print 

   if modelTime > MAX_MODEL_TIME:
      raise Exception, "Did not finish model (%s) in time" % expId

   sleep(SLEEP_INTERVAL)
   projectionTime = SLEEP_INTERVAL
   exp = cl.sdm.getExperiment(expId)
   
   while min([int(prj.status) for prj in exp.projections]) < JobStatus.COMPLETE and projectionTime <= MAX_PROJECTION_TIME:
      sleep(SLEEP_INTERVAL)
      exp = cl.sdm.getExperiment(expId)
      print "Waiting on projections...", projectionTime
      projectionTime += SLEEP_INTERVAL
   print
   
   if projectionTime > MAX_PROJECTION_TIME:
      raise Exception, "Did not finish projection in time"


   # Delete everything that was created
   exp2 = scribe.getExperimentForModel(expId)
   scribe.deleteExperiment(exp2.model)


# .............................................................................
if __name__ == "__main__":
   try:
      testExperiment()
   except Exception, e:
      from LmServer.notifications.email import EmailNotifier
      msg = "End to end test of SDM experiment failed: %s" % str(e)
      notifier = EmailNotifier()
      notifier.sendMessage(TROUBLESHOOTERS, "SDM experiment test failed", msg)
   
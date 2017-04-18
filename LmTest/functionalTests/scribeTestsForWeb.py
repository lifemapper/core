"""
@summary: This test script checks that the scribe functions needed for web 
             requests are working properly
@author: CJ Grady
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

@todo: Count and list functions may not be created yet as they were not needed
          previously
"""
import os
from random import randint

from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import APP_PATH
from LmServer.common.log import UnittestLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.scenario import Scenario
from LmServer.legion.sdmproj import SDMProjection
from LmServer.base.lmobj import LMError

# .............................................................................
# Environmental layers
def test_environmental_layers(scribe, userId, scenarioId=None):
   """
   @note: Environmental layers are inserted after a scenario, and require the
          scenarioId
   @todo: Fix and uncomment this in Main
   """
   # Post
   epsg = 4326
    
   # We will have a better way to get the data path later, this is more of a 
   #    temporary test, so I'm not worried about it
   fn = os.path.join(APP_PATH, 'LmTest', 'data', 'layers', 'lyr266.tif') 
   postName = 'testLyr{0}'.format(randint(0, 10000))
   
   postELyr = EnvLayer(postName, userId, epsg, dlocation=fn, dataFormat='GTiff')
   postedELyr = scribe.findOrInsertEnvLayer(postELyr, scenarioId=scenarioId)    
   assert postedELyr.getId() is not None
    
   # Get
   elyrId = postedELyr.getId()
   getELyr1 = scribe.getEnvLayer(envlyrId=elyrId)
   assert getELyr1 is not None
   assert getELyr1.getId() == elyrId
    
   getELyr2 = scribe.getEnvLayer(lyrId=postedELyr.getLayerId())
   assert getELyr2 is not None
   assert getELyr2.getId() == elyrId

   getELyr3 = scribe.getEnvLayer(lyrVerify=postedELyr.verify)
   assert getELyr3 is not None
   assert getELyr3.getId() == elyrId

   getELyr4 = scribe.getEnvLayer(userId=postedELyr.getUserId(), 
                              lyrName=postedELyr.name, epsg=postedELyr.epsgcode)
   assert getELyr4 is not None
   assert getELyr4.getId() == elyrId

   # Count
   lyrCountUsr = scribe.countEnvLayers(userId=userId)
   lyrCountPub = scribe.countEnvLayers()
   assert lyrCountUsr >= 1 # Posted a layer in this function
   assert lyrCountPub >= 0
    
   # List
   lyrListUsr = scribe.listEnvLayers(0, 100, userId=userId)
   lyrListPub = scribe.listEnvLayers(0, 100)
   assert len(lyrListUsr) >= 1
   assert len(lyrListPub) >= 0 # Check that it is at least a list
    
   # Delete
   # Assert we successfully delete the layer we retrieved
   assert scribe.deleteEnvLayer(getELyr1)

# .............................................................................
# Occurrence sets
def test_occurrence_sets(scribe, userId):
   # Post
   postName = 'testOcc{}'.format(randint(0, 10000))
   epsg = 4326
   features = {
      '1' : OccurrenceLayer.getUserPointFeature('1', -49.32, 29.34),
      '2' : OccurrenceLayer.getUserPointFeature('2', -48.23, 28.89),
      '3' : OccurrenceLayer.getUserPointFeature('3', -47.87, 27.00),
      '4' : OccurrenceLayer.getUserPointFeature('4', -49.10, 28.26),
      '5' : OccurrenceLayer.getUserPointFeature('5', -48.02, 29.65),
   }
   
   postOcc = OccurrenceLayer(postName, userId, epsg, len(features.keys()), 
                             features=features)
   postedOcc = scribe.findOrInsertOccurrenceSet(postOcc)
   assert postedOcc.getId() is not None
   
   # Get
   occId = postedOcc.getId()
   getOcc = scribe.getOccurrenceSet(occId=occId)
   assert getOcc is not None
   assert getOcc.getId() == occId
   
   # Count
   occCountUsr = scribe.countOccurrenceSets(userId=userId)
   occCountPub = scribe.countOccurrenceSets()
   assert occCountUsr >= 1 # We just posted one
   assert occCountPub >= 0
   
   # List
   occListUsr = scribe.listOccurrenceSets(0, 100, userId=userId)
   occListPub = scribe.listOccurrenceSets(0, 100)
   
   assert len(occListUsr) >= 1 and len(occListUsr) <= 100
   assert len(occListPub) >= 0 and len(occListPub) <= 100

   # Delete
   # Assert that we successfully delete the occurrence set we just posted
   assert scribe.deleteObject(getOcc)

# .............................................................................
# projections
def test_projections(scribe, userId):
   # Post
   postOccName = 'testOccForPrj{}'.format(randint(0, 10000))
   epsg = 4326
   features = {
      '1' : OccurrenceLayer.getUserPointFeature('1', -49.32, 29.34),
      '2' : OccurrenceLayer.getUserPointFeature('2', -48.23, 28.89),
      '3' : OccurrenceLayer.getUserPointFeature('3', -47.87, 27.00),
      '4' : OccurrenceLayer.getUserPointFeature('4', -49.10, 28.26),
      '5' : OccurrenceLayer.getUserPointFeature('5', -48.02, 29.65),
   }
   
   postOcc = OccurrenceLayer(postOccName, userId, epsg, len(features.keys()), 
                             features=features)
   postedOcc = scribe.findOrInsertOccurrenceSet(postOcc)

   postScnCode = 'testScnForPrj{}'.format(randint(0, 10000))
   postScn = Scenario(postScnCode, userId, epsg)
   postedScn = scribe.findOrInsertScenario(postScn)

   algo = Algorithm('ATT_MAXENT')
   
   postPrj = SDMProjection(postedOcc, algo, postedScn, postedScn, epsgcode=epsg)
   postedPrj = scribe.findOrInsertSDMProject(postPrj)
   assert postedPrj.getId() is not None
   
   # Get
   prjId = postedPrj.getId()
   getPrj = scribe.getSDMProject(prjId)
   assert getPrj is not None
   assert getPrj.getId() == prjId
   
   # Count
   prjCountUsr = scribe.countProjections(userId=userId)
   prjCountPub = scribe.countProjections()
   assert prjCountUsr >= 1
   assert prjCountPub >= 0
   
   # List
   prjListUsr = scribe.listProjections(0, 100, userId=userId)
   prjListPub = scribe.listProjections(0, 100)
   
   assert len(prjListUsr) >= 1 and len(prjListUsr) <= 100
   assert len(prjListPub) >= 0 and len(prjListPub) <= 100
   
   # Delete
   assert scribe.deleteObject(getPrj)
   
   assert scribe.deleteObject(postedScn)
   assert scribe.deleteObject(postedOcc)

# .............................................................................
# scenarios
def test_scenarios(scribe, userId):
   # Post
   postCode = 'testScn{}'.format(randint(0, 10000))
   epsg = 4326
   postScn = Scenario(postCode, userId, epsg)
   postedScn = scribe.findOrInsertScenario(postScn)
   
   # Get
   scnId = postedScn.getId()
   getScn = scribe.getScenario(scnId)
   
   # Count
   scnCountUsr = scribe.countScenarios(userId=userId)
   scnCountPub = scribe.countScenarios()
   
   # List
   scnListUsr = scribe.listScenarios(0, 100, userId=userId)
   scnListPub = scribe.listScenarios(0, 100)
   
   # Delete
   # Assert that we successfully delete the retrieved scenario (that we posted)
   assert scribe.deleteObject(getScn)

# .............................................................................
# user
def test_user(scribe, userId, email):
   # Post
   postCode = 'testUser{}'.format(randint(0, 10000))
   usr = LMUser(userId, email, 'testing', isEncrypted=False)
   postedUser = scribe.findOrInsertUser(usr)
   if postedUser is not None:
      # Find by userId
      getById = scribe.findUser(userId=userId)
      # Find by email
      getByEmail = scribe.findUser(email=email)
   else:
      raise LMError(currargs='Failed to insert User {}'.format(userId))
   return postedUser

# .............................................................................
# user
def delete_user(scribe, user):
   # Delete
   # Assert that we successfully delete the retrieved scenario (that we posted)
   assert scribe.deleteObject(user)

# .............................................................................
if __name__ == '__main__':
   scribe = BorgScribe(UnittestLogger())
   scribe.openConnections()
   
   # Create Test user
   testUser = test_user(scribe, 'tester', 'tester@null.nowhere')
   
   # Run each of the tests, they will throw exceptions if they are not correct
   test_scenarios(scribe, testUser.userid)
   test_environmental_layers(scribe, DEFAULT_POST_USER)
   test_occurrence_sets(scribe, testUser.userid)
   test_projections(scribe, testUser.userid)
   
   # Delete Test user
   delete_user(scribe, testUser)
   
   scribe.closeConnections()
   
   
"""
import os
from random import randint

from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import APP_PATH, PUBLIC_USER
from LmServer.common.log import UnittestLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.scenario import Scenario
from LmServer.legion.sdmproj import SDMProjection
from LmServer.base.lmobj import LMError
from LmTest.functionalTests.scribeTestsForWeb import *

epsg = 4326
userId = DEFAULT_POST_USER
scribe = BorgScribe(UnittestLogger())
scribe.openConnections()

elyrs = scribe.listEnvLayers(0, 10, userId=PUBLIC_USER, atom=True)
lyrs = scribe.listLayers(0, 10, userId=PUBLIC_USER, atom=True)

# Create Test user
postUser = test_user(scribe, 'tester', 'tester@null.nowhere')

fn = os.path.join(APP_PATH, 'LmTest', 'data', 'layers', 'lyr266.tif')
postName = 'testLyr{0}'.format(randint(0, 10000))

postELyr = EnvLayer(postName, userId, epsg, dlocation=fn, dataFormat='GTiff')
postedELyr = scribe.findOrInsertEnvLayer(postELyr)    
assert postedELyr.getId() is not None
 
# Get
elyrId = postedELyr.getId()
getELyr1 = scribe.getEnvLayer(envlyrId=elyrId)
assert getELyr1 is not None
assert getELyr1.getId() == elyrId
 
getELyr2 = scribe.getEnvLayer(lyrId=postedELyr.getLayerId())
assert getELyr2 is not None
assert getELyr2.getId() == elyrId

getELyr3 = scribe.getEnvLayer(lyrVerify=postedELyr.verify)
assert getELyr3 is not None
assert getELyr3.getId() == elyrId

getELyr4 = scribe.getEnvLayer(userId=postedELyr.getUserId(), 
                           lyrName=postedELyr.name, epsg=postedELyr.epsgcode)
assert getELyr4 is not None
assert getELyr4.getId() == elyrId

# Count
lyrCountUsr = scribe.countEnvLayers(userId=userId)
lyrCountPub = scribe.countEnvLayers()
assert lyrCountUsr >= 1 # Posted a layer in this function
assert lyrCountPub >= 0
 
# List
lyrListUsr = scribe.listEnvLayers(0, 100, userId=userId)
lyrListPub = scribe.listEnvLayers(0, 100)
assert len(lyrListUsr) >= 1
assert len(lyrListPub) >= 0 # Check that it is at least a list
 
# Delete
# Assert we successfully delete the layer we retrieved
assert scribe.deleteEnvLayer(getELyr1)

"""
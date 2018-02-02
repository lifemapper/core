"""
@summary: Test occurrence set services
@status: alpha
@author: CJ Grady
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
@note: These tests should be incorporated into some kind of framework
@todo: Evaluate which tests are missing
@todo: User tests
"""
import glob
import json
import os
import random
import shutil
import StringIO
import unittest
import urllib2
import zipfile

from LmCommon.common.lmconstants import JobStatus

from LmServer.base.layer2 import Vector
from LmServer.common.lmconstants import API_PATH
from LmServer.common.localconstants import SCRATCH_PATH
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.occlayer import OccurrenceLayer

# .............................................................................
class TestOccLayerService_anon_COUNT(unittest.TestCase):
   """
   @summary: This is a test class for environmental layer count service
   """
   # ............................
   def setUp(self):
      """
      @summary: Set up test
      """
      self.scribe = BorgScribe(ConsoleLogger())
      self.scribe.openConnections()
      self.deleteFilenames = []
   
   # ............................
   def tearDown(self):
      """
      @summary: Clean up after test
      """
      self.scribe.closeConnections()
      for fn in self.deleteFilenames:
         try:
            shutil.rmtree(fn)
         except:
            pass

   # ............................
   def test_http_anon(self):
      """
      @summary: Test count over http with the anonymous user
      """
      scribeCount = self.scribe.countOccurrenceSets()
      countUrl = '{}/occurrence/count'.format(API_PATH)
      with open(urllib2.urlopen(countUrl)) as countReq:
         countDict = json.loads(countReq.read())
         
      assert scribeCount == int(countDict)
   
   # ............................
   def test_scribe_anon(self):
      """
      @summary: Test count with scribe
      """
      scribeCount = self.scribe.countOccurrenceSets()
      assert scribeCount >= 0
   
# .............................................................................
class TestOccLayerService_anon_GET_data(unittest.TestCase):
   """
   @summary: This is a test class for environmental layer services
   """
   # ............................
   def setUp(self):
      """
      @summary: Set up test
      """
      self.scribe = BorgScribe(ConsoleLogger())
      self.scribe.openConnections()
   
   # ............................
   def tearDown(self):
      """
      @summary: Clean up after test
      """
      self.scribe.closeConnections()
   
   # ............................
   def test_http_anon_public(self):
      """
      @summary: Test that data for public occurrence layer can be 
                   retrieved via HTTP
      """
      occLyrListUrl = '{}/occurrence/?status={}'.format(API_PATH, 
                                                        JobStatus.COMPLETE)
      with open(urllib2.urlopen(occLyrListUrl)) as occListReq:
         occsResp = json.loads(occListReq.read())
         
      occ = random.choice(occsResp)
      
      occLyrUrl = '{}/shapefile'.format(occ.url)

      with open(urllib2.urlopen(occLyrUrl)) as occLyrReq:
         occLyrCont = occLyrReq.read()


      # Get a place to write the files
      tempDir = os.path.join(SCRATCH_PATH, 'testOcc{}'.format(
                                                     random.randint(0, 10000)))
      
      # Write the files
      with open(zipfile.ZipFile(StringIO.StringIO(occLyrCont))) as zipF:
         zipF.extractall(tempDir)

      # Add the directory to the delete list
      self.deleteFilenames.append(tempDir)

      # Test the files
      for fn in glob.glob(os.path.join(tempDir, '*')):
         if fn.endswith('.shp'):
            assert Vector.testVector(fn)
      
   ## ............................
   #def test_http_anon_user(self):
   #   """
   #   @summary: Test that attempting to retrieve metadata for a user object 
   #                fails with a 403 permission error when tried to access 
   #                publicly.
   #   """
   #   envLyrAtoms = self.scribe.listEnvLayers(0, 10, userId=)
   #   lyrAtom = random.choice(envLyrAtoms)
   #   envLyrUrl = '{}/occurrence/{}'.format(API_PATH, lyrAtom.id)
   #   with open(urllib2.urlopen(envLyrUrl)) as envLyrReq:
   #      lyrJson = json.loads(envLyrReq.read())
   #   
   #   # TODO: Check that fails
   #   assert True
   
   # ............................
   def test_scribe_anon(self):
      """
      @summary: Test that an occurrence layer object can be retrieved via 
                   the scribe.
      """
      occAtoms = self.scribe.listOccurrenceSets(0, 10, status=JobStatus.COMPLETE)
      occAtom = random.choice(occAtoms)
      occ = self.scribe.getOccurrenceSet(occId=occAtom.id)
      assert isinstance(occ, OccurrenceLayer)
      assert Vector.testVector(occ.getDLocation())
   
# .............................................................................
class TestOccLayerService_anon_GET_map(unittest.TestCase):
   """
   @summary: This is a test class for occurrence layer maps
   """
   # ............................
   def setUp(self):
      """
      @summary: Set up test
      """
      self.scribe = BorgScribe(ConsoleLogger())
      self.scribe.openConnections()
   
   # ............................
   def tearDown(self):
      """
      @summary: Clean up after test
      """
      self.scribe.closeConnections()
   
   # ............................
   def test_http_anon_public(self):
      """
      @summary: Test that data for public occurrence layer can be 
                   retrieved via HTTP
      """
      occLyrListUrl = '{}/occurrence/?status={}'.format(API_PATH, 
                                                        JobStatus.COMPLETE)
      with open(urllib2.urlopen(occLyrListUrl)) as occListReq:
         occsResp = json.loads(occListReq.read())
         
      occAtom = random.choice(occsResp)
      
      # Get the occurrence set
      with open(urllib2.urlopen(occAtom.url)) as occReq:
         occ = json.loads(occReq.read())
         
      # Get capabilities url
      endpoint = occ['map']['endpoint']
      mapName = occ['map']['mapName']
      lyrName = occ['map']['layerName']
      
      getCapabilitiesUrl = '{}?mapName={}&service=WMS&request=GetCapabilities&version=1.1.0'.format(endpoint, mapName)
      
      # Get occurrence layer map
      occMap = '{}?mapName={}&service=WMS&request=GetMap&height=200&width=400&bbox=-180,-90,180,90&srs=epsg:4326&format=image/png&color=ff0000&version=1.1.0&styles=&layers={}'.format(endpoint, mapName, lyrName)

      # Get occurrence layer map with background
      occMapBmng = '{}?mapName={}&service=WMS&request=GetMap&height=200&width=400&bbox=-180,-90,180,90&srs=epsg:4326&format=image/png&color=ff0000&version=1.1.0&styles=&layers=bmng,{}'.format(endpoint, mapName, lyrName)
      
      
      # Get capabilities
      with open(urllib2.urlopen(getCapabilitiesUrl)) as occGetCapabilitiesReq:
         getCapResp = occGetCapabilitiesReq.read()
      
      # TODO: test get capabilities response
      assert True
      
      # Get occurrence layer map
      with open(urllib2.urlopen(occMap)) as occGetMapReq:
         getMapResp = occGetMapReq.read()
      
      # TODO: Test get map response
      assert True
      
      # Get occurrence layer map with background
      with open(urllib2.urlopen(occMapBmng)) as occGetMapBmngReq:
         getMapBmngResp = occGetMapBmngReq.read()
      
      # TODO: Test get map response with background
      assert True
      
# .............................................................................
class TestOccLayerService_anon_GET_metadata(unittest.TestCase):
   """
   @summary: This is a test class for environmental layer services
   """
   # ............................
   def setUp(self):
      """
      @summary: Set up test
      """
      self.scribe = BorgScribe(ConsoleLogger())
      self.scribe.openConnections()
   
   # ............................
   def tearDown(self):
      """
      @summary: Clean up after test
      """
      self.scribe.closeConnections()
   
   # ............................
   def test_http_anon_public(self):
      """
      @summary: Test that metadata for public occurrence layer can be 
                   retrieved via HTTP
      """
      occLyrListUrl = '{}/occurrence/?status={}'.format(API_PATH, 
                                                        JobStatus.COMPLETE)
      with open(urllib2.urlopen(occLyrListUrl)) as occListReq:
         occsResp = json.loads(occListReq.read())
         
      occ = random.choice(occsResp)
      
      with open(urllib2.urlopen(occ.url)) as occLyrReq:
         occJson = json.loads(occLyrReq.read())

      # TODO: Test occJson
      assert True
      
   ## ............................
   #def test_http_anon_user(self):
   #   """
   #   @summary: Test that attempting to retrieve metadata for a user object 
   #                fails with a 403 permission error when tried to access 
   #                publicly.
   #   """
   #   envLyrAtoms = self.scribe.listEnvLayers(0, 10, userId=)
   #   lyrAtom = random.choice(envLyrAtoms)
   #   envLyrUrl = '{}/occurrence/{}'.format(API_PATH, lyrAtom.id)
   #   with open(urllib2.urlopen(envLyrUrl)) as envLyrReq:
   #      lyrJson = json.loads(envLyrReq.read())
   #   
   #   # TODO: Check that fails
   #   assert True
   
   # ............................
   def test_scribe_anon(self):
      """
      @summary: Test that an occurrence layer object can be retrieved via 
                   the scribe.
      """
      occAtoms = self.scribe.listOccurrenceSets(0, 10, status=JobStatus.COMPLETE)
      occAtom = random.choice(occAtoms)
      occ = self.scribe.getOccurrenceSet(occId=occAtom.id)
      assert isinstance(occ, OccurrenceLayer)
      assert Vector.testVector(occ.getDLocation())
   
# .............................................................................
class TestOccLayerService_anon_LIST(unittest.TestCase):
   """
   @summary: This is a test class for environmental layer services
   """
   # ............................
   def setUp(self):
      """
      @summary: Set up test
      """
      self.scribe = BorgScribe(ConsoleLogger())
      self.scribe.openConnections()
   
   # ............................
   def tearDown(self):
      """
      @summary: Clean up after test
      """
      self.scribe.closeConnections()
   
   # ............................
   def test_http_anon(self):
      """
      @summary: Test list over http with the anonymous user
      """
      occLyrAtoms = self.scribe.listOccurrenceSets(0, 20)
      listUrl = '{}/occurrence/?limit=20'.format(API_PATH)
      with open(urllib2.urlopen(listUrl)) as listReq:
         listDict = json.loads(listReq.read())

      assert len(occLyrAtoms) == len(listDict)

   # ............................
   def test_scribe_anon(self):
      """
      @summary: Test list with scribe
      """
      occLyrAtoms = self.scribe.listOccurrenceSets(0, 10)
      assert len(occLyrAtoms) <= 10
   
# .............................................................................
class TestOccLayerService_anon_POST(unittest.TestCase):
   """
   @summary: This is a test class for environmental layer services
   """
   # ............................
   def setUp(self):
      """
      @summary: Set up test
      """
      self.scribe = BorgScribe(ConsoleLogger())
      self.scribe.openConnections()
   
   # ............................
   def tearDown(self):
      """
      @summary: Clean up after test
      """
      self.scribe.closeConnections()
   
# .............................................................................
def getTestSuites():
   """
   @summary: Get the test suites from this module
   """
   loader = unittest.TestLoader()
   testSuites = []
   testSuites.append(
      loader.loadTestsFromTestCase(TestOccLayerService_anon_COUNT))
   testSuites.append(
      loader.loadTestsFromTestCase(TestOccLayerService_anon_GET_data))
   testSuites.append(
      loader.loadTestsFromTestCase(TestOccLayerService_anon_GET_map))
   testSuites.append(
      loader.loadTestsFromTestCase(TestOccLayerService_anon_GET_metadata))
   testSuites.append(
      loader.loadTestsFromTestCase(TestOccLayerService_anon_LIST))
   testSuites.append(
      loader.loadTestsFromTestCase(TestOccLayerService_anon_POST))

   return testSuites

# .............................................................................
if __name__ == '__main__':
   import logging
   logging.basicConfig(level=logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
      
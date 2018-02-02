"""
@summary: Test bare layer services
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
import json
import random
import shutil
import tempfile
import unittest
import urllib2

from LmServer.base.layer2 import Raster
from LmServer.common.log import ConsoleLogger
from LmServer.common.lmconstants import API_PATH
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
class TestLayerService_anon_COUNT(unittest.TestCase):
   """
   @summary: This is a test class for layer count service
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
      scribeCount = self.scribe.countLayers()
      countUrl = '{}/layer/count'.format(API_PATH)
      with open(urllib2.urlopen(countUrl)) as countReq:
         countDict = json.loads(countReq.read())
         
      assert scribeCount == int(countDict)
   
   # ............................
   def test_scribe_anon(self):
      """
      @summary: Test count with scribe
      """
      scribeCount = self.scribe.countLayers()
      assert scribeCount >= 0
   
# .............................................................................
class TestLayerService_anon_GET_data(unittest.TestCase):
   """
   @summary: This is a test class for layer services
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
      @summary: Test that metadata for public layer can be 
                   retrieved via HTTP
      """
      lyrListUrl = '{}/layer/'.format(API_PATH)
      with open(urllib2.urlopen(lyrListUrl)) as lyrListReq:
         lyrsResp = json.loads(lyrListReq.read())
         
      lyr = random.choice(lyrsResp)
      
      lyrUrl = '{}/GTiff'.format(lyr.url)

      with open(urllib2.urlopen(lyrUrl)) as lyrReq:
         lyrCont = lyrReq.read()


      # TODO: Check content type and handle vector


      tmpF = tempfile.NamedTemporaryFile(delete=False)
      tmpF.write(lyrCont)
      tmpFn = tmpF.name
      tmpF.close()
      self.deleteFilenames.append(tmpFn)

      assert Raster.testRaster(tmpFn)
      
   ## ............................
   #def test_http_anon_user(self):
   #   """
   #   @summary: Test that attempting to retrieve metadata for a user object 
   #                fails with a 403 permission error when tried to access 
   #                publicly.
   #   """
   #   envLyrAtoms = self.scribe.listEnvLayers(0, 10, userId=)
   #   lyrAtom = random.choice(envLyrAtoms)
   #   envLyrUrl = '{}/layer/{}'.format(API_PATH, lyrAtom.id)
   #   with open(urllib2.urlopen(envLyrUrl)) as envLyrReq:
   #      lyrJson = json.loads(envLyrReq.read())
   #   
   #   # TODO: Check that fails
   #   assert True
   
   # ............................
   def test_scribe_anon(self):
      """
      @summary: Test that a layer object can be retrieved via 
                   the scribe.
      """
      lyrAtoms = self.scribe.listLayers(0, 10)
      lyrAtom = random.choice(lyrAtoms)
      lyr = self.scribe.getLayer(lyrId=lyrAtom.id)
      assert isinstance(lyr, Raster)
      assert Raster.testRaster(lyr.getDLocation())
   
# .............................................................................
class TestLayerService_anon_GET_metadata(unittest.TestCase):
   """
   @summary: This is a test class for layer services
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
      @summary: Test that metadata for public layer can be 
                   retrieved via HTTP
      """
      lyrListUrl = '{}/layer/'.format(API_PATH)
      with open(urllib2.urlopen(lyrListUrl)) as lyrListReq:
         lyrsResp = json.loads(lyrListReq.read())
         
      lyr = random.choice(lyrsResp)
      
      with open(urllib2.urlopen(lyr.url)) as lyrReq:
         lyrJson = json.loads(lyrReq.read())

      # TODO: Test lyrJson
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
   #   envLyrUrl = '{}/layer/{}'.format(API_PATH, lyrAtom.id)
   #   with open(urllib2.urlopen(envLyrUrl)) as envLyrReq:
   #      lyrJson = json.loads(envLyrReq.read())
   #   
   #   # TODO: Check that fails
   #   assert True
   
   # ............................
   def test_scribe_anon(self):
      """
      @summary: Test that an environmental layer object can be retrieved via 
                   the scribe.
      """
      lyrAtoms = self.scribe.listLayers(0, 10)
      lyrAtom = random.choice(lyrAtoms)
      lyr = self.scribe.getLayer(lyrId=lyrAtom.id)
      assert isinstance(lyr, Raster)
   
# .............................................................................
class TestLayerService_anon_LIST(unittest.TestCase):
   """
   @summary: This is a test class for layer services
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
      lyrAtoms = self.scribe.listLayers(0, 20)
      listUrl = '{}/layer/?limit=20'.format(API_PATH)
      with open(urllib2.urlopen(listUrl)) as listReq:
         listDict = json.loads(listReq.read())

      assert len(lyrAtoms) == len(listDict)

   # ............................
   def test_scribe_anon(self):
      """
      @summary: Test list with scribe
      """
      lyrAtoms = self.scribe.listLayers(0, 10)
      assert len(lyrAtoms) <= 10
   
# .............................................................................
class TestLayerService_anon_POST(unittest.TestCase):
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
      loader.loadTestsFromTestCase(TestLayerService_anon_COUNT))
   testSuites.append(
      loader.loadTestsFromTestCase(TestLayerService_anon_GET_data))
   testSuites.append(
      loader.loadTestsFromTestCase(TestLayerService_anon_GET_metadata))
   testSuites.append(
      loader.loadTestsFromTestCase(TestLayerService_anon_LIST))
   testSuites.append(
      loader.loadTestsFromTestCase(TestLayerService_anon_POST))

   return testSuites

# .............................................................................
if __name__ == '__main__':
   import logging
   logging.basicConfig(level=logging.DEBUG)

   for suite in getTestSuites():
      unittest.TextTestRunner(verbosity=2).run(suite)
      
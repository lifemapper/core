"""
@summary: Test layer services
@status: alpha
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
"""
import argparse
import contextlib
import json
import unittest
import warnings

from LmServer.base.layer2 import Raster, Vector
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmTest.webTestsLite.common.userUnitTest import UserTestCase
from LmTest.webTestsLite.common.webClient import LmWebClient

# .............................................................................
class TestScribeLayerService(UserTestCase):
   """
   @summary: This is a test class for running scribe tests for the layer service
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
   def test_count(self):
      """
      @summary: Basic test counting all layers for a user
      """
      count = self.scribe.countLayers(userId=self._get_session_user())
      assert count >= 0
      if count == 0:
         warnings.warn(
            'Count returned 0 layers for user: {}'.format(
                                                     self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an layer object belonging to a  
                   user
      """
      lyrAtoms = self.scribe.listLayers(0, 1, userId=self._get_session_user())
      if len(lyrAtoms) == 0:
         self.fail('Cannot get a layer because listing found none')
      else:
         lyr = self.scribe.getLayer(lyrId=lyrAtoms[0].id, 
                                       userId=self._get_session_user())
         self.assertIsInstance(lyr, (Raster, Vector))
         self.assertEqual(lyr.getUserId(), self._get_session_user(), 
               'User id on layer = {}, session user = {}'.format(
                  lyr.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_atoms(self):
      """
      @summary: Basic test that tries to get a list of layer atoms from the 
                   scribe
      """
      lyrAtoms = self.scribe.listLayers(0, 1, userId=self._get_session_user())
      self.assertGreaterEqual(len(lyrAtoms), 0)
      if len(lyrAtoms) == 0:
         warnings.warn('List returned 0 layers for user: {}'.format(
                                                     self._get_session_user()))
      else:
         lyr = self.scribe.getLayer(lyrId=lyrAtoms[0].id, 
                                       userId=self._get_session_user())
         self.assertEqual(lyr.getUserId(), self._get_session_user(), 
               'User id on layer = {}, session user = {}'.format(
                  lyr.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_objects(self):
      """
      @summary: Basic test that tries to get a list of layer objects from the 
                   scribe
      """
      lyrObjs = self.scribe.listLayers(0, 1, atom=False,
                                              userId=self._get_session_user())
      self.assertGreaterEqual(len(lyrObjs), 0)
      if len(lyrObjs) == 0:
         warnings.warn('List returned 0 layers for user: {}'.format(
               self._get_session_user()))
      else:
         self.assertIsInstance(lyrObjs[0], (Raster, Vector))
         self.assertEqual(lyrObjs[0].getUserId(), self._get_session_user(), 
               'User id on layer = {}, session user = {}'.format(
                  lyrObjs[0].getUserId(), self._get_session_user()))

# .............................................................................
class TestWebLayerService(UserTestCase):
   """
   @summary: This is a test class for running web tests for the layer service
   """
   # ............................
   def setUp(self):
      """
      @summary: Set up test
      """
      self.cl = LmWebClient()
      if self.userId is not None:
         # Log in
         self.cl.login(self.userId, self.passwd)
      
   # ............................
   def tearDown(self):
      """
      @summary: Clean up after test
      """
      if self.userId is not None:
         # Log out
         self.cl.logout()
      self.cl = None
      
   # ............................
   def test_count(self):
      """
      @summary: Basic test counting all layers for a user
      """
      with contextlib.closing(self.cl.count_layers()) as x:
         ret = json.load(x)
      count = int(ret['count'])
      
      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn('Count returned 0 layers for user: {}'.format(
               self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get a layer object belonging to a user
      """
      with contextlib.closing(self.cl.list_layers()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         self.fail('Cannot get a layer because listing found none')
      else:
         layerId = ret[0]['id']
         
         with contextlib.closing(self.cl.get_layer(layerId)) as x:
            lyrMeta = json.load(x)
            
         self.assertTrue(lyrMeta.has_key('id'))
         self.assertEqual(lyrMeta['user'], self._get_session_user(), 
               'User id on layer = {}, session user = {}'.format(
                  lyrMeta['user'], self._get_session_user()))
   
   # ............................
   def test_list(self):
      """
      @summary: Basic test that tries to get a list of layer atoms from the web
      """
      with contextlib.closing(self.cl.list_layers()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         warnings.warn('Count returned 0 layers for user: {}'.format(
               self._get_session_user()))
   
# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   return [
      TestScribeLayerService,
      TestWebLayerService
   ]

# .............................................................................
def get_test_suite(userId=None, pwd=None):
   """
   @summary: Get test suite for this module.  Always get public tests and get
                user tests if user information is provided
   @param userId: The id of the user to use for tests
   @param pwd: The password of the specified user
   """
   suite = unittest.TestSuite()
   suite.addTest(UserTestCase.parameterize(TestScribeLayerService))
   suite.addTest(UserTestCase.parameterize(TestWebLayerService))

   if userId is not None:
      suite.addTest(UserTestCase.parameterize(TestScribeLayerService, 
                                              userId=userId, pwd=pwd))
      suite.addTest(UserTestCase.parameterize(TestWebLayerService, 
                                              userId=userId, pwd=pwd))
      
   return suite

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Run layer service tests')
   parser.add_argument('-u', '--user', type=str, 
                 help='If provided, run tests for this user (and anonymous)' )
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')
   
   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

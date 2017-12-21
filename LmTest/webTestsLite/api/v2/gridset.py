"""
@summary: Test gridset services
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
@todo: Recurse into subservices
"""
import argparse
import contextlib
import json
import unittest
import warnings

from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.gridset import Gridset
from LmTest.webTestsLite.common.userUnitTest import UserTestCase
from LmTest.webTestsLite.common.webClient import LmWebClient

# .............................................................................
class TestScribeGridsetService(UserTestCase):
   """
   @summary: This is a test class for running scribe tests for the gridset 
                service
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
      @summary: Basic test counting all gridsets for a user
      """
      count = self.scribe.countGridsets(self._get_session_user())
      assert count >= 0
      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn(
            'Count returned 0 gridsets for user: {}'.format(
               self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get a gridset object belonging to a  
                   user
      """
      gsAtoms = self.scribe.listGridsets(0, 1, userId=self._get_session_user())
      if len(gsAtoms) == 0:
         self.fail(
            'Cannot get a gridset because listing found none')
      else:
         gs = self.scribe.getGridset(gridsetId=gsAtoms[0].id, 
                                     userId=self._get_session_user())
         self.assertIsInstance(gs, Gridset)
         self.assertEqual(gs.getUserId(), self._get_session_user(), 
               'User id on gridset = {}, session user = {}'.format(
                  gs.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_atoms(self):
      """
      @summary: Basic test that tries to get a list of gridset atoms from the
                   scribe
      """
      gsAtoms = self.scribe.listGridsets(0, 1, userId=self._get_session_user())
      self.assertGreaterEqual(len(gsAtoms), 0)
      if len(gsAtoms) == 0:
         warnings.warn('List returned 0 gridsets for user: {}'.format(
               self._get_session_user()))
      else:
         gs = self.scribe.getGridset(gridsetId=gsAtoms[0].id, 
                                       userId=self._get_session_user())
         self.assertEqual(gs.getUserId(), self._get_session_user(), 
               'User id on gridset = {}, session user = {}'.format(
                  gs.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_objects(self):
      """
      @summary: Basic test that tries to get a list of gridset objects from the
                   scribe
      """
      gsObjs = self.scribe.listGridsets(0, 1, atom=False,
                                        userId=self._get_session_user())
      self.assertGreaterEqual(len(gsObjs), 0)
      if len(gsObjs) == 0:
         warnings.warn(
            'List returned 0 gridsets for user: {}'.format(
               self._get_session_user()))
      else:
         self.assertIsInstance(gsObjs[0], Gridset)
         self.assertEqual(gsObjs[0].getUserId(), self._get_session_user(), 
               'User id on gridset {} = {}, session user = {}'.format(
                  gsObjs[0].getId(), gsObjs[0].getUserId(), 
                  self._get_session_user()))

# .............................................................................
class TestWebGridsetService(UserTestCase):
   """
   @summary: This is a test class for running web tests for the gridset service
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
      @summary: Basic test counting all gridsets for a user
      """
      with contextlib.closing(self.cl.count_gridsets()) as x:
         ret = json.load(x)
      count = int(ret['count'])
      
      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn(
            'Count returned 0 gridsets for user: {}'.format(
               self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get a gridset object belonging to a 
                   user
      """
      with contextlib.closing(self.cl.list_gridsets()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         self.fail(
            'Cannot get a gridset listing found none')
      else:
         layerId = ret[0]['id']
         
         with contextlib.closing(self.cl.get_gridset(layerId)) as x:
            gsMeta = json.load(x)
            
         self.assertTrue(gsMeta.has_key('name'))
         self.assertEqual(gsMeta['user'], self._get_session_user(), 
               'User id on gridset {} = {}, session user = {}'.format(
                  gsMeta['id'], gsMeta['user'], self._get_session_user()))
   
   # ............................
   def test_list(self):
      """
      @summary: Basic test that tries to get a list of gridset atoms from the 
                   web
      """
      with contextlib.closing(self.cl.list_gridsets()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         warnings.warn(
            'Count returned 0 gridsets for user: {}'.format(
               self._get_session_user()))
   
# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   return [
      TestScribeGridsetService,
      TestWebGridsetService
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
   suite.addTest(UserTestCase.parameterize(TestScribeGridsetService))
   suite.addTest(UserTestCase.parameterize(TestWebGridsetService))

   if userId is not None:
      suite.addTest(UserTestCase.parameterize(TestScribeGridsetService, 
                                              userId=userId, pwd=pwd))
      suite.addTest(UserTestCase.parameterize(TestWebGridsetService, 
                                              userId=userId, pwd=pwd))
      
   return suite

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Run gridset service tests')
   parser.add_argument('-u', '--user', type=str, 
                 help='If provided, run tests for this user (and anonymous)' )
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')
   
   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

"""
@summary: Test occurrence set services
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

from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.occlayer import OccurrenceLayer
from LmTest.webTestsLite.common.userUnitTest import UserTestCase
from LmTest.webTestsLite.common.webClient import LmWebClient

# .............................................................................
class TestScribeOccurrenceService(UserTestCase):
   """
   @summary: This is a test class for running scribe tests for the occurrence 
                set service
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
      @summary: Basic test counting all occurrence sets for a user
      """
      count = self.scribe.countOccurrenceSets(userId=self._get_session_user())
      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn(
            'Count returned 0 occurrence sets for user: {}'.format(
               self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an occurrence set object 
                   belonging to a user
      """
      occAtoms = self.scribe.listOccurrenceSets(0, 1,
                                               userId=self._get_session_user())
      if len(occAtoms) == 0:
         self.fail(
            'Cannot get an occurrence set because listing found none')
      else:
         occ = self.scribe.getOccurrenceSet(occId=occAtoms[0].id, 
                                       userId=self._get_session_user())
         self.assertIsInstance(occ, OccurrenceLayer)
         self.assertEqual(occ.getUserId(), self._get_session_user(), 
               'User id on occurrence = {}, session user = {}'.format(
                                    occ.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_atoms(self):
      """
      @summary: Basic test that tries to get a list of occurrence set atoms 
                   from the scribe
      """
      occAtoms = self.scribe.listOccurrenceSets(0, 1, 
                                              userId=self._get_session_user())
      self.assertGreaterEqual(len(occAtoms), 0)
      if len(occAtoms) == 0:
         warnings.warn(
            'List returned 0 occurrence sets for user: {}'.format(
               self._get_session_user()))
      else:
         occ = self.scribe.getOccurrenceSet(occId=occAtoms[0].id, 
                                       userId=self._get_session_user())
         self.assertEqual(occ.getUserId(), self._get_session_user(), 
               'User id on occurrence set = {}, session user = {}'.format(
                                    occ.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_objects(self):
      """
      @summary: Basic test that tries to get a list of occurrence set objects
                   from the scribe
      """
      occObjs = self.scribe.listOccurrenceSets(0, 1, atom=False,
                                              userId=self._get_session_user())
      self.assertGreaterEqual(len(occObjs), 0)
      if len(occObjs) == 0:
         warnings.warn(
            'List returned 0 occurrence sets for user: {}'.format(
                                                     self._get_session_user()))
      else:
         self.assertIsInstance(occObjs[0], OccurrenceLayer)
         self.assertEqual(occObjs[0].getUserId(), self._get_session_user(), 
               'User id on occurrence set = {}, session user = {}'.format(
                             occObjs[0].getUserId(), self._get_session_user()))

# .............................................................................
class TestWebOccurrenceService(UserTestCase):
   """
   @summary: This is a test class for running web tests for the occurrence set 
                service
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
      @summary: Basic test counting all occurrence sets for a user
      """
      with contextlib.closing(self.cl.count_occurrence_sets()) as x:
         ret = json.load(x)
      count = int(ret['count'])
      
      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn(
            'Count returned 0 occurrence sets for user: {}'.format(
                                                     self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an occurrence set object belonging 
                   to a user
      """
      with contextlib.closing(self.cl.list_occurrence_sets()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         self.fail(
            'Cannot get an occurrence set because listing found none')
      else:
         occId = ret[0]['id']
         
         with contextlib.closing(self.cl.get_occurrence_set(occId)) as x:
            occMeta = json.load(x)
            
         self.assertTrue(occMeta.has_key('speciesName'))
         self.assertEqual(occMeta['user'], self._get_session_user(), 
               'User id on occurrence set = {}, session user = {}'.format(
                                    occMeta['user'], self._get_session_user()))
   
   # ............................
   def test_list(self):
      """
      @summary: Basic test that tries to get a list of occurrence set atoms 
                   from the web
      """
      with contextlib.closing(self.cl.list_occurrence_sets()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         warnings.warn(
            'Count returned 0 occurrence sets for user: {}'.format(
               self._get_session_user()))
   
# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   return [
      TestScribeOccurrenceService,
      TestWebOccurrenceService
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
   suite.addTest(UserTestCase.parameterize(TestScribeOccurrenceService))
   suite.addTest(UserTestCase.parameterize(TestWebOccurrenceService))

   if userId is not None:
      suite.addTest(UserTestCase.parameterize(TestScribeOccurrenceService, 
                                              userId=userId, pwd=pwd))
      suite.addTest(UserTestCase.parameterize(TestWebOccurrenceService, 
                                              userId=userId, pwd=pwd))
      
   return suite

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                        description='Run occurrence service tests')
   parser.add_argument('-u', '--user', type=str, 
                 help='If provided, run tests for this user (and anonymous)' )
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')
   
   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

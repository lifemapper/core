"""
@summary: Test scenario services
@status: alpha
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import EML_INTERFACE, JSON_INTERFACE
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.scenario import Scenario
from LmTest.formatTests.emlValidator import validate_eml
from LmTest.formatTests.jsonValidator import validate_json
from LmTest.webTestsLite.common.userUnitTest import UserTestCase
from LmTest.webTestsLite.common.webClient import LmWebClient


# .............................................................................
class TestScribeScenarioService(UserTestCase):
   """
   @summary: This is a test class for running scribe tests for the 
                scenario service
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
      @summary: Basic test counting all scenarios for a user
      """
      count = self.scribe.countScenarios(userId=self._get_session_user())
      assert count >= 0
      if count == 0:
         warnings.warn(
            'Count returned 0 scenarios for user: {}'.format(
               self._get_session_user()))

   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an scenario object belonging to a 
                   user
      """
      scnAtoms = self.scribe.listScenarios(0, 1,
                                              userId=self._get_session_user())
      if len(scnAtoms) == 0:
         self.fail(
            'Cannot get an scenario because listing found none')
      else:
         scn = self.scribe.getScenario(scnAtoms[0].get_id(),
                                               userId=self._get_session_user())
         self.assertIsInstance(scn, Scenario)
         self.assertEqual(scn.getUserId(), self._get_session_user(),
               'User id on scenario = {}, session user = {}'.format(
                                    scn.getUserId(), self._get_session_user()))

   # ............................
   def test_list_atoms(self):
      """
      @summary: Basic test that tries to get a list of scenario atoms from the 
                   scribe
      """
      scnAtoms = self.scribe.listScenarios(0, 1,
                                              userId=self._get_session_user())
      self.assertGreaterEqual(len(scnAtoms), 0)
      if len(scnAtoms) == 0:
         warnings.warn(
            'List returned 0 scenarios for user: {}'.format(
               self._get_session_user()))
      else:
         scn = self.scribe.getScenario(scnAtoms[0].get_id(),
                                               userId=self._get_session_user())
         self.assertEqual(scn.getUserId(), self._get_session_user(),
               'User id on scenario = {}, session user = {}'.format(
                                    scn.getUserId(), self._get_session_user()))

   # ............................
   def test_list_objects(self):
      """
      @summary: Basic test that tries to get a list of scenario objects from 
                   the scribe
      """
      scnObjs = self.scribe.listScenarios(0, 1, atom=False,
                                              userId=self._get_session_user())
      self.assertGreaterEqual(len(scnObjs), 0)
      if len(scnObjs) == 0:
         warnings.warn(
            'List returned 0 scenarios for user: {}'.format(
               self._get_session_user()))
      else:
         self.assertIsInstance(scnObjs[0], Scenario)
         self.assertEqual(scnObjs[0].getUserId(), self._get_session_user(),
               'User id on scenario = {}, session user = {}'.format(
                             scnObjs[0].getUserId(), self._get_session_user()))


# .............................................................................
class TestWebScenarioService(UserTestCase):
   """
   @summary: This is a test class for running web tests for the scenario  
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
      @summary: Basic test counting all scenarios for a user
      """
      with contextlib.closing(self.cl.count_scenarios()) as x:
         ret = json.load(x)
      count = int(ret['count'])

      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn(
            'Count returned 0 scenarios for user: {}'.format(
               self._get_session_user()))

   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an scenario object 
                   belonging to a user
      """
      with contextlib.closing(self.cl.list_scenarios()) as x:
         ret = json.load(x)

      if len(ret) == 0:
         self.fail(
            'Cannot get an scenario because listing found none')
      else:
         scnId = ret[0]['id']

         with contextlib.closing(self.cl.get_scenario(scnId)) as x:
            scnMeta = json.load(x)

         self.assertTrue('code' in scnMeta)
         self.assertEqual(scnMeta['user'], self._get_session_user(),
               'User id on scenario = {}, session user = {}'.format(
                                    scnMeta['user'], self._get_session_user()))

         # EML
         with contextlib.closing(self.cl.get_scenario(scnId,
                                          responseFormat=EML_INTERFACE)) as x:
            self.assertTrue(validate_eml(x))

         # JSON
         with contextlib.closing(self.cl.get_scenario(scnId,
                                          responseFormat=JSON_INTERFACE)) as x:
            self.assertTrue(validate_json(x))

   # ............................
   def test_list(self):
      """
      @summary: Basic test that tries to get a list of scenario atoms from the 
                   web
      """
      with contextlib.closing(self.cl.list_scenarios()) as x:
         ret = json.load(x)

      if len(ret) == 0:
         warnings.warn(
            'Count returned 0 scenarios for user: {}'.format(
                                                     self._get_session_user()))


# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   return [
      TestScribeScenarioService,
      TestWebScenarioService
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
   suite.addTest(UserTestCase.parameterize(TestScribeScenarioService))
   suite.addTest(UserTestCase.parameterize(TestWebScenarioService))

   if userId is not None:
      suite.addTest(UserTestCase.parameterize(TestScribeScenarioService,
                                              userId=userId, pwd=pwd))
      suite.addTest(UserTestCase.parameterize(TestWebScenarioService,
                                              userId=userId, pwd=pwd))

   return suite


# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                        description='Run scenario service tests')
   parser.add_argument('-u', '--user', type=str,
                 help='If provided, run tests for this user (and anonymous)')
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')

   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

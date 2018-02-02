"""
@summary: Test matrix services
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
"""
import argparse
import contextlib
import json
import unittest
import warnings

from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.lmmatrix import Matrix
from LmTest.webTestsLite.common.userUnitTest import UserTestCase
from LmTest.webTestsLite.common.webClient import LmWebClient

# .............................................................................
class TestScribeMatrixService(UserTestCase):
   """
   @summary: This is a test class for running scribe tests for the matrix 
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
      @summary: Basic test counting all matrices for a user
      """
      count = self.scribe.countMatrices(userId=self._get_session_user())
      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn('Count returned 0 matrices for user: {}'.format(
                                                     self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an matrix object belonging to a 
                   user
      """
      mtxAtoms = self.scribe.listMatrices(0, 1, 
                                              userId=self._get_session_user())
      if len(mtxAtoms) == 0:
         self.fail('Cannot get an matrix because listing found none')
      else:
         mtx = self.scribe.getMatrix(mtxId=mtxAtoms[0].id, 
                                               userId=self._get_session_user())
         self.assertIsInstance(mtx, Matrix)
         self.assertEqual(mtx.getUserId(), self._get_session_user(), 
               'User id on matrix = {}, session user = {}'.format(
                                    mtx.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_atoms(self):
      """
      @summary: Basic test that tries to get a list of matrix atoms from the 
                   scribe
      """
      mtxAtoms = self.scribe.listMatrices(0, 1, 
                                              userId=self._get_session_user())
      self.assertGreaterEqual(len(mtxAtoms), 0)
      if len(mtxAtoms) == 0:
         warnings.warn('List returned 0 matrices for user: {}'.format(
                                                     self._get_session_user()))
      else:
         mtx = self.scribe.getMatrix(mtxId=mtxAtoms[0].id, 
                                               userId=self._get_session_user())
         self.assertEqual(mtx.getUserId(), self._get_session_user(), 
                          'User id on matrix = {}, session user = {}'.format(
                                    mtx.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_objects(self):
      """
      @summary: Basic test that tries to get a list of matrix objects from the
                   scribe
      """
      mtxObjs = self.scribe.listMatrices(0, 1, atom=False,
                                              userId=self._get_session_user())
      self.assertGreaterEqual(len(mtxObjs), 0)
      if len(mtxObjs) == 0:
         warnings.warn('List returned 0 matrices for user: {}'.format(
               self._get_session_user()))
      else:
         self.assertIsInstance(mtxObjs[0], Matrix)
         self.assertEqual(mtxObjs[0].getUserId(), self._get_session_user(), 
                          'User id on matrix = {}, session user = {}'.format(
                             mtxObjs[0].getUserId(), self._get_session_user()))

# .............................................................................
class TestWebMatrixService(UserTestCase):
   """
   @summary: This is a test class for running web tests for the matrix service
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
      @summary: Basic test counting all matrices for a user
      """
      scribe = BorgScribe(ConsoleLogger())
      scribe.openConnections()
      mtxAtoms = scribe.listMatrices(0, 1, userId=self._get_session_user(), 
                                     atom=False)
      scribe.closeConnections()
      with contextlib.closing(self.cl.count_matrices(
                                                mtxAtoms[0].gridsetId)) as x:
         ret = json.load(x)
      count = int(ret['count'])
      
      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn('Count returned 0 matrices for user: {}'.format(
                                                   self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an matrix object belonging to a 
                   user
      """
      scribe = BorgScribe(ConsoleLogger())
      scribe.openConnections()
      mtxAtoms = scribe.listMatrices(0, 1, userId=self._get_session_user(), 
                                     atom=False)
      scribe.closeConnections()
      if len(mtxAtoms) == 0:
         self.fail('Cannot get an matrix because listing found none')
      else:
         gridsetId = mtxAtoms[0].gridsetId
         mtxId = mtxAtoms[0].id
      
         with contextlib.closing(self.cl.get_matrix(gridsetId, mtxId)) as x:
            mtxMeta = json.load(x)
            
         self.assertTrue(mtxMeta.has_key('matrixType'))
         self.assertEqual(mtxMeta['user'], self._get_session_user(), 
                          'User id on matrix = {}, session user = {}'.format(
                             mtxMeta['user'], self._get_session_user()))
   
   # ............................
   def test_list(self):
      """
      @summary: Basic test that tries to get a list of matrix atoms from the 
                   web
      """
      scribe = BorgScribe(ConsoleLogger())
      scribe.openConnections()
      mtxAtoms = scribe.listMatrices(0, 1, userId=self._get_session_user(), 
                                     atom=False)
      scribe.closeConnections()
      with contextlib.closing(self.cl.list_matrices(mtxAtoms[0].gridsetId)) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         warnings.warn('Count returned 0 matrices for user: {}'.format(
                                                   self._get_session_user()))
   
# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   return [
      TestScribeMatrixService,
      TestWebMatrixService
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
   suite.addTest(UserTestCase.parameterize(TestScribeMatrixService))
   suite.addTest(UserTestCase.parameterize(TestWebMatrixService))

   if userId is not None:
      suite.addTest(UserTestCase.parameterize(TestScribeMatrixService, 
                                              userId=userId, pwd=pwd))
      suite.addTest(UserTestCase.parameterize(TestWebMatrixService, 
                                              userId=userId, pwd=pwd))
      
   return suite

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                        description='Run matrix service tests')
   parser.add_argument('-u', '--user', type=str, 
                 help='If provided, run tests for this user (and anonymous)' )
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')
   
   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

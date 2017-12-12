"""
@summary: Test tree services
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

from LmTest.webTestsLite.common.userUnitTest import UserTestCase
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.log import ConsoleLogger
from LmServer.legion.tree import LmTree
from LmTest.webTestsLite.common.webClient import LmWebClient

# .............................................................................
class TestScribeTreeService(UserTestCase):
   """
   @summary: This is a test class for running scribe tests for the 
                tree service
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
      @summary: Basic test counting all trees for a user
      """
      count = self.scribe.countTrees(userId=self._get_session_user())
      self.assertGreaterEqual(count, 0)
      assert count >= 0
      if count == 0:
         warnings.warn('Count returned 0 trees for user: {}'.format(
               self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an tree object belonging to a user
      """
      treeAtoms = self.scribe.listTrees(0, 1, userId=self._get_session_user())
      if len(treeAtoms) == 0:
         self.fail('Cannot get an tree because listing found none')
      else:
         tree = self.scribe.getTree(treeId=treeAtoms[0].id)
         self.assertIsInstance(tree, LmTree)
         self.assertEqual(tree.getUserId(), self._get_session_user(), 
                              'User id on tree = {}, session user = {}'.format(
                                   tree.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_atoms(self):
      """
      @summary: Basic test that tries to get a list of tree atoms from the 
                   scribe
      """
      treeAtoms = self.scribe.listTrees(0, 1, userId=self._get_session_user())
      self.assertGreaterEqual(len(treeAtoms), 0)
      if len(treeAtoms) == 0:
         warnings.warn('List returned 0 trees for user: {}'.format(
                                                     self._get_session_user()))
      else:
         tree = self.scribe.getTree(treeId=treeAtoms[0].id)
         self.assertEqual(tree.getUserId(), self._get_session_user(), 
                              'User id on tree = {}, session user = {}'.format(
                                   tree.getUserId(), self._get_session_user()))
   
   # ............................
   def test_list_objects(self):
      """
      @summary: Basic test that tries to get a list of tree objects from the 
                   scribe
      """
      treeObjs = self.scribe.listTrees(0, 1, atom=False,
                                              userId=self._get_session_user())
      self.assertGreaterEqual(len(treeObjs), 0)
      if len(treeObjs) == 0:
         warnings.warn('List returned 0 trees for user: {}'.format(
                                                     self._get_session_user()))
      else:
         self.assertIsInstance(treeObjs[0], LmTree)
         self.assertEqual(treeObjs[0], self._get_session_user(), 
                              'User id on tree = {}, session user = {}'.format(
                            treeObjs[0].getUserId(), self._get_session_user()))

# .............................................................................
class TestWebTreeService(UserTestCase):
   """
   @summary: This is a test class for running web tests for the tree service
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
      @summary: Basic test counting all trees for a user
      """
      with contextlib.closing(self.cl.count_trees()) as x:
         ret = json.load(x)
      count = int(ret['count'])
      
      self.assertGreaterEqual(count, 0)
      if count == 0:
         warnings.warn('Count returned 0 trees for user: {}'.format(
                                                     self._get_session_user()))
   
   # ............................
   def test_get(self):
      """
      @summary: Basic test that tries to get an tree object belonging to a user
      """
      with contextlib.closing(self.cl.list_trees()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         self.fail('Cannot get an tree because listing found none')
      else:
         treeId = ret[0]['id']
         
         with contextlib.closing(self.cl.get_tree(treeId)) as x:
            treeMeta = json.load(x)
            
         self.assertTrue(treeMeta.has_key('ultrametric'))
         self.assertEqual(treeMeta['user'], self._get_session_user(), 
                              'User id on tree = {}, session user = {}'.format(
                                 treeMeta['user'], self._get_session_user()))
   
   # ............................
   def test_list(self):
      """
      @summary: Basic test that tries to get a list of tree
                   atoms from the web
      """
      with contextlib.closing(self.cl.list_trees()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         warnings.warn('Count returned 0 trees for user: {}'.format(
                                                     self._get_session_user()))
   
# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   return [
      TestScribeTreeService,
      TestWebTreeService
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
   suite.addTest(UserTestCase.parameterize(TestScribeTreeService))
   suite.addTest(UserTestCase.parameterize(TestWebTreeService))

   if userId is not None:
      suite.addTest(UserTestCase.parameterize(TestScribeTreeService, 
                                              userId=userId, pwd=pwd))
      suite.addTest(UserTestCase.parameterize(TestWebTreeService, 
                                              userId=userId, pwd=pwd))
      
   return suite

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                        description='Run tree service tests')
   parser.add_argument('-u', '--user', type=str, 
                 help='If provided, run tests for this user (and anonymous)' )
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')
   
   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

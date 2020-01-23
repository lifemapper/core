"""Test tree services
"""
import argparse
import contextlib
import json
import unittest
import warnings

from lmpy import TreeWrapper

from LmCommon.common.lmconstants import (EML_INTERFACE, JSON_INTERFACE, 
                                                      NEWICK_INTERFACE, NEXUS_INTERFACE)

from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

from LmTest.formatTests.emlValidator import validate_eml
from LmTest.formatTests.jsonValidator import validate_json
from LmTest.formatTests.treeValidator import validate_newick, validate_nexus
from LmTest.webTestsLite.common.userUnitTest import UserTestCase
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
            self.assertIsInstance(tree, TreeWrapper)
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
            self.assertIsInstance(treeObjs[0], TreeWrapper)
            self.assertEqual(treeObjs[0].getUserId(), self._get_session_user(), 
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
                
            self.assertTrue('ultrametric' in treeMeta)
            self.assertEqual(treeMeta['user'], self._get_session_user(), 
                                        'User id on tree = {}, session user = {}'.format(
                                            treeMeta['user'], self._get_session_user()))
        
            # EML
            with contextlib.closing(self.cl.get_tree(treeId, 
                                                         responseFormat=EML_INTERFACE)) as x:
                self.assertTrue(validate_eml(x))
            # JSON
            with contextlib.closing(self.cl.get_tree(treeId, 
                                                        responseFormat=JSON_INTERFACE)) as x:
                self.assertTrue(validate_json(x))
            # Newick
            with contextlib.closing(self.cl.get_tree(treeId, 
                                                     responseFormat=NEWICK_INTERFACE)) as x:
                self.assertTrue(validate_newick(x))
            # Nexus
            with contextlib.closing(self.cl.get_tree(treeId, 
                                                      responseFormat=NEXUS_INTERFACE)) as x:
                self.assertTrue(validate_nexus(x))
    
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

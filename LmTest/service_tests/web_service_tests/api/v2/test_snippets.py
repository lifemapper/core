"""
@summary: Test snippet services
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

from LmTest.webTestsLite.common.userUnitTest import UserTestCase
from LmTest.webTestsLite.common.webClient import LmWebClient


# .............................................................................
class TestWebSnippetService(UserTestCase):
   """
   @summary: This is a test class for running web tests for the snippet service
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
   def test_list(self):
      """
      @summary: Basic test that tries to get a list of snippets from the web
      """
      with contextlib.closing(self.cl.list_snippets()) as x:
         ret = json.load(x)
      
      if len(ret) == 0:
         warnings.warn('Count returned 0 snippets for user: {}'.format(
                                                     self._get_session_user()))
   
# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   return [
      TestWebSnippetService
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
   suite.addTest(UserTestCase.parameterize(TestWebSnippetService))

   if userId is not None:
      suite.addTest(UserTestCase.parameterize(TestWebSnippetService, 
                                              userId=userId, pwd=pwd))
      
   return suite

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                        description='Run snippet service tests')
   parser.add_argument('-u', '--user', type=str, 
                 help='If provided, run tests for this user (and anonymous)' )
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')
   
   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

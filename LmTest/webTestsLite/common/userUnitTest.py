"""
@summary: This module contains unit test classes that can be parameterized
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

@see: https://eli.thegreenplace.net/2011/08/02/python-unit-testing-parametrized-test-cases
"""
import unittest

from LmServer.common.localconstants import PUBLIC_USER

# .............................................................................
class UserTestCase(unittest.TestCase):
   """
   @summary: Test classes that inherit from this class can use a userId and
                password parameters
   """
   # ............................
   def __init__(self, methodName='runTest', userId=None, pwd=None):
      super(UserTestCase, self).__init__(methodName)
      self.userId = userId
      self.passwd = pwd

   # ............................
   @staticmethod
   def parameterize(testcase_klass, userId=None, pwd=None):
      """
      @summary: Create a suite containing all tests taken from the given
                   subclass, passing the parameters 'userId' and 'pwd'
      """
      testLoader = unittest.TestLoader()
      testNames = testLoader.getTestCaseNames(testcase_klass)
      suite = unittest.TestSuite()
      for name in testNames:
         suite.addTest(testcase_klass(name, userId=userId, pwd=pwd))
      return suite

   # ............................
   def _get_session_user(self):
      """
      @summary: Get the user associated with the session.  If no user is 
                   provided to the instance, return public user
      """
      if self.userId is None:
         return PUBLIC_USER
      else:
         return self.userId
      
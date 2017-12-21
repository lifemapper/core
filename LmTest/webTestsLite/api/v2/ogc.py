"""
@summary: Test ogc services
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
import unittest

from LmCommon.common.lmconstants import JobStatus
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmTest.webTestsLite.common.userUnitTest import UserTestCase
from LmTest.webTestsLite.common.webClient import LmWebClient

# .............................................................................
class TestWebOgcService(UserTestCase):
   """
   @summary: This is a test class for running web tests for the ogc service
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
   def test_get(self):
      """
      @summary: Basic test that tries to get an ogc object 
                   belonging to a user
      """
      # Get a completed projection that should have a map
      scribe = BorgScribe(ConsoleLogger())
      scribe.openConnections()
      prjs = scribe.listSDMProjects(0, 1, userId=self._get_session_user(), 
                                    afterStatus=JobStatus.COMPLETE-1, 
                                    beforeStatus=JobStatus.COMPLETE+1, 
                                    atom=False)
      scribe.closeConnections()
      
      if len(prjs) == 0:
         self.fail(
           'Cannot get a map because there are no completed projections for {}'
            .format(self._get_session_user()))
      else:
         prj = prjs[0]
         occ = prj._occurrenceSet
         mapName = EarlJr().createBasename(LMFileType.SDM_MAP, 
                                           objCode=occ.getId(), 
                                           usr=occ.getUserId(), 
                                           epsg=occ.epsgcode)
   
         with contextlib.closing(self.cl.get_ogc(mapName, 
                                    bbox=','.join(prj.bbox), color='#ff0000', 
                                    height=200, layer=prj.name, 
                                    request='GetMap', format='image/png', 
                                    service='WMS', 
                                    srs='EPSG:{}'.format(prj.epsgcode), 
                                    version='1.3.0', width=400)) as x:
            resp = x.read()
         
         self.assertTrue(resp is not None, 'OGC response was None')
      
   
# .............................................................................
def get_test_classes():
   """
   @summary: Return a list of the available test classes in this module.  This 
                should be returned to a test suite builder that will 
                parameterize tests appropriately
   """
   return [
      TestWebOgcService
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
   suite.addTest(UserTestCase.parameterize(TestWebOgcService))

   if userId is not None:
      suite.addTest(UserTestCase.parameterize(TestWebOgcService, 
                                              userId=userId, pwd=pwd))
      
   return suite

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                        description='Run ogc service tests')
   parser.add_argument('-u', '--user', type=str, 
                 help='If provided, run tests for this user (and anonymous)' )
   parser.add_argument('-p', '--pwd', type=str, help='Password for user')
   
   args = parser.parse_args()
   suite = get_test_suite(userId=args.user, pwd=args.pwd)
   unittest.TextTestRunner(verbosity=2).run(suite)

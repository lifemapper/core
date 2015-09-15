"""
@summary: This module will check if a user has permission to do something
@note: At this time, we only check to see if the session user matches the 
          user in the url, later this module should be more complex.
@author: CJ Grady
@status: alpha 
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmCommon.common.localconstants import ARCHIVE_USER

from LmServer.common.log import LmPublicLogger
from LmServer.db.scribe import Scribe

def checkUserPermission(urlUser, sessionUser, method, service=None, id=None):
   """
   @summary: Checks to see if the session user has permission to perform
                action specified by method
   @param urlUser: The name of the user specified in the url - /services/{user}/
   @param sessionUser: The name of the user determined by the session
   @param service: The service in question (ex. models, projections, etc).
   @param id: The id of a specific object (optional)
   @param method: The method used (GET, POST, DELETE)
   """
   log = LmPublicLogger()
   
   log.debug(' '.join(("Url user:", str(urlUser))))
   log.debug(' '.join(("Session user:", str(sessionUser))))
   log.debug(' '.join(("Method:", str(method))))
   log.debug(' '.join(("Service:", str(service))))
   log.debug(' '.join(("Id:", str(id))))
   
   if urlUser == sessionUser or urlUser == ARCHIVE_USER or urlUser == DEFAULT_POST_USER:
      return True
   else:
      return False
   
def checkUserLogin(username, pwd):
   """
   @summary: Checks to see if a user has entered valid credentials
   """
   log = LmPublicLogger()
   lmm = Scribe(log)
   lmm.openConnections()
   user = lmm.getUser(username)
   lmm.closeConnections()
   if user is None:
      return False
   else:
      return user.checkPassword(pwd)

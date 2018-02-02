"""
@summary: This module provides access control methods for Lifemapper objects
@author: CJ Grady
@version: 1.0
@status: alpha

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
from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.common.lmconstants import HTTPMethod

# .............................................................................
def checkUserPermission(sessionUser, obj, method):
   """
   @summary: Checks that the user has permission to perform the desired action 
                on the object
   @param sessionUser: The user logged into Lifemapper
   @param obj: The object to check permission for
   @param method: The HTTP method used
   """
   # For now, the HTTP method requires the userId for the object to match:
   #      GET - session user, public, or anonymous
   #      PUT - session user - can't update public or anonymous
   #      DELETE - session user - can't delete public or anonymous
   if method.upper() in [HTTPMethod.DELETE, HTTPMethod.PUT]:
      validUsers = [sessionUser]
   elif method.upper() == HTTPMethod.GET:
      validUsers = [sessionUser, PUBLIC_USER, DEFAULT_POST_USER]
   else:
      raise Exception, "Unknown HTTP method: {}".format(method.upper())
   
   # Return boolean indicating if the object's user id is in the valid user 
   #    list for the HTTP method
   return obj.getUserId() in validUsers

"""
@summary: This dispatcher casts query parameters appropriately so that they can
             be used directly by functions down the line.  We handle them all
             here to prevent code redundancy.
@author: CJ Grady
@version: 2.0
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

import cherrypy

from LmWebServer.common.lmconstants import QUERY_PARAMETERS

# .............................................................................
def castParameters():
   """
   @summary: Cast the provided parameters and change the names to match what we
                expect.  This allows query parameter names to be 
                case-insensitive and of the type we expect for processing
   """
   newParameters = {}
   inParams = cherrypy.request.params
   for key in inParams:
      # Conver the key to lower case and remove any underscores
      modKey = key.replace('_', '').lower()
      if QUERY_PARAMETERS.has_key(modKey):
         qp = QUERY_PARAMETERS[modKey]
         if qp.has_key('processIn'):
            # If we have a processing instruction, do it
            newParameters[qp['name']] = qp['processIn'](inParams[key])
         else:
            # If not, just set to what was passed in but for new parameter name
            newParameters[qp['name']] = inParams[key]
   
   # Set the request parameters to the new values
   cherrypy.request.params = newParameters
   
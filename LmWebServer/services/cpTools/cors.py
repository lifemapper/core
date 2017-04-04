"""
@summary: This module contains a tool to allow (CORS) Cross-Origin Resource 
             Sharing 
@author: CJ Grady
@version: 2.0
@status: alpha

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

import cherrypy

# .............................................................................
def CORS():
   """
   @summary: Function to be called before processing a request.  This will add
                response headers required for CORS (Cross-Origin Resource 
                Sharing) requests.  This is needed for browsers running 
                JavaScript code from a different domain. 
   """
   cherrypy.response.headers["Access-Control-Allow-Origin"] = "*"
   cherrypy.response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
   cherrypy.response.headers["Access-Control-Allow-Headers"] = "*"
   cherrypy.response.headers["Access-Control-Allow-Credentials"] = "true"
   if cherrypy.request.method.lower() == 'options':
      cherrypy.response.headers['Content-Type'] = 'text/plain'
      return 'OK'

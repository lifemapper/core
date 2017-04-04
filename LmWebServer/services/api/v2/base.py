"""
@summary: The module provides a base Lifemapper service class
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
class LmService(object):
   """
   @summary: This is the base Lifemapper service object that the services can
                inherit from.  It is responsible for getting a database 
                connection and logger that can be used for the service.
   """
   # ..........................
   def __init__(self):
      """
      @summary: The constructor is only responsible for getting a logger, user
                   and a scribe instance for the service.  We do that here in a 
                   simple base class in case we decide that we need to use a 
                   different mechanism (such as a CherryPy Tool)
      """
      self.scribe = cherrypy.thread_data.scribeRetriever.getScribe()
      self.log = cherrypy.session.log
      self.userId = cherrypy.session.user
      
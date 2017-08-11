"""
@summary: This module provides services for querying snippets
@author: CJ Grady
@version: 1.0
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

from LmServer.common.solr import querySnippetIndex

from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
class SnippetService(LmService):
   """
   @summary: This class is responsible for the Lifemapper snippet services. The 
                dispatcher is responsible for calling the correct method
   """
   # ................................
   @lmFormatter
   def GET(self, ident1=None, provider=None, collection=None, 
                      catalogNumber=None, operation=None, afterTime=None,
                      beforeTime=None, ident2=None, url=None, who=None,
                      agent=None, why=None):
      """
      @summary: A snippet get request will query the Lifemapper snippet index
                   and return matching entries.
      """
      return self._makeSolrQuery(ident1=ident1, provider=provider, 
                      collection=collection, catalogNumber=catalogNumber, 
                      operation=operation, afterTime=afterTime, 
                      beforeTime=beforeTime, ident2=ident2, url=url, who=who, 
                      agent=agent, why=why)
   
   # ................................
   def _makeSolrQuery(self, ident1=None, provider=None, collection=None, 
                      catalogNumber=None, operation=None, afterTime=None,
                      beforeTime=None, ident2=None, url=None, who=None,
                      agent=None, why=None):
      
      return querySnippetIndex(ident1=ident1, provider=provider, 
                      collection=collection, catalogNumber=catalogNumber, 
                      operation=operation, afterTime=afterTime, 
                      beforeTime=beforeTime, ident2=ident2, url=url, who=who, 
                      agent=agent, why=why)
   

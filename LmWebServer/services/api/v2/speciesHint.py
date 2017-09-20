"""
@summary: This module provides services for query of what occurrence sets are
             available
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

from LmServer.common.solr import queryArchiveIndex

from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
class SpeciesHintService(LmService):
   """
   @summary: This class is responsible for the Global PAM services.  The 
                dispatcher is responsible for calling the correct method
   """
   # ................................
   @lmFormatter
   def GET(self, searchString, limit=20, urlUser=None):#, taxonGenus=None, taxonSpecies=None):
      """
      @summary: Search the index for occurrence sets matching the given search
                   string
      """
      if len(searchString) < 3:
         raise cherrypy.HTTPError(400, 
                     'Need to provide at least 3 characters for search string')
      else:
         # Split on a space if exists
         parts = searchString.replace('%20', '_').split(' ')
         if len(parts) > 1:
            genus = parts[0]
            sp = '{}*'.format(parts[1])
         else:
            genus = '{}*'.format(parts[0])
            sp = None
      
         matches = queryArchiveIndex(taxGenus=genus, taxSpecies=sp, 
                                     userId=self.getUserId(urlUser=urlUser))
         
         occIds = []
         ret = []
         
         for match in matches:
            occId = match['occurrenceId']
            pointCount = match['pointCount']
            displayName = match['displayName']
            binomial = '{} {}'.format(match['taxonGenus'], match['taxonSpecies'])
            if not occId in occIds:
               ret.append({
                  'binomial' : binomial,
                  'name' : displayName,
                  'numPoints' : pointCount,
                  'occurrenceSet' : occId
               })
         return ret
   

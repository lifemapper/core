"""
@summary: This tool will check to see if the last segment of the URL path is a
             format string.  If it is, set the accept header to match it
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

from LmCommon.common.lmconstants import LMFormat

# .............................................................................
def findFormatRequest():
   """
   @summary: This tool looks to see if the last segment of the URL path is a 
                format string, like 'json', 'kml', or 'GTiff', and if it is
                it will update the request accept headers so that the 
                formatter will know what to do with the results of the 
                handler that gets called by the request
   @todo: Consider Lifemapper mime types for packages and others
   """
   # Get the last element of the path
   formatReq = cherrypy.request.script_path.split('/')[-1]
   print formatReq
   
   setAccept = None
   
   if formatReq.lower() == 'json':
      setAccept = LMFormat.JSON.getMimeType()
   elif formatReq.lower() == 'kml':
      setAccept = LMFormat.KML.getMimeType()
   elif formatReq.lower() == 'gtiff':
      setAccept = LMFormat.GTIFF.getMimeType()
   elif formatReq.lower() == 'csv':
      setAccept = LMFormat.CSV.getMimeType()
   elif formatReq.lower() == 'shapefile':
      setAccept = LMFormat.SHAPE.getMimeType()
      
   if setAccept is not None:
      print "Setting header to:", setAccept
      cherrypy.request.header['Accept'] = setAccept

"""
@summary: The Lifemapper dispatcher looks for a format as the last portion of 
             the path_info string from the request.  If one is found, it is 
             removed from path_info and the accept header is updated.  This 
             dispatcher also sets the path_info string to lower case so that 
             the services can be case insensitive.  Finally, forward to the
             MethodDispatcher base class
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
import cherrypy
from cherrypy._cpdispatch import MethodDispatcher

from LmCommon.common.lmconstants import (CSV_INTERFACE, EML_INTERFACE, 
                        GEO_JSON_INTERFACE, GEOTIFF_INTERFACE, JSON_INTERFACE, 
                        KML_INTERFACE, LMFormat, NEXUS_INTERFACE, 
                        NEWICK_INTERFACE, PACKAGE_INTERFACE, 
                        SHAPEFILE_INTERFACE)

# .............................................................................
class LmDispatcher(MethodDispatcher):
   # ...........................
   def __call__(self, path_info):
      
      path_info_pieces = path_info.lower().strip('/').split('/')
      new_path_info = path_info.lower()
      setAccept = None
      lastSegment = path_info_pieces[-1]
      
      if lastSegment == JSON_INTERFACE:
         setAccept = LMFormat.JSON.getMimeType()
      elif lastSegment == GEO_JSON_INTERFACE:
         setAccept = LMFormat.GEO_JSON.getMimeType()
      elif lastSegment == CSV_INTERFACE:
         setAccept = LMFormat.CSV.getMimeType()
      elif lastSegment == KML_INTERFACE:
         setAccept = LMFormat.KML.getMimeType()
      elif lastSegment == GEOTIFF_INTERFACE:
         setAccept = LMFormat.GTIFF.getMimeType()
      elif lastSegment == SHAPEFILE_INTERFACE:
         setAccept = LMFormat.SHAPE.getMimeType()
      elif lastSegment == EML_INTERFACE:
         setAccept = LMFormat.EML.getMimeType()
      elif lastSegment == PACKAGE_INTERFACE:
         setAccept = LMFormat.ZIP.getMimeType()
      elif lastSegment == NEXUS_INTERFACE:
         setAccept = LMFormat.NEXUS.getMimeType()
      elif lastSegment == NEWICK_INTERFACE:
         setAccept = LMFormat.NEWICK.getMimeType()
      
      if setAccept is not None:
         cherrypy.request.headers['Accept'] = setAccept
         
         new_path_info = '/'.join(path_info_pieces[:-1])

      return MethodDispatcher.__call__(self, new_path_info)






"""
@summary: This module provides a tool for formatting outputs of service calls 
             based on the accept headers of the request
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

from LmCommon.common.lmconstants import LMFormat, HTTPStatus

from LmWebServer.formatters.fileFormatter import (gtiffObjectFormatter,
                                                  shapefileObjectFormatter)
from LmWebServer.formatters.jsonFormatter import jsonObjectFormatter

# .............................................................................
def lmFormatter(f):
   """
   @summary: Use this as a decorator for methods that return objects that 
                should be sent through formatting before being returned
   """
   def wrapper(*args, **kwargs):
      """
      @summary: Wrapper function
      """
      # Call the handler and get the object result
      handler_result = f(*args, **kwargs)
      
      acceptHeaders = cherrypy.request.headers.get('Accept')
      
      rawHeaders = acceptHeaders.split(',')
      valuedAccepts = []
      for h in rawHeaders:
         if len(h.split(';')) > 1:
            mime, val = h.split(';')
            valuedAccepts.append((mime, float(val.strip('q='))))
         else:
            valuedAccepts.append((h, 0.0))
      
      sortedAccepts = sorted(valuedAccepts, key=lambda x: x[1], reverse=True)
      
      for ah, _ in sortedAccepts:
         try:
            # If JSON or default
            if ah in [LMFormat.JSON.getMimeType(), '*/*']:
               return jsonObjectFormatter(handler_result)
            #elif ah == LMFormat.KML.getMimeType():
            #   return kmlObjectFormatter(handler_result)
            elif ah == LMFormat.GTIFF.getMimeType():
               return gtiffObjectFormatter(handler_result)
            elif ah == LMFormat.SHAPE.getMimeType():
               return shapefileObjectFormatter(handler_result)
         except Exception:
            # Ignore and try next accept header
            pass
      # If we cannot find an acceptable formatter, raise HTTP error
      raise cherrypy.HTTPError(HTTPStatus.NOT_ACCEPTABLE, 
                               'Could not an acceptable format')
            
      
      
      return jsonObjectFormatter(handler_result)
   
   return wrapper

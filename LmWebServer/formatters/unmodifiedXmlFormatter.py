"""
@summary: Module containing Unmodified XML Formatter class and helping functions
@author: CJ Grady
@version: 1.0
@status: beta
@note: Part of the Factory pattern
@see: Formatter
@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
from types import StringType

from LmCommon.common.lmXml import iselement, tostring

from LmServer.base.lmobj import LMError

from LmWebServer.formatters.formatter import Formatter, FormatterResponse

# .............................................................................
class UnmodifiedXmlFormatter(Formatter):
   """
   @summary: Formatter class for unmodified XML output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      if isinstance(self.obj, StringType):
         resp = self.obj
      elif iselement(self.obj):
         resp = tostring(self.obj)
      else:
         raise LMError("Content passed to UnmodifiedXmlFormatter does not appear to be XML")

      ct = "application/xml"
      fn = "response.xml"
      
      return FormatterResponse(resp, contentType=ct, filename=fn)

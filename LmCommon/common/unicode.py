# coding=utf-8
"""
@summary: Module containing functions to handle unicode
@author: CJ Grady
@version: 1.0
@status: alpha
@note: Adapted from http://farmdev.com/talks/unicode/

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
from LmCommon.common.localconstants import ENCODING

# .............................................................................
def toUnicode(item, encoding=ENCODING):
   """
   @summary: Converts an item to unicode if it is not already
   @param item: The object to make into unicode
   @param encoding: (optional) The encoding of the text
   """
   value = item
   if isinstance(item, basestring):
      if not isinstance(item, unicode):
         value = unicode(item, encoding)
   else:
      value = unicode(str(item), encoding)
   return value

# .............................................................................
def fromUnicode(uItem, encoding=ENCODING):
   """
   @summary: Converts a unicode string to text for display
   @param uItem: A unicode object
   @param encoding: (optional) The encoding to use
   """
   return uItem.encode(ENCODING)

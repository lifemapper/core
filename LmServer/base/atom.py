"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
from LmBackend.common.lmobj import LMObject

# ..............................................................................
class Atom(LMObject):
   """
   Used for returning simple objects for REST url construction 
   """
   
   def __init__(self, id, name, url, modTime, epsg=None):
      """
      @summary: Constructor for the Atom class
      @param id: database id of the object
      @param name: name of the object
      @param modTime: time/date last modified
      """
      LMObject.__init__(self)
      self.id = id
      self.name = name
      self.url = url
      self.modTime = modTime
      self.epsgcode = epsg

# ...............................................
# included only to allow use of full or atom objects in Scribe/Peruser methods
   def getId(self):
      """
      @summary: Return the database id for this object
      """
      return self.id
   

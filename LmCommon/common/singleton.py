"""
@summary: The module contains a singleton function that can be used as a 
             decorator for classes that should be singletons.
@author: CJ Grady
@contact: cjgrady [at] ku [dot] edu
@version: 1.0
@status: beta

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
import hashlib
# .............................................................................
def singleton(cls):
   """
   @summary: Creates singletons for all unique sets of arguments for a class
   """
   instances = {}
   def getInstance(*args, **kwargs):
      name = hashlib.md5(''.join([str(args), str(kwargs)])).hexdigest()
      if name not in instances:
         instances[name] = cls(*args, **kwargs)
      return instances[name]
   return getInstance


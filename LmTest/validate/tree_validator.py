"""
@summary: This module contains functions for validating a tree
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
@todo: Determine if file or file-like object, then validate
@todo: Generalize
"""
import os

from LmCommon.trees.lmTree import LmTree
from LmCommon.common.lmconstants import LMFormat

# .............................................................................
def validate_tree_file(tree_filename, schema=None):
   """
   @summary: Validates a tree file by seeing if it can be loaded
   """
   msg = 'Valid'
   valid = 'False'
   if os.path.exists(tree_filename):
      # If a schema was not provided, try to get it from the file name
      if schema is None:
         _, ext = os.path.splitext(tree_filename)
         if ext == LMFormat.NEWICK.ext:
            schema = 'newick'
         elif ext == LMFormat.NEXUS.ext:
            schema = 'nexus'
         else:
            msg = 'Extension {} did not map to a known tree format'.format(ext)
      
      if schema is not None:
         t = LmTree.initFromFile(tree_filename, schema)
         valid = True
   else:
      msg = 'File does not exist'
   return valid, msg

"""
@summary: This module contains functions for validating XML
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
@todo: Validate against schema
@todo: Determine if file or file-like object, then validate
@todo: Generalize
"""
import json
import os

# .............................................................................
def validate_json_file(json_filename):
   """
   @summary: Validates a JSON document by seeing if it can be loaded
   """
   msg = 'Valid'
   valid = False
   if os.path.exists(json_filename):
      try:
         with open(json_filename) as in_json:
            json_obj = json.load(in_json)
         valid = True
      except Exception, e:
         msg = str(e)
   else:
      msg = 'File does not exist'
   
   return valid, msg
   
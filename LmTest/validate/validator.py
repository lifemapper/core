"""
@summary: This module contains functions for validating files
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

from LmCommon.common.lmconstants import LMFormat

from LmTest.validate.csv_validator import validate_csv_file
from LmTest.validate.json_validator import validate_json_file
from LmTest.validate.lm_matrix_validator import validate_lm_matrix_file
from LmTest.validate.raster_validator import validate_raster_file
from LmTest.validate.text_validator import validate_text_file
from LmTest.validate.tree_validator import validate_tree_file
from LmTest.validate.vector_validator import validate_vector_file
from LmTest.validate.xml_validator import validate_xml_file
from LmTest.validate.zip_validator import validate_zip_file

# .............................................................................
def validate_file(filename):
   """
   @summary: Attempts to validate a file by inspecting the extension and 
                selecting the correct validation function
   """
   _, ext = os.path.splitext(filename)
   
   if ext == LMFormat.ASCII.ext:
      valid, msg = validate_raster_file(filename, raster_format=LMFormat.ASCII)
   elif ext == LMFormat.CSV.ext:
      valid, msg = validate_csv_file(filename)
   elif ext == LMFormat.GTIFF.ext:
      valid, msg = validate_raster_file(filename, raster_format=LMFormat.GTIFF)
   elif ext == LMFormat.JSON.ext:
      valid, msg = validate_json_file(filename)
   elif ext == LMFormat.MATRIX.ext:
      valid, msg = validate_lm_matrix_file(filename)
   elif ext in [LMFormat.NEWICK.ext, LMFormat.NEXUS.ext]:
      valid, msg = validate_tree_file(filename)
   elif ext == LMFormat.SHAPE.ext:
      valid, msg = validate_vector_file(filename)
   elif ext in [LMFormat.TAR_GZ.ext, LMFormat.ZIP.ext]:
      valid, msg = validate_zip_file(filename)
   elif ext == LMFormat.TXT.ext:
      valid, msg = validate_text_file(filename)
   elif ext == LMFormat.XML.ext:
      valid, msg = validate_xml_file(filename)
   else:
      valid = False
      msg = 'Unknown file extension: {}'.format(ext)

   return valid, msg
   
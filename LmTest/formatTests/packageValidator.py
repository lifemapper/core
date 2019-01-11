"""
@summary: Contains functions for validating package files 
@status: alpha
@author: CJ Grady
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
import os
import random
import shutil
import zipfile

from LmServer.common.lmconstants import TEMP_PATH

def validate_package(obj_generator):
   
   try:
      outDir = os.path.join(TEMP_PATH, 'temp_dir_{}'.format(random.randint(10000)))
      
      os.makedirs(outDir)
   
   
      with zipfile.ZipFile(obj_generator) as zf:
         zf.extractall(outDir)
   
      shutil.rmtree(outDir)
      return True
   except Exception, e:
      return False

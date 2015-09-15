"""
@summary: This script is used to seed layers into the layers database for a 
             compute resource so that they are not downloaded unnecessarily
@author: CJ Grady
@version: 1.0
@status: alpha

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
import os
import sys

from LmCompute.common.layerManager import LayerManager
from LmCompute.common.lmconstants import INPUT_LAYER_DIR
from LmCompute.common.localconstants import JOB_DATA_PATH

SEED_DIR = os.path.join(JOB_DATA_PATH, INPUT_LAYER_DIR)

def processFile(fn):
   f = open(fn)
   lyrs = [tuple(line.split(',')) for line in f.readlines()]
   f.close()
   return lyrs

# .............................................................................
if __name__ == "__main__":
   if len(sys.argv) < 2:
      print("Usage: python layerSeeder.py file")
      print("   File should be pairs of url, local file path separated by line feeds")
   else:
      lyrFile = sys.argv[1]
   lm = LayerManager(JOB_DATA_PATH)
   
   for ident, fn in processFile(lyrFile):
      lm.seedLayer(ident.strip(), os.path.join(SEED_DIR, fn.strip()))
   
   lm.close()
   

"""
@summary: This script is used to seed layers into the layers database for a 
             compute resource so that they are not downloaded unnecessarily
@author: CJ Grady
@version: 2.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import argparse
import os

from LmCompute.common.layerManager import LayerManager
from LmCompute.common.lmconstants import INPUT_LAYER_DIR
from LmCompute.common.localconstants import JOB_DATA_PATH

SEED_DIR = os.path.join(JOB_DATA_PATH, INPUT_LAYER_DIR)

def processFile(fn, seedDir=SEED_DIR):
   lyrs = []
   with open(fn) as f:
      for line in f:
         ident, fp = line.split(',')
         lyrs.append((ident.strip(), os.path.join(seedDir, fp.strip())))
   return lyrs

# .............................................................................
if __name__ == "__main__":
   
   parser = argparse.ArgumentParser(prog="Lifemapper LmCompute Layer Seeder",
                           description="Seeds the LmCompute layers database",
                           version="2.0.0")
   parser.add_argument('-a', '--ascii', type=int, choices=[0,1], default=1,
                       help="Generate and seed ASCII grids (default 1 - yes)")
   parser.add_argument('-m', '--mxe', type=int, choices=[0,1], default=1,
                       help="Generate and seed MXE files (default 1 - yes)")
   parser.add_argument('-d', '--seedDir', 
                       help="Use this directory as the base for seeded layers", 
                       default=SEED_DIR)
   parser.add_argument('scnPkgCsvFn', 
                       help="Scenario package CSV with (identifier, relative path) pairs",
                       nargs="*")
   
   args = parser.parse_args()
   
   # Should we generate ASCIIs and MXEs?
   asciis = bool(args.ascii)
   mxes = bool(args.mxe)
   
   # Check seed directory
   seedDir = args.seedDir
   if not os.path.exists(seedDir):
      raise Exception, "The specified layer directory does not exist: %s" % seedDir
   
   lm = LayerManager(JOB_DATA_PATH)
   
   for fn in args.scnPkgCsvFn:
      if os.path.exists(fn):
         lyrTups = processFile(fn)
         lm.seedLayers(lyrTups, makeASCIIs=asciis, makeMXEs=mxes)
      else:
         print "The CSV file %s does not exist, skipping" % fn
   
   lm.close()
   
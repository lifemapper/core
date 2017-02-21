#!/bin/bash
"""
@summary: This script attempts to generate a Lifemapper occurrence set from 
             user CSV
@author: CJ Grady
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import ast

from LmCompute.plugins.single.occurrences.csvOcc import createUserShapefile

# .............................................................................
if __name__ == "__main__":
   
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script attempts to generate a Lifemapper occurrence set from user CSV") 
   
   parser.add_argument('pointsCsvFn', type=str,
                       help="A path to a raw user CSV file")
   parser.add_argument('metadataFile', type=str, 
                       help="JSON file of occurrence set metadata")
   parser.add_argument('outFile', type=str, 
                  help="The file location to write the shapefile for modeling")
   parser.add_argument('bigFile', type=str, 
           help="The file location to write the full occurrence set shapefile")
   parser.add_argument('maxPoints', type=int, 
               help="The maximum number of points for the modelable shapefile")
   args = parser.parse_args()
   
   with open(args.metadataFile) as metaIn:
      tmp = metaIn.read()
      meta = ast.literal_eval(tmp)
   
   createUserShapefile(args.pointsCsvFn, meta, args.outFile, 
                       args.bigFile, args.maxPoints)
   

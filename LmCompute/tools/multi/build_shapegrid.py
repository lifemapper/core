#!/bin/bash
"""
@summary: This script attempts to build a shapegrid
@author: CJ Grady
@version: 4.0.0
@status: beta
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
import argparse

from LmCommon.shapes.buildShapegrid import buildShapegrid

# .............................................................................
if __name__ == "__main__":
   
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script attempts to build a shapegrid") 
   
   parser.add_argument('shapegridFn', type=str, 
                       help="File location for new shapegrid")
   parser.add_argument('minX', type=float, 
                       help="The minimum X value for the shapegrid")
   parser.add_argument('minY', type=float, 
                       help="The minimum Y value for the shapegrid")
   parser.add_argument('maxX', type=float, 
                       help="The maximum X value for the shapegrid")
   parser.add_argument('maxY', type=float, 
                       help="The maximum Y value for the shapegrid")
   parser.add_argument('cellSize', type=float, 
                       help="The size of each cell in the appropriate units")
   parser.add_argument('epsg', type=int, 
                       help="The EPSG code to use for this shapegrid")
   parser.add_argument('cellSides', type=int, choices=[4,6],
                       help="The number of cides for each cell")
   parser.add_argument('--cutoutWktFn', dest='cutoutFn', type=str,
                       help="File location of a cutout WKT")
   
   args = parser.parse_args()
   
   cutout = None
   if args.cutoutFn is not None:
      cutout = args.cutoutFn

   buildShapegrid(args.shapegridFn, args.minX, args.minY, args.maxX, args.maxY,
                  args.cellSize, args.epsg, args.cellSides, cutoutWKT=cutout)
   
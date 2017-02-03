#!/bin/bash
"""
@summary: This script intersects a shapegrid and a raster layer to create a 
             GRIM column
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

from LmCompute.plugins.multi.intersect.radIntersect import grimRasterIntersect

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(
      description="This script performs a raster intersect with a shapegrid to produce a GRIM column")
   
   parser.add_argument("shapegridFn", type=str, 
                      help="This is the shapegrid to intersect the layer with")
   parser.add_argument("rasterFn", 
         type=str, help="This is the file location of the raster file to use for intersection")
   parser.add_argument("grimColFn", type=str, 
                       help="Location to write the GRIM column Matrix object")
   parser.add_argument("resolution", type=float, 
                                           help="The resolution of the raster")
   parser.add_argument("--minPercent", dest="minPercent", type=int,
             "If provided, use the largest class method, otherwise, use weighted mean [0,100]")
   parser.add_argument("--ident", type=str, dest="ident", 
                    help="An identifer to be used as metadata for this column")

   args = parser.parse_args()
   
   ident = None
   if args.ident is not None:
      ident = args.ident

   minPercent = None
   if args.minPercent is not None:
      minPercent = args.minPercent

   grimCol = grimRasterIntersect(args.shapegridFn, args.rasterFn, 
                                 args.resolution, minPercent=minPercent, 
                                 ident=ident)
   
   with open(args.grimColFn, 'w') as grimColOutF:
      grimCol.save(grimColOutF)

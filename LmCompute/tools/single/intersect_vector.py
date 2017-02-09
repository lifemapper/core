#!/bin/bash
"""
@summary: This script intersects a shapegrid and a vector layer to create a 
             Presence Absence Vector (PAV)
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

from LmCompute.plugins.multi.intersect.radIntersect import pavVectorIntersect

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(
      description="This script performs a vector intersect with a shapegrid to produce a PAV")
   
   parser.add_argument("shapegridFn", type=str, 
                      help="This is the shapegrid to intersect the layer with")
   parser.add_argument("vectorFn", 
         type=str, help="This is the file location of the vector file to use for intersection")
   parser.add_argument("pavFn", type=str, 
                       help="Location to write the PAV Matrix object")
   parser.add_argument("presenceAttrib", type=str, 
                     help="The vector attribute used for determining presence")
   parser.add_argument("minPresence", type=float, 
                       help="The minimum value to be considered present")
   parser.add_argument("maxPresence", type=float, 
                       help="The maximum value to be considered present")
   parser.add_argument("percentPresence", type=int, 
        help="The percentage [0,100] of a cell that must be covered to be called present")
   parser.add_argument("--squid", type=str, dest="squid", 
       help="A species identifier to be attached to the PAV Matrix column as metadata")

   args = parser.parse_args()
   
   squid = None
   if args.squid is not None:
      squid = args.squid
      
   pav = pavVectorIntersect(args.shapegridFn, args.vectorFn, 
                            args.presenceAttrib, args.minPresence, 
                            args.maxPresence, args.percentPresence, squid=squid)
   
   with open(args.pavFn, 'w') as pavOutF:
      pav.save(pavOutF)

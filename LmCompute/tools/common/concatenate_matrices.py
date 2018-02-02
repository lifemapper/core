#!/bin/bash
"""
@summary: This script concatenates two (or more) matrices along a specified axis
@author: CJ Grady
@version: 4.0.0
@status: beta
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
"""
import argparse

from LmBackend.common.lmobj import LMObject
from LmCommon.common.matrix import Matrix

# .............................................................................
if __name__ == "__main__":
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script concatenates two (or more) matrices along an axis")
   
   #parser.add_argument('-s', '--status_fn', dest='statusFn', type=str,
   #              help="If this is not None, output the status of the job here")
   parser.add_argument("outFn", type=str, 
                        help="The file location to write the resulting matrix")
   parser.add_argument("axis", type=int, 
                      help="The (Matrix) axis to concatenate these matrices on")
   parser.add_argument("mtxFn", type=str, nargs='*',
                       help="The file location of the first matrix")
   parser.add_argument("--mashedPotato", type=str, dest="mashedPotato",
                       help="A mashed potato file of file names to concatenate")
   
   args = parser.parse_args()
   
   mtxs = []
   if args.mashedPotato is not None:
      with open(args.mashedPotato, 'r') as mashIn:
         for line in mashIn:
            squid, pav = line.split(':')
            mtxs.append(Matrix.load(pav.strip()))
   if args.mtxFn:
      for mtxFn in args.mtxFn:
         mtxs.append(Matrix.load(mtxFn))
   
   joinedMtx = Matrix.concatenate(mtxs, axis=args.axis)

   # Make sure directory exists
   LMObject().readyFilename(args.outFn)
   
   with open(args.outFn, 'w') as outF:
      joinedMtx.save(outF)


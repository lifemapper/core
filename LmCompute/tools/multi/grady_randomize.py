#!/bin/bash
"""
@summary: This script randomizes a PAM using the parallel Grady method while
            maintaining marginal totals
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
@todo: Consider just calculating stats and writing those rather than 
          intermediate matrix
"""
import argparse

from LmCommon.common.matrix import Matrix
from LmCompute.plugins.multi.randomize.grady import gradyRandomize

# .............................................................................
if __name__ == "__main__":
   
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script randomizes a PAM using the parallel Grady method while maintaining marginal totals") 

   parser.add_argument('pamFn', type=str, help="File location for PAM data")
   parser.add_argument('outRandomFn', type=str, 
                       help="File location to write randomized PAM")
   
   args = parser.parse_args()
   
   pam = Matrix.load(args.pamFn)
   
   randPam = gradyRandomize(pam)
   
   with open(args.outRandomFn, 'w') as outPamF:
      randPam.save(outPamF)
   
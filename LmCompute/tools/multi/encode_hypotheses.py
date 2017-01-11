#!/bin/bash
"""
@summary: This script encodes a Biogeographic hypothesis shapefile into a matrix
             by utilizing a shapegrid
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
@note: If you want to encode multiple layers with different event fields, call
          this script for each set of layers and then use the 
          concatenate_matrices script to stitch the results togther.
@todo: Consider providing a status parameter in case things go poorly
@todo: Consider providing option other than shapegrid, ex. (site id, x, y)
"""
import argparse
import numpy as np

from LmCommon.encoding.contrasts import BioGeoEncoding

# .............................................................................
if __name__ == "__main__":
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script encodes a biogeographic hypothesis shapegrid") 

   parser.add_argument('e', '--eventField', dest='eventField', type=str,
                    help="Use this field in the shapegrid to determine events")
   parser.add_argument("outFn", type=str, 
                        help="The file location to write the resulting matrix")
   parser.add_argument('shapegridFn', type=str, 
                 help="The file location of the shapegrid to use for encoding")
   parser.add_argument("lyr", type=str, nargs='+', 
      help="A file location of a shapegrid with one or more BioGeo hypotheses")
   
   args = parser.parse_args()
   
   encoder = BioGeoEncoding(args.shapegridFn)
   encoder.addLayers(args.lyr, eventField=args.eventField)
   
   bgEncoding = encoder.encodeHypotheses()
   np.save(args.outFn, bgEncoding)

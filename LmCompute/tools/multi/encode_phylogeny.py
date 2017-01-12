#!/bin/bash
"""
@summary: This script encodes a Phylogenetic tree into a matrix by using a PAM
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
@todo: Consider providing a status parameter in case things go poorly
@note: If no indices mapping file is provided, assume that the tree already has 
          matrix indices in it
"""
import argparse
import json
import numpy as np

from LmCommon.encoding.contrasts import BioGeoEncoding
from LmCommon.encoding.lmTree import LmTree

# .............................................................................
if __name__ == "__main__":
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script encodes a Phylogenetic tree with a PAM") 

   parser.add_argument("-m", "--matrixIndicesFn", dest="mtxIdxFn", type=str,
      help="""File location of a JSON document where the keys are tip labels 
      and values are matrix indices for this PAM""")
   parser.add_argument("outFn", type=str, 
                        help="The file location to write the resulting matrix")
   parser.add_argument("treeFn", type=str, 
                       help="The location of the Phylogenetic tree")
   parser.add_argument("pamFn", type=str, 
                       help="The location of the PAM (numpy)")
   
   args = parser.parse_args()
   
   tree = LmTree.fromFile(args.treeFn)
   if args.mtxIdxFn:
      pamMetadata = json.load(args.mtxIdxFn)
      tree.addMatrixIndices(pamMetadata)
   pam = np.load(args.pamFn)
   
   encoder = PhyloEncoding(tree, pam)

   pMtx = encoder.encodePhylogeny()
   
   np.save(args.outFn, pMtx)
   
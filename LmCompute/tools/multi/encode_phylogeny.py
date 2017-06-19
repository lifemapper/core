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
@note: If no indices mapping file is provided, assume that the tree already has 
          matrix indices in it
"""
import argparse

from LmCommon.common.matrix import Matrix
from LmCommon.encoding.phylo import PhyloEncoding
from LmCommon.trees.lmTree import LmTree

# .............................................................................
if __name__ == "__main__":
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script encodes a Phylogenetic tree with a PAM") 

   parser.add_argument("-m", "--mashedPotato", dest="mashedPotato", type=str,
      help="""File location of a mashed potato of SQUID : pav file lines, used
      to determine matrix indices""")
   parser.add_argument("treeFn", type=str, 
                       help="The location of the Phylogenetic tree")
   parser.add_argument("pamFn", type=str, 
                       help="The location of the PAM (numpy)")
   parser.add_argument("outFn", type=str, 
                        help="The file location to write the resulting matrix")
   
   args = parser.parse_args()
   
   tree = LmTree.fromFile(args.treeFn)
   
   # Check if we can encode tree
   if tree.hasBranchLengths() and not tree.isUltrametric():
      raise Exception, "Tree must be ultrametric for encoding"

   # If the tree is not binary, resolve the polytomies
   if not tree.isBinary():
      tree.resolvePolytomies()
   
   # Load the PAM
   pam = Matrix.load(args.pamFn)

   # If we should insert matrix indices
   if args.mashedPotato is not None:
      idx = 0
      squidDict = {}
      with open(args.mashedPotato, 'r') as mashIn:
         for line in mashIn:
            squid, pav = line.split(':')
            squidDict[squid.strip()] = idx
            idx += 1
   else:
      squidDict = {}
      squids = pam.getColumnHeaders()
      if len(squids) == 0:
         raise Exception, 'Cannot encode tree without squids in PAM or mashed potato'
      for i in range(len(squids)):
         squidDict[squids[i]] = i
   
   tree.addSquidMatrixIndices(squidDict)
   
   # Prune tree
   tree.pruneTipsWithoutMatrixIndices()
   
   encoder = PhyloEncoding(tree, pam)

   pMtx = encoder.encodePhylogeny()
   
   with open(args.outFn, 'w') as outF:
      pMtx.save(outF)
   
"""
@summary: This script encodes a PAM and Tree into a quarternary site by 
             ancestral node matrix indicating which sides of a clade are 
             present for each cell
@author: CJ Grady
@version: 1.0
@status: alpha
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

@note: +1 is for left side (first) children, -1 is for right side (second) 
          children, 0 means neither side of the clade is present, and 2 means
          that both sides are present
"""
import argparse
import numpy as np

from LmCommon.common.matrix import Matrix
from LmCommon.trees.lmTree import LmTree
from LmCommon.common.lmconstants import PhyloTreeKeys

# Local constants for this module
LEFT_SQUIDS_KEY = 'leftSquids'
RIGHT_SQUIDS_KEY = 'rightSquids'

# .............................................................................
def _getSquidsInClade(clade):
   """
   @summary: Builds a dictionary of clad id keys with left and right squids
                for each internal tree node
   @param clade: A clade to build this dictionary for
   @note: This function is recursive and will build a lookup dictionary for the
             the entire subtree under the clade
   @note: The tree must be binary
   """
   cladeDict = {}
   allSquids = []
   if len(clade[PhyloTreeKeys.CHILDREN]) == 2:
      # Process children
      leftCladeDict, leftSquids = _getSquidsInClade(
                                             clade[PhyloTreeKeys.CHILDREN][0])
      rightCladeDict, rightSquids = _getSquidsInClade(
                                             clade[PhyloTreeKeys.CHILDREN][1])
      # Add to all squids
      allSquids.extend(leftSquids)
      allSquids.extend(rightSquids)
      
      # Merge dictionaries
      cladeDict.update(leftCladeDict)
      cladeDict.update(rightCladeDict)
      # Add this clade to dictionary
      cladeDict[clade[PhyloTreeKeys.CLADE_ID]] = {
         LEFT_SQUIDS_KEY : leftSquids,
         RIGHT_SQUIDS_KEY : rightSquids
      }
   else:
      if clade.has_key(PhyloTreeKeys.SQUID):
         allSquids.append(clade[PhyloTreeKeys.SQUID])
   
   return cladeDict, allSquids

# .............................................................................
def build_ancestral_pam(pam, tree):
   """
   @summary: Builds a site by internal tree node matrix with the values of each
                cell indicating if only the left clade is present (+1), only
                the right clade is present (-1), both clades are present (2),
                or neither clade is present (0).
   @param pam: A PAM to use to build the ancestral PAM
   @param tree: An LmTree object to use for phylogenetic information
   @note: Tree should be binary
   """
   # Get squid lookup
   squidHeaders = pam.getColumnHeaders()
   squidLookup = {}
   for i in range(len(squidHeaders)):
      squidLookup[squidHeaders[i]] = i

   # Get the lookup dictionary
   cladeDict, _ = _getSquidsInClade(tree.tree)
   
   # Initialize new matrix
   numRows = pam.data.shape[0]
   nodeData = np.zeros((numRows, len(cladeDict.keys())))
   
   col = 0
   cols = []
   for cladeId in cladeDict.keys():
      cols.append(str(cladeId))
      
      # Get left and right squids for the clade
      leftSquids = cladeDict[cladeId][LEFT_SQUIDS_KEY]
      rightSquids = cladeDict[cladeId][RIGHT_SQUIDS_KEY]
      
      # Create the left and right squid indexes from the PAM
      leftIdxs = [squidLookup[squid] for squid in leftSquids if squidLookup.has_key(squid)]
      rightIdxs = [squidLookup[squid] for squid in rightSquids if squidLookup.has_key(squid)]
      
      # Get the left and right side (clades) binary column of presences
      leftSide = np.any(pam.data[:,leftIdxs], axis=1).astype(int)
      rightSide = np.any(pam.data[:,rightIdxs], axis=1).astype(int)

      # Build the column of quaternary values indicating which clade is present
      #a1 - a2 + 2*((a1+a2)/2)
      nodeData[:,col] = leftSide - rightSide + 2*((leftSide+rightSide)/2)
      col += 1
   
   nodeMtx = Matrix(nodeData, headers={'0' : pam.getRowHeaders(),
                                       '1' : cols})
   return nodeMtx

# .............................................................................
if __name__ == '__main__':
   
   parser = argparse.ArgumentParser(
      description='Create a quarternary site by ancestral node matrix indicating which sides of a clade are present')
   parser.add_argument('PAM_FILENAME', dest='pamFn', type=str, 
               help='The file location of the PAM to use to build this matrix')
   parser.add_argument('TREE_FILENAME', dest='treeFn', type=str,
              help='The file location of the tree to use to build this matrix')
   parser.add_argument('OUTPUT_FILENAME', dest='outFn', type=str,
                       help='The file location to write the generated matrix')
   
   args = parser.parse_args()
   
   # Read in inputs
   pam = Matrix.load(args.pamFn)
   tree = LmTree.fromFile(args.treeFn)
   
   # Build the Ancestral PAM
   ancPam = build_ancestral_pam(pam, tree)
   
   with open(args.outFn, 'w') as outF:
      ancPam.save(outF)
   
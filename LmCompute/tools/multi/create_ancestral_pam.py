#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Encode a PAM and a Tree into a ancestral node matrix

This script encodes a PAM and Tree into a quarternary site by ancestral node
matrix indicating which sides of a clade are present for each cell

Note:
    * +1 is for left side (first) children, -1 is for right side (second)
        children, 0 means neither side of the clade is present, and 2 means
        that both sides are present
"""
import argparse
import numpy as np

from LmCommon.common.matrix import Matrix
from LmCommon.trees.lmTree import LmTree
from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA, PhyloTreeKeys

# Local constants for this module
LEFT_SQUIDS_KEY = 'leftSquids'
RIGHT_SQUIDS_KEY = 'rightSquids'

# .............................................................................
def _getSquidsInClade(node):
    """Builds a squid dictionary

    Builds a dictionary of clade id keys with left and right squids for each
    internal tree node

    Args:
        node: A tree node to build this dictionary for

    Note:
        * This function is recursive and will build a lookup dictionary for the
            the entire subtree under the clade
        * The tree must be binary
    """
    cladeDict = {}
    allSquids = []
    
    if node.num_child_nodes() == 2:
        leftCladeDict, leftSquids = _getSquidsInClade(node.child_nodes()[0])
        rightCladeDict, rightSquids = _getSquidsInClade(node.child_nodes()[1])
        
        # Add to all squids
        allSquids.extend(leftSquids)
        allSquids.extend(rightSquids)
        
        # Merge dictionary
        cladeDict.update(leftCladeDict)
        cladeDict.update(rightCladeDict)
        
        # Add this clade to dictionary
        cladeDict[node.label] = {
            LEFT_SQUIDS_KEY : leftSquids,
            RIGHT_SQUIDS_KEY : rightSquids
        }
        
    else:
        try:
            allSquids.append(
                node.taxon.annotations.get_value(PhyloTreeKeys.SQUID))
        except:
            pass
            
    return cladeDict, allSquids

# .............................................................................
def build_ancestral_pam(pam, tree):
    """Builds an ancestral PAM

    Builds a site by internal tree node matrix with the values of each cell
    indicating if only the left clade is present (+1), only the right clade is
    present (-1), both clades are present (2), or neither clade is present (0).

    Args:
        pam: A PAM to use to build the ancestral PAM
        tree: An LmTree object to use for phylogenetic information

    Note:
        * Tree should be binary
    """
    # Get squid lookup
    squidHeaders = pam.getColumnHeaders()
    squidLookup = {}
    for i in range(len(squidHeaders)):
        squidLookup[squidHeaders[i]] = i

    # Get the lookup dictionary
    cladeDict, _ = _getSquidsInClade(tree.tree.seed_node)
    
    # Initialize new matrix
    numRows = pam.data.shape[0]
    nodeData = np.zeros((numRows, len(cladeDict.keys())), dtype=np.int8)
    
    col = 0
    cols = []
    for cladeId in cladeDict.keys():
        cols.append(str(cladeId))
        
        # Get left and right squids for the clade
        leftSquids = cladeDict[cladeId][LEFT_SQUIDS_KEY]
        rightSquids = cladeDict[cladeId][RIGHT_SQUIDS_KEY]
        
        # Create the left and right squid indexes from the PAM
        leftIdxs = [
            squidLookup[squid] for squid in leftSquids if squidLookup.has_key(
                squid)]
        rightIdxs = [
            squidLookup[squid] for squid in rightSquids if squidLookup.has_key(
                squid)]
        
        # Get the left and right side (clades) binary column of presences
        leftSide = np.any(pam.data[:,leftIdxs], axis=1).astype(int)
        rightSide = np.any(pam.data[:,rightIdxs], axis=1).astype(int)

        # Build the column of quaternary values indicating which clade is
        #    present; a1 - a2 + 2*((a1+a2)/2)
        nodeData[:,col] = leftSide - rightSide + 2*((leftSide+rightSide)/2)
        col += 1
    
    nodeMtx = Matrix(
        nodeData, headers={'0' : pam.getRowHeaders(), '1' : cols})
    return nodeMtx

# .............................................................................
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(
        description=('Create a quarternary site by ancestral node matrix'
                     ' indicating which sides of a clade are present'))
    parser.add_argument(
        'pamFn', type=str,
        help='The file location of the PAM to use to build this matrix')
    parser.add_argument(
        'treeFn', type=str,
        help='The file location of the tree to use to build this matrix')
    parser.add_argument(
        'outFn', type=str,
        help='The file location to write the generated matrix')
    
    args = parser.parse_args()
    
    # Read in inputs
    pam = Matrix.load(args.pamFn)
    tree = LmTree.initFromFile(args.treeFn, DEFAULT_TREE_SCHEMA)
    
    # Build the Ancestral PAM
    ancPam = build_ancestral_pam(pam, tree)
    
    with open(args.outFn, 'w') as outF:
        ancPam.save(outF)

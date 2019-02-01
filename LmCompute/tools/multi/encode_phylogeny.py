#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This script encodes a Phylogenetic tree into a matrix by using a PAM

Note:
    * If no indices mapping file is provided, assume that the tree already has
        matrix indices in it

Todo:
    * Remove or reinstate mashed potato parameter
"""
import argparse

from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA
from LmCommon.common.matrix import Matrix
from LmCommon.encoding.phylo import PhyloEncoding
from LmCommon.trees.lmTree import LmTree

# .............................................................................
if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="This script encodes a Phylogenetic tree with a PAM") 

    parser.add_argument(
        "-m", "--mashedPotato", dest="mashedPotato", type=str,
        help=('File location of a mashed potato of SQUID : pav file lines,'
              ' used to determine matrix indices'))
    parser.add_argument(
        "treeFn", type=str, help="The location of the Phylogenetic tree")
    parser.add_argument(
        "pamFn", type=str, help="The location of the PAM (numpy)")
    parser.add_argument(
        "outFn", type=str,
        help="The file location to write the resulting matrix")
    
    args = parser.parse_args()
    
    tree = LmTree.initFromFile(args.treeFn, DEFAULT_TREE_SCHEMA)
    
    # Check if we can encode tree
    if tree.hasBranchLengths() and not tree.isUltrametric(relTol=0.01):
        raise Exception, "Tree must be ultrametric for encoding"

    # If the tree is not binary, resolve the polytomies
    if not tree.isBinary():
        tree.resolvePolytomies()
    
    # Load the PAM
    pam = Matrix.load(args.pamFn)

    encoder = PhyloEncoding(tree, pam)

    pMtx = encoder.encodePhylogeny()
    
    with open(args.outFn, 'w') as outF:
        pMtx.save(outF)

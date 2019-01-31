#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Synchronize a PAM and tree for computation

This script modifies a tree and a PAM as necessary so that they can be used
together for MCPA

Todo:
    * Should we add SQUIDs if they are not in the tree?  Will they always be?
"""
import argparse
import json
import numpy as np

from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA, PhyloTreeKeys
from LmCommon.common.matrix import Matrix
from LmCommon.trees.lmTree import LmTree

# .............................................................................
def prunePamAndTree(pam, tree):
    """Prune the PAM and tree so they match

    Prune the SQUIDs from the tree and PAM that are not in the other so that we
    can use them for MCPA

    Args:
        pam: A PAM matrix to prune.  We assume that the column headers are
            present and are SQUIDs
        tree: A LmTree to prune.  We assume that it has SQUIDs.

    Return:
        A pruned PAM, a pruned Tree (with matrix indexes), metadata documenting
            what was pruned
    """
    treeSquids = [
        squid for _, squid in tree.getAnnotations(PhyloTreeKeys.SQUID)]
    pamSquids = pam.getColumnHeaders()
    
    metadata = {}
    
    # Prune PAM
    delCols = []
    prunedPAMSquids = []
    goodPAMSquids = []
    
    for i in xrange(len(pamSquids)):
        if not pamSquids[i] in treeSquids:
            delCols.append(i)
            prunedPAMSquids.append(pamSquids[i])
        else:
            goodPAMSquids.append(pamSquids[i])
    
    # If we need to, prune the PAM
    if len(prunedPAMSquids) > 0:
        pam.data = np.delete(pam.data, delCols, axis=1)
        pam.setColumnHeaders(goodPAMSquids)
        
        metadata['pruned_PAM_squids'] = prunedPAMSquids

    # Add matrix indices to tree
    squidDict = dict([(v, k) for k, v in enumerate(pam.getColumnHeaders())])
    tree.annotateTree(PhyloTreeKeys.MTX_IDX, squidDict, 
                            labelAttribute=PhyloTreeKeys.SQUID)
    # Prune tips not in PAM
    tree.pruneTipsWithoutAttribute(searchAttribute=PhyloTreeKeys.MTX_IDX)
    
    # Add pruned tree squids to metadata
    prunedTreeSquids = []
    for squid in treeSquids:
        if not squid in pamSquids:
            prunedTreeSquids.append(squid)
    if len(prunedTreeSquids) > 0:
        metadata['pruned_Tree_squids'] = prunedTreeSquids

    return pam, tree, metadata

# .............................................................................
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(
        description='Prune tree and PAM so that they match for MCPA')
    parser.add_argument(
        'inPamFn', type=str, help='The file location of the input PAM')
    parser.add_argument(
        'outPamFn', type=str,
        help='The file location of the pruned output PAM')
    parser.add_argument(
        'inTreeFn', type=str,
        help='The file location of the input (nexus) tree')
    parser.add_argument(
        'outTreeFn', type=str,
        help='The file location of the pruned output tree')
    parser.add_argument(
        'metadataFn', type=str,
        help='The file location to write metadata summarizing pruning')
    
    args = parser.parse_args()
    
    # Get the inputs
    pam = Matrix.load(args.inPamFn)
    tree = LmTree.initFromFile(args.inTreeFn, DEFAULT_TREE_SCHEMA)
    
    # Prune the PAM and tree
    outPam, outTree, metadata = prunePamAndTree(pam, tree)
    
    # Write the outputs
    with open(args.outPamFn, 'w') as outF:
        outPam.save(outF)

    outTree.writeTree(args.outTreeFn)
    
    with open(args.metadataFn, 'w') as outM:
        json.dump(metadata, outM)

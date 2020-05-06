"""Synchronize a PAM and tree for computation

This script modifies a tree and a PAM as necessary so that they can be used
together for MCPA

Todo:
    * Should we add SQUIDs if they are not in the tree?  Will they always be?
"""
import argparse
import json

import numpy as np

from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA, ENCODING
from lmpy import Matrix, PhyloTreeKeys, TreeWrapper


# .............................................................................
def prune_pam_and_tree(pam, tree):
    """Prune the PAM and tree so they match

    Prune the SQUIDs from the tree and PAM that are not in the other so that we
    can use them for MCPA

    Args:
        pam: A PAM matrix to prune.  We assume that the column headers are
            present and are SQUIDs
        tree: A TreeWrapper to prune.  We assume that it has SQUIDs.

    Return:
        A pruned PAM, a pruned Tree (with matrix indexes), metadata documenting
            what was pruned
    """
    tree_squids = [
        squid for _, squid in tree.get_annotations(PhyloTreeKeys.SQUID)]
    pam_squids = pam.get_column_headers()

    metadata = {}

    # Prune PAM
    del_cols = []
    pruned_pam_squids = []
    good_pam_squids = []

    for i, squid in enumerate(pam_squids):
        if squid not in tree_squids:
            del_cols.append(i)
            pruned_pam_squids.append(squid)
        else:
            good_pam_squids.append(squid)

    # If we need to, prune the PAM
    if len(pruned_pam_squids) > 0:
        pam = np.delete(pam, del_cols, axis=1)
        pam.set_column_headers(good_pam_squids)

        metadata['pruned_PAM_squids'] = pruned_pam_squids

    # Add matrix indices to tree
    squid_dict = {val: idx for idx, val in enumerate(pam.get_column_headers())}
    tree.annotate_tree_tips(
        PhyloTreeKeys.MTX_IDX, squid_dict, label_attribute=PhyloTreeKeys.SQUID)
    # Prune tips not in PAM
    tree.prune_tips_without_attribute(search_attribute=PhyloTreeKeys.MTX_IDX)

    # Add pruned tree squids to metadata
    pruned_tree_squids = []
    for squid in tree_squids:
        if squid not in pam_squids:
            pruned_tree_squids.append(squid)
    if len(pruned_tree_squids) > 0:
        metadata['pruned_Tree_squids'] = pruned_tree_squids

    return pam, tree, metadata


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(
        description='Prune tree and PAM so that they match for MCPA')
    parser.add_argument(
        'in_pam_fn', type=str, help='The file location of the input PAM')
    parser.add_argument(
        'out_pam_fn', type=str,
        help='The file location of the pruned output PAM')
    parser.add_argument(
        'in_tree_fn', type=str,
        help='The file location of the input (nexus) tree')
    parser.add_argument(
        'out_tree_fn', type=str,
        help='The file location of the pruned output tree')
    parser.add_argument(
        'metadata_fn', type=str,
        help='The file location to write metadata summarizing pruning')

    args = parser.parse_args()

    # Get the inputs
    pam = Matrix.load(args.in_pam_fn)
    tree = TreeWrapper.from_filename(args.in_tree_fn)

    # Prune the PAM and tree
    out_pam, out_tree, metadata = prune_pam_and_tree(pam, tree)

    # Write the outputs
    out_pam.write(args.out_pam_fn)

    out_tree.write(path=args.out_tree_fn, schema=DEFAULT_TREE_SCHEMA)

    with open(args.metadata_fn, 'w', encoding=ENCODING) as out_metadata:
        json.dump(metadata, out_metadata)


# .............................................................................
if __name__ == '__main__':
    main()

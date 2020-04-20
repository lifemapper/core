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

from LmCommon.encoding.phylo import PhyloEncoding
from lmpy import Matrix, TreeWrapper


# .............................................................................
def main():
    """Main method for the script.
    """
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description='This script encodes a Phylogenetic tree with a PAM')

    parser.add_argument(
        'tree_file_name', type=str,
        help='The location of the Phylogenetic tree')
    parser.add_argument(
        'pam_file_name', type=str, help='The location of the PAM (numpy)')
    parser.add_argument(
        'out_file_name', type=str,
        help='The file location to write the resulting matrix')

    args = parser.parse_args()
    tree = TreeWrapper.from_filename(args.tree_file_name)

    # Check if we can encode tree
    if tree.has_branch_lengths() and not tree.is_ultrametric(rel_tol=0.01):
        raise Exception('Tree must be ultrametric for encoding')

    # If the tree is not binary, resolve the polytomies
    if not tree.is_binary():
        tree.resolve_polytomies()

    # Load the PAM
    pam = Matrix.load(args.pam_file_name)

    encoder = PhyloEncoding(tree, pam)

    p_mtx = encoder.encode_phylogeny()

    p_mtx.write(args.out_file_name)


# .............................................................................
if __name__ == '__main__':
    main()

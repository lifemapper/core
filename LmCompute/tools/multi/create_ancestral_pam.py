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

from lmpy import Matrix, PhyloTreeKeys, TreeWrapper

# Local constants for this module
LEFT_SQUIDS_KEY = 'left_squids'
RIGHT_SQUIDS_KEY = 'right_squids'


# .............................................................................
def _get_squids_in_clade(node):
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
    clade_dict = {}
    all_squids = []

    if node.num_child_nodes() == 2:
        left_clade_dict, left_squids = _get_squids_in_clade(
            node.child_nodes()[0])
        right_clade_dict, right_squids = _get_squids_in_clade(
            node.child_nodes()[1])

        # Add to all squids
        all_squids.extend(left_squids)
        all_squids.extend(right_squids)

        # Merge dictionary
        clade_dict.update(left_clade_dict)
        clade_dict.update(right_clade_dict)

        # Add this clade to dictionary
        clade_dict[node.label] = {
            LEFT_SQUIDS_KEY: left_squids,
            RIGHT_SQUIDS_KEY: right_squids
        }

    else:
        try:
            all_squids.append(
                node.taxon.annotations.get_value(PhyloTreeKeys.SQUID))
        except Exception:
            pass

    return clade_dict, all_squids


# .............................................................................
def build_ancestral_pam(pam, tree):
    """Builds an ancestral PAM

    Builds a site by internal tree node matrix with the values of each cell
    indicating if only the left clade is present (+1), only the right clade is
    present (-1), both clades are present (2), or neither clade is present (0).

    Args:
        pam: A PAM to use to build the ancestral PAM
        tree: An TreeWrapper object to use for phylogenetic information

    Note:
        * Tree should be binary
    """
    # Get squid lookup
    squid_headers = pam.get_column_headers()
    squid_lookup = {}
    for i, squid_hdr in enumerate(squid_headers):
        squid_lookup[squid_hdr] = i

    # Get the lookup dictionary
    clade_dict, _ = _get_squids_in_clade(tree.seed_node)

    # Initialize new matrix
    num_rows = pam.shape[0]
    node_data = np.zeros(
        (num_rows, len(list(clade_dict.keys()))), dtype=np.int8)

    col = 0
    cols = []
    for clade_id in list(clade_dict.keys()):
        cols.append(str(clade_id))

        # Get left and right squids for the clade
        left_squids = clade_dict[clade_id][LEFT_SQUIDS_KEY]
        right_squids = clade_dict[clade_id][RIGHT_SQUIDS_KEY]

        # Create the left and right squid indexes from the PAM
        left_idxs = [
            squid_lookup[squid
                         ] for squid in left_squids if squid in squid_lookup]
        right_idxs = [
            squid_lookup[squid
                         ] for squid in right_squids if squid in squid_lookup]

        # Get the left and right side (clades) binary column of presences
        left_side = np.any(pam[:, left_idxs], axis=1).astype(int)
        right_side = np.any(pam[:, right_idxs], axis=1).astype(int)

        # Build the column of quaternary values indicating which clade is
        #    present; a1 - a2 + 2*((a1+a2)/2)
        node_data[:, col] = left_side - right_side + 2 * (
            (left_side + right_side) / 2)
        col += 1

    node_mtx = Matrix(
        node_data, headers={'0': pam.get_row_headers(), '1': cols})
    return node_mtx


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(
        description=('Create a quarternary site by ancestral node matrix'
                     ' indicating which sides of a clade are present'))
    parser.add_argument(
        'pam_fn', type=str,
        help='The file location of the PAM to use to build this matrix')
    parser.add_argument(
        'tree_fn', type=str,
        help='The file location of the tree to use to build this matrix')
    parser.add_argument(
        'out_fn', type=str,
        help='The file location to write the generated matrix')

    args = parser.parse_args()

    # Read in inputs
    pam = Matrix.load(args.pam_fn)
    tree = TreeWrapper.from_filename(args.tree_fn)

    # Build the Ancestral PAM
    anc_pam = build_ancestral_pam(pam, tree)

    anc_pam.write(args.out_fn)


# .............................................................................
if __name__ == '__main__':
    main()

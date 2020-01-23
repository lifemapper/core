"""Build a tree using open tree services and SQUID teh resulting tree.
"""
import argparse
import os

from lmpy import TreeWrapper
import ot_service_wrapper.open_tree as ot

from LmCommon.common.lmconstants import PhyloTreeKeys, DEFAULT_TREE_SCHEMA
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe


# .............................................................................
def add_squids_to_tree(tree, scribe, userId=None):
    """
    @summary: Add Lifemapper SQUIDs to a tree
    @param tree: A Lifemapper tree object (either LmTree or Tree)
    @param scribe: A scribe instance to use for SQUIDs
    @param userId: The user id to get SQUIDs for, will attempt to get from tree
                            object if not provided
    """
    if userId is None:
        try:
            userId = tree.getUserId()
        except:
            raise Exception('Must provider user id to get SQUIDs')
    squid_dict = {}
    for label in tree.getLabels():
        taxLabel = label.replace(' ', '_')
        sno = scribe.getTaxon(userId=userId, taxonName=taxLabel)
        if sno is not None:
            squid_dict[label] = sno.squid

    tree.annotate_tree_tips(PhyloTreeKeys.SQUID, squid_dict)
    return tree

# .............................................................................
def get_tree_from_gbif_ids(gbif_ids):
    """
    @summary: Get a Lifemapper tree object from gbif ids
    @note: Calls Open Tree services to get ott ids for the gbif ids and then
                 retrieve a newick tree containing those ott ids.  Converts the
                 newick tree into a Lifemapper tree
    """
    ott_ids = ot.get_ottids_from_gbifids(gbif_ids)
    newick_string = ot.induced_subtree(ott_ids)

    tree = TreeWrapper.get(data=newick_string, schema='newick')
    return tree

# .............................................................................
if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Build a LmTree object from GBIF IDs')
    parser.add_argument(
        '-u', '--userId', type=str,
        help='The user id to use to retrieve SQUIDs')
    parser.add_argument(
        '-s', '--squids', action='store_true', help='Add SQUIDs to the tree')
    parser.add_argument(
        '-g', '--id_filename', type=str,
        help='A file of GBIF IDs, one per line.')
    parser.add_argument(
        'out_filename', type=str, help='Write the tree to this location')
    parser.add_argument(
        'gbif_id', nargs='*', type=int, help='GBIF IDs to use for the tree')

    args = parser.parse_args()

    # Check arguments
    if args.squids and args.userId is None:
        raise Exception('Cannot add SQUIDs without a user id')

    gbif_ids = args.gbif_id

    if args.id_filename is not None:
        if os.path.exists(args.id_filename):
            with open(args.id_filename, 'r') as inF:
                for line in inF:
                    gbif_ids.apend(int(line.strip()))
        else:
            raise Exception('Could not open: {}'.format(args.id_filename))

    if len(gbif_ids) < 1:
        raise Exception('No GBIF IDs to build tree')

    tree = get_tree_from_gbif_ids(gbif_ids)

    if args.squids:
        scribe = BorgScribe(ScriptLogger('tree_builder'))
        scribe.openConnections()
        add_squids_to_tree(tree, scribe, userId=args.userId)
        scribe.closeConnections()

    tree.write(path=args.out_filename, schema=DEFAULT_TREE_SCHEMA)

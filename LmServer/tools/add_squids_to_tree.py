"""This script adds SQUIDs to the tips of a tree
"""
import argparse

from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(
      description='This script adds SQUIDs to the tips of a tree and labels nodes')

    parser.add_argument('tree_id', type=int, help='The id of this tree')
    parser.add_argument(
        'user_id', type=str, help='The user this tree belongs to')
    parser.add_argument(
        'success_filename', type=str,
        help='Write an indication of success here')

    args = parser.parse_args()

    user_id = args.user_id

    # Do stuff
    scribe = BorgScribe(ScriptLogger('squid_tree'))
    scribe.openConnections()

    tree = scribe.getTree(treeId=args.tree_id)

    squid_dict = {}

    for label in tree.getLabels():
        sno = scribe.getTaxon(userId=user_id, taxonName=label)
        if sno is not None:
            squid_dict[label] = sno.squid

    tree.annotateTree(PhyloTreeKeys.SQUID, squid_dict)

    # Write tree
    tree.clearDLocation()
    tree.set_dlocation()
    tree.writeTree()

    # Update metadata
    tree.updateModtime(gmt().mjd)
    success = scribe.updateObject(tree)

    scribe.closeConnections()

    ready_filename(args.success_filename, overwrite=True)
    with open(args.success_filename, 'w') as out_f:
        out_f.write('1\n')

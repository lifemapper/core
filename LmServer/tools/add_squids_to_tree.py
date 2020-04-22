"""This script adds SQUIDs to the tips of a tree
"""
import argparse

from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe


# .............................................................................
def main():
    """Main method for script
    """
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description=(
            'This script adds SQUIDs to the tips of a tree and labels nodes'))

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
    scribe.open_connections()

    tree = scribe.get_tree(tree_id=args.tree_id)

    squid_dict = {}

    for label in tree.get_labels():
        sno = scribe.get_taxon(user_id=user_id, taxon_name=label)
        if sno is not None:
            squid_dict[label] = sno.squid

    tree.annotate_tree(PhyloTreeKeys.SQUID, squid_dict)

    # Write tree
    tree.clear_dlocation()
    tree.set_dlocation()
    tree.write_tree()

    # Update metadata
    tree.update_mod_time(gmt().mjd)
    _ = scribe.update_object(tree)

    scribe.close_connections()

    ready_filename(args.success_filename, overwrite=True)
    with open(args.success_filename, 'w', encoding=ENCODING) as out_f:
        out_f.write('1\n')


# .............................................................................
if __name__ == '__main__':
    main()

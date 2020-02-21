# !/bin/bash
"""Script for creating a splitting / sorting / grouping makeflow for CSV
"""
import argparse
import os

from LmBackend.command.multi import (
    OccurrenceBucketeerCommand, OccurrenceSorterCommand)
from LmBackend.command.server import TouchFileCommand
from LmServer.legion.process_chain import MFChain


# .............................................................................
def get_rules_for_file(in_filename, group_position, width=1, depth=1,
                       basename='', headers=False, pos=0, out_dir='.'):
    """Gets a list of Makeflow rules for splitting the CSV file.

    Args:
        in_filename: The CSV input file name
        group_position: The field in the CSV file to use for grouping
        width: The number of characters to use for grouping at each iteration
        depth: The depth of the grouping (ex. 12/34/56 has a depth of 3 and
            width of 2)
        basename: The base name of the output bucket files
        headers: Does the input file have a header row
        pos: The position in the group field to use for bucketing
    """
    rules = []
    if depth == 0:
        # Sort
        sorted_fn = os.path.join(out_dir, '{}_sorted.csv'.format(basename))

        sort_cmd = OccurrenceSorterCommand(
            in_filename, sorted_fn, group_position)
        rules.append(sort_cmd.get_makeflow_rule())
    else:
        # More splitting
        base_names = []
        for i in range(10 ** width):
            b_name = '{}{}'.format(basename, (str(i) + '0' * width)[0:width])
            base_names.append(
                (b_name, os.path.join(out_dir, '{}.csv'.format(b_name))))

        # Add out directory touch rule
        touch_fn = os.path.join(out_dir, 'touch.out')
        touch_cmd = TouchFileCommand(touch_fn)
        rules.append(touch_cmd.get_makeflow_rule(local=True))
        bucketeer_cmd = OccurrenceBucketeerCommand(
            os.path.join(out_dir, basename), group_position, in_filename,
            position=pos, width=width, header_row=headers)
        bucketeer_cmd.inputs.append(touch_fn)
        bucketeer_cmd.outputs.extend([bfn for _, bfn in base_names])

        rules.append(bucketeer_cmd.get_makeflow_rule())

        # Recurse
        for base_n, base_filename in base_names:
            rules.extend(
                get_rules_for_file(
                    base_filename, group_position, width=width, depth=depth-1,
                    basename=base_n, headers=False, pos=pos + width,
                    out_dir=out_dir))
    return rules


# .............................................................................
def main():
    """Main method for script
    """
    # width, depth, input files,
    parser = argparse.ArgumentParser(
        description=(
            'This script generates a Makeflow file for processing raw '
            'CSV data'))

    # workspace
    parser.add_argument('outfile_filename', type=str, help='Write DAG here')
    parser.add_argument(
        'group_position', type=int,
        help='The position of the field to group on')
    parser.add_argument(
        'width', type=int, help='Use this many characters for buckets')
    parser.add_argument('depth', type=int, help='Recurse this many levels')
    parser.add_argument('user_id', type=str, help='User id for workflow')
    parser.add_argument(
        'input_filename', type=str, nargs='+', help='Input CSV file')
    parser.add_argument(
        'out_dir', type=str, help='Directory to store output CSVs')

    args = parser.parse_args()

    mf_chain = MFChain(args.user_id)

    # Recursively create rules
    for filename in args.input_filename:
        rules = get_rules_for_file(
            filename, args.group_position, width=args.width, depth=args.depth,
            basename='bucket_', headers=True, out_dir=args.out_dir)
        mf_chain.add_commands(rules)
    mf_chain.write(args.outfile_filename)


# .............................................................................
if __name__ == '__main__':
    main()

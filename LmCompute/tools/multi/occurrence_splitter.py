"""Splits a sorted CSV file on the group field
"""
import argparse
import csv
import os

from LmCommon.common.lmconstants import ENCODING
# .............................................................................
def group_by_field(input_filename, out_dir, group_position,
                   file_prefix='taxon_'):
    """Creates groups from the CSV file based on the specified field

    Args:
        input_filename: The file name of the sorted input CSV file
        out_dir: The directory to store the output files
        group_position: The field in the CSV to group / split on
        file_prefix: The prefix of the output file names
    """
    group_id = None
    out_file = None

    with open(input_filename, 'r', encoding=ENCODING) as in_file:
        reader = csv.reader(in_file)
        for row in reader:
            if row[group_position] != group_id:
                group_id = row[group_position]
                try:
                    out_file.close()
                except Exception:
                    pass
                out_filename = os.path.join(
                    out_dir, '{}{}.csv'.format(file_prefix, group_id))
                out_file = open(out_filename, 'w', encoding=ENCODING)
                writer = csv.writer(out_file)

            writer.writerow(row)
    try:
        out_file.close()
    except Exception:
        pass


# .............................................................................
def main():
    """Script main method
    """
    parser = argparse.ArgumentParser(
        description='This script takes in a CSV input file and groups it')

    parser.add_argument(
        'group_position', type=int,
        help='The position of the field to group on')
    parser.add_argument('input_filename', type=str, help='Input CSV file')

    parser.add_argument(
        'out_dir', type=str, help='Output directory to write files')
    parser.add_argument(
        '-p', '--prefix', type=str, default='taxon_',
        help='Prefix for output files')

    args = parser.parse_args()

    group_by_field(
        args.input_filename, args.out_dir, args.group_position,
        file_prefix=args.prefix)


# .............................................................................
if __name__ == '__main__':
    main()

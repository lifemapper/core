"""Sorts a CSV file on the group field
"""
import argparse
import csv
from operator import itemgetter

from LmCommon.common.lmconstants import ENCODING

# .............................................................................
def sort_file_on_field(input_filename, output_filename, group_position):
    """Sorts a CSV file on the group field

    Args:
        input_filename: The input CSV file name to use for sorting
        output_filename: Where to write the output file
        group_position: The column to sort on
    """
    rows = []
    with open(input_filename, 'r', encoding=ENCODING) as in_file:
        reader = csv.reader(in_file)
        for row in reader:
            rows.append(row)

    with open(output_filename, 'w', encoding=ENCODING) as out_file:
        writer = csv.writer(out_file)
        for row in sorted(rows, key=itemgetter(group_position)):
            writer.writerow(row)


# .............................................................................
def main():
    """Script main method
    """
    parser = argparse.ArgumentParser(
        description='This script takes in a CSV input file and sorts it')

    parser.add_argument(
        'output_filename', type=str, help='Write sorted output file here')
    parser.add_argument(
        'group_position', type=int,
        help='The position of the field to group on')
    parser.add_argument('input_filename', type=str, help='Input CSV file')

    args = parser.parse_args()

    sort_file_on_field(
        args.input_filename, args.output_filename, args.group_position)


# .............................................................................
if __name__ == '__main__':
    main()

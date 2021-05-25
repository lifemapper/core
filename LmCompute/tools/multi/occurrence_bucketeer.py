"""Split a CSV into buckets

This script takes an input file (or files) of occurrence records, in CSV
format, and splits them into buckets grouped on a portion of the records group
field (such as taxon id)
"""
import argparse
import csv
import os

from LmCommon.common.lmconstants import ENCODING

BASE_BUCKET_NUM = 10  # 10 possibilities for numeric characters
DEF_CHAR = '0'  # Default character


# .............................................................................
def split_into_buckets(input_filenames, output_basename, group_pos, str_pos=0,
                       num_cmp=1, headers=False):
    """Split input files into output bucket files

    Args:
        input_filenames: A list of CSV filenames of input data
        ouput_basename: The base name for output CSV bucket files
        group_pos: The column in the CSV file that should be used for grouping
        str_pos: Position in the group field to be used for bucketing
        num_cmp: The number of characters in the string to use for bucketing
        headers: Do the input files have a header row
    """
    # Initialize bucket files
    num_buckets = BASE_BUCKET_NUM ** num_cmp
    buckets = {}
    for i in range(num_buckets):
        # k = (str(i) + DEF_CHAR*numCmp)[0:numCmp]
        k = (DEF_CHAR * num_cmp + str(i))[-num_cmp:]
        # Some keys end up duplicated so only add if doesn't exist (9 can
        #     become '900', etc)
        if k not in buckets:
            buckets[k] = csv.writer(
                open('{}{}.csv'.format(output_basename, k), 'w', 
                     encoding=ENCODING))

    for filename in input_filenames:
        with open(filename, 'r', encoding=ENCODING) as in_file:
            reader = csv.reader(in_file)
            i = 0
            for row in reader:
                if i == 0 and headers:
                    i = 1
                else:
                    try:
                        g_str = row[group_pos]
                        try:
                            bucket_str = g_str[str_pos:str_pos + num_cmp]
                        except Exception:
                            bucket_str = DEF_CHAR
                        bucket_str += DEF_CHAR * num_cmp
                        buckets[bucket_str[0:num_cmp]].writerow(row)
                    except Exception as err:
                        print(str(err))

    # Close bucket files
    for k in list(buckets.keys()):
        buckets[k].stream.close()


# .............................................................................
def main():
    """Main method of script
    """
    parser = argparse.ArgumentParser(
        description=('This script takes in CSV input files '
                     'and splits them into buckets'))
    parser.add_argument(
        'output_basename', type=str,
        help='Write output files with this base name')
    parser.add_argument(
        'group_position', type=int,
        help='The position of the field to group on')
    parser.add_argument(
        'input_filename', type=str, nargs='+', help='Input CSV file')
    parser.add_argument(
        '-pos', dest='pos', type=int,
        help='Position in group field to create buckets from')
    parser.add_argument(
        '-num', dest='num', type=int,
        help='How many characters to use for buckets')
    parser.add_argument(
        '-header', dest='header', action='store_true',
        help='Do the input files have a header row')
    args = parser.parse_args()

    if args.pos is not None:
        str_pos = args.pos
    else:
        str_pos = 0

    num_cmp = 1
    if args.num is not None:
        num_cmp = args.num

    split_into_buckets(
        args.input_filename, os.path.abspath(args.output_basename),
        args.group_position, str_pos=str_pos, num_cmp=num_cmp,
        headers=args.header)


# .............................................................................
if __name__ == '__main__':
    main()

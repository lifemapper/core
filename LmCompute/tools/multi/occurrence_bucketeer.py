#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Split a CSV into buckets

This script takes an input file (or files) of occurrence records, in CSV
format, and splits them into buckets grouped on a portion of the records group
field (such as taxon id)
"""
import argparse
import os

from LmCommon.common.unicodeCsv import UnicodeReader, UnicodeWriter

BASE_BUCKET_NUM = 10 # 10 possibilities for numeric characters
DEF_CHAR = '0' # Default character

# .............................................................................
def splitIntoBuckets(inputFilenames, outputBasename, groupPos, strPos=0,
                     numCmp=1, headers=False):
    """Split input files into output bucket files

    Args:
        inputFilenames: A list of CSV filenames of input data
        ouputBasename: The base name for output CSV bucket files
        groupPos: The column in the CSV file that should be used for grouping
        strPos: Position in the group field to be used for bucketing
        numCmp: The number of characters in the string to use for bucketing
        headers: Do the input files have a header row
    """
    # Initialize bucket files
    numBuckets = BASE_BUCKET_NUM ** numCmp
    buckets = {}
    for i in range(numBuckets):
        #k = (str(i) + DEF_CHAR*numCmp)[0:numCmp]
        k = (DEF_CHAR*numCmp + str(i))[-numCmp:]
        # Some keys end up duplicated so only add if doesn't exist (9 can become 
        #     '900', etc)
        if k not in buckets:
            buckets[k] = UnicodeWriter(
                open('{}{}.csv'.format(outputBasename, k), 'w'))
            #buckets[k] = csv.writer(
            #    open('{}{}.csv'.format(outputBasename, k), 'w'))

    for fn in inputFilenames:
        with open(fn) as inF:
            reader = UnicodeReader(inF)
            #reader = csv.reader(inF)
            i = 0
            for row in reader:
                if i == 0 and headers:
                    i = 1
                else:
                    try:
                        gStr = row[groupPos]
                        try:
                            b = gStr[strPos:strPos+numCmp]
                        except:
                            b = DEF_CHAR
                        b += DEF_CHAR*numCmp
                        buckets[b[0:numCmp]].writerow(row)
                    except Exception as e:
                        print(str(e))
            
    # Close bucket files
    for k in list(buckets.keys()):
        buckets[k].stream.close()

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=('This script takes in CSV input files '
                     'and splits them into buckets'))
    parser.add_argument(
        'outputBasename', type=str,
        help='Write output files with this base name')
    parser.add_argument(
        'groupPosition', type=int,
        help='The position of the field to group on')
    parser.add_argument(
        'inputFilename', type=str, nargs='+', help='Input CSV file')
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
        strPos = args.pos
    else:
        strPos = 0
        
    numCmp = 1
    if args.num is not None:
        numCmp = args.num
        
    splitIntoBuckets(
        args.inputFilename, os.path.abspath(args.outputBasename),
        args.groupPosition, strPos=strPos, numCmp=numCmp, headers=args.header)

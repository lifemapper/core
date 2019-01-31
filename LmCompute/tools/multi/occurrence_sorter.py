#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Sorts a CSV file on the group field
"""
import argparse
from operator import itemgetter

from LmCommon.common.unicodeCsv import UnicodeReader, UnicodeWriter

# .............................................................................
def sortFileOnField(inputFilename, outputFilename, groupPosition):
    """Sorts a CSV file on the group field

    Args:
        inputFilename: The input CSV file name to use for sorting
        outputFilename: Where to write the output file
        groupPosition: The column to sort on
    """
    rows = []
    with open(inputFilename) as inF:
        reader = UnicodeReader(inF)
        for row in reader:
            rows.append(row)
    
    with open(outputFilename, 'w') as outF:
        writer = UnicodeWriter(outF)
        for row in sorted(rows, key=itemgetter(groupPosition)):
            writer.writerow(row)

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='This script takes in a CSV input file and sorts it')
    
    parser.add_argument(
        'outputFilename', type=str, help='Write sorted output file here')
    parser.add_argument(
        'groupPosition', type=int, help='The position of the field to group on')
    parser.add_argument('inputFilename', type=str, help='Input CSV file')

    args = parser.parse_args()
    
    sortFileOnField(
        args.inputFilename, args.outputFilename, args.groupPosition)

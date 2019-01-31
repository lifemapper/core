#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Splits a sorted CSV file on the group field
"""
import argparse
import os
#from operator import itemgetter

from LmCommon.common.unicodeCsv import UnicodeReader, UnicodeWriter

# .............................................................................
def groupByField(inputFilename, outDir, groupPosition, filePrefix='taxon_'):
    """Creates groups from the CSV file based on the specified field

    Args:
        inputFilename: The file name of the sorted input CSV file
        outDir: The directory to store the output files
        groupPosition: The field in the CSV to group / split on
        filePrefix: The prefix of the output file names
    """
    groupId = None
    outF = None

    with open(inputFilename) as inF:
        reader = UnicodeReader(inF)
        for row in reader:
            if row[groupPosition] != groupId:
                groupId = row[groupPosition]
                try:
                    outF.close()
                except:
                    pass
                outFilename = os.path.join(
                    outDir, '{}{}.csv'.format(filePrefix, groupId))
                outF = open(outFilename, 'w')
                writer = UnicodeWriter(outF)
          
            writer.writerow(row)
    try:
        outF.close()
    except:
        pass
    
# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='This script takes in a CSV input file and groups it')
    
    parser.add_argument(
        'groupPosition', type=int,
        help='The position of the field to group on')
    parser.add_argument('inputFilename', type=str, help='Input CSV file')

    parser.add_argument(
        'outDir', type=str, help='Output directory to write files')
    parser.add_argument(
        '-p', '--prefix', type=str, default='taxon_',
        help='Prefix for output files')

    args = parser.parse_args()
    
    groupByField(
        args.inputFilename, args.outDir, args.groupPosition,
        filePrefix=args.prefix)

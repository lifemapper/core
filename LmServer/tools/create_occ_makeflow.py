# !/bin/bash
"""Script for creating a splitting / sorting / grouping makeflow for CSV
"""
import argparse
import os

from LmBackend.command.multi import (OccurrenceBucketeerCommand,
                                 OccurrenceSorterCommand)  # , OccurrenceSplitterCommand)
from LmBackend.command.server import TouchFileCommand
from LmServer.legion.processchain import MFChain


# .............................................................................
def getRulesForFile(inFn, groupPos, width=1, depth=1, basename='',
                          headers=False, pos=0, outDir='.'):
    """
    @summary: Gets a list of Makeflow rules for splitting a CSV file into many 
                     CSVs based on the group field
    @param inFn: The CSV input file name
    @param groupPos: The field in the CSV file to use for grouping
    @param width: The number of characters to use for grouping at each iteration
    @param depth: The depth of the grouping (ex. 12/34/56 has a depth of 3 and 
                          width of 2)
    @param basename: The base name of the output bucket files
    @param headers: Does the input file have a header row
    @param pos: The position in the group field to use for bucketing
    """
    rules = []
    if depth == 0:
        # Sort
        sortedFn = os.path.join(outDir, '{}_sorted.csv'.format(basename))

        sortCmd = OccurrenceSorterCommand(inFn, sortedFn, groupPos)
        rules.append(sortCmd.get_makeflow_rule())

        # TODO: Add split command (unknown outputs)
        # Split
        # splitCmd = OccurrenceSplitterCommand(groupPos, sortedFn, outDir,
        #                                                 prefix='taxon_')
        # rules.append(splitCmd.get_makeflow_rule())
    else:
        # More splitting
        baseNames = []
        for i in range(10 ** width):
            bn = '{}{}'.format(basename, (str(i) + '0' * width)[0:width])
            baseNames.append((bn, os.path.join(outDir, '{}.csv'.format(bn))))

        # Add out directory touch rule
        touchFn = os.path.join(outDir, 'touch.out')
        touchCmd = TouchFileCommand(touchFn)
        rules.append(touchCmd.get_makeflow_rule(local=True))
        bucketeerCmd = OccurrenceBucketeerCommand(os.path.join(outDir, basename),
                                                                groupPos, inFn,
                                                                position=pos, width=width,
                                                                headerRow=headers)
        bucketeerCmd.inputs.append(touchFn)
        bucketeerCmd.outputs.extend([bFn for _, bFn in baseNames])

        rules.append(bucketeerCmd.get_makeflow_rule())

        # Recurse
        for bn, bFn in baseNames:
            rules.extend(getRulesForFile(bFn, groupPos, width=width,
                                                depth=depth - 1, basename=bn, headers=False,
                                                pos=pos + width, outDir=outDir))
    return rules


# .............................................................................
if __name__ == '__main__':

    # width, depth, input files,
    parser = argparse.ArgumentParser(
                  description='This script generates a Makeflow file for processing raw CSV data')

    # workspace

    parser.add_argument('outputFilename', type=str, help='Write DAG here')
    parser.add_argument('groupPosition', type=int,
                                             help='The position of the field to group on')
    parser.add_argument('width', type=int,
                                              help='Use this many characters for buckets')
    parser.add_argument('depth', type=int, help='Recurse this many levels')
    parser.add_argument('userId', type=str, help='User id for workflow')
    parser.add_argument('inputFilename', type=str, nargs='+',
                                                                            help='Input CSV file')
    parser.add_argument('outDir', type=str, help='Directory to store output CSVs')

    args = parser.parse_args()

    mf = MFChain(args.userId)

    # Recursively create rules
    for fn in args.inputFilename:
        rules = getRulesForFile(fn, args.groupPosition, width=args.width,
                                     depth=args.depth, basename='bucket_', headers=True,
                                     outDir=args.outDir)
        mf.addCommands(rules)
    mf.write(args.outputFilename)

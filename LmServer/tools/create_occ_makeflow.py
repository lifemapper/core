#!/bin/bash
"""
@summary: This script creates a makeflow for splitting / sorting / grouping a 
             CSV file for occurrence set creation
@author: CJ Grady
@version: 1.0.0
@status: beta
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import argparse
import os

from LmBackend.command.multi import (OccurrenceBucketeerCommand,
                         OccurrenceSorterCommand)#, OccurrenceSplitterCommand)

from LmServer.legion.processchain import MFChain
from LmBackend.command.server import LmTouchCommand

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
      rules.append(sortCmd.getMakeflowRule())
      
      # TODO: Add split command (unknown outputs)
      # Split
      #splitCmd = OccurrenceSplitterCommand(groupPos, sortedFn, outDir, 
      #                                     prefix='taxon_')
      #rules.append(splitCmd.getMakeflowRule())
   else:
      # More splitting
      baseNames = []
      for i in range(10**width):
         bn = '{}{}'.format(basename, (str(i) + '0'*width)[0:width])
         baseNames.append((bn, os.path.join(outDir, '{}.csv'.format(bn))))
      
      # Add out directory touch rule
      touchFn = os.path.join(outDir, 'touch.out')
      touchCmd = LmTouchCommand(touchFn)
      rules.append(touchCmd.getMakeflowRule(local=True))
      bucketeerCmd = OccurrenceBucketeerCommand(basename, groupPos, inFn, 
                                                position=pos, width=width, 
                                                headerRow=headers, outDir=outDir)
      bucketeerCmd.inputs.append(touchFn)
      bucketeerCmd.outputs.extend([bFn for _, bFn in baseNames])
      
      rules.append(bucketeerCmd.getMakeflowRule())
      
      # Recurse
      for bn, bFn in baseNames:
         rules.extend(getRulesForFile(bFn, groupPos, width=width, 
                                    depth=depth-1, basename=bn, headers=False, 
                                    pos=pos+width, outDir=outDir))
   return rules
         
# .............................................................................
if __name__ == '__main__':
   
   #width, depth, input files,    
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
   
#!/bin/bash
"""
@summary: Splits a sorted CSV file on the group field
@author: CJ Grady
@version: 1.0.0
@status: beta
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
#from operator import itemgetter

from LmCommon.common.unicodeCsv import UnicodeReader, UnicodeWriter

# .............................................................................
def groupByField(inputFilename, outDir, groupPosition, filePrefix='taxon_'):
   """
   @summary: Creates groups from the CSV file based on the specified field
   @param inputFilename: The file name of the sorted input CSV file
   @param outDir: The directory to store the output files
   @param groupPosition: The field in the CSV to group / split on
   @param filePrefix: The prefix of the output file names
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
            outFilename = os.path.join(outDir, 
                                       '{}{}.csv'.format(filePrefix, groupId))
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
   
   parser.add_argument('groupPosition', type=int, 
                                  help='The position of the field to group on')
   parser.add_argument('inputFilename', type=str, help='Input CSV file')

   parser.add_argument('outDir', type=str, 
                       help='Output directory to write files')
   parser.add_argument('-p', '--prefix', type=str, default='taxon_', 
                       help='Prefix for output files')

   args = parser.parse_args()
   
   groupByField(args.inputFilename, args.outDir, args.groupPosition, 
                filePrefix=args.prefix)
   
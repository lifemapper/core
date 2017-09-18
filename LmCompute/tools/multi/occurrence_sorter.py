#!/bin/bash
"""
@summary: Sorts a CSV file on the group field
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
from operator import itemgetter

from LmCommon.common.unicodeCsv import UnicodeReader, UnicodeWriter

# .............................................................................
def sortFileOnField(inputFilename, outputFilename, groupPosition):
   """
   @summary: Sorts a CSV file on the group field
   @param inputFilename: The input CSV file name to use for sorting
   @param outputFilename: Where to write the output file
   @param groupPosition: The column to sort on
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
   
   parser.add_argument('outputFilename', type=str, 
                                          help='Write sorted output file here')
   parser.add_argument('groupPosition', type=int, 
                                  help='The position of the field to group on')
   parser.add_argument('inputFilename', type=str, help='Input CSV file')

   args = parser.parse_args()
   
   sortFileOnField(args.inputFilename, args.outputFilename, args.groupPosition)
      

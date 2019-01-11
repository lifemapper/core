#!/bin/bash
"""
@summary: This script modifies the headers of an ASCII file
@author: CJ Grady
@version: 4.0.0
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
import re

# ....................................
def _processFloatHeader(headerRow, numDigits):
   """
   @summary: This method will process a header row and truncate a floating 
                point value if necessary
   @param headerRow: This is a string in the format 
                        "{header name}   {header value}"
   @param numDigits: Truncate a decimal after this many places, keep all if 
                        this is None
   """
   # Split out header name and value (replace tabs with spaces and use 
   #    regular expression to split
   header, value = re.split(r' +', headerRow.replace('\t', ' '))
   # Truncate the value by finding the decimal (if it exists) and adding numDigits places
   truncatedValue = value[:value.find('.')+numDigits+1] if value.find('.') >= 0 else value
   return "%s     %s\n" % (header, truncatedValue)

# .............................................................................
if __name__ == "__main__":
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description='This script modifies ASCII headers')
   
   parser.add_argument('inAsciiFilename', type=str, 
                       help='The file location of the original ASCII file')
   parser.add_argument('outAsciiFilename', type=str, 
                       help='The file location of the modified ASCII file')
   parser.add_argument('-d', type=int, help='The number of digits to keep')
   
   args = parser.parse_args()
   
   cont = True
   with open(args.inAsciiFilename) as ascIn:
      with open(args.outAsciiFilename, 'w') as ascOut:
         for line in ascIn:
            if cont:
               if line.lower().startswith('ncols'):
                  # This will be an integer, just write it
                  ascOut.write(line)
               elif line.lower().startswith('nrows'):  
                  # This will be an integer, just write it
                  ascOut.write(line)
               elif line.lower().startswith('xllcorner'):
                  ascOut.write(_processFloatHeader(line, numDigits=args.d))
               elif line.lower().startswith('yllcorner'):
                  ascOut.write(_processFloatHeader(line, numDigits=args.d))
               elif line.lower().startswith('cellsize'):
                  ascOut.write(_processFloatHeader(line, numDigits=args.d))
               elif line.lower().startswith('dx'):
                  ascOut.write(_processFloatHeader(line, numDigits=args.d)).replace('dx', 'cellsize')
               elif line.lower().startswith('dy'):
                  # Cell size should be the same for now
                  pass
               elif line.lower().startswith('nodata_value'):
                  ascOut.write(line)
               else: # Data line
                  cont = False
                  ascOut.write(line)
            else:
               ascOut.write(line)
   
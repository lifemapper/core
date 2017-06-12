"""
@summary: This module provides functions for compressing / decompressing a list
             of binary integer data.  It is very simple and just uses run 
             length encoding.
@author: CJ Grady
@version: 1.0
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
@note: We are using this for compressing / decompressing matrix columns with 
          the Lifemapper Global PAM solr index.  It only works with PAVs.
"""
VERSION = 1

# .............................................................................
def compress(lst):
   """
   @summary: Compresses a list of zeros and ones into a string of run lengths
   """
   runs = []
   startVal = int(lst[0])
   val = startVal
   runLength = 0

   for i in lst:
      # If we have the same value, extend the run
      if i == val:
         runLength += 1
      else:
         # Add the run length and switch to the other value
         runs.append(str(runLength))
         val = i
         runLength = 1
         
   return 'v{}s{} {}'.format(VERSION, startVal, ' '.join(runs))

# .............................................................................
def decompress(cmpStr):
   """
   @summary: Decompress a string of run lengths into a list of binary values
   """
   vals = []
   parts = cmpStr.split(' ')
   header = parts[0]
   version = int(header.split('s')[0].split('v')[1])
   startVal = int(header.split('s')[1])
   val = startVal
   
   for rl in parts[1:]:
      vals.extend(int(rl) * [val])
      # Will change 0 to 1 and 1 to 0
      val = abs(val - 1)
   
   return vals

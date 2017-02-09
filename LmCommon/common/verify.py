# coding=utf-8
"""
@summary: Module containing functions to handle data verification
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
import hashlib
import os

# .............................................................................
def _getHexHashValue(dlocation=None, content=None):
   """
   @summary: Returns a hexidecimal representation of the sha256sum of a datafile
   @param dlocation: The file on which to compute the hash
   @param content: The data on which to compute the hash
   @note: content is checked first, and if it exists, dlocation is ignored
   """
   hexhash = None
   if content is None:
      if dlocation and os.path.exists(dlocation):
         f = open(dlocation, 'r')
         content = f.read()
         f.close()
      else:
         print('Failed to hash missing file {}'.format(dlocation))
   if content is not None:
      hashval = hashlib.sha256(content)
      hexhash = hashval.hexdigest()
   else:
      print('Failed to hash empty content')
   return hexhash

# .............................................................................
def computeHash(dlocation=None, content=None):
   """
   @summary: Computes an sha256sum on data or a datafile 
   @param dlocation: File on which to compute hash
   @param content: Data or object on which to compute hash
   """
   hexhash = _getHexHashValue(dlocation=dlocation, content=content)
   return hexhash

# .............................................................................
def verifyHash(verify, dlocation=None, content=None):
   """
   @summary: Computes an sha256sum on a datafile and compares it to the one sent
   @param dlocation: The file on which to compute the hash
   @param verify: The hash to compare results
   """
   hexhash = _getHexHashValue(dlocation=dlocation, content=content)
   return hexhash == verify

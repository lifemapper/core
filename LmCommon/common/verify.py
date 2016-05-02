# coding=utf-8
"""
@summary: Module containing functions to handle data verification
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
   """
   hexhash = None
   if dlocation:
      if os.path.exists(dlocation):
         f = open(dlocation, 'r')
         content = f.read()
         f.close()
      else:
         print('{} does not exist'.format(dlocation))
         
   if content:
      hashval = hashlib.sha256(content)
      hexhash = hashval.hexdigest()

   return hexhash

# .............................................................................
def computeHash(dlocation=None, content=None):
   """
   @summary: Computes an sha256sum on a datafile
   @param dlocation: The file on which to compute the hash
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

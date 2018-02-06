"""
@summary: This class wraps a command and ensures that the outputs are always
             created
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import subprocess

from LmBackend.common.lmobj import LMObject

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser()
   parser.add_argument('cmd', type=str, help='This is the command to be wrapped')
   parser.add_argument('touch_files', type=str, nargs='*', 
               help='These files will be created by this script if necessary')
   args = parser.parse_args()

   try:
      subprocess.call(args.cmd, shell=True)
   except Exception, e:
      print str(e)
   
   for fn in args.touch_files:
      if not os.path.exists(fn):
         lmo = LMObject()
         lmo.readyFilename(fn)
         with open(fn, 'a') as outF:
            os.utime(fn, None)
         

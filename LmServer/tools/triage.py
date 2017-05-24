"""
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

from LmBackend.common.lmobj import LMObject
from LmServer.common.log import ScriptLogger

# .............................................................................
class EMT(LMObject):
   """
   @summary: Class to assess the outputs of a list of single-species-workflows
             which are inputs to a multi-species workflow 
   @note: Failed single-species workflows will be ignored, only successful 
          ones are moved into a multi-species workflow 
   @note: Failed single-species workflows are updated in the database and not
          deleted 
   @note: This class will be called with a multi-species workflow, dependencies 
          
   """
   # .............................
   def __init__(self, logger=None):      
      super(EMT, self).__init__()
      self.name = self.__class__.__name__.lower()
      # Optionally use parent process logger
      if logger is None:
         logger = ScriptLogger(self.name)

   # .............................
   def triage(self, infname, outfname):
      """
      @summary: Get a potato (CSV filename), read all targets, assess if they are ok
      @param infname: a filename containing filenames indicating completion
             of a Spud (single-species MF). Files are created regardless of 
             success or failure of Spud
      @param outfname: a filename containing filenames indicating **successful**
             completion of a Spud (single-species MF). 
      """
      with open(infname, 'r') as inPotato:
         with open(outfname, 'w') as outMashed:
            for line in inPotato:
               # Split out squid to get PAV file path
               _, pav = line.split(':')
               if self._isGoodTarget(pav.strip()):
                  # If good target, write the line to the mashed potato
                  outMashed.write(line)
         
   # .............................
   def _isGoodTarget(self, targetFilename):
      """
      @summary: Checks to see if the target file exists on the file system
      @param targetFile: The target file path to check for
      """
      return os.path.exists(targetFilename)
   
   
# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='This script takes a list of ' +
                             'filenames showing single-species workflow completion.')
   # Inputs
   parser.add_argument('input_filename', type=str, 
                       help='Input file containing a list of target filenames ' +
                            'indicating single-species workflow completion')
   parser.add_argument('output_filename', type=str, 
                       help='Output file containing a subset of target '  +
                            'filenames indicating successful completion')
   args = parser.parse_args()
   
   # Status comes in as an integer or file 
   infname = args.input_filename
   outfname = args.output_filename   
   
   success = EMT().triage(infname, outfname)
      
   
"""
Call like:
$PYTHON triage.py potatoFname  mashedFname
"""
   
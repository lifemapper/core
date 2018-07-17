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
import signal

from LmBackend.common.lmobj import LMObject
from LmBackend.common.subprocessManager import SubprocessRunner

# Need this to be module global for handling signals
current_procs = set()
shutdown = False

# .............................................................................
def handle_signal(signum, frame):
   """
   @summary: Handle a signal sent to the process and pass it on to subprocess
   """
   print("Received signal: {}".format(signum))
   
   for proc in current_procs:
      print('Signaling: "{}"'.format(proc.cmd))
      proc.signal(signum)

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser()
   parser.add_argument('-i', dest='input_files', action='append', 
                                    help='These input files must be non-empty')
   parser.add_argument('cmd', type=str, 
                                     help='This is the command to be wrapped')
   parser.add_argument('touch_files', type=str, nargs='*', 
               help='These files will be created by this script if necessary')
   args = parser.parse_args()

   # Install signal handler
   signal.signal(signal.SIGINT, handle_signal)
   signal.signal(signal.SIGTERM, handle_signal)

   skipComps = False
   # Ensure required input files are not empty
   if args.input_files is not None:
      for fn in args.input_files:
         if os.path.getsize(fn) < 1:
            print('File: {} has 0 length'.format(fn))
            skipComps = True

   if not skipComps:
      try:
         spr = SubprocessRunner(args.cmd)
         current_procs.add(spr)
         exitCode, stdErr = spr.run()
         print('Exit code: {}'.format(exitCode))
         print(stdErr)
      except Exception, e:
         print str(e)
   else:
      print('One or more required input files had zero length, skipping computations')
   
   # Don't try this if we were told to shut down
   if not shutdown:
      for fn in args.touch_files:
         if not os.path.exists(fn):
            lmo = LMObject()
            lmo.readyFilename(fn)
            with open(fn, 'a') as outF:
               os.utime(fn, None)
         

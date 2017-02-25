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
import mx.DateTime as dt
import os, sys, time

from LmBackend.common.daemon import Daemon
from LmDbServer.common.lmconstants import BOOM_PID_FILE
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import ARCHIVE_NAME
from LmServer.common.localconstants import ARCHIVE_USER
from LmServer.common.log import ScriptLogger
from LmServer.tools.cwalken import ChristopherWalken

# .............................................................................
class Walker(Daemon):
   # .............................
   def __init__(self, pidfile, userId, archiveName, log=None):      
      Daemon.__init__(self, pidfile, log=log)
      self.name = self.__class__.__name__.lower()
      self.userId = userId
      self.archiveName = archiveName

   # .............................
   def initialize(self):
      """
      @summary: Creates objects (OccurrenceSets, SMDModels, SDMProjections, 
                and MatrixColumns (intersections with the default grid)) 
                and job requests for their calculation.
      @note: The argument to this script/daemon contains variables to override 
             installed defaults
      """
      try:
         self.chris = ChristopherWalken(self.userId, self.archiveName, 
                                         jsonFname=None, priority=None, 
                                         logger=self.log)
      except Exception, e:
         raise LMError(currargs='Failed to initialize Walker ({})'.format(e))
      
   # .............................
   def run(self):
      try:
         self.chris.moveToStart()
         self.log.debug('Starting walker at location {} ... '
                        .format(self.chris.currRecnum))
         while self.keepRunning:
            try:
               self.log.info('Next species ...')
               self.chris.startWalken()
               if self.keepRunning:
                  self.keepRunning = not self.boomer.complete
            except:
               self.log.info('Saving next start {} ...'
                             .format(self.boomer.nextStart))
               self.chris.saveNextStart()
               raise
            else:
               time.sleep(10)
      finally:
         self.boomer.close()
      self.log.debug('Stopped Archivist')
    
   # .............................
   def onUpdate(self):
      self.log.debug("Update signal caught!")
       
   # .............................
   def onShutdown(self):
      self.boomer.saveNextStart()
      self.boomer.close()
      self.log.debug("Shutdown signal caught!")
      Daemon.onShutdown(self)

   # .............................
   @staticmethod
   def isCorrectUser():
      """ find current user """
      import subprocess
      cmd = "/usr/bin/whoami"
      info, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE).communicate()
      usr = info.split()[0]
      if usr == 'lmwriter':
         return True
      return False

# .............................................................................
if __name__ == "__main__":
   if not Walker.isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)

   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-n', '--archive_name', default=ARCHIVE_NAME,
            help=('Name for the existing archive, gridset, and grid created for ' +
                  'these data.  This name was created in initBoom.'))
   parser.add_argument('-u', '--user', default=ARCHIVE_USER,
            help=('Owner of this archive this archive. The default is the '
                  'configured ARCHIVE_USER.'))
   parser.add_argument('cmd', choices=['start', 'stop', 'restart'],
              help="The action that should be performed by the Boomer daemon")

   args = parser.parse_args()
   archiveName = args.archive_name
   if archiveName is not None:
      archiveName = archiveName.replace(' ', '_')
   userId = args.user
   cmd = args.cmd.lower()
      
   if os.path.exists(BOOM_PID_FILE):
      pid = open(BOOM_PID_FILE).read().strip()
   else:
      pid = os.getpid()
           
   secs = time.time()
   tuple = time.localtime(secs)
   timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", tuple))
   logger = ScriptLogger('archivist.{}'.format(timestamp))
   boomer = Walker(BOOM_PID_FILE, userId, archiveName, log=logger)
     
   if cmd == 'start':
      boomer.start()
   elif cmd == 'stop':
      boomer.stop()
   elif cmd == 'restart':
      boomer.restart()
   elif cmd == 'status':
      boomer.status()
   else:
      print("Unknown command: {}".format(cmd))
      sys.exit(2)

"""
$PYTHON LmDbServer/boom/boom.py --help
$PYTHON LmDbServer/boom/boom.py  --archive_name "Heuchera archive" --user ryan start
$PYTHON LmDbServer/boom/boom.py --archive_name "Aimee test archive"  --user aimee start
"""

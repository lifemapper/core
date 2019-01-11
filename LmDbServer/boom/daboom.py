"""
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
import logging
import sys
import time
import traceback

from LmBackend.common.daemon import Daemon
from LmCommon.common.lmconstants import LM_USER
from LmDbServer.common.lmconstants import BOOM_PID_FILE
from LmDbServer.boom.boomer import Boomer
from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmconstants import LMFileType, PUBLIC_ARCHIVE_NAME
from LmServer.common.log import ScriptLogger

# .............................................................................
class DaBoom(Daemon):
   """
   Class to run the Boomer as a Daemon process
   """
   # .............................
   def __init__(self, pidfile, configFname, assemblePams=True, priority=None):
      # Logfile
      secs = time.time()
      timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
      logname = '{}.{}'.format(self.__class__.__name__.lower(), timestamp)
      log = ScriptLogger(logname, level=logging.INFO)

      Daemon.__init__(self, pidfile, log=log)
      self.boomer = Boomer(configFname, assemblePams=assemblePams, log=log)

   # .............................
   def initialize(self):
      self.boomer.initializeMe()
      
   # .............................
   def run(self):
      print('Running daBoom with configFname = {}'.format(self.boomer.configFname))
      try:
         while self.boomer.keepWalken:
            self.boomer.processSpud()
      except Exception, e:
         self.log.debug('Exception {} on potato'.format(str(e)))         
         tb = traceback.format_exc()
         self.log.error("An error occurred")
         self.log.error(str(e))
         self.log.error(tb)
      finally:
         self.log.debug('Daboom finally stopping')
         self.onShutdown()
                
   # .............................
   def onUpdate(self):
      self.log.info('Update signal caught!')
       
   # .............................
   def onShutdown(self):
      self.log.info('Shutdown!')
      # Stop walken the archive and saveNextStart
      self.boomer.close()
      Daemon.onShutdown(self)
      
   # ...............................................
   @property
   def logFilename(self):
      try:
         fname = self.log.baseFilename
      except:
         fname = None
      return fname
   


# .............................................................................
if __name__ == "__main__":
   if not isLMUser():
      print("Run this script as `{}`".format(LM_USER))
      sys.exit(2)
   earl = EarlJr()
   defaultConfigFile = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                           objCode=PUBLIC_ARCHIVE_NAME, 
                                           usr=PUBLIC_USER)   
#    pth = earl.createDataPath(PUBLIC_USER, LMFileType.BOOM_CONFIG)
#    defaultConfigFile = os.path.join(pth, '{}{}'.format(PUBLIC_ARCHIVE_NAME, 
#                                                        LMFormat.CONFIG.ext))
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper archive with metadata ' +
                         'for single- or multi-species computations ' + 
                         'specific to the configured input data or the ' +
                         'data package named.'))
   parser.add_argument('-', '--config_file', default=defaultConfigFile,
            help=('Configuration file for the archive, gridset, and grid ' +
                  'to be created from these data.'))
   parser.add_argument('cmd', choices=['start', 'stop', 'restart'],
              help="The action that should be performed by the Boom daemon")

   args = parser.parse_args()
   configFname = args.config_file
   cmd = args.cmd.lower()
   
   print('')
   print('Running daboom with configFilename={} and command={}'
         .format(configFname, cmd))
   print('')
   boomer = DaBoom(BOOM_PID_FILE, configFname)
   
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
"""
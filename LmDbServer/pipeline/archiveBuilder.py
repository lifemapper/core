"""
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
from LmBackend.common.daemon import Daemon
from LmDbServer.pipeline.boom import BisonBoom, GBIFBoom, iDigBioBoom, UserBoom

# .............................................................................
class _Archivist(Daemon):
   # .............................
   def initialize(self):
      self.name = self.__class__.__name__.lower()
      
   # .............................
   def run(self):
      while self.keepRunning:
         self._runProcess()
      self.log.debug("self.cont: %s" % self.cont)
    
   # .............................
   def onUpdate(self):
      self.log.debug("Update signal caught!")
       
   # .............................
   def onShutdown(self):
      self.log.debug("Shutdown signal caught!")
      Daemon.onShutdown(self)

# .............................................................................
if __name__ == "__main__":
   pass

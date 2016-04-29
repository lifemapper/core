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
import mx.DateTime as dt
import os, sys
import time

from LmBackend.common.daemon import Daemon
from LmCommon.common.log import DaemonLogger
from LmDbServer.common.lmconstants import (BOOM_PID_FILE, BISON_TSN_FILE, 
         GBIF_DUMP_FILE, PROVIDER_DUMP_FILE, IDIGBIO_FILE, TAXONOMIC_SOURCE,
         USER_OCCURRENCE_CSV, USER_OCCURRENCE_META)
from LmDbServer.common.localconstants import (DEFAULT_ALGORITHMS, 
         DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, DEFAULT_GRID_NAME, 
         SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, SPECIES_EXP_DAY)

from LmDbServer.pipeline.boom import BisonBoom, GBIFBoom, iDigBioBoom, UserBoom
from LmServer.base.lmobj import LMError
from LmServer.common.localconstants import ARCHIVE_USER, DATASOURCE

# .............................................................................
class Archivist(Daemon):
   # .............................
   def initialize(self):
      self.name = self.__class__.__name__.lower()
      expdate = dt.DateTime(SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, 
                                     SPECIES_EXP_DAY)
      taxname = TAXONOMIC_SOURCE[DATASOURCE]['name']
      try:
         if DATASOURCE == 'BISON':
            self.boomer = BisonBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            BISON_TSN_FILE, expdate.mjd, taxonSourceName=taxname,
                            mdlMask=None, prjMask=None, 
                            intersectGrid=DEFAULT_GRID_NAME)
            
         elif DATASOURCE == 'GBIF':
            self.boomer = GBIFBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            GBIF_DUMP_FILE, expdate.mjd, taxonSourceName=taxname,
                            providerListFile=PROVIDER_DUMP_FILE,
                            mdlMask=None, prjMask=None, 
                            intersectGrid=DEFAULT_GRID_NAME)
            
         elif DATASOURCE == 'IDIGBIO':
            self.boomer = iDigBioBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            IDIGBIO_FILE, expdate.mjd, taxonSourceName=taxname,
                            mdlMask=None, prjMask=None, 
                            intersectGrid=DEFAULT_GRID_NAME)
   
         elif DATASOURCE == 'USER':
            # Set variables in config/site.ini to override installed defaults
            self.boomer = UserBoom(ARCHIVE_USER, DEFAULT_ALGORITHMS, 
                            DEFAULT_MODEL_SCENARIO, DEFAULT_PROJECTION_SCENARIOS, 
                            USER_OCCURRENCE_CSV, USER_OCCURRENCE_META, expdate.mjd, 
                            mdlMask=None, prjMask=None, 
                            intersectGrid=DEFAULT_GRID_NAME)
      except Exception, e:
         raise LMError(currargs='Failed to initialize Archivist ({})'.format(e))
      
   # .............................
   def run(self):
      try:
         self.log.info('Moving to start ...')
         self.boomer.moveToStart()
         while self.keepRunning:
            try:
               self.log.info('Chaining one ...')
               self.boomer.chainOne()
            except:
               self.log.info('Saving next start ...')
               self.boomer.saveNextStart()
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
      self.boomer.close()
      self.boomer.saveNextStart()
      self.log.debug("Shutdown signal caught!")
      Daemon.onShutdown(self)

# .............................................................................
if __name__ == "__main__":
   if os.path.exists(BOOM_PID_FILE):
      pid = open(BOOM_PID_FILE).read().strip()
   else:
      pid = os.getpid()
     
   idig = Archivist(BOOM_PID_FILE, log=DaemonLogger(pid))
     
   if len(sys.argv) == 2:
      if sys.argv[1].lower() == 'start':
         idig.start()
      elif sys.argv[1].lower() == 'stop':
         idig.stop()
      elif sys.argv[1].lower() == 'restart':
         idig.restart()
      else:
         print("Unknown command: {}" % sys.argv[1].lower())
         sys.exit(2)
   else:
      print("usage: {} start|stop|update" % sys.argv[0])
      sys.exit(2)

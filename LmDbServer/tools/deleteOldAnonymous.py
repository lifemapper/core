"""
@summary: Deletes old data for a user
@author: CJ Grady

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
from mx.DateTime import gmt

from LmCommon.common.lmconstants import ONE_DAY
from LmServer.common.log import ConsoleLogger
from LmServer.db.scribe import Scribe
from LmServer.notifications.email import EmailNotifier

USER = "anon"
DAYS_OLD = 14
failedFile = "/home/cjgrady/failed.txt"

def reportFailure(mesgs):
   notifier = EmailNotifier()
   notifier.sendMessage(['cjgrady@ku.edu'], "Failed to delete user occurrence sets", '\n'.join(mesgs))

# -----------------------------------------------------------------------------
if __name__ == "__main__":
   failed = []
   tooOldTime = gmt().mjd - (ONE_DAY * DAYS_OLD)
   log = ConsoleLogger()
   scribe = Scribe(log)
   scribe.openConnections()
   
   # Should be able to just list old occurrence sets and then have the scribe 
   #    delete experiments associated with them
   occAtoms = scribe.listOccurrenceSets(0, 1000, 
                          minOccurrenceCount=0,
                          hasProjections=True,
                          userId=USER, 
                          beforeTime=tooOldTime)
   
   for occAtom in occAtoms:
      occ = scribe.getOccurrenceSet(occAtom.id)
      # Look for models for the occurrence set
      log.info("Deleting occurrence set id: %s" % occ.id)
      success = scribe.completelyRemoveOccurrenceSet(occ)
      if not success:
         failed.append(str(occ.id))

   if len(failed) > 0:
      reportFailure(failed)
   
   scribe.closeConnections()


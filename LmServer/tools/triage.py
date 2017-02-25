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
from LmCommon.common.lmconstants import JobStatus
from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
class EMT(LMObject):
   # .............................
   def __init__(self, logger=None):      
      super(EMT, self).__init__()
      self.name = self.__class__.__name__.lower()
      # Optionally use parent process logger
      if logger is None:
         logger = ScriptLogger(self.name)
      # Database connection
      try:
         scribe = BorgScribe(self.log)
         success = scribe.openConnections()
      except Exception, e:
         raise LMError(currargs='Exception opening database', prevargs=e.args)
      else:
         if not success:
            raise LMError(currargs='Failed to open database')
         else:
            logger.info('{} opened databases'.format(self.name))
      return logger, scribe

   # .............................
   def triage(self, potato):
      """
      @summary: Get a potato, read all targets, assess if they are ok
      """
      mashed = self._removeFailures(potato.targets)
      return mashed

   # .............................
   def _removeFailures(self, targets):
      """
      @TODO: figure out what is good
      """
      goodTargets = []
      for tgt in targets:
         if self._isGoodTarget(tgt):
            goodTargets.append(tgt)
      return goodTargets
         
   # .............................
   def _isGoodTarget(self, target):
      """
      @TODO: figure out what is good
      """
      if target.status == JobStatus.COMPLETE:
         return True
      return False
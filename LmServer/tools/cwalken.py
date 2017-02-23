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
# .............................................................................
import os

from LmServer.base.lmobj import LMObject
from LmServer.common.log import ScriptLogger

class ChristopherWalken(LMObject):
   """
   Class to ChristopherWalken.
   
   [occ]  ( filename of taxonids, csv of datablocks, etc each handled differently)
   [algs/params]
   mdlscen
   [prjscens]
   
   {Occtype: type,
    Occurrencedriver: [occ, occ ...], 
    Algorithm: [algs/params]
    MdlScenario: mdlscen
    ProjScenario: [prjscens]
   }
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger=None):
      """
      @summary Constructor for ChristopherWalken class
      @param logger: LmLogger to use for Borg
      @param dbHost: hostname for database machine
      @param dbPort: port for database connection
      """
      if logger is None:
         log = ScriptLogger(os.path.basename(self.__class__.__name__.lower()))

# ...............................................
   def getRequest(self):
      pass

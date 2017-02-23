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

def itemGenerator(items):
   """
   @summary: Makes everything an iterable.  One item acts like a list with one
                item
   """
   # If it is already iterable, iterate over it
   if hasattr(items, '__iter__'):
      for item in items:
         yield item
   else:
      # Not iterable, so just return the item at the first iteration
      yield item

# .............................................................................
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
      self.rules = []
      
      if logger is None:
         log = ScriptLogger(os.path.basename(self.__class__.__name__.lower()))

   # ...............................
   def startWalken(objList):
      """
      @summary: Walks a list of Lifemapper objects for computation
      """
      for woc in objList:
         if isinstance(woc, IntersectWoC):
            self.processIntersect(woc)
         elif isinstance(woc, ProjectionWoC):
            self.processProjection(woc)
         elif isinstance(woc, OccurrenceSetWoC):
            self.processOccurrenceSet(woc)
         else:
            raise Exception, "Don't know how to process {0}".format(type(woc))

   # ...............................
   def processIntersect(self, intersectWoC):
      pass

   # ...............................
   def processProjection(self, projWoC):
      """
      @summary: Process a projection request
      """
      projections = []
      for occ in itemGenerator(projWoC.occurrenceSet):
         occSet = self.processOccurrenceSet(occ)
         for algo in itemGenerator(projWoC.algorithm):
            for prjScn in itemGenerator(projWoC.prjScenarios):
               # Probably need to get the scenario objects
               prj = SDMProjection(occSet, algo, projWoC.modelScenario, prjScn)
               self.rules.extend(prj.computeMe())
               projections.append(prj)
      return projections
                    
   # ...............................
   def processOccurrenceSet(self, occWoC):
      """
      @summary: This expects that occWoC will contain information necessary for processing an occurrence set
      """
      occ = None
      self.rules.extend(occ.computeMe())
      return occ

# ...............................

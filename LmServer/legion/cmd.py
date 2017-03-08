"""
@summary: Module containing Makeflow Rule class for Lifemapper
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
from LmServer.base.lmobj import LMObject

# ............................................................................
class MfRule(LMObject):
   """
   @summary: Class to create commands for a makeflow document
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, command, targets, dependencies=None, comment=''):
      """
      @summary Constructor for commands used by Makeflow
      @param command: string used by LmCompute to compute this object
      @param targets: list of outputs for this object
      @param dependencies: list of dependencies for this object
      @param comment: A comment that can be added to a Makeflow document for 
                         clarity
      """
      self.command = command
      self.targets = targets
      if dependencies is None:
         self.dependencies = []
      else:
         self.dependencies = dependencies
      self.comment = comment
